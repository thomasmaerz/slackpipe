from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import duckdb
import pytest

from slackpipe.sqlite_snapshot import snapshot_sqlite
from slackpipe.transform import (
    SourceContinuityError,
    ingest_slackdump,
    validate_source_vs_canonical,
)


SAFE_SOURCE = Path("/tmp/opencode/kmnr-slackdump.sqlite")


@pytest.fixture(scope="module")
def kmnr_warehouse(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, object, object]:
    if not SAFE_SOURCE.is_file():
        pytest.skip(
            f"missing safe Slackdump fixture: {SAFE_SOURCE} "
            "(generate a sanitized kmnr snapshot to run these)"
        )
    warehouse = tmp_path_factory.mktemp("kmnr") / "slackpipe.duckdb"
    first = ingest_slackdump(SAFE_SOURCE, warehouse, workspace_slug="kmnr")
    second = ingest_slackdump(SAFE_SOURCE, warehouse, workspace_slug="kmnr")
    return warehouse, first, second


def test_real_source_distinct_messages_are_canonical_and_duplicates_collapse(
    kmnr_warehouse: tuple[Path, object, object],
) -> None:
    warehouse, first, _ = kmnr_warehouse
    with sqlite3.connect(SAFE_SOURCE) as source, duckdb.connect(
        str(warehouse), read_only=True
    ) as target:
        source_keys = set(source.execute(
            """
            SELECT DISTINCT CHANNEL_ID, TS FROM MESSAGE
            """
        ).fetchall())
        raw_rows = source.execute("SELECT count(*) FROM MESSAGE").fetchone()[0]
        duplicate = source.execute(
            """
            SELECT CHANNEL_ID, TS FROM MESSAGE
            GROUP BY CHANNEL_ID, TS HAVING count(*) > 1
            ORDER BY CHANNEL_ID, TS LIMIT 1
            """
        ).fetchone()
        newest = source.execute(
            """
            SELECT CHUNK_ID, CAST(DATA AS TEXT) FROM MESSAGE
            WHERE CHANNEL_ID = ? AND TS = ? ORDER BY CHUNK_ID DESC LIMIT 1
            """,
            duplicate,
        ).fetchone()
        canonical_keys = set(target.execute(
            "SELECT channel_id, ts FROM messages WHERE workspace_id = ?",
            [first.workspace_id],
        ).fetchall())
        canonical_duplicate = target.execute(
            """
            SELECT source_chunk_id, raw_json FROM messages
            WHERE workspace_id = ? AND channel_id = ? AND ts = ?
            """,
            [first.workspace_id, *duplicate],
        ).fetchone()

    assert raw_rows > len(source_keys)
    assert canonical_keys == source_keys
    assert len(canonical_keys) == first.canonical_rows
    assert canonical_duplicate == newest


def test_real_thread_content_and_raw_json_match_source(
    kmnr_warehouse: tuple[Path, object, object],
) -> None:
    warehouse, first, _ = kmnr_warehouse
    with sqlite3.connect(SAFE_SOURCE) as source:
        source.row_factory = sqlite3.Row
        sample = source.execute(
            """
            SELECT * FROM MESSAGE
            WHERE THREAD_TS IS NOT NULL AND THREAD_TS <> TS AND TXT <> ''
            ORDER BY CHUNK_ID DESC, CHANNEL_ID, TS LIMIT 1
            """
        ).fetchone()
    with duckdb.connect(str(warehouse), read_only=True) as target:
        canonical = target.execute(
            """
            SELECT text, thread_ts, parent_ts, is_thread_reply, raw_json
            FROM messages WHERE workspace_id = ? AND channel_id = ? AND ts = ?
            """,
            [first.workspace_id, sample["CHANNEL_ID"], sample["TS"]],
        ).fetchone()

    assert canonical == (
        sample["TXT"],
        sample["THREAD_TS"],
        sample["THREAD_TS"],
        True,
        bytes(sample["DATA"]).decode("utf-8"),
    )


def test_rerun_is_idempotent_and_records_lineage(
    kmnr_warehouse: tuple[Path, object, object],
) -> None:
    warehouse, first, second = kmnr_warehouse
    with duckdb.connect(str(warehouse), read_only=True) as target:
        counts = target.execute(
            """
            SELECT
                (SELECT count(*) FROM messages),
                (SELECT count(*) FROM message_events),
                (SELECT count(*) FROM ingestion_runs WHERE status = 'succeeded'),
                (SELECT count(*) FROM source_archives),
                (SELECT max(ingest_count) FROM source_archives),
                (SELECT count(*) FROM ingestion_checkpoints)
            """
        ).fetchone()

    assert second.inserted_messages == second.updated_messages == 0
    assert second.unchanged_messages == first.canonical_rows
    assert counts == (first.canonical_rows, first.canonical_rows, 2, 1, 2, 1)


def test_multi_workspace_keys_flags_metadata_and_views_are_segregated(tmp_path: Path) -> None:
    first_source = _synthetic_source(tmp_path / "one.sqlite", "T_ONE", "one")
    second_source = _synthetic_source(tmp_path / "two.sqlite", "T_TWO", "two")
    warehouse = tmp_path / "multi.duckdb"

    first = ingest_slackdump(first_source, warehouse, workspace_slug="one")
    second = ingest_slackdump(second_source, warehouse, workspace_slug="two")

    with duckdb.connect(str(warehouse), read_only=True) as target:
        same_natural_key = target.execute(
            """
            SELECT workspace_id, text, source_chunk_id, is_edited, is_thread_parent
            FROM messages WHERE channel_id = 'C_SHARED' AND ts = '1700000000.000001'
            ORDER BY workspace_id
            """
        ).fetchall()
        reply = target.execute(
            """
            SELECT workspace_id, text, thread_ts, parent_ts, is_thread_reply
            FROM messages WHERE ts = '1700000001.000002' ORDER BY workspace_id
            """
        ).fetchall()
        deleted = target.execute(
            "SELECT count(*) FROM messages WHERE is_deleted"
        ).fetchone()[0]
        reactions = target.execute("SELECT count(*) FROM reactions").fetchone()[0]
        files = target.execute(
            "SELECT count(*), count(local_object_path) FROM files"
        ).fetchone()
        segregated_entities = target.execute(
            """
            SELECT
                (SELECT count(*) FROM workspaces),
                (SELECT count(*) FROM channels WHERE channel_id = 'C_SHARED'),
                (SELECT count(*) FROM users WHERE user_id = 'U_SHARED'),
                (SELECT count(*) FROM files WHERE file_id = 'F_SHARED')
            """
        ).fetchone()
        documents = target.execute(
            "SELECT count(DISTINCT workspace_id), count(*) FROM v_search_documents"
        ).fetchone()
        chunks = target.execute("SELECT count(*) FROM document_chunks").fetchone()[0]

    assert first.canonical_rows == second.canonical_rows == 3
    assert same_natural_key == [
        ("T_ONE", "one newest", 11, True, True),
        ("T_TWO", "two newest", 11, True, True),
    ]
    assert reply == [
        ("T_ONE", "one reply", "1700000000.000001", "1700000000.000001", True),
        ("T_TWO", "two reply", "1700000000.000001", "1700000000.000001", True),
    ]
    assert deleted == 2
    assert reactions == 2
    assert files == (2, 0)
    assert segregated_entities == (2, 2, 2, 2)
    assert documents == (2, 4)
    assert chunks == 0


def test_canonical_records_present_attachment_paths(tmp_path: Path) -> None:
    source = _synthetic_source(tmp_path / "source.sqlite", "TPATHS", "Paths")
    # The archive dir is the sqlite's parent: stage uploads beside a copy.
    archive = tmp_path / "archive" / "slackdump.sqlite"
    archive.parent.mkdir(parents=True)
    with sqlite3.connect(source) as src, sqlite3.connect(archive) as dst:
        dst.executescript("\n".join(src.iterdump()))
    uploads = archive.parent / "__uploads" / "F_SHARED"
    uploads.mkdir(parents=True)
    (uploads / "Paths.txt").write_bytes(b"0123456789ab")
    warehouse = tmp_path / "warehouse.duckdb"

    ingest_slackdump(archive, warehouse, workspace_slug="paths")

    with duckdb.connect(str(warehouse), read_only=True) as target:
        assert target.execute(
            "SELECT local_object_path FROM files WHERE file_id = 'F_SHARED'"
        ).fetchone() == ("__uploads/F_SHARED/Paths.txt",)


def test_fast_path_rerun_records_later_downloaded_attachment_paths(
    tmp_path: Path,
) -> None:
    source = _synthetic_source(tmp_path / "source.sqlite", "TLATE", "Late")
    archive = tmp_path / "archive" / "slackdump.sqlite"
    archive.parent.mkdir(parents=True)
    with sqlite3.connect(source) as src, sqlite3.connect(archive) as dst:
        dst.executescript("\n".join(src.iterdump()))
    warehouse = tmp_path / "warehouse.duckdb"

    ingest_slackdump(archive, warehouse, workspace_slug="late")
    with duckdb.connect(str(warehouse), read_only=True) as target:
        assert target.execute(
            "SELECT local_object_path FROM files WHERE file_id = 'F_SHARED'"
        ).fetchone() == (None,)

    uploads = archive.parent / "__uploads" / "F_SHARED"
    uploads.mkdir(parents=True)
    (uploads / "Late.txt").write_bytes(b"0123456789ab")

    # The sqlite is byte-identical, so this rerun takes the fingerprint
    # fast-path — which must still record newly downloaded paths.
    rerun = ingest_slackdump(archive, warehouse, workspace_slug="late")

    assert rerun.inserted_messages == rerun.updated_messages == 0
    assert rerun.unchanged_messages == rerun.canonical_rows
    with duckdb.connect(str(warehouse), read_only=True) as target:
        assert target.execute(
            "SELECT local_object_path FROM files WHERE file_id = 'F_SHARED'"
        ).fetchone() == ("__uploads/F_SHARED/Late.txt",)


def test_membership_replacement_is_set_based_and_scoped_to_observed_channels(
    tmp_path: Path,
) -> None:
    source = _synthetic_source(tmp_path / "members.sqlite", "T_MEMBERS", "members")
    warehouse = tmp_path / "members.duckdb"
    ingest_slackdump(source, warehouse, workspace_slug="members")

    with duckdb.connect(str(warehouse)) as target:
        target.execute(
            """
            INSERT INTO channel_members VALUES
                ('T_MEMBERS', 'C_SHARED', 'U_STALE', 1, 'old', 'old'),
                ('T_MEMBERS', 'C_UNOBSERVED', 'U_KEEP', 1, 'old', 'old')
            """
        )

    # A same-file rerun is a verified no-op and leaves external rows alone;
    # replacement runs on chunks newer than the checkpoint, so observe the
    # shared channel once more in a fresh chunk first.
    ingest_slackdump(source, warehouse, workspace_slug="members")
    with sqlite3.connect(source) as db:
        db.execute("INSERT INTO SESSION VALUES (2, 1)")
        db.execute("INSERT INTO CHUNK VALUES (20, 2, 0, 1, 'C_SHARED', '', 0)")
        db.execute(
            "INSERT INTO CHANNEL_USER VALUES ('C_SHARED', 'U_SHARED', 20, '', 0)"
        )
        db.commit()

    ingest_slackdump(source, warehouse, workspace_slug="members")

    with duckdb.connect(str(warehouse), read_only=True) as target:
        members = target.execute(
            """
            SELECT channel_id, user_id FROM channel_members
            WHERE workspace_id = 'T_MEMBERS'
            ORDER BY channel_id, user_id
            """
        ).fetchall()

    assert ("C_SHARED", "U_STALE") not in members
    assert ("C_SHARED", "U_SHARED") in members
    assert ("C_UNOBSERVED", "U_KEEP") in members


def test_snapshot_helper_copies_a_coherent_local_database(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    destination = tmp_path / "scratch" / "snapshot.sqlite"
    db = sqlite3.connect(source)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE sample (value TEXT)")
        db.execute("INSERT INTO sample VALUES ('coherent')")
        db.commit()

        result = snapshot_sqlite(source, destination)
    finally:
        db.close()

    assert result == destination
    with sqlite3.connect(f"{destination.as_uri()}?mode=ro", uri=True) as db:
        assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert db.execute("SELECT value FROM sample").fetchone() == ("coherent",)


def test_nonfinal_page_is_valid_when_stream_has_final_page(tmp_path: Path) -> None:
    source = _synthetic_source(tmp_path / "paged.sqlite", "TPAGED", "Paged")
    with sqlite3.connect(source) as db:
        db.execute("UPDATE CHUNK SET FINAL = 0 WHERE ID = 13")
        db.execute("INSERT INTO CHUNK VALUES (14, 1, 0, 1, 'C_SHARED', '', 0)")
        db.commit()

    result = ingest_slackdump(
        source, tmp_path / "warehouse.duckdb", workspace_slug="paged"
    )

    assert result.canonical_rows == 3


def test_rejects_truncated_archive_before_canonical_upserts(tmp_path: Path) -> None:
    source = _synthetic_source(tmp_path / "source.sqlite", "TLINEAGE", "Lineage")
    warehouse = tmp_path / "warehouse.duckdb"
    first = ingest_slackdump(source, warehouse, workspace_slug="lineage")

    with sqlite3.connect(source) as db:
        db.execute("DELETE FROM MESSAGE WHERE TS = '1700000002.000003'")
        db.commit()

    with pytest.raises(SourceContinuityError, match="no longer contains 1 canonical"):
        ingest_slackdump(source, warehouse, workspace_slug="lineage")

    with duckdb.connect(str(warehouse), read_only=True) as db:
        assert db.execute("SELECT count(*) FROM messages").fetchone()[0] == first.canonical_rows
        assert db.execute(
            "SELECT status, error_class FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone() == ("failed", "SourceContinuityError")


def test_rejects_replacement_archive_with_same_workspace_and_high_waters(
    tmp_path: Path,
) -> None:
    source = _synthetic_source(tmp_path / "source.sqlite", "TREPLACE", "Original")
    replacement = _synthetic_source(
        tmp_path / "replacement.sqlite", "TREPLACE", "Replacement"
    )
    warehouse = tmp_path / "warehouse.duckdb"
    ingest_slackdump(source, warehouse, workspace_slug="replace")

    with pytest.raises(SourceContinuityError, match="rewrote canonical message"):
        ingest_slackdump(replacement, warehouse, workspace_slug="replace")

    with duckdb.connect(str(warehouse), read_only=True) as db:
        assert db.execute(
            "SELECT text FROM messages WHERE ts = '1700000000.000001'"
        ).fetchone()[0] == "Original newest"


def test_accepts_dedupe_shaped_prune_of_identical_copies(tmp_path: Path) -> None:
    """Deleting interior identical copies (dedupe/compaction) stays continuous."""
    source = _synthetic_source(tmp_path / "source.sqlite", "TDEDUPE", "Dedupe")
    warehouse = tmp_path / "warehouse.duckdb"
    first = ingest_slackdump(source, warehouse, workspace_slug="dedupe")

    with sqlite3.connect(source) as db:
        # Two older byte-identical copies of the old parent (copied DATA
        # straight from the DB so separators and key order match exactly),
        # plus a brand-new chunk that re-observes the newest payload, again
        # byte-copied (resume lookback overlap).
        old_data = db.execute(
            "SELECT CAST(DATA AS TEXT) FROM MESSAGE WHERE CHUNK_ID = 10"
        ).fetchone()[0]
        new_data = db.execute(
            "SELECT CAST(DATA AS TEXT) FROM MESSAGE WHERE CHUNK_ID = 11"
        ).fetchone()[0]
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, '', 'C_SHARED', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1699999998000001, 5, "1700000000.000001", None,
                "1700000000.000001", "1700000001.000002", 1, 0, 0,
                "Dedupe old", old_data,
            ),
        )
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, '', 'C_SHARED', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1699999999000001, 6, "1700000000.000001", None,
                "1700000000.000001", "1700000001.000002", 1, 0, 0,
                "Dedupe old", old_data,
            ),
        )
        db.execute("INSERT INTO SESSION VALUES (2, 1)")
        db.execute("INSERT INTO CHUNK VALUES (20, 2, 0, 1, 'C_SHARED', '', 0)")
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, '', 'C_SHARED', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1700000000000001, 20, "1700000000.000001", None,
                "1700000000.000001", "1700000001.000002", 1, 0, 1,
                "Dedupe newest", new_data,
            ),
        )
        db.commit()
        remaining = _dedupe_identical_copies(db)
        db.commit()

    # 4 original + 2 copies + 1 re-observation, minus the 3 pruned copies
    # (chunks 5, 6 identical to chunk 10; chunk 11 identical to chunk 20).
    assert remaining == 4
    pruned = ingest_slackdump(source, warehouse, workspace_slug="dedupe")

    assert pruned.canonical_rows == first.canonical_rows
    assert pruned.inserted_messages == pruned.updated_messages == 0
    with duckdb.connect(str(warehouse), read_only=True) as target:
        assert target.execute(
            "SELECT text FROM messages WHERE ts = '1700000000.000001'"
        ).fetchone()[0] == "Dedupe newest"


def test_accepts_prune_only_change_via_full_read_fallback(tmp_path: Path) -> None:
    """A fingerprint change with no new chunks falls back to a full read."""
    source = _synthetic_source(tmp_path / "source.sqlite", "TPRUNE", "Prune")
    warehouse = tmp_path / "warehouse.duckdb"
    first = ingest_slackdump(source, warehouse, workspace_slug="prune")

    with sqlite3.connect(source) as db:
        # Older byte-identical copy of the newest parent, then prune it: no
        # new chunks exist, so ingest must prove continuity with a full read.
        new_data = db.execute(
            "SELECT CAST(DATA AS TEXT) FROM MESSAGE WHERE CHUNK_ID = 11"
        ).fetchone()[0]
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, '', 'C_SHARED', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1699999999000001, 5, "1700000000.000001", None,
                "1700000000.000001", "1700000001.000002", 1, 0, 0,
                "Prune newest", new_data,
            ),
        )
        db.commit()
        pruned_count = _dedupe_identical_copies(db)
        db.commit()

    assert pruned_count == 4
    pruned = ingest_slackdump(source, warehouse, workspace_slug="prune")
    assert pruned.canonical_rows == first.canonical_rows
    assert pruned.inserted_messages == pruned.updated_messages == 0


def _dedupe_identical_copies(db: sqlite3.Connection) -> int:
    """Delete rows identical to a kept newer copy; return survivor count."""
    db.execute(
        """
        DELETE FROM MESSAGE WHERE rowid IN (
            SELECT rowid FROM (
                SELECT rowid, row_number() OVER (
                    PARTITION BY CHANNEL_ID, TS, CAST(DATA AS TEXT)
                    ORDER BY CHUNK_ID DESC, IDX DESC, rowid DESC
                ) AS keep_rank
                FROM MESSAGE
            ) WHERE keep_rank > 1
        )
        """
    )
    return db.execute("SELECT count(*) FROM MESSAGE").fetchone()[0]


def test_accepts_unchanged_and_appended_persistent_archive(tmp_path: Path) -> None:
    source = _synthetic_source(tmp_path / "source.sqlite", "TAPPEND", "Append")
    warehouse = tmp_path / "warehouse.duckdb"
    first = ingest_slackdump(source, warehouse, workspace_slug="append")
    unchanged = ingest_slackdump(source, warehouse, workspace_slug="append")

    appended_ts = "1700000003.000004"
    appended_payload = {
        "type": "message",
        "user": "U_SHARED",
        "text": "resumed append",
        "ts": appended_ts,
    }
    with sqlite3.connect(source) as db:
        db.execute("INSERT INTO SESSION VALUES (2, 1)")
        db.execute("INSERT INTO CHUNK VALUES (20, 2, 0, 1, 'C_SHARED', '', 0)")
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, '', ?, ?, NULL, '', '', 0, 0, 0, ?, ?)",
            (1700000003000004, 20, "C_SHARED", appended_ts, "resumed append", json.dumps(appended_payload)),
        )
        db.commit()

    appended = ingest_slackdump(source, warehouse, workspace_slug="append")

    assert unchanged.unchanged_messages == first.canonical_rows
    assert appended.canonical_rows == first.canonical_rows + 1
    assert appended.inserted_messages == 1
    with duckdb.connect(str(warehouse), read_only=True) as db:
        checkpoint = db.execute(
            """
            SELECT session_high_water, chunk_high_water,
                   lineage_signature IS NOT NULL, lineage_manifest_json IS NOT NULL
            FROM ingestion_checkpoints WHERE workspace_id = 'TAPPEND'
            """
        ).fetchone()
    assert checkpoint == (2, 20, True, True)


def test_source_vs_canonical_validation_returns_exact_safe_results(tmp_path: Path) -> None:
    source = _synthetic_source(tmp_path / "source.sqlite", "TVALID", "Validate")
    warehouse = tmp_path / "warehouse.duckdb"
    ingest_slackdump(source, warehouse, workspace_slug="validate")

    result = validate_source_vs_canonical(source, warehouse, workspace_id="TVALID")

    assert result.as_dict() == {
        "passed": True,
        "source_message_rows": 4,
        "source_natural_key_count": 3,
        "canonical_message_rows": 3,
        "canonical_natural_key_count": 3,
        "natural_keys_match": True,
        "missing_canonical_key_count": 0,
        "extra_canonical_key_count": 0,
        "selected_payload_mismatch_count": 0,
        "raw_json_mismatch_count": 0,
        "source_chunk_mismatch_count": 0,
        "latest_timestamp_matches": True,
        "source_latest_timestamp": "1700000002.000003",
        "canonical_latest_timestamp": "1700000002.000003",
        "source_invalid_json_count": 0,
        "canonical_invalid_json_count": 0,
        "source_duplicate_key_count": 1,
        "source_duplicate_row_count": 1,
        "canonical_duplicate_key_count": 0,
        "canonical_duplicate_row_count": 0,
        "checkpoint_exists": True,
        "checkpoint_matches_source": True,
        "checkpoint_run_succeeded": True,
        "checkpoint_valid": True,
        "orphan_reply_count": 0,
    }


def test_source_vs_canonical_validation_counts_exact_corruption(tmp_path: Path) -> None:
    source = _synthetic_source(tmp_path / "source.sqlite", "TBADVALID", "Validate")
    warehouse = tmp_path / "warehouse.duckdb"
    ingest_slackdump(source, warehouse, workspace_slug="validate")
    with duckdb.connect(str(warehouse)) as db:
        db.execute(
            """
            UPDATE messages SET raw_json = '{bad', source_chunk_id = source_chunk_id + 1
            WHERE workspace_id = 'TBADVALID' AND ts = '1700000001.000002'
            """
        )
        db.execute(
            """
            DELETE FROM messages
            WHERE workspace_id = 'TBADVALID' AND ts = '1700000000.000001'
            """
        )

    result = validate_source_vs_canonical(source, warehouse)

    assert result.passed is False
    assert result.natural_keys_match is False
    assert result.missing_canonical_key_count == 1
    assert result.selected_payload_mismatch_count == 1
    assert result.raw_json_mismatch_count == 1
    assert result.source_chunk_mismatch_count == 1
    assert result.canonical_invalid_json_count == 1
    assert result.orphan_reply_count == 1


def _synthetic_source(path: Path, workspace_id: str, label: str) -> Path:
    schema = """
        CREATE TABLE WORKSPACE (
            ID INTEGER, CHUNK_ID INTEGER, LOAD_DTTM TEXT, TEAM TEXT, USERNAME TEXT,
            TEAM_ID TEXT, USER_ID TEXT, ENTERPRISE_ID TEXT, URL TEXT, DATA BLOB
        );
        CREATE TABLE CHANNEL (
            ID TEXT, CHUNK_ID INTEGER, LOAD_DTTM TEXT, NAME TEXT, IDX INTEGER, DATA BLOB
        );
        CREATE TABLE S_USER (
            ID TEXT, CHUNK_ID INTEGER, LOAD_DTTM TEXT, IDX INTEGER,
            USERNAME TEXT, DATA BLOB
        );
        CREATE TABLE CHANNEL_USER (
            CHANNEL_ID TEXT, USER_ID TEXT, CHUNK_ID INTEGER, LOAD_DTTM TEXT, IDX INTEGER
        );
        CREATE TABLE MESSAGE (
            ID INTEGER, CHUNK_ID INTEGER, LOAD_DTTM TEXT, CHANNEL_ID TEXT, TS TEXT,
            PARENT_ID INTEGER, THREAD_TS TEXT, LATEST_REPLY TEXT, IS_PARENT INTEGER,
            IDX INTEGER, NUM_FILES INTEGER, TXT TEXT, DATA BLOB
        );
        CREATE TABLE FILE (
            ID TEXT, CHUNK_ID INTEGER, LOAD_DTTM TEXT, CHANNEL_ID TEXT,
            MESSAGE_ID INTEGER, THREAD_ID INTEGER, IDX INTEGER, MODE TEXT,
            FILENAME TEXT, URL TEXT, DATA BLOB, SIZE INTEGER
        );
        CREATE TABLE SESSION (ID INTEGER, FINISHED INTEGER);
        CREATE TABLE CHUNK (
            ID INTEGER, SESSION_ID INTEGER, TYPE_ID INTEGER, FINAL INTEGER,
            CHANNEL_ID TEXT, SEARCH_QUERY TEXT, THREAD_ONLY INTEGER
        );
    """
    parent_ts = "1700000000.000001"
    reply_ts = "1700000001.000002"
    deleted_ts = "1700000002.000003"
    old_parent = {
        "type": "message",
        "user": "U_SHARED",
        "text": f"{label} old",
        "ts": parent_ts,
        "thread_ts": parent_ts,
        "reply_count": 1,
    }
    new_parent = {
        **old_parent,
        "text": f"{label} newest",
        "edited": {"user": "U_SHARED", "ts": "1700000000.500000"},
        "reactions": [{"name": "wave", "count": 1, "users": ["U_SHARED"]}],
        "files": [
            {
                "id": "F_SHARED",
                "name": f"{label}.txt",
                "mimetype": "text/plain",
                "size": 12,
                "mode": "hosted",
                "user": "U_SHARED",
                "url_private": f"https://files.invalid/{label}",
            }
        ],
    }
    reply = {
        "type": "message",
        "user": "U_SHARED",
        "text": f"{label} reply",
        "ts": reply_ts,
        "thread_ts": parent_ts,
    }
    deleted = {
        "type": "message",
        "subtype": "message_deleted",
        "hidden": True,
        "deleted_ts": deleted_ts,
        "ts": deleted_ts,
        "text": "This message was deleted.",
    }
    workspace = {
        "url": f"https://{label}.slack.com/",
        "team": label,
        "team_id": workspace_id,
        "user_id": "U_SHARED",
    }
    channel = {
        "id": "C_SHARED",
        "name": "general",
        "is_private": False,
        "is_archived": False,
        "topic": {"value": "topic"},
        "purpose": {"value": "purpose"},
    }
    user = {
        "id": "U_SHARED",
        "team_id": workspace_id,
        "name": "user",
        "real_name": f"{label} user",
        "deleted": False,
        "is_bot": False,
        "is_restricted": False,
        "profile": {"display_name": label},
    }

    with sqlite3.connect(path) as db:
        db.executescript(schema)
        db.execute(
            "INSERT INTO WORKSPACE VALUES (1, 1, '', ?, '', ?, 'U_SHARED', NULL, ?, ?)",
            [label, workspace_id, workspace["url"], _blob(workspace)],
        )
        db.execute(
            "INSERT INTO CHANNEL VALUES ('C_SHARED', 2, '', 'general', 0, ?)",
            [_blob(channel)],
        )
        db.execute(
            "INSERT INTO S_USER VALUES ('U_SHARED', 3, '', 0, 'user', ?)",
            [_blob(user)],
        )
        db.execute("INSERT INTO CHANNEL_USER VALUES ('C_SHARED', 'U_SHARED', 4, '', 0)")
        db.executemany(
            "INSERT INTO MESSAGE VALUES (?, ?, '', 'C_SHARED', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (1700000000000001, 10, parent_ts, None, parent_ts, reply_ts, 1, 0, 0, old_parent["text"], _blob(old_parent)),
                (1700000000000001, 11, parent_ts, None, parent_ts, reply_ts, 1, 0, 1, new_parent["text"], _blob(new_parent)),
                (1700000001000002, 12, reply_ts, 1700000000000001, parent_ts, None, 0, 0, 0, reply["text"], _blob(reply)),
                (1700000002000003, 13, deleted_ts, None, None, None, 0, 0, 0, deleted["text"], _blob(deleted)),
            ],
        )
        db.execute("INSERT INTO SESSION VALUES (1, 1)")
        db.execute("INSERT INTO CHUNK VALUES (13, 1, 0, 1, 'C_SHARED', '', 0)")
    return path


def _blob(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")
