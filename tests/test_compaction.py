from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import duckdb
import pytest

from slackpipe.compaction import (
    compact_archive,
    rebase_checkpoint,
    verify_compaction,
)
from slackpipe.transform import ingest_slackdump


def _blob(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _archive(path: Path) -> Path:
    parent_ts = "1700000000.000001"
    new_parent = {
        "type": "message", "user": "U", "text": "newest",
        "ts": parent_ts, "thread_ts": parent_ts, "reply_count": 1,
    }
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE WORKSPACE (
                ID INTEGER, CHUNK_ID INTEGER, LOAD_DTTM TEXT, TEAM TEXT,
                USERNAME TEXT, TEAM_ID TEXT, USER_ID TEXT, ENTERPRISE_ID TEXT,
                URL TEXT, DATA BLOB
            );
            CREATE TABLE CHANNEL (
                ID TEXT, CHUNK_ID INTEGER, LOAD_DTTM TEXT, NAME TEXT,
                IDX INTEGER, DATA BLOB
            );
            CREATE TABLE S_USER (
                ID TEXT, CHUNK_ID INTEGER, LOAD_DTTM TEXT, IDX INTEGER,
                USERNAME TEXT, DATA BLOB
            );
            CREATE TABLE CHANNEL_USER (
                CHANNEL_ID TEXT, USER_ID TEXT, CHUNK_ID INTEGER,
                LOAD_DTTM TEXT, IDX INTEGER
            );
            CREATE TABLE MESSAGE (
                ID INTEGER, CHUNK_ID INTEGER, LOAD_DTTM TEXT, CHANNEL_ID TEXT,
                TS TEXT, PARENT_ID INTEGER, THREAD_TS TEXT, LATEST_REPLY TEXT,
                IS_PARENT INTEGER, IDX INTEGER, NUM_FILES INTEGER, TXT TEXT,
                DATA BLOB
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
            CREATE TABLE goose_db_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT, version_id INTEGER NOT NULL,
                is_applied INTEGER NOT NULL, tstamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        db.execute(
            "INSERT INTO WORKSPACE VALUES (1, 1, '', 'team', '', 'TCOMP', 'U', NULL, 'https://t.slack.com/', ?)",
            [_blob({"url": "https://t.slack.com/", "team": "team", "team_id": "TCOMP", "user_id": "U"})],
        )
        db.execute(
            "INSERT INTO CHANNEL VALUES ('C', 2, '', 'general', 0, ?)",
            [_blob({"id": "C", "name": "general"})],
        )
        db.execute("INSERT INTO S_USER VALUES ('U', 3, '', 0, 'u', ?)", [_blob({"id": "U"})])
        db.execute("INSERT INTO CHANNEL_USER VALUES ('C', 'U', 4, '', 0)")
        # Two identical old copies plus the newest payload: dedupe shape.
        for rowid, chunk in ((1, 5), (2, 6)):
            _ = rowid
            db.execute(
                "INSERT INTO MESSAGE VALUES (?, ?, '', 'C', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    1700000000000001, chunk, parent_ts, None, parent_ts, None,
                    0, 0, 0, "old",
                    _blob({
                        "type": "message", "user": "U", "text": "old",
                        "ts": parent_ts,
                    }),
                ),
            )
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, '', 'C', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1700000000000001, 11, parent_ts, None, parent_ts, None,
                0, 0, 1, "newest", _blob(new_parent),
            ),
        )
        db.execute("INSERT INTO SESSION VALUES (1, 1)")
        db.execute("INSERT INTO CHUNK VALUES (11, 1, 0, 1, 'C', '', 0)")
        db.execute("INSERT INTO goose_db_version (version_id, is_applied) VALUES (1, 1)")
    return path


def test_compact_verify_rebase_cycle(tmp_path: Path) -> None:
    source = _archive(tmp_path / "source.sqlite")
    warehouse = tmp_path / "warehouse.duckdb"
    first = ingest_slackdump(source, warehouse, workspace_slug="comp")

    compacted = tmp_path / "compacted.sqlite"
    kept = compact_archive(source, compacted)

    assert kept["MESSAGE"] == 1
    assert compacted.stat().st_size <= source.stat().st_size
    with sqlite3.connect(compacted) as db:
        assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert db.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert db.execute("SELECT max(ID) FROM CHUNK").fetchone() == (11,)
        assert db.execute("SELECT max(ID) FROM SESSION").fetchone() == (1,)
        assert db.execute(
            "SELECT version_id FROM goose_db_version"
        ).fetchall() == [(1,)]

    stats = verify_compaction(source, compacted, warehouse, workspace_id="TCOMP")
    assert stats["MESSAGE_original"] == 1
    assert stats["MESSAGE_compacted"] == 1

    rebased = rebase_checkpoint(warehouse, workspace_id="TCOMP", source_path=compacted)
    assert rebased["chunk_high_water"] == 11

    second = ingest_slackdump(compacted, warehouse, workspace_slug="comp")
    assert second.canonical_rows == first.canonical_rows
    assert second.inserted_messages == second.updated_messages == 0


def test_verify_rejects_lost_key(tmp_path: Path) -> None:
    source = _archive(tmp_path / "source.sqlite")
    warehouse = tmp_path / "warehouse.duckdb"
    ingest_slackdump(source, warehouse, workspace_slug="comp")

    compacted = tmp_path / "compacted.sqlite"
    compact_archive(source, compacted)
    with sqlite3.connect(compacted) as db:
        db.execute("DELETE FROM MESSAGE")
        db.commit()

    with pytest.raises(ValueError, match="diverges"):
        verify_compaction(source, compacted, warehouse, workspace_id="TCOMP")


def test_compact_refuses_existing_destination(tmp_path: Path) -> None:
    source = _archive(tmp_path / "source.sqlite")
    compacted = tmp_path / "compacted.sqlite"
    compacted.touch()
    with pytest.raises(ValueError, match="already exists"):
        compact_archive(source, compacted)
