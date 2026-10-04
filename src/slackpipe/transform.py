"""Canonical Slackdump SQLite to DuckDB transformation."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import uuid
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb

from slackpipe.sqlite_snapshot import snapshot_sqlite
from slackpipe.slackdump import GlobalFileLock


SCHEMA_VERSION = 2
REQUIRED_COLUMNS = {
    "WORKSPACE": {"CHUNK_ID", "TEAM", "TEAM_ID", "URL", "DATA"},
    "CHANNEL": {"ID", "CHUNK_ID", "NAME", "DATA"},
    "S_USER": {"ID", "CHUNK_ID", "USERNAME", "DATA"},
    "CHANNEL_USER": {"CHANNEL_ID", "USER_ID", "CHUNK_ID"},
    "MESSAGE": {
        "ID",
        "CHUNK_ID",
        "CHANNEL_ID",
        "TS",
        "THREAD_TS",
        "IS_PARENT",
        "TXT",
        "DATA",
    },
    "FILE": {"ID", "CHUNK_ID", "CHANNEL_ID", "MESSAGE_ID", "THREAD_ID", "DATA"},
    "SESSION": {"ID", "FINISHED"},
    "CHUNK": {"ID", "SESSION_ID", "TYPE_ID", "FINAL"},
}


@dataclass(frozen=True)
class IngestionResult:
    run_id: str
    workspace_id: str
    source_rows: int
    canonical_rows: int
    inserted_messages: int
    updated_messages: int
    unchanged_messages: int


@dataclass(frozen=True)
class SourceCanonicalValidation:
    """Content-free integrity results safe to expose as check metadata."""

    passed: bool
    source_message_rows: int
    source_natural_key_count: int
    canonical_message_rows: int
    canonical_natural_key_count: int
    natural_keys_match: bool
    missing_canonical_key_count: int
    extra_canonical_key_count: int
    selected_payload_mismatch_count: int
    raw_json_mismatch_count: int
    source_chunk_mismatch_count: int
    latest_timestamp_matches: bool
    source_latest_timestamp: str | None
    canonical_latest_timestamp: str | None
    source_invalid_json_count: int
    canonical_invalid_json_count: int
    source_duplicate_key_count: int
    source_duplicate_row_count: int
    canonical_duplicate_key_count: int
    canonical_duplicate_row_count: int
    checkpoint_exists: bool
    checkpoint_matches_source: bool
    checkpoint_run_succeeded: bool
    checkpoint_valid: bool
    orphan_reply_count: int

    def as_dict(self) -> dict[str, bool | int | str | None]:
        return asdict(self)


class SourceContinuityError(RuntimeError):
    """The source cannot be proven to continue the last accepted archive."""


def _checkpoint_chunk_high_water(
    db: duckdb.DuckDBPyConnection, workspace_id: str
) -> int | None:
    """Newest accepted chunk, or None when nothing was accepted yet."""
    row = db.execute(
        "SELECT chunk_high_water FROM ingestion_checkpoints WHERE workspace_id = ?",
        [workspace_id],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def ingest_slackdump(
    source: str | Path,
    warehouse: str | Path,
    *,
    workspace_slug: str,
    mode: str = "development",
    dagster_run_id: str | None = None,
    require_complete: bool = True,
    lock_path: str | Path | None = None,
    allow_lineage_bootstrap: bool = False,
    status: Callable[[str], None] | None = None,
) -> IngestionResult:
    """Ingest one Slackdump archive into a workspace-segregated DuckDB file."""

    if mode not in {"development", "backfill", "incremental"}:
        raise ValueError("mode must be development, backfill, or incremental")
    if not workspace_slug.strip():
        raise ValueError("workspace_slug must not be empty")

    source_path = Path(source).expanduser().resolve(strict=True)
    warehouse_path = Path(warehouse).expanduser().resolve()
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    started_at = _now()

    report = status or (lambda _message: None)
    lock = GlobalFileLock(lock_path, timeout=300.0) if lock_path else nullcontext()
    with lock, tempfile.TemporaryDirectory(prefix="slackpipe-snapshot-") as directory:
        report("snapshotting Slackdump source")
        snapshot_path = snapshot_sqlite(source_path, Path(directory) / "slackdump.sqlite")
        fingerprint = _sha256_file(snapshot_path)
        with _open_source(snapshot_path) as source_db:
            _validate_source(source_db, require_complete=require_complete)
            workspace_team_id = source_db.execute(
                "SELECT TEAM_ID FROM WORKSPACE ORDER BY CHUNK_ID DESC, ID DESC LIMIT 1"
            ).fetchone()[0]
            if workspace_team_id is None:
                raise ValueError("Slackdump source has no workspace")
            report("reading and canonicalizing Slackdump source tables")
            connection = duckdb.connect(str(warehouse_path))
            try:
                connection.execute("SET threads = 1")
                connection.execute("SET memory_limit = '2300MB'")
                connection.execute("SET preserve_insertion_order = false")
                _create_schema(connection)
                connection.execute(
                    """
                    INSERT INTO ingestion_runs (
                        run_id, workspace_id, mode, status, started_at, dagster_run_id,
                        source_path, source_fingerprint
                    ) VALUES (?, ?, ?, 'started', ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        workspace_team_id,
                        mode,
                        started_at,
                        dagster_run_id,
                        str(source_path),
                        fingerprint,
                    ],
                )
                checkpoint = connection.execute(
                    """
                    SELECT c.source_fingerprint, r.status, c.chunk_high_water
                    FROM ingestion_checkpoints c
                    LEFT JOIN ingestion_runs r ON r.run_id = c.successful_run_id
                    WHERE c.workspace_id = ?
                    """,
                    [workspace_team_id],
                ).fetchone()
                if (
                    checkpoint is not None
                    and checkpoint[0] == fingerprint
                    and checkpoint[1] == "succeeded"
                ):
                    # Fingerprint fast-path: this exact archive already
                    # ingested successfully, so there is nothing to re-read
                    # or re-hash. Report the steady state instead.
                    canonical_rows = connection.execute(
                        "SELECT count(*) FROM messages WHERE workspace_id = ?",
                        [workspace_team_id],
                    ).fetchone()[0]
                    source_message_rows = source_db.execute(
                        "SELECT count(*) FROM MESSAGE"
                    ).fetchone()[0]
                    connection.execute("BEGIN TRANSACTION")
                    _record_attachment_paths(
                        connection,
                        workspace_team_id,
                        Path(source_path).parent,
                    )
                    connection.execute(
                        """
                        UPDATE ingestion_checkpoints
                        SET successful_run_id = ?, checkpointed_at = ?
                        WHERE workspace_id = ?
                        """,
                        [run_id, _now(), workspace_team_id],
                    )
                    connection.execute(
                        """
                        UPDATE ingestion_runs SET
                            status = 'succeeded', completed_at = ?,
                            source_message_rows = ?,
                            canonical_message_rows = ?, inserted_messages = 0,
                            updated_messages = 0, unchanged_messages = ?
                        WHERE run_id = ?
                        """,
                        [
                            _now(),
                            source_message_rows,
                            canonical_rows,
                            canonical_rows,
                            run_id,
                        ],
                    )
                    connection.execute("COMMIT")
                    report("canonical DuckDB transaction committed")
                    return IngestionResult(
                        run_id=run_id,
                        workspace_id=workspace_team_id,
                        source_rows=source_message_rows,
                        canonical_rows=canonical_rows,
                        inserted_messages=0,
                        updated_messages=0,
                        unchanged_messages=canonical_rows,
                    )
                min_chunk_id = _checkpoint_chunk_high_water(connection, workspace_team_id)
                if min_chunk_id is not None:
                    source_max_chunk = source_db.execute(
                        "SELECT max(ID) FROM CHUNK"
                    ).fetchone()[0]
                    if source_max_chunk is not None and source_max_chunk <= min_chunk_id:
                        # Fingerprint changed but no new chunks exist: rows
                        # were pruned or rewritten in place (dedupe, VACUUM,
                        # or a foreign file), so only a full read can prove
                        # continuity. Rare path; new-chunk resumes stay scoped.
                        min_chunk_id = None
                source_data = _read_source(source_db, min_chunk_id=min_chunk_id)
                report(
                    "source ready: "
                    f"channels={len(source_data['channels'])}, users={len(source_data['users'])}, "
                    f"memberships={len(source_data['channel_members'])}, "
                    f"messages={len(source_data['messages'])}, files={len(source_data['files'])}"
                )
                workspace_id = source_data["workspace"]["workspace_id"]
                try:
                    connection.execute("BEGIN TRANSACTION")
                    report("applying canonical DuckDB transaction")
                    _assert_source_continuity(
                        connection,
                        source_db,
                        source_data,
                        source_fingerprint=fingerprint,
                        allow_lineage_bootstrap=allow_lineage_bootstrap,
                    )
                    result = _apply_source(
                        connection,
                        source_data,
                        run_id=run_id,
                        workspace_slug=workspace_slug,
                        source_path=str(source_path),
                        source_fingerprint=fingerprint,
                        status=report,
                    )
                    connection.execute(
                        """
                        UPDATE ingestion_runs SET
                            status = 'succeeded', completed_at = ?, source_message_rows = ?,
                            canonical_message_rows = ?, inserted_messages = ?,
                            updated_messages = ?, unchanged_messages = ?
                        WHERE run_id = ?
                        """,
                        [
                            _now(),
                            result.source_rows,
                            result.canonical_rows,
                            result.inserted_messages,
                            result.updated_messages,
                            result.unchanged_messages,
                            run_id,
                        ],
                    )
                    connection.execute("COMMIT")
                    report("canonical DuckDB transaction committed")
                    return result
                except BaseException as error:
                    connection.execute("ROLLBACK")
                    connection.execute(
                        """
                        UPDATE ingestion_runs SET status = 'failed', completed_at = ?,
                            error_class = ?, error_message = ? WHERE run_id = ?
                        """,
                        [_now(), type(error).__name__, str(error), run_id],
                    )
                    raise
            finally:
                connection.close()


def validate_source_vs_canonical(
    source: str | Path,
    warehouse: str | Path,
    *,
    workspace_id: str | None = None,
) -> SourceCanonicalValidation:
    """Compare an archive and canonical messages without returning message content."""

    source_path = Path(source).expanduser().resolve(strict=True)
    warehouse_path = Path(warehouse).expanduser().resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="slackpipe-validation-") as directory:
        snapshot_path = snapshot_sqlite(source_path, Path(directory) / "slackdump.sqlite")
        with _open_source(snapshot_path) as source_db:
            _validate_source(source_db, require_complete=False)
            source_workspace_id = source_db.execute(
                "SELECT TEAM_ID FROM WORKSPACE ORDER BY CHUNK_ID DESC, ID DESC LIMIT 1"
            ).fetchone()[0]
            if workspace_id is not None and workspace_id != source_workspace_id:
                raise ValueError("Requested workspace does not match Slackdump source")
            workspace_id = source_workspace_id
            source_rows = source_db.execute("SELECT count(*) FROM MESSAGE").fetchone()[0]
            source_duplicates = source_db.execute(
                """
                SELECT count(*), coalesce(sum(row_count - 1), 0) FROM (
                    SELECT count(*) AS row_count FROM MESSAGE
                    GROUP BY CHANNEL_ID, TS HAVING count(*) > 1
                )
                """
            ).fetchone()
            selected_rows = _newest_rows(source_db, "MESSAGE", ["CHANNEL_ID", "TS"])
            source_selected = {
                (row["CHANNEL_ID"], row["TS"]): (row["CHUNK_ID"], _raw_text(row["DATA"]))
                for row in selected_rows
            }
            source_invalid_json = sum(
                not _is_json_object(row["DATA"])
                for row in source_db.execute("SELECT DATA FROM MESSAGE")
            )
            source_latest = _latest_slack_timestamp(source_selected)
            session_high_water = source_db.execute("SELECT max(ID) FROM SESSION").fetchone()[0]
            chunk_high_water = source_db.execute("SELECT max(ID) FROM CHUNK").fetchone()[0]

    with duckdb.connect(str(warehouse_path), read_only=True) as target:
        canonical_rows = target.execute(
            "SELECT channel_id, ts, source_chunk_id, raw_json FROM messages WHERE workspace_id = ?",
            [workspace_id],
        ).fetchall()
        canonical_map = {(row[0], row[1]): (row[2], row[3]) for row in canonical_rows}
        canonical_duplicate_key_count, canonical_duplicate_row_count = target.execute(
            """
            SELECT count(*), coalesce(sum(row_count - 1), 0) FROM (
                SELECT count(*) AS row_count FROM messages WHERE workspace_id = ?
                GROUP BY channel_id, ts HAVING count(*) > 1
            )
            """,
            [workspace_id],
        ).fetchone()
        canonical_invalid_json = sum(not _is_json_object(row[3]) for row in canonical_rows)
        orphan_replies = target.execute(
            """
            SELECT count(*) FROM messages reply
            WHERE reply.workspace_id = ? AND reply.is_thread_reply
              AND NOT EXISTS (
                  SELECT 1 FROM messages parent
                  WHERE parent.workspace_id = reply.workspace_id
                    AND parent.channel_id = reply.channel_id
                    AND parent.ts = reply.parent_ts
              )
            """,
            [workspace_id],
        ).fetchone()[0]
        checkpoint = target.execute(
            """
            SELECT c.source_fingerprint, c.session_high_water, c.chunk_high_water,
                   c.lineage_signature, c.lineage_manifest_json,
                   r.status
            FROM ingestion_checkpoints c
            LEFT JOIN ingestion_runs r ON r.run_id = c.successful_run_id
            WHERE c.workspace_id = ?
            """,
            [workspace_id],
        ).fetchone()

    source_keys = set(source_selected)
    canonical_keys = set(canonical_map)
    common_keys = source_keys & canonical_keys
    raw_mismatch_keys = {
        key for key in common_keys if source_selected[key][1] != canonical_map[key][1]
    }
    chunk_mismatch_keys = {
        key for key in common_keys if source_selected[key][0] != canonical_map[key][0]
    }
    canonical_latest = _latest_slack_timestamp(canonical_map)
    checkpoint_exists = checkpoint is not None
    checkpoint_matches = False
    checkpoint_run_succeeded = False
    if checkpoint is not None:
        _, checkpoint_session, checkpoint_chunk, signature, manifest_json, run_status = checkpoint
        checkpoint_run_succeeded = run_status == "succeeded"
        try:
            checkpoint_manifest = json.loads(manifest_json) if manifest_json else None
        except (TypeError, json.JSONDecodeError):
            checkpoint_manifest = None
        checkpoint_matches = (
            _valid_lineage_manifest(checkpoint_manifest)
            and signature == _lineage_signature(checkpoint_manifest)
            and checkpoint_session == session_high_water
            and checkpoint_chunk == chunk_high_water
        )
    natural_keys_match = source_keys == canonical_keys
    latest_matches = source_latest == canonical_latest
    checkpoint_valid = checkpoint_exists and checkpoint_matches and checkpoint_run_succeeded
    passed = all(
        (
            natural_keys_match,
            not raw_mismatch_keys,
            not chunk_mismatch_keys,
            latest_matches,
            source_invalid_json == 0,
            canonical_invalid_json == 0,
            canonical_duplicate_key_count == 0,
            checkpoint_valid,
        )
    )
    return SourceCanonicalValidation(
        passed=passed,
        source_message_rows=source_rows,
        source_natural_key_count=len(source_keys),
        canonical_message_rows=len(canonical_rows),
        canonical_natural_key_count=len(canonical_keys),
        natural_keys_match=natural_keys_match,
        missing_canonical_key_count=len(source_keys - canonical_keys),
        extra_canonical_key_count=len(canonical_keys - source_keys),
        selected_payload_mismatch_count=len(raw_mismatch_keys | chunk_mismatch_keys),
        raw_json_mismatch_count=len(raw_mismatch_keys),
        source_chunk_mismatch_count=len(chunk_mismatch_keys),
        latest_timestamp_matches=latest_matches,
        source_latest_timestamp=source_latest,
        canonical_latest_timestamp=canonical_latest,
        source_invalid_json_count=source_invalid_json,
        canonical_invalid_json_count=canonical_invalid_json,
        source_duplicate_key_count=source_duplicates[0],
        source_duplicate_row_count=source_duplicates[1],
        canonical_duplicate_key_count=canonical_duplicate_key_count,
        canonical_duplicate_row_count=canonical_duplicate_row_count,
        checkpoint_exists=checkpoint_exists,
        checkpoint_matches_source=checkpoint_matches,
        checkpoint_run_succeeded=checkpoint_run_succeeded,
        checkpoint_valid=checkpoint_valid,
        orphan_reply_count=orphan_replies,
    )


def _create_schema(db: duckdb.DuckDBPyConnection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_metadata (
            schema_version INTEGER PRIMARY KEY,
            installed_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id VARCHAR PRIMARY KEY,
            workspace_id VARCHAR NOT NULL,
            mode VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            dagster_run_id VARCHAR,
            source_path VARCHAR NOT NULL,
            source_fingerprint VARCHAR NOT NULL,
            source_message_rows BIGINT,
            canonical_message_rows BIGINT,
            inserted_messages BIGINT DEFAULT 0,
            updated_messages BIGINT DEFAULT 0,
            unchanged_messages BIGINT DEFAULT 0,
            error_class VARCHAR,
            error_message VARCHAR
        );
        CREATE TABLE IF NOT EXISTS source_archives (
            workspace_id VARCHAR NOT NULL,
            source_fingerprint VARCHAR NOT NULL,
            source_path VARCHAR NOT NULL,
            sqlite_schema_version BIGINT,
            slackdump_schema_version BIGINT,
            session_high_water BIGINT,
            chunk_high_water BIGINT,
            source_message_rows BIGINT NOT NULL,
            source_table_counts_json VARCHAR NOT NULL,
            first_run_id VARCHAR NOT NULL,
            latest_run_id VARCHAR NOT NULL,
            first_ingested_at TIMESTAMPTZ NOT NULL,
            last_ingested_at TIMESTAMPTZ NOT NULL,
            ingest_count BIGINT NOT NULL,
            PRIMARY KEY (workspace_id, source_fingerprint)
        );
        CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
            workspace_id VARCHAR PRIMARY KEY,
            source_fingerprint VARCHAR NOT NULL,
            session_high_water BIGINT,
            chunk_high_water BIGINT,
            lineage_signature VARCHAR,
            lineage_manifest_json VARCHAR,
            successful_run_id VARCHAR NOT NULL,
            checkpointed_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            workspace_id VARCHAR PRIMARY KEY,
            workspace_slug VARCHAR NOT NULL,
            team_name VARCHAR,
            workspace_url VARCHAR,
            enterprise_id VARCHAR,
            raw_json VARCHAR NOT NULL,
            source_chunk_id BIGINT NOT NULL,
            latest_run_id VARCHAR NOT NULL,
            first_observed_at TIMESTAMPTZ NOT NULL,
            last_observed_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS channels (
            workspace_id VARCHAR NOT NULL,
            channel_id VARCHAR NOT NULL,
            name VARCHAR,
            is_private BOOLEAN,
            is_archived BOOLEAN,
            topic VARCHAR,
            purpose VARCHAR,
            raw_json VARCHAR NOT NULL,
            source_chunk_id BIGINT NOT NULL,
            latest_run_id VARCHAR NOT NULL,
            first_observed_at TIMESTAMPTZ NOT NULL,
            last_observed_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, channel_id)
        );
        CREATE TABLE IF NOT EXISTS users (
            workspace_id VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            username VARCHAR,
            real_name VARCHAR,
            display_name VARCHAR,
            is_bot BOOLEAN,
            is_deleted BOOLEAN,
            is_restricted BOOLEAN,
            profile_json VARCHAR,
            raw_json VARCHAR NOT NULL,
            source_chunk_id BIGINT NOT NULL,
            latest_run_id VARCHAR NOT NULL,
            first_observed_at TIMESTAMPTZ NOT NULL,
            last_observed_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS channel_members (
            workspace_id VARCHAR NOT NULL,
            channel_id VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            source_chunk_id BIGINT NOT NULL,
            first_observed_run_id VARCHAR NOT NULL,
            last_observed_run_id VARCHAR NOT NULL,
            PRIMARY KEY (workspace_id, channel_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS messages (
            message_key VARCHAR PRIMARY KEY,
            workspace_id VARCHAR NOT NULL,
            channel_id VARCHAR NOT NULL,
            ts VARCHAR NOT NULL,
            ts_us BIGINT NOT NULL,
            source_message_id BIGINT,
            source_chunk_id BIGINT NOT NULL,
            user_id VARCHAR,
            subtype VARCHAR,
            text VARCHAR,
            search_text VARCHAR NOT NULL,
            thread_ts VARCHAR,
            parent_ts VARCHAR,
            latest_reply_ts VARCHAR,
            is_thread_parent BOOLEAN NOT NULL,
            is_thread_reply BOOLEAN NOT NULL,
            is_thread_broadcast BOOLEAN NOT NULL,
            is_edited BOOLEAN NOT NULL,
            edited_ts VARCHAR,
            is_deleted BOOLEAN NOT NULL,
            deleted_ts VARCHAR,
            file_metadata_json VARCHAR,
            raw_json VARCHAR NOT NULL,
            payload_hash VARCHAR NOT NULL,
            latest_run_id VARCHAR NOT NULL,
            first_observed_at TIMESTAMPTZ NOT NULL,
            last_changed_at TIMESTAMPTZ NOT NULL,
            last_observed_at TIMESTAMPTZ NOT NULL,
            UNIQUE (workspace_id, channel_id, ts)
        );
        CREATE TABLE IF NOT EXISTS reactions (
            workspace_id VARCHAR NOT NULL,
            channel_id VARCHAR NOT NULL,
            message_ts VARCHAR NOT NULL,
            emoji_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            reported_count BIGINT,
            latest_run_id VARCHAR NOT NULL,
            PRIMARY KEY (workspace_id, channel_id, message_ts, emoji_name, user_id)
        );
        CREATE TABLE IF NOT EXISTS files (
            workspace_id VARCHAR NOT NULL,
            file_id VARCHAR NOT NULL,
            channel_id VARCHAR,
            message_ts VARCHAR,
            thread_ts VARCHAR,
            user_id VARCHAR,
            mode VARCHAR,
            filename VARCHAR,
            mime_type VARCHAR,
            size_bytes BIGINT,
            source_url VARCHAR,
            local_object_path VARCHAR,
            raw_json VARCHAR NOT NULL,
            source_chunk_id BIGINT NOT NULL,
            latest_run_id VARCHAR NOT NULL,
            first_observed_at TIMESTAMPTZ NOT NULL,
            last_observed_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, file_id)
        );
        CREATE TABLE IF NOT EXISTS message_events (
            event_id VARCHAR PRIMARY KEY,
            workspace_id VARCHAR NOT NULL,
            channel_id VARCHAR NOT NULL,
            message_ts VARCHAR NOT NULL,
            message_key VARCHAR NOT NULL,
            event_type VARCHAR NOT NULL,
            payload_hash VARCHAR NOT NULL,
            raw_json VARCHAR NOT NULL,
            run_id VARCHAR NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS attachment_backfills (
            backfill_id VARCHAR PRIMARY KEY,
            workspace_id VARCHAR NOT NULL,
            run_id VARCHAR,
            status VARCHAR NOT NULL,
            channel_id VARCHAR,
            from_ts VARCHAR,
            to_ts VARCHAR,
            requested_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            files_processed BIGINT DEFAULT 0,
            bytes_stored BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS document_chunks (
            chunk_id VARCHAR PRIMARY KEY,
            message_key VARCHAR NOT NULL,
            workspace_id VARCHAR NOT NULL,
            channel_id VARCHAR NOT NULL,
            chunk_kind VARCHAR NOT NULL,
            chunk_version VARCHAR NOT NULL,
            chunk_index INTEGER NOT NULL,
            text VARCHAR NOT NULL,
            token_count BIGINT,
            metadata_json VARCHAR,
            created_run_id VARCHAR NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            UNIQUE (message_key, chunk_kind, chunk_version, chunk_index)
        );
        """
    )
    db.execute(
        "ALTER TABLE ingestion_checkpoints ADD COLUMN IF NOT EXISTS lineage_signature VARCHAR"
    )
    db.execute(
        "ALTER TABLE ingestion_checkpoints ADD COLUMN IF NOT EXISTS lineage_manifest_json VARCHAR"
    )
    db.execute(
        "INSERT INTO schema_metadata VALUES (?, ?) ON CONFLICT DO NOTHING",
        [SCHEMA_VERSION, _now()],
    )
    db.execute(
        """
        CREATE OR REPLACE VIEW v_messages_enriched AS
        SELECT m.*, w.workspace_slug, w.team_name, c.name AS channel_name,
               u.username, u.real_name, u.display_name
        FROM messages m
        JOIN workspaces w USING (workspace_id)
        LEFT JOIN channels c USING (workspace_id, channel_id)
        LEFT JOIN users u USING (workspace_id, user_id);

        CREATE OR REPLACE VIEW v_channel_timeline AS
        SELECT * FROM v_messages_enriched
        WHERE NOT is_deleted AND (NOT is_thread_reply OR is_thread_broadcast);

        CREATE OR REPLACE VIEW v_threads AS
        SELECT workspace_id, channel_id, COALESCE(thread_ts, ts) AS thread_ts,
               count(*) AS message_count, min(ts_us) AS first_ts_us,
               max(ts_us) AS last_ts_us,
               list(message_key ORDER BY ts_us) AS message_keys
        FROM messages
        WHERE thread_ts IS NOT NULL OR is_thread_parent
        GROUP BY workspace_id, channel_id, COALESCE(thread_ts, ts);

        CREATE OR REPLACE VIEW v_search_documents AS
        SELECT message_key AS document_id, message_key, workspace_id, channel_id,
               user_id, ts, ts_us, thread_ts, 'message' AS document_kind,
               search_text AS document_text,
               json_object('subtype', subtype, 'is_edited', is_edited,
                           'has_files', file_metadata_json IS NOT NULL) AS metadata_json,
               payload_hash AS document_version
        FROM messages
        WHERE NOT is_deleted AND search_text <> '';
        """
    )


def _open_source(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA temp_store = FILE")
    db.execute("PRAGMA cache_size = -32768")
    db.execute(
        "CREATE INDEX IF NOT EXISTS slackpipe_channel_newest ON CHANNEL (ID, CHUNK_ID DESC, IDX DESC)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS slackpipe_user_newest ON S_USER (ID, CHUNK_ID DESC, IDX DESC)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS slackpipe_message_newest ON MESSAGE (CHANNEL_ID, TS, CHUNK_ID DESC, IDX DESC)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS slackpipe_membership_newest ON CHANNEL_USER (CHANNEL_ID, CHUNK_ID DESC)"
    )
    return db


def _validate_source(db: sqlite3.Connection, *, require_complete: bool) -> None:
    tables = {
        row[0].upper()
        for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    for table, required in REQUIRED_COLUMNS.items():
        if table not in tables:
            raise ValueError(f"Slackdump source is missing table {table}")
        columns = {row[1].upper() for row in db.execute(f'PRAGMA table_info("{table}")')}
        missing = sorted(required - columns)
        if missing:
            raise ValueError(f"Slackdump table {table} is missing columns: {', '.join(missing)}")

    workspace_ids = {
        row[0]
        for row in db.execute("SELECT DISTINCT TEAM_ID FROM WORKSPACE WHERE TEAM_ID <> ''")
    }
    if len(workspace_ids) != 1:
        raise ValueError(f"Expected exactly one workspace in source, found {len(workspace_ids)}")
    if not require_complete:
        return
    latest_session = db.execute(
        "SELECT ID, FINISHED FROM SESSION ORDER BY ID DESC LIMIT 1"
    ).fetchone()
    if latest_session is None or latest_session[1] != 1:
        raise ValueError("Slackdump source latest session is unfinished")
    incomplete_streams = db.execute(
        """
        SELECT count(*) FROM (
            SELECT SESSION_ID, TYPE_ID, coalesce(CHANNEL_ID, ''),
                   coalesce(SEARCH_QUERY, ''), coalesce(THREAD_ONLY, 0)
            FROM CHUNK
            WHERE SESSION_ID = ? AND TYPE_ID IN (0, 1)
            GROUP BY SESSION_ID, TYPE_ID, coalesce(CHANNEL_ID, ''),
                     coalesce(SEARCH_QUERY, ''), coalesce(THREAD_ONLY, 0)
            HAVING max(CASE WHEN FINAL THEN 1 ELSE 0 END) = 0
        )
        """,
        [latest_session[0]],
    ).fetchone()[0]
    if incomplete_streams:
        raise ValueError(
            f"Slackdump source latest session has {incomplete_streams} incomplete stream(s)"
        )


def _read_source(
    db: sqlite3.Connection, *, min_chunk_id: int | None = None
) -> dict[str, Any]:
    """Read canonical inputs, optionally scoped to chunks newer than a checkpoint.

    Scoped reads return the true global newest row per touched key (never a
    downgrade), so untouched history is never re-read or re-hashed. Pass
    ``min_chunk_id=None`` for the full bootstrap read.
    """
    workspace_row = db.execute(
        "SELECT * FROM WORKSPACE ORDER BY CHUNK_ID DESC, ID DESC LIMIT 1"
    ).fetchone()
    workspace_payload, workspace_raw = _payload(workspace_row["DATA"], "WORKSPACE")
    workspace = {
        "workspace_id": workspace_row["TEAM_ID"],
        "team_name": workspace_row["TEAM"],
        "workspace_url": workspace_row["URL"],
        "enterprise_id": workspace_row["ENTERPRISE_ID"],
        "source_chunk_id": workspace_row["CHUNK_ID"],
        "raw_json": workspace_raw,
        "payload": workspace_payload,
    }

    if min_chunk_id is None:
        channel_rows = _newest_rows(db, "CHANNEL", ["ID"])
        user_rows = _newest_rows(db, "S_USER", ["ID"])
        message_rows = _newest_rows(db, "MESSAGE", ["CHANNEL_ID", "TS"])
        member_rows = _newest_memberships(db)
    else:
        touched_channels = [key[0] for key in _touched_keys(db, "CHANNEL", ["ID"], min_chunk_id)]
        touched_users = [key[0] for key in _touched_keys(db, "S_USER", ["ID"], min_chunk_id)]
        touched_messages = _touched_keys(db, "MESSAGE", ["CHANNEL_ID", "TS"], min_chunk_id)
        touched_member_channels = [
            key[0] for key in _touched_keys(db, "CHANNEL_USER", ["CHANNEL_ID"], min_chunk_id)
        ]
        channel_rows = _newest_rows_for_keys(db, "CHANNEL", ["ID"], [(c,) for c in touched_channels])
        user_rows = _newest_rows_for_keys(db, "S_USER", ["ID"], [(u,) for u in touched_users])
        message_rows = _newest_rows_for_keys(db, "MESSAGE", ["CHANNEL_ID", "TS"], touched_messages)
        member_rows = _newest_memberships(db, channel_ids=touched_member_channels)

    channels = []
    for row in channel_rows:
        payload, raw = _payload(row["DATA"], f"CHANNEL {row['ID']}")
        channels.append(
            {
                "channel_id": row["ID"],
                "name": row["NAME"] or payload.get("name"),
                "is_private": _bool(payload.get("is_private")),
                "is_archived": _bool(payload.get("is_archived")),
                "topic": _nested_value(payload, "topic", "value"),
                "purpose": _nested_value(payload, "purpose", "value"),
                "source_chunk_id": row["CHUNK_ID"],
                "raw_json": raw,
            }
        )

    users = []
    for row in user_rows:
        payload, raw = _payload(row["DATA"], f"S_USER {row['ID']}")
        profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
        users.append(
            {
                "user_id": row["ID"],
                "username": row["USERNAME"] or payload.get("name"),
                "real_name": payload.get("real_name") or profile.get("real_name"),
                "display_name": profile.get("display_name"),
                "is_bot": _bool(payload.get("is_bot")),
                "is_deleted": _bool(payload.get("deleted")),
                "is_restricted": _bool(payload.get("is_restricted")),
                "profile_json": _json_dump(profile),
                "source_chunk_id": row["CHUNK_ID"],
                "raw_json": raw,
            }
        )

    messages = [_message(row, workspace["workspace_id"]) for row in message_rows]
    files = _read_files(db, messages)
    table_counts = {
        table: db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
        for table in REQUIRED_COLUMNS
    }
    slackdump_version = db.execute(
        "SELECT max(version_id) FROM goose_db_version WHERE is_applied = 1"
    ).fetchone()[0] if _has_table(db, "goose_db_version") else None
    return {
        "workspace": workspace,
        "channels": channels,
        "users": users,
        "channel_members": member_rows,
        "messages": messages,
        "files": files,
        "source_message_rows": table_counts["MESSAGE"],
        "table_counts": table_counts,
        "sqlite_schema_version": db.execute("PRAGMA schema_version").fetchone()[0],
        "slackdump_schema_version": slackdump_version,
        "session_high_water": db.execute("SELECT max(ID) FROM SESSION").fetchone()[0],
        "chunk_high_water": db.execute("SELECT max(ID) FROM CHUNK").fetchone()[0],
        "lineage_manifest": _source_lineage_manifest(db, min_chunk_id=min_chunk_id),
    }


def _assert_source_continuity(
    db: duckdb.DuckDBPyConnection,
    source_db: sqlite3.Connection,
    source: dict[str, Any],
    *,
    source_fingerprint: str,
    allow_lineage_bootstrap: bool = False,
) -> None:
    """Require the current archive to still cover every accepted row.

    A Slackdump database fingerprint legitimately changes as the same SQLite
    file resumes, and row counts legitimately shrink when identical
    lookback duplicates are pruned (dedupe/compaction). Continuity is
    therefore proven by key coverage instead of rowid prefixes: every
    canonical natural key must still exist in the source, and for keys
    re-observed in new chunks the payload must be equal or strictly newer
    (higher CHUNK_ID, which is how legitimate edits arrive). Same-chunk
    payload rewrites and vanished keys fail. High-waters must not regress.
    """

    workspace_id = source["workspace"]["workspace_id"]
    checkpoint = db.execute(
        """
        SELECT source_fingerprint, session_high_water, chunk_high_water,
               lineage_signature, lineage_manifest_json
        FROM ingestion_checkpoints WHERE workspace_id = ?
        """,
        [workspace_id],
    ).fetchone()
    if checkpoint is None:
        return
    prior_fingerprint, prior_session, prior_chunk, prior_signature, prior_json = checkpoint
    if not prior_signature or not prior_json:
        if allow_lineage_bootstrap:
            return
        raise SourceContinuityError(
            "Existing checkpoint lacks source lineage evidence; refusing unsafe replacement"
        )
    try:
        prior_manifest = json.loads(prior_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise SourceContinuityError("Checkpoint source lineage evidence is invalid") from error
    if (
        not _valid_lineage_manifest(prior_manifest)
        or _lineage_signature(prior_manifest) != prior_signature
    ):
        raise SourceContinuityError("Checkpoint source lineage signature is invalid")

    canonical_messages = {
        (row[0], row[1]): (row[2], row[3])
        for row in db.execute(
            """
            SELECT channel_id, ts, source_chunk_id, payload_hash FROM messages
            WHERE workspace_id = ?
            """,
            [workspace_id],
        ).fetchall()
    }
    scoped_newest = {
        (message["channel_id"], message["ts"]): message
        for message in source["messages"]
    }
    source_keys = {
        (row[0], row[1])
        for row in source_db.execute("SELECT CHANNEL_ID, TS FROM MESSAGE").fetchall()
    }
    missing = sorted(set(canonical_messages) - source_keys)
    if missing:
        sample = ", ".join(f"{channel_id}/{ts}" for channel_id, ts in missing[:5])
        raise SourceContinuityError(
            f"Slackdump source no longer contains {len(missing)} canonical "
            f"message(s) (e.g. {sample})"
        )
    for key in sorted(set(canonical_messages) & set(scoped_newest)):
        canonical_chunk, canonical_hash = canonical_messages[key]
        message = scoped_newest[key]
        if message["source_chunk_id"] < canonical_chunk:
            raise SourceContinuityError(
                f"Slackdump source regressed canonical message {key[0]}/{key[1]} "
                f"to an older chunk"
            )
        if (
            message["source_chunk_id"] == canonical_chunk
            and message["payload_hash"] != canonical_hash
        ):
            raise SourceContinuityError(
                f"Slackdump source rewrote canonical message {key[0]}/{key[1]} "
                f"inside its own chunk"
            )
    for table, column, canonical_table, canonical_column in (
        ("CHANNEL", "ID", "channels", "channel_id"),
        ("S_USER", "ID", "users", "user_id"),
    ):
        source_ids = {
            row[0]
            for row in source_db.execute(f'SELECT DISTINCT "{column}" FROM "{table}"').fetchall()
        }
        canonical_ids = {
            row[0]
            for row in db.execute(
                f'SELECT DISTINCT "{canonical_column}" FROM "{canonical_table}" '
                "WHERE workspace_id = ?",
                [workspace_id],
            ).fetchall()
        }
        vanished = sorted(set(canonical_ids) - source_ids)
        if vanished:
            raise SourceContinuityError(
                f"Slackdump source table {table} no longer contains "
                f"{len(vanished)} canonical row(s) (e.g. {', '.join(vanished[:5])})"
            )
    if _less_than(source["session_high_water"], prior_session):
        raise SourceContinuityError("Slackdump session high-water regressed")
    if _less_than(source["chunk_high_water"], prior_chunk):
        raise SourceContinuityError("Slackdump chunk high-water regressed")


def _apply_source(
    db: duckdb.DuckDBPyConnection,
    source: dict[str, Any],
    *,
    run_id: str,
    workspace_slug: str,
    source_path: str,
    source_fingerprint: str,
    status: Callable[[str], None] | None = None,
) -> IngestionResult:
    report = status or (lambda _message: None)
    now = _now()
    workspace = source["workspace"]
    workspace_id = workspace["workspace_id"]
    _upsert_workspace(db, workspace, workspace_slug, run_id, now)
    _upsert_channels(db, workspace_id, source["channels"], run_id, now)
    _upsert_users(db, workspace_id, source["users"], run_id, now)
    report("canonical users and channels staged")
    _replace_memberships(db, workspace_id, source["channel_members"], run_id)
    report("canonical memberships staged")

    inserted = updated = unchanged = 0
    classified_messages: list[tuple[dict[str, Any], bool, str | None]] = []
    existing_hashes = {
        (channel_id, ts): payload_hash
        for channel_id, ts, payload_hash in db.execute(
            """
            SELECT channel_id, ts, payload_hash FROM messages
            WHERE workspace_id = ?
            """,
            [workspace_id],
        ).fetchall()
    }
    for message in source["messages"]:
        existing_hash = existing_hashes.get((message["channel_id"], message["ts"]))
        if existing_hash is None:
            inserted += 1
            event_type = "observed_deleted" if message["is_deleted"] else "created"
            changed = True
        elif existing_hash != message["payload_hash"]:
            updated += 1
            event_type = "observed_deleted" if message["is_deleted"] else "edited"
            changed = True
        else:
            unchanged += 1
            changed = False
        classified_messages.append((message, changed, event_type if changed else None))

    _upsert_messages(
        db, workspace_id, classified_messages, run_id=run_id, now=now, status=report
    )
    _replace_all_reactions(db, workspace_id, source["messages"], run_id)
    _insert_message_events(
        db, workspace_id, classified_messages, run_id=run_id, now=now
    )

    _upsert_files(db, workspace_id, source["files"], run_id, now)
    _record_attachment_paths(db, workspace_id, Path(source_path).parent)
    report("canonical messages, reactions, events, and files staged")
    canonical_rows = db.execute(
        "SELECT count(*) FROM messages WHERE workspace_id = ?", [workspace_id]
    ).fetchone()[0]
    db.execute(
        """
        INSERT INTO source_archives VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT (workspace_id, source_fingerprint) DO UPDATE SET
            source_path = excluded.source_path,
            latest_run_id = excluded.latest_run_id,
            last_ingested_at = excluded.last_ingested_at,
            ingest_count = source_archives.ingest_count + 1
        """,
        [
            workspace_id, source_fingerprint, source_path,
            source["sqlite_schema_version"], source["slackdump_schema_version"],
            source["session_high_water"], source["chunk_high_water"],
            source["source_message_rows"], _json_dump(source["table_counts"]),
            run_id, run_id, now, now,
        ],
    )
    db.execute(
        """
        INSERT INTO ingestion_checkpoints (
            workspace_id, source_fingerprint, session_high_water, chunk_high_water,
            lineage_signature, lineage_manifest_json, successful_run_id, checkpointed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (workspace_id) DO UPDATE SET
            source_fingerprint = excluded.source_fingerprint,
            session_high_water = excluded.session_high_water,
            chunk_high_water = excluded.chunk_high_water,
            lineage_signature = excluded.lineage_signature,
            lineage_manifest_json = excluded.lineage_manifest_json,
            successful_run_id = excluded.successful_run_id,
            checkpointed_at = excluded.checkpointed_at
        """,
        [
            workspace_id, source_fingerprint, source["session_high_water"],
            source["chunk_high_water"],
            _lineage_signature(source["lineage_manifest"]),
            _json_dump(source["lineage_manifest"]),
            run_id, now,
        ],
    )
    return IngestionResult(
        run_id=run_id,
        workspace_id=workspace_id,
        source_rows=source["source_message_rows"],
        canonical_rows=canonical_rows,
        inserted_messages=inserted,
        updated_messages=updated,
        unchanged_messages=unchanged,
    )


def _upsert_workspace(db: duckdb.DuckDBPyConnection, row: dict[str, Any], slug: str, run_id: str, now: datetime) -> None:
    db.execute(
        """
        INSERT INTO workspaces VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (workspace_id) DO UPDATE SET
            workspace_slug = excluded.workspace_slug, team_name = excluded.team_name,
            workspace_url = excluded.workspace_url, enterprise_id = excluded.enterprise_id,
            raw_json = excluded.raw_json, source_chunk_id = excluded.source_chunk_id,
            latest_run_id = excluded.latest_run_id, last_observed_at = excluded.last_observed_at
        """,
        [row["workspace_id"], slug, row["team_name"], row["workspace_url"], row["enterprise_id"], row["raw_json"], row["source_chunk_id"], run_id, now, now],
    )


def _upsert_channels(db: duckdb.DuckDBPyConnection, workspace_id: str, rows: Iterable[dict[str, Any]], run_id: str, now: datetime) -> None:
    for row in rows:
        db.execute(
            """
            INSERT INTO channels VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (workspace_id, channel_id) DO UPDATE SET
                name = excluded.name, is_private = excluded.is_private,
                is_archived = excluded.is_archived, topic = excluded.topic,
                purpose = excluded.purpose, raw_json = excluded.raw_json,
                source_chunk_id = excluded.source_chunk_id,
                latest_run_id = excluded.latest_run_id,
                last_observed_at = excluded.last_observed_at
            """,
            [workspace_id, row["channel_id"], row["name"], row["is_private"], row["is_archived"], row["topic"], row["purpose"], row["raw_json"], row["source_chunk_id"], run_id, now, now],
        )


def _upsert_users(db: duckdb.DuckDBPyConnection, workspace_id: str, rows: Iterable[dict[str, Any]], run_id: str, now: datetime) -> None:
    rows = list(rows)
    if not rows:
        return
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE incoming_users (
            workspace_id VARCHAR, user_id VARCHAR, username VARCHAR,
            real_name VARCHAR, display_name VARCHAR, is_bot BOOLEAN,
            is_deleted BOOLEAN, is_restricted BOOLEAN, profile_json VARCHAR,
            raw_json VARCHAR, source_chunk_id BIGINT
        )
        """
    )
    _insert_temp_rows(
        db,
        "incoming_users",
        [
            [
                workspace_id,
                row["user_id"],
                row["username"],
                row["real_name"],
                row["display_name"],
                row["is_bot"],
                row["is_deleted"],
                row["is_restricted"],
                row["profile_json"],
                row["raw_json"],
                row["source_chunk_id"],
            ]
            for row in rows
        ],
    )
    db.execute(
        """
        INSERT INTO users
        SELECT incoming_users.*, ?, ?, ? FROM incoming_users
        ON CONFLICT (workspace_id, user_id) DO UPDATE SET
            username = excluded.username, real_name = excluded.real_name,
            display_name = excluded.display_name, is_bot = excluded.is_bot,
            is_deleted = excluded.is_deleted, is_restricted = excluded.is_restricted,
            profile_json = excluded.profile_json, raw_json = excluded.raw_json,
            source_chunk_id = excluded.source_chunk_id,
            latest_run_id = excluded.latest_run_id,
            last_observed_at = excluded.last_observed_at
        """,
        [run_id, now, now],
    )


def _replace_memberships(db: duckdb.DuckDBPyConnection, workspace_id: str, rows: list[dict[str, Any]], run_id: str) -> None:
    if not rows:
        return
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE incoming_channel_members (
            workspace_id VARCHAR, channel_id VARCHAR, user_id VARCHAR,
            source_chunk_id BIGINT
        )
        """
    )
    _insert_temp_rows(
        db,
        "incoming_channel_members",
        [
            [workspace_id, row["channel_id"], row["user_id"], row["source_chunk_id"]]
            for row in rows
        ],
        batch_size=1_000,
    )
    db.execute(
        """
        DELETE FROM channel_members existing
        WHERE existing.workspace_id = ?
          AND existing.channel_id IN (
              SELECT DISTINCT channel_id FROM incoming_channel_members
          )
          AND NOT EXISTS (
              SELECT 1 FROM incoming_channel_members incoming
              WHERE incoming.workspace_id = existing.workspace_id
                AND incoming.channel_id = existing.channel_id
                AND incoming.user_id = existing.user_id
          )
        """,
        [workspace_id],
    )
    db.execute(
        """
        INSERT INTO channel_members
        SELECT workspace_id, channel_id, user_id, source_chunk_id, ?, ?
        FROM incoming_channel_members
        ON CONFLICT (workspace_id, channel_id, user_id) DO UPDATE SET
            source_chunk_id = excluded.source_chunk_id,
            last_observed_run_id = excluded.last_observed_run_id
        """,
        [run_id, run_id],
    )


def _insert_temp_rows(
    db: duckdb.DuckDBPyConnection,
    table: str,
    rows: list[list[Any]],
    *,
    batch_size: int = 500,
) -> None:
    """Insert into an internal temporary table with bounded statement counts."""

    if not rows:
        return
    width = len(rows[0])
    placeholder = f"({', '.join('?' for _ in range(width))})"
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        parameters = [value for row in batch for value in row]
        db.execute(
            f"INSERT INTO {table} VALUES {', '.join(placeholder for _ in batch)}",
            parameters,
        )


def _upsert_message(db: duckdb.DuckDBPyConnection, workspace_id: str, row: dict[str, Any], run_id: str, now: datetime, changed: bool) -> None:
    db.execute(
        """
        INSERT INTO messages VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        ON CONFLICT (workspace_id, channel_id, ts) DO UPDATE SET
            source_message_id = excluded.source_message_id,
            source_chunk_id = excluded.source_chunk_id, user_id = excluded.user_id,
            subtype = excluded.subtype, text = excluded.text,
            search_text = excluded.search_text, thread_ts = excluded.thread_ts,
            parent_ts = excluded.parent_ts, latest_reply_ts = excluded.latest_reply_ts,
            is_thread_parent = excluded.is_thread_parent,
            is_thread_reply = excluded.is_thread_reply,
            is_thread_broadcast = excluded.is_thread_broadcast,
            is_edited = excluded.is_edited, edited_ts = excluded.edited_ts,
            is_deleted = excluded.is_deleted, deleted_ts = excluded.deleted_ts,
            file_metadata_json = excluded.file_metadata_json,
            raw_json = excluded.raw_json, payload_hash = excluded.payload_hash,
            latest_run_id = excluded.latest_run_id,
            last_changed_at = CASE WHEN ? THEN excluded.last_changed_at ELSE messages.last_changed_at END,
            last_observed_at = excluded.last_observed_at
        """,
        [
            row["message_key"], workspace_id, row["channel_id"], row["ts"], row["ts_us"],
            row["source_message_id"], row["source_chunk_id"], row["user_id"], row["subtype"],
            row["text"], row["search_text"], row["thread_ts"], row["parent_ts"],
            row["latest_reply_ts"], row["is_thread_parent"], row["is_thread_reply"],
            row["is_thread_broadcast"], row["is_edited"], row["edited_ts"],
            row["is_deleted"], row["deleted_ts"], row["file_metadata_json"], row["raw_json"],
            row["payload_hash"], run_id, now, now, now, changed,
        ],
    )


def _upsert_messages(
    db: duckdb.DuckDBPyConnection,
    workspace_id: str,
    rows: list[tuple[dict[str, Any], bool, str | None]],
    *,
    run_id: str,
    now: datetime,
    status: Callable[[str], None],
) -> None:
    """Stage once, then update/insert messages without repeated conflict plans."""

    if not rows:
        return
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE incoming_messages AS
        SELECT messages.*, false AS changed FROM messages WITH NO DATA
        """
    )
    values = [
        [
            message["message_key"], workspace_id, message["channel_id"], message["ts"],
            message["ts_us"], message["source_message_id"], message["source_chunk_id"],
            message["user_id"], message["subtype"], message["text"],
            message["search_text"], message["thread_ts"], message["parent_ts"],
            message["latest_reply_ts"], message["is_thread_parent"],
            message["is_thread_reply"], message["is_thread_broadcast"],
            message["is_edited"], message["edited_ts"], message["is_deleted"],
            message["deleted_ts"], message["file_metadata_json"], message["raw_json"],
            message["payload_hash"], run_id, now, now, now, changed,
        ]
        for message, changed, _event_type in rows
    ]
    for start in range(0, len(values), 100):
        _insert_temp_rows(
            db,
            "incoming_messages",
            values[start : start + 100],
            batch_size=100,
        )
        status(f"canonical messages staged={min(start + 100, len(values))}")
    db.execute(
        """
        UPDATE messages existing SET
            source_message_id = incoming.source_message_id,
            source_chunk_id = incoming.source_chunk_id,
            user_id = incoming.user_id, subtype = incoming.subtype,
            text = incoming.text, search_text = incoming.search_text,
            thread_ts = incoming.thread_ts, parent_ts = incoming.parent_ts,
            latest_reply_ts = incoming.latest_reply_ts,
            is_thread_parent = incoming.is_thread_parent,
            is_thread_reply = incoming.is_thread_reply,
            is_thread_broadcast = incoming.is_thread_broadcast,
            is_edited = incoming.is_edited, edited_ts = incoming.edited_ts,
            is_deleted = incoming.is_deleted, deleted_ts = incoming.deleted_ts,
            file_metadata_json = incoming.file_metadata_json,
            raw_json = incoming.raw_json, payload_hash = incoming.payload_hash,
            latest_run_id = incoming.latest_run_id,
            last_changed_at = CASE
                WHEN incoming.changed THEN incoming.last_changed_at
                ELSE existing.last_changed_at
            END,
            last_observed_at = incoming.last_observed_at
        FROM incoming_messages incoming
        WHERE existing.workspace_id = incoming.workspace_id
          AND existing.channel_id = incoming.channel_id
          AND existing.ts = incoming.ts
        """
    )
    db.execute(
        """
        INSERT INTO messages
        SELECT incoming.* EXCLUDE (changed) FROM incoming_messages incoming
        WHERE NOT EXISTS (
            SELECT 1 FROM messages existing
            WHERE existing.workspace_id = incoming.workspace_id
              AND existing.channel_id = incoming.channel_id
              AND existing.ts = incoming.ts
        )
        """
    )
    status(f"canonical messages applied={len(values)}")


def _replace_all_reactions(
    db: duckdb.DuckDBPyConnection,
    workspace_id: str,
    messages: list[dict[str, Any]],
    run_id: str,
) -> None:
    if not messages:
        return
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE incoming_message_keys (
            workspace_id VARCHAR, channel_id VARCHAR, message_ts VARCHAR
        )
        """
    )
    _insert_temp_rows(
        db,
        "incoming_message_keys",
        [[workspace_id, row["channel_id"], row["ts"]] for row in messages],
    )
    db.execute(
        """
        DELETE FROM reactions existing
        WHERE existing.workspace_id = ? AND EXISTS (
            SELECT 1 FROM incoming_message_keys incoming
            WHERE incoming.workspace_id = existing.workspace_id
              AND incoming.channel_id = existing.channel_id
              AND incoming.message_ts = existing.message_ts
        )
        """,
        [workspace_id],
    )
    reaction_rows = [
        [
            workspace_id,
            message["channel_id"],
            message["ts"],
            reaction["emoji_name"],
            reaction["user_id"],
            reaction["reported_count"],
            run_id,
        ]
        for message in messages
        for reaction in message["reactions"]
    ]
    if reaction_rows:
        db.execute(
            """
            CREATE OR REPLACE TEMP TABLE incoming_reactions AS
            SELECT * FROM reactions WITH NO DATA
            """
        )
        _insert_temp_rows(db, "incoming_reactions", reaction_rows)
        db.execute("INSERT INTO reactions SELECT * FROM incoming_reactions")


def _insert_message_events(
    db: duckdb.DuckDBPyConnection,
    workspace_id: str,
    rows: list[tuple[dict[str, Any], bool, str | None]],
    *,
    run_id: str,
    now: datetime,
) -> None:
    event_rows = [
        [
            str(uuid.uuid4()),
            workspace_id,
            message["channel_id"],
            message["ts"],
            message["message_key"],
            event_type,
            message["payload_hash"],
            message["raw_json"],
            run_id,
            now,
        ]
        for message, changed, event_type in rows
        if changed
    ]
    if event_rows:
        _insert_temp_rows(db, "message_events", event_rows, batch_size=100)


def _replace_reactions(db: duckdb.DuckDBPyConnection, workspace_id: str, message: dict[str, Any], run_id: str) -> None:
    db.execute(
        "DELETE FROM reactions WHERE workspace_id = ? AND channel_id = ? AND message_ts = ?",
        [workspace_id, message["channel_id"], message["ts"]],
    )
    for reaction in message["reactions"]:
        db.execute(
            "INSERT INTO reactions VALUES (?, ?, ?, ?, ?, ?, ?)",
            [workspace_id, message["channel_id"], message["ts"], reaction["emoji_name"], reaction["user_id"], reaction["reported_count"], run_id],
        )


def _upsert_files(db: duckdb.DuckDBPyConnection, workspace_id: str, rows: Iterable[dict[str, Any]], run_id: str, now: datetime) -> None:
    rows = list(rows)
    if not rows:
        return
    db.execute(
        "CREATE OR REPLACE TEMP TABLE incoming_files AS SELECT * FROM files WITH NO DATA"
    )
    _insert_temp_rows(
        db,
        "incoming_files",
        [
            [
                workspace_id, row["file_id"], row["channel_id"], row["message_ts"],
                row["thread_ts"], row["user_id"], row["mode"], row["filename"],
                row["mime_type"], row["size_bytes"], row["source_url"], None,
                row["raw_json"], row["source_chunk_id"], run_id, now, now,
            ]
            for row in rows
        ],
        batch_size=100,
    )
    db.execute(
        """
        UPDATE files existing SET
            channel_id = incoming.channel_id, message_ts = incoming.message_ts,
            thread_ts = incoming.thread_ts, user_id = incoming.user_id,
            mode = incoming.mode, filename = incoming.filename,
            mime_type = incoming.mime_type, size_bytes = incoming.size_bytes,
            source_url = incoming.source_url, raw_json = incoming.raw_json,
            source_chunk_id = incoming.source_chunk_id,
            latest_run_id = incoming.latest_run_id,
            last_observed_at = incoming.last_observed_at
        FROM incoming_files incoming
        WHERE existing.workspace_id = incoming.workspace_id
          AND existing.file_id = incoming.file_id
        """
    )
    db.execute(
        """
        INSERT INTO files SELECT incoming.* FROM incoming_files incoming
        WHERE NOT EXISTS (
            SELECT 1 FROM files existing
            WHERE existing.workspace_id = incoming.workspace_id
              AND existing.file_id = incoming.file_id
        )
        """
    )


def _record_attachment_paths(
    db: duckdb.DuckDBPyConnection, workspace_id: str, archive_dir: Path
) -> int:
    """Record local paths for downloaded attachments (workspace-wide).

    Attachments run before canonicalization, so files downloaded by any
    earlier backfill — including ones whose messages were not re-observed
    in this ingest — get their present paths recorded. Missing files keep
    NULL, which is exactly what the attachment-backlog metric counts.
    """
    uploads = Path(archive_dir) / "__uploads"
    if not uploads.is_dir():
        return 0
    present: list[tuple[str, str, str]] = []
    for file_dir in sorted(uploads.iterdir()):
        if not file_dir.is_dir() or file_dir.is_symlink():
            continue
        for child in sorted(file_dir.iterdir()):
            if child.is_file() and not child.is_symlink():
                present.append(
                    (
                        f"__uploads/{file_dir.name}/{child.name}",
                        workspace_id,
                        file_dir.name,
                    )
                )
    if not present:
        return 0
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE incoming_attachment_paths (
            local_object_path VARCHAR, workspace_id VARCHAR, file_id VARCHAR
        )
        """
    )
    _insert_temp_rows(db, "incoming_attachment_paths", [list(row) for row in present])
    db.execute(
        """
        UPDATE files existing SET local_object_path = incoming.local_object_path
        FROM incoming_attachment_paths incoming
        WHERE existing.workspace_id = incoming.workspace_id
          AND existing.file_id = incoming.file_id
          AND existing.local_object_path IS DISTINCT FROM incoming.local_object_path
        """
    )
    return db.execute(
        """
        SELECT count(*) FROM files existing
        WHERE existing.workspace_id = ?
          AND existing.local_object_path IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM incoming_attachment_paths incoming
              WHERE incoming.workspace_id = existing.workspace_id
                AND incoming.file_id = existing.file_id
          )
        """,
        [workspace_id],
    ).fetchone()[0]


def _newest_rows(db: sqlite3.Connection, table: str, keys: list[str]) -> list[sqlite3.Row]:
    partition = ", ".join(f'"{key}"' for key in keys)
    query = f"""
        SELECT * FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {partition} ORDER BY CHUNK_ID DESC, IDX DESC, rowid DESC
            ) AS _newest_rank
            FROM "{table}"
        ) WHERE _newest_rank = 1
    """
    return db.execute(query).fetchall()


def _touched_keys(
    db: sqlite3.Connection, table: str, keys: list[str], min_chunk_id: int
) -> list[tuple[str, ...]]:
    """Natural keys with at least one row in chunks newer than a checkpoint.

    Resume only appends new chunks, so these are the only keys whose
    newest row can have changed. Everything else is immutable history
    (dedupe/compaction only remove identical copies of kept rows) and
    needs no re-read.
    """
    columns = ", ".join(f'"{key}"' for key in keys)
    return [
        tuple(str(value) for value in row)
        for row in db.execute(
            f'SELECT DISTINCT {columns} FROM "{table}" WHERE CHUNK_ID > ?',
            [min_chunk_id],
        ).fetchall()
    ]


def _newest_rows_for_keys(
    db: sqlite3.Connection,
    table: str,
    keys: list[str],
    key_values: list[tuple[str, ...]],
) -> list[sqlite3.Row]:
    """Newest-per-key rows over all chunks, restricted to touched keys.

    Unlike a chunk-filtered newest selection (which would downgrade keys
    whose newest row sits in an older chunk), this always returns the
    true global newest row for each touched key.
    """
    if not key_values:
        return []
    columns = ", ".join(f'"{key}"' for key in keys)
    placeholders = ", ".join(["?"] * len(keys))
    flat = [value for key in key_values for value in key]
    return db.execute(
        f"""
        SELECT * FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {columns} ORDER BY CHUNK_ID DESC, IDX DESC, rowid DESC
            ) AS _newest_rank
            FROM "{table}"
            WHERE ({columns}) IN (VALUES {", ".join([f"({placeholders})"] * len(key_values))})
        ) WHERE _newest_rank = 1
        """,
        flat,
    ).fetchall()


def _newest_memberships(
    db: sqlite3.Connection, *, channel_ids: list[str] | None = None
) -> list[dict[str, Any]]:
    """Latest-chunk membership snapshot, optionally restricted to channels.

    Restricting is safe for chunk-scoped ingests: replacement is scoped to
    observed channels, so untouched channels keep their canonical state.
    """
    if channel_ids is not None and not channel_ids:
        return []
    scope = ""
    parameters: list[str] = []
    if channel_ids is not None:
        scope = f"WHERE CHANNEL_ID IN ({', '.join(['?'] * len(channel_ids))})"
        parameters = list(channel_ids)
    rows = db.execute(
        f"""
        WITH latest AS (
            SELECT CHANNEL_ID, max(CHUNK_ID) AS CHUNK_ID
            FROM CHANNEL_USER {scope}
            GROUP BY CHANNEL_ID
        )
        SELECT membership.CHANNEL_ID, membership.USER_ID, membership.CHUNK_ID
        FROM CHANNEL_USER membership
        JOIN latest USING (CHANNEL_ID, CHUNK_ID)
        {scope.replace('CHANNEL_ID', 'membership.CHANNEL_ID') if scope else ''}
        GROUP BY membership.CHANNEL_ID, membership.USER_ID, membership.CHUNK_ID
        """,
        parameters + parameters if parameters else [],
    ).fetchall()
    return [
        {
            "channel_id": row["CHANNEL_ID"],
            "user_id": row["USER_ID"],
            "source_chunk_id": row["CHUNK_ID"],
        }
        for row in rows
    ]


def _message(row: sqlite3.Row, workspace_id: str) -> dict[str, Any]:
    payload, raw = _payload(row["DATA"], f"MESSAGE {row['CHANNEL_ID']} {row['TS']}")
    effective = payload.get("message") if isinstance(payload.get("message"), dict) else payload
    subtype = payload.get("subtype") or effective.get("subtype")
    text = row["TXT"] if row["TXT"] is not None else effective.get("text")
    text = text or ""
    thread_ts = row["THREAD_TS"] or effective.get("thread_ts")
    is_parent = _bool(row["IS_PARENT"]) or (thread_ts == row["TS"] and bool(effective.get("reply_count")))
    is_reply = bool(thread_ts and thread_ts != row["TS"])
    edited = effective.get("edited") if isinstance(effective.get("edited"), dict) else {}
    is_deleted = (
        subtype in {"message_deleted", "tombstone"}
        or bool(payload.get("deleted_ts"))
        or _bool(effective.get("deleted"))
    )
    files = effective.get("files") if isinstance(effective.get("files"), list) else []
    reactions = []
    for reaction in effective.get("reactions") or []:
        if not isinstance(reaction, dict) or not reaction.get("name"):
            continue
        for user_id in reaction.get("users") or []:
            reactions.append({"emoji_name": reaction["name"], "user_id": user_id, "reported_count": reaction.get("count")})
    return {
        "message_key": _stable_key(workspace_id, row["CHANNEL_ID"], row["TS"]),
        "channel_id": row["CHANNEL_ID"],
        "ts": row["TS"],
        "ts_us": _ts_us(row["TS"]),
        "source_message_id": row["ID"],
        "source_chunk_id": row["CHUNK_ID"],
        "user_id": effective.get("user") or payload.get("user"),
        "subtype": subtype,
        "text": text,
        "search_text": "" if is_deleted else text.strip(),
        "thread_ts": thread_ts,
        "parent_ts": thread_ts if is_reply else None,
        "latest_reply_ts": row["LATEST_REPLY"] or effective.get("latest_reply"),
        "is_thread_parent": is_parent,
        "is_thread_reply": is_reply,
        "is_thread_broadcast": subtype == "thread_broadcast",
        "is_edited": bool(edited) or subtype == "message_changed",
        "edited_ts": edited.get("ts"),
        "is_deleted": is_deleted,
        "deleted_ts": payload.get("deleted_ts") or effective.get("deleted_ts"),
        "file_metadata_json": _json_dump(files) if files else None,
        "embedded_files": files,
        "reactions": reactions,
        "raw_json": raw,
        "payload_hash": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    }


def _read_files(db: sqlite3.Connection, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    messages_by_id = {message["source_message_id"]: message for message in messages}
    files: dict[str, dict[str, Any]] = {}
    for row in _newest_rows(db, "FILE", ["ID"]):
        payload, raw = _payload(row["DATA"], f"FILE {row['ID']}")
        message = messages_by_id.get(row["MESSAGE_ID"])
        files[row["ID"]] = _file_record(
            payload,
            raw,
            file_id=row["ID"],
            channel_id=row["CHANNEL_ID"],
            message_ts=message["ts"] if message else _id_to_ts(row["MESSAGE_ID"]),
            thread_ts=_id_to_ts(row["THREAD_ID"]),
            source_chunk_id=row["CHUNK_ID"],
            mode=row["MODE"],
            filename=row["FILENAME"],
            source_url=row["URL"],
            size_bytes=row["SIZE"],
        )
    for message in messages:
        for payload in message["embedded_files"]:
            if not isinstance(payload, dict) or not payload.get("id") or payload["id"] in files:
                continue
            raw = _json_dump(payload)
            files[payload["id"]] = _file_record(
                payload, raw, file_id=payload["id"], channel_id=message["channel_id"],
                message_ts=message["ts"], thread_ts=message["thread_ts"],
                source_chunk_id=message["source_chunk_id"], mode=payload.get("mode"),
                filename=payload.get("name"),
                source_url=payload.get("url_private_download") or payload.get("url_private"),
                size_bytes=payload.get("size"),
            )
    return list(files.values())


def _file_record(payload: dict[str, Any], raw: str, **source: Any) -> dict[str, Any]:
    return {
        **source,
        "user_id": payload.get("user"),
        "mode": source.get("mode") or payload.get("mode"),
        "filename": source.get("filename") or payload.get("name"),
        "mime_type": payload.get("mimetype"),
        "size_bytes": source.get("size_bytes") or payload.get("size"),
        "source_url": source.get("source_url") or payload.get("url_private_download") or payload.get("url_private"),
        "raw_json": raw,
    }


def _payload(value: Any, context: str) -> tuple[dict[str, Any], str]:
    raw = bytes(value).decode("utf-8") if isinstance(value, (bytes, bytearray, memoryview)) else str(value)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid JSON in {context}: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {context}")
    return payload, raw


def _raw_text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _is_json_object(value: Any) -> bool:
    try:
        return isinstance(json.loads(_raw_text(value)), dict)
    except json.JSONDecodeError:
        return False


def _latest_slack_timestamp(rows: Iterable[tuple[str, str]]) -> str | None:
    timestamps = [key[1] for key in rows]
    return max(timestamps, key=_ts_us) if timestamps else None


def _source_lineage_manifest(
    db: sqlite3.Connection, *, min_chunk_id: int | None = None
) -> dict[str, Any]:
    """Content digest per required table, optionally scoped to new chunks.

    The manifest is tamper-evidence for the checkpoint plus observability;
    nothing compares manifest content across runs anymore (continuity is
    proven by key coverage), so scoped manifests only need a scope marker.
    """
    tables: dict[str, dict[str, Any]] = {}
    for table in sorted(REQUIRED_COLUMNS):
        query = f'SELECT rowid, * FROM "{table}"'
        parameters: list[int] = []
        # SESSION and CHUNK are tiny append-only ledgers with no CHUNK_ID;
        # they are always digested whole (high-water monotonicity covers them).
        if min_chunk_id is not None and table not in ("SESSION", "CHUNK"):
            query += " WHERE CHUNK_ID > ?"
            parameters.append(min_chunk_id)
        query += " ORDER BY rowid"
        digest = hashlib.sha256()
        count = 0
        for row in db.execute(query, parameters):
            encoded = _lineage_row(row)
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
            count += 1
        tables[table] = {"row_count": count, "sha256": digest.hexdigest()}
    if min_chunk_id is None:
        return {"version": 1, "tables": tables}
    return {"version": 2, "scope": {"min_chunk_id": min_chunk_id}, "tables": tables}


def _lineage_row(row: Iterable[Any]) -> bytes:
    values = []
    for value in row:
        if value is None:
            values.append(["null", None])
        elif isinstance(value, (bytes, bytearray, memoryview)):
            values.append(["blob", bytes(value).hex()])
        elif isinstance(value, int):
            values.append(["integer", str(value)])
        elif isinstance(value, float):
            values.append(["real", value.hex()])
        else:
            values.append(["text", str(value)])
    return _json_dump(values).encode("utf-8")


def _lineage_signature(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(_json_dump(manifest).encode("utf-8")).hexdigest()


def _valid_lineage_manifest(value: Any) -> bool:
    if not isinstance(value, dict) or value.get("version") not in (1, 2):
        return False
    if value["version"] == 2 and not isinstance(value.get("scope"), dict):
        return False
    tables = value.get("tables")
    if not isinstance(tables, dict) or set(tables) != set(REQUIRED_COLUMNS):
        return False
    return all(
        isinstance(entry, dict)
        and isinstance(entry.get("row_count"), int)
        and entry["row_count"] >= 0
        and isinstance(entry.get("sha256"), str)
        and len(entry["sha256"]) == 64
        for entry in tables.values()
    )


def _less_than(value: int | None, checkpoint: int | None) -> bool:
    return checkpoint is not None and (value is None or value < checkpoint)


def _nested_value(payload: dict[str, Any], parent: str, child: str) -> Any:
    value = payload.get(parent)
    return value.get(child) if isinstance(value, dict) else None


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _stable_key(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _ts_us(ts: str) -> int:
    seconds, separator, fraction = ts.partition(".")
    if not seconds.isdigit() or (separator and not fraction.isdigit()):
        raise ValueError(f"Invalid Slack timestamp: {ts!r}")
    return int(seconds) * 1_000_000 + int((fraction + "000000")[:6])


def _id_to_ts(value: int | None) -> str | None:
    if value is None:
        return None
    digits = str(value)
    if len(digits) <= 6:
        return f"0.{digits.zfill(6)}"
    return f"{digits[:-6]}.{digits[-6:]}"


def _bool(value: Any) -> bool:
    return bool(value)


def _has_table(db: sqlite3.Connection, table: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?)",
        [table],
    ).fetchone() is not None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)
