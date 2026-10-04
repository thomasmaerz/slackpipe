"""Offline Slackdump archive compaction with checkpoint rebase.

Resume lookback overlap appends identical row copies on every pass (up to
90% of MESSAGE rows in the oldest archives). This module rebuilds an
archive keeping only the rows canonicalization can ever select, so the
compacted file is smaller but semantically identical:

- schema (tables, indexes, views, triggers), ``goose_db_version`` rows,
  ``TYPES``/``SEARCH_*``/``ALIAS`` tables: copied verbatim;
- ``SESSION``/``CHUNK``: all rows kept, so max IDs (high-waters) and
  future resume appends (``max(ID) + 1``) are unaffected;
- entity tables: newest-per-key rows only, with original ``CHUNK_ID`` /
  ``IDX`` / payload values, so newest-selection, chunk-match validation,
  and key coverage are bit-identical;
- ``CHANNEL_USER``: latest-chunk rows per channel (mirrors the membership
  snapshot semantics);
- ``WORKSPACE``: newest row only.

Because rowids are renumbered, the compacted file always needs an
explicit checkpoint rebase (``rebase_checkpoint``), allowed only after
``verify_compaction`` proves every canonical key still resolves to the
same payload and chunk.
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path


ENTITY_NEWEST_KEYS: dict[str, list[str]] = {
    "MESSAGE": ["CHANNEL_ID", "TS"],
    "CHANNEL": ["ID"],
    "S_USER": ["ID"],
}

#: Tables rebuilt by key selection (plus WORKSPACE newest-only,
#: CHANNEL_USER latest-per-channel, FILE linked-to-kept-messages).
MANAGED_TABLES = frozenset(
    [*ENTITY_NEWEST_KEYS, "WORKSPACE", "CHANNEL_USER", "FILE", "sqlite_sequence"]
)


def _json_dump(value: object) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def compact_archive(source: str | Path, destination: str | Path) -> dict[str, int]:
    """Rebuild a smaller equivalent archive; return per-table kept counts."""
    src = Path(source).expanduser().resolve(strict=True)
    dst = Path(destination).expanduser().resolve()
    if dst.exists():
        raise ValueError(f"compaction destination already exists: {dst}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    kept: dict[str, int] = {}
    with sqlite3.connect(f"{src.as_uri()}?mode=ro", uri=True) as sdb:
        sdb.execute("PRAGMA query_only=ON")
        schema_objects = [
            row
            for row in sdb.execute(
                """
                SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL
                ORDER BY CASE type
                             WHEN 'table' THEN 0
                             ELSE 1
                         END, name
                """
            ).fetchall()
            if row[0] != "sqlite_sequence"
        ]
        managed_or_internal = set(MANAGED_TABLES) | {"sqlite_sequence"}
        verbatim = [
            row[0]
            for row in sdb.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            if row[0] not in managed_or_internal
        ]
        with sqlite3.connect(dst) as ddb:
            ddb.execute("ATTACH DATABASE ? AS attached", [str(src)])
            # Match slackdump-created archives: WAL mode lets readers
            # (verification, snapshots) coexist with a writer instead of
            # hitting SQLITE_BUSY on a rollback-journal file. First
            # statement on the fresh file, so no transaction is open.
            ddb.execute("PRAGMA journal_mode=WAL")
            for _name, statement in schema_objects:
                ddb.execute(statement)
            for table in verbatim:
                kept[table] = ddb.execute(
                    f'INSERT INTO "{table}" SELECT * FROM attached."{table}"'
                ).rowcount
            for table, keys in ENTITY_NEWEST_KEYS.items():
                partition = ", ".join(f'"{key}"' for key in keys)
                columns = ", ".join(
                    f'"{row[1]}"'
                    for row in sdb.execute(
                        f'PRAGMA table_info("{table}")'
                    ).fetchall()
                )
                kept[table] = ddb.execute(
                    f"""
                    INSERT INTO "{table}" ({columns})
                    SELECT {columns} FROM (
                        SELECT kept_src.*,
                               row_number() OVER (
                                   PARTITION BY {partition}
                                   ORDER BY CHUNK_ID DESC, IDX DESC, rowid DESC
                               ) AS _keep_rank
                        FROM attached."{table}" AS kept_src
                    ) WHERE _keep_rank = 1
                    """
                ).rowcount
            kept["WORKSPACE"] = ddb.execute(
                """
                INSERT INTO WORKSPACE
                SELECT * FROM attached.WORKSPACE
                ORDER BY CHUNK_ID DESC, ID DESC LIMIT 1
                """
            ).rowcount
            cu_columns = ", ".join(
                f'"{row[1]}"'
                for row in sdb.execute('PRAGMA table_info("CHANNEL_USER")').fetchall()
            )
            kept["CHANNEL_USER"] = ddb.execute(
                f"""
                INSERT INTO CHANNEL_USER ({cu_columns})
                SELECT {cu_columns} FROM (
                    SELECT kept_src.*,
                           row_number() OVER (
                               PARTITION BY kept_src.CHANNEL_ID, kept_src.USER_ID
                               ORDER BY kept_src.CHUNK_ID DESC, kept_src.rowid DESC
                           ) AS _keep_rank
                    FROM attached.CHANNEL_USER AS kept_src
                    JOIN (
                        SELECT CHANNEL_ID, max(CHUNK_ID) AS _latest_chunk
                        FROM attached.CHANNEL_USER GROUP BY CHANNEL_ID
                    ) latest
                      ON latest.CHANNEL_ID = kept_src.CHANNEL_ID
                     AND latest._latest_chunk = kept_src.CHUNK_ID
                ) WHERE _keep_rank = 1
                """
            ).rowcount
            file_count = ddb.execute(
                "SELECT count(*) FROM attached.FILE"
            ).fetchone()[0]
            if file_count:
                file_columns = ", ".join(
                    f'"{row[1]}"'
                    for row in sdb.execute('PRAGMA table_info("FILE")').fetchall()
                )
                # Newest row per file ID over the whole table: FILE is
                # small next to MESSAGE, and rows linked to superseded
                # message versions are attachment history worth keeping.
                kept["FILE"] = ddb.execute(
                    f"""
                    INSERT INTO FILE ({file_columns})
                    SELECT {file_columns} FROM (
                        SELECT kept_src.*,
                               row_number() OVER (
                                   PARTITION BY ID
                                   ORDER BY CHUNK_ID DESC, IDX DESC, rowid DESC
                               ) AS _keep_rank
                        FROM attached.FILE AS kept_src
                    ) WHERE _keep_rank = 1
                    """
                ).rowcount
            else:
                kept["FILE"] = 0
            for (name, seq) in ddb.execute(
                "SELECT name, seq FROM attached.sqlite_sequence"
            ).fetchall():
                ddb.execute("DELETE FROM sqlite_sequence WHERE name = ?", [name])
                ddb.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                    [name, seq],
                )
            ddb.commit()
            ddb.execute("DETACH DATABASE attached")
    return kept


def verify_compaction(
    original: str | Path, compacted: str | Path, warehouse: str | Path, *, workspace_id: str
) -> dict[str, int]:
    """Prove a compacted archive still covers every canonical row.

    For every canonical message/channel/user key, the compacted newest
    selection must resolve to the same chunk and payload hash. Returns
    row counts ``{original, compacted}`` per entity table. Raises
    ``ValueError`` on the first divergence.
    """
    import duckdb

    orig = Path(original).expanduser().resolve(strict=True)
    comp = Path(compacted).expanduser().resolve(strict=True)
    stats: dict[str, int] = {}
    with (
        sqlite3.connect(f"{orig.as_uri()}?mode=ro", uri=True) as odb,
        sqlite3.connect(f"{comp.as_uri()}?mode=ro", uri=True) as cdb,
        duckdb.connect(str(warehouse), read_only=True) as target,
    ):
        for table, keys, canonical_table, canonical_columns in (
            ("MESSAGE", ["CHANNEL_ID", "TS"], "messages", ["channel_id", "ts"]),
            ("CHANNEL", ["ID"], "channels", ["channel_id"]),
            ("S_USER", ["ID"], "users", ["user_id"]),
            ("FILE", ["ID"], None, []),
        ):
            orig_newest = _newest_map(odb, table, keys)
            comp_newest = _newest_map(cdb, table, keys)
            stats[f"{table}_original"] = len(orig_newest)
            stats[f"{table}_compacted"] = len(comp_newest)
            if comp_newest != orig_newest:
                only_orig = sorted(set(orig_newest) - set(comp_newest))[:3]
                changed = sorted(
                    key for key in set(orig_newest) & set(comp_newest)
                    if orig_newest[key] != comp_newest[key]
                )[:3]
                raise ValueError(
                    f"compacted {table} diverges: "
                    f"{len(only_orig)} lost keys, {len(changed)} changed rows "
                    f"(e.g. lost={only_orig or None}, changed={changed or None})"
                )
            if canonical_table is None:
                # FILE rows also surface via message-embedded JSON (verified
                # through MESSAGE); the table map equality above is the proof.
                continue
            canonical_keys = {
                tuple(str(value) for value in row)
                for row in target.execute(
                    f"SELECT {', '.join(canonical_columns)} FROM {canonical_table} "
                    "WHERE workspace_id = ?",
                    [workspace_id],
                ).fetchall()
            }
            missing = sorted(set(canonical_keys) - set(comp_newest))
            if missing:
                raise ValueError(
                    f"compacted {table} lost {len(missing)} canonical keys "
                    f"(e.g. {missing[0]})"
                )
            for key in sorted(set(canonical_keys) & set(orig_newest)):
                if key not in comp_newest:
                    raise ValueError(f"compacted {table} lost canonical key {key}")
                if comp_newest[key] != orig_newest[key]:
                    raise ValueError(
                        f"compacted {table} changed canonical key {key}"
                    )
        for table in ("SESSION", "CHUNK"):
            orig_max = odb.execute(f"SELECT max(ID) FROM {table}").fetchone()[0]
            comp_max = cdb.execute(f"SELECT max(ID) FROM {table}").fetchone()[0]
            if orig_max != comp_max:
                raise ValueError(
                    f"compacted {table} high-water changed: {orig_max} -> {comp_max}"
                )
    return stats


def _newest_map(
    db: sqlite3.Connection, table: str, keys: list[str]
) -> dict[tuple[str, ...], tuple[int, str]]:
    """Natural key -> (CHUNK_ID, sha256(DATA)) of the newest row.

    Streams rows (never materializes the table) so multi-GB archives fit
    in small containers; only one digest per key is retained.
    """
    partition = ", ".join(f'"{key}"' for key in keys)
    select_keys = ", ".join(f'ranked."{key}"' for key in keys)
    db.row_factory = None
    result: dict[tuple[str, ...], tuple[int, str]] = {}
    try:
        cursor = db.execute(
            f"""
            SELECT {select_keys}, ranked.CHUNK_ID, ranked.DATA FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY {partition} ORDER BY CHUNK_ID DESC, IDX DESC, rowid DESC
                ) AS _newest_rank
                FROM "{table}"
            ) AS ranked WHERE _newest_rank = 1
            """
        )
        for row in cursor:
            *key_parts, chunk_id, data = row
            if isinstance(data, (bytes, bytearray, memoryview)):
                text = bytes(data).decode("utf-8")
            else:
                text = str(data)
            result[tuple(str(part) for part in key_parts)] = (
                int(chunk_id),
                hashlib.sha256(text.encode("utf-8")).hexdigest(),
            )
        return result
    finally:
        db.row_factory = None


def rebase_checkpoint(
    warehouse: str | Path, *, workspace_id: str, source_path: str | Path
) -> dict[str, object]:
    """Accept a verified compacted archive as the new checkpoint lineage.

    Must only run after ``verify_compaction`` passes for the same file:
    it replaces the stored fingerprint, manifest, and high-waters while
    keeping the last successful run id, so the next ingest validates
    against the compacted file instead of failing continuity.
    """
    import duckdb
    from datetime import datetime, timezone

    from slackpipe.transform import _json_dump as _dump
    from slackpipe.transform import _lineage_signature, _source_lineage_manifest

    warehouse_path = Path(warehouse).expanduser().resolve(strict=True)
    archive = Path(source_path).expanduser().resolve(strict=True)
    digest = hashlib.sha256()
    with open(archive, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    fingerprint = digest.hexdigest()
    with sqlite3.connect(f"{archive.as_uri()}?mode=ro", uri=True) as source_db:
        manifest = _source_lineage_manifest(source_db)
        session_hw = source_db.execute("SELECT max(ID) FROM SESSION").fetchone()[0]
        chunk_hw = source_db.execute("SELECT max(ID) FROM CHUNK").fetchone()[0]
    with duckdb.connect(str(warehouse_path)) as target:
        row = target.execute(
            "SELECT successful_run_id FROM ingestion_checkpoints WHERE workspace_id = ?",
            [workspace_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"no checkpoint to rebase for {workspace_id}")
        target.execute(
            """
            UPDATE ingestion_checkpoints SET
                source_fingerprint = ?, session_high_water = ?,
                chunk_high_water = ?, lineage_signature = ?,
                lineage_manifest_json = ?, checkpointed_at = ?
            WHERE workspace_id = ?
            """,
            [
                fingerprint,
                session_hw,
                chunk_hw,
                _lineage_signature(manifest),
                _dump(manifest),
                datetime.now(timezone.utc),
                workspace_id,
            ],
        )
    return {
        "workspace_id": workspace_id,
        "source_fingerprint": fingerprint,
        "session_high_water": session_hw,
        "chunk_high_water": chunk_hw,
    }
