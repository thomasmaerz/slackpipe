"""Coherent local snapshots of Slackdump SQLite databases."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


def snapshot_sqlite(source: str | Path, destination: str | Path) -> Path:
    """Copy a live SQLite database coherently using SQLite's backup API.

    The destination is replaced atomically after a successful backup. SQLite
    reads any WAL state through the source connection; no sidecar copying or
    source writes are required.
    """

    source_path = Path(source).expanduser().resolve(strict=True)
    destination_path = Path(destination).expanduser().resolve()
    if source_path == destination_path:
        raise ValueError("SQLite snapshot destination must differ from source")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination_path.with_name(f".{destination_path.name}.tmp")
    temporary_path.unlink(missing_ok=True)

    source_uri = f"{source_path.as_uri()}?mode=ro"
    try:
        with sqlite3.connect(source_uri, uri=True) as source_db:
            with sqlite3.connect(temporary_path) as destination_db:
                source_db.backup(destination_db)
                result = destination_db.execute("PRAGMA integrity_check").fetchone()
                if result != ("ok",):
                    raise RuntimeError(f"SQLite snapshot integrity check failed: {result!r}")
        os.replace(temporary_path, destination_path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise

    return destination_path
