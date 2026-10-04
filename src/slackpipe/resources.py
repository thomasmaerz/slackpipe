"""Typed Dagster runtime configuration for Slackpipe."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import re

from dagster import ConfigurableResource


class SlackpipeRuntimeResource(ConfigurableResource):
    """Non-secret paths and rollout settings used by Slackpipe runs.

    Slack credentials are intentionally absent. They are selected from
    ``workspace_file`` at execution time and never become Dagster config or
    metadata.
    """

    workspace_file: str = ""
    slackdump_root: str = "/opt/slackpipe/slackdump"
    slackdump_lock: str = "/opt/slackpipe/data/slackdump.lock"
    duckdb_path: str = "/opt/slackpipe/data/slackpipe.duckdb"
    duckdb_lock: str = "/opt/slackpipe/data/duckdb.lock"
    pushgateway_url: str = ""
    deferred_backfills_enabled: bool = False
    lineage_bootstrap_enabled: bool = False
    # ISO 8601 staleness window (e.g. "p90d") applied to incremental resume
    # runs via Slackdump's -skip-stale-threads/-skip-stale-channels. Empty
    # disables stale skipping (full sweep). Initial ingestions never skip.
    incremental_stale_after: str = "p90d"
    # Extra file_id=bytes entries extending the built-in documented size
    # variance record (comma-separated, e.g. "F0123456789=70095357").
    attachment_size_accept: str = ""
    # Comma-separated workspace IDs or slugs for SLACKPIPE_SOLO_WORKSPACES.
    # Kept as a raw string: Dagster's config system rejects typing generics
    # such as tuple[str, ...] on resources. Parse via solo_workspace_slugs.
    solo_workspaces: str = ""

    @classmethod
    def from_environ(
        cls, environ: Mapping[str, str] | None = None
    ) -> "SlackpipeRuntimeResource":
        if environ is None:
            import os

            environ = os.environ
        return cls(
            workspace_file=environ.get("SLACKPIPE_WORKSPACE_FILE", ""),
            slackdump_root=environ.get(
                "SLACKPIPE_SLACKDUMP_ROOT", "/opt/slackpipe/slackdump"
            ),
            slackdump_lock=environ.get(
                "SLACKPIPE_SLACKDUMP_LOCK", "/opt/slackpipe/data/slackdump.lock"
            ),
            duckdb_path=environ.get(
                "SLACKPIPE_DUCKDB_PATH", "/opt/slackpipe/data/slackpipe.duckdb"
            ),
            duckdb_lock=environ.get(
                "SLACKPIPE_DUCKDB_LOCK", "/opt/slackpipe/data/duckdb.lock"
            ),
            pushgateway_url=environ.get("SLACKPIPE_PUSHGATEWAY_URL", ""),
            deferred_backfills_enabled=_boolean_setting(
                environ, "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS"
            ),
            lineage_bootstrap_enabled=_boolean_setting(
                environ, "SLACKPIPE_ALLOW_LINEAGE_BOOTSTRAP"
            ),
            incremental_stale_after=_stale_window_setting(
                environ, "SLACKPIPE_INCREMENTAL_STALE_AFTER", "p90d"
            ),
            attachment_size_accept=environ.get(
                "SLACKPIPE_ATTACHMENT_SIZE_ACCEPT", ""
            ).strip(),
            solo_workspaces=environ.get("SLACKPIPE_SOLO_WORKSPACES", ""),        )

    @property
    def workspace_path(self) -> Path:
        if not self.workspace_file:
            raise RuntimeError("SLACKPIPE_WORKSPACE_FILE is required for Slack execution")
        return Path(self.workspace_file)

    @property
    def accepted_attachment_sizes(self) -> dict[str, int]:
        """Parse attachment_size_accept into file_id -> bytes entries."""
        accepted: dict[str, int] = {}
        for token in self.attachment_size_accept.split(","):
            token = token.strip()
            if not token:
                continue
            file_id, _, size = token.partition("=")
            if not re.fullmatch(r"[A-Za-z0-9_-]+", file_id.strip()) or not size.strip():
                raise ValueError(
                    "SLACKPIPE_ATTACHMENT_SIZE_ACCEPT entries must look like F0123456789=12345"
                )
            try:
                accepted[file_id.strip()] = int(size.strip())
            except ValueError:
                raise ValueError(
                    "SLACKPIPE_ATTACHMENT_SIZE_ACCEPT entries must look like F0123456789=12345"
                ) from None
            if accepted[file_id.strip()] <= 0:
                raise ValueError(
                    "SLACKPIPE_ATTACHMENT_SIZE_ACCEPT sizes must be positive"
                )
        return accepted

    @property
    def solo_workspace_slugs(self) -> tuple[str, ...]:
        """Casefolded workspace IDs/slugs routed to solo (manual) workflows.

        Solo workspaces are left out of the all-workspaces rollout jobs and
        their schedules default to stopped. Unknown entries are kept; the
        caller ignores non-matching ones so a typo can never break code
        loading.
        """
        seen: list[str] = []
        for token in self.solo_workspaces.split(","):
            normalized = token.strip().casefold()
            if normalized and normalized not in seen:
                seen.append(normalized)
        return tuple(seen)


def _boolean_setting(environ: Mapping[str, str], name: str) -> bool:
    value = environ.get(name, "false").strip().lower()
    if value not in {"false", "true"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


def _stale_window_setting(environ: Mapping[str, str], name: str, default: str) -> str:
    value = environ.get(name, default).strip()
    if value and not value.startswith(("p", "P")):
        raise ValueError(f"{name} must be empty or an ISO 8601 duration (e.g. p90d)")
    return value
