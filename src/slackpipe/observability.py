"""Small, dependency-free Prometheus metrics and Pushgateway client.

The metrics describe the latest pipeline run for one workspace and stage.  They
are gauges because a Pushgateway ``PUT`` replaces that workspace/stage group;
operation and failure values therefore describe the latest run, not process-
lifetime counters.
"""

from __future__ import annotations

import base64
import math
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Mapping


_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


@dataclass(frozen=True)
class GroupingKey:
    """The bounded Pushgateway grouping key used by Slackpipe."""

    workspace: str
    stage: str

    def __post_init__(self) -> None:
        _validate_label("workspace", self.workspace)
        _validate_label("stage", self.stage)

    def as_dict(self) -> dict[str, str]:
        return {"workspace": self.workspace, "stage": self.stage}


@dataclass(frozen=True)
class PipelineMetrics:
    """A complete latest-run snapshot for one workspace and pipeline stage.

    ``pipeline_completion_lag_seconds`` is derived from pipeline completion,
    not from the latest Slack message.  Consequently, a successful run over a
    quiet workspace remains healthy even when its latest message is old.
    """

    workspace: str
    stage: str
    run_succeeded: bool
    pipeline_completed_at: float
    observed_at: float | None = None
    last_success_at: float | None = None
    source_latest_message_at: float | None = None
    canonical_latest_message_at: float | None = None
    source_row_count: int = 0
    canonical_row_count: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    run_duration_seconds: float = 0.0
    run_failures: int = 0
    lock_wait_seconds: float = 0.0
    archive_size_bytes: int = 0
    attachment_backlog: int = 0
    workspace_discovered: bool = True
    workspace_materialized: bool = True

    def __post_init__(self) -> None:
        _validate_label("workspace", self.workspace)
        _validate_label("stage", self.stage)

        if self.observed_at is None:
            object.__setattr__(self, "observed_at", time.time())
        if self.run_succeeded and self.last_success_at is None:
            object.__setattr__(self, "last_success_at", self.pipeline_completed_at)

        for name in (
            "run_succeeded",
            "workspace_discovered",
            "workspace_materialized",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be a boolean")

        timestamp_fields = (
            "pipeline_completed_at",
            "observed_at",
            "last_success_at",
            "source_latest_message_at",
            "canonical_latest_message_at",
        )
        count_fields = (
            "source_row_count",
            "canonical_row_count",
            "rows_inserted",
            "rows_updated",
            "rows_deleted",
            "run_failures",
            "archive_size_bytes",
            "attachment_backlog",
        )
        duration_fields = ("run_duration_seconds", "lock_wait_seconds")

        for name in timestamp_fields:
            value = getattr(self, name)
            if value is not None:
                _validate_nonnegative_number(name, value)
        for name in count_fields:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in duration_fields:
            _validate_nonnegative_number(name, getattr(self, name))

        if self.observed_at < self.pipeline_completed_at:
            raise ValueError("observed_at must not precede pipeline_completed_at")

    @property
    def grouping_key(self) -> GroupingKey:
        return GroupingKey(workspace=self.workspace, stage=self.stage)

    @property
    def pipeline_completion_lag_seconds(self) -> float:
        return self.observed_at - self.pipeline_completed_at

    @property
    def source_canonical_latest_message_lag_seconds(self) -> float | None:
        if (
            self.source_latest_message_at is None
            or self.canonical_latest_message_at is None
        ):
            return None
        return max(
            0.0,
            self.source_latest_message_at - self.canonical_latest_message_at,
        )


@dataclass(frozen=True)
class _Metric:
    name: str
    help: str
    attribute: str


_METRICS = (
    _Metric(
        "slackpipe_run_success",
        "Whether the latest pipeline run succeeded (1 or 0).",
        "run_succeeded",
    ),
    _Metric(
        "slackpipe_last_success_timestamp_seconds",
        "Unix timestamp of the latest successful pipeline completion.",
        "last_success_at",
    ),
    _Metric(
        "slackpipe_pipeline_completed_timestamp_seconds",
        "Unix timestamp when the latest pipeline run completed.",
        "pipeline_completed_at",
    ),
    _Metric(
        "slackpipe_pipeline_completion_lag_seconds",
        "Seconds between pipeline completion and metric observation.",
        "pipeline_completion_lag_seconds",
    ),
    _Metric(
        "slackpipe_source_latest_message_timestamp_seconds",
        "Unix timestamp of the latest message in the source.",
        "source_latest_message_at",
    ),
    _Metric(
        "slackpipe_canonical_latest_message_timestamp_seconds",
        "Unix timestamp of the latest message in canonical storage.",
        "canonical_latest_message_at",
    ),
    _Metric(
        "slackpipe_source_canonical_latest_message_lag_seconds",
        "Seconds by which the source latest message is ahead of canonical storage.",
        "source_canonical_latest_message_lag_seconds",
    ),
    _Metric(
        "slackpipe_source_row_count",
        "Current source row count.",
        "source_row_count",
    ),
    _Metric(
        "slackpipe_canonical_row_count",
        "Current canonical row count.",
        "canonical_row_count",
    ),
    _Metric(
        "slackpipe_rows_inserted",
        "Rows inserted by the latest pipeline run.",
        "rows_inserted",
    ),
    _Metric(
        "slackpipe_rows_updated",
        "Rows updated by the latest pipeline run.",
        "rows_updated",
    ),
    _Metric(
        "slackpipe_rows_deleted",
        "Rows deleted by the latest pipeline run.",
        "rows_deleted",
    ),
    _Metric(
        "slackpipe_run_duration_seconds",
        "Duration of the latest pipeline run.",
        "run_duration_seconds",
    ),
    _Metric(
        "slackpipe_run_failures",
        "Failures observed in the latest pipeline run.",
        "run_failures",
    ),
    _Metric(
        "slackpipe_lock_wait_seconds",
        "Time the latest pipeline run spent waiting for locks.",
        "lock_wait_seconds",
    ),
    _Metric(
        "slackpipe_archive_size_bytes",
        "Current archive size in bytes.",
        "archive_size_bytes",
    ),
    _Metric(
        "slackpipe_attachment_backlog",
        "Current attachment processing backlog.",
        "attachment_backlog",
    ),
    _Metric(
        "slackpipe_workspace_discovered",
        "Whether the workspace was discovered (1 or 0).",
        "workspace_discovered",
    ),
    _Metric(
        "slackpipe_workspace_materialized",
        "Whether the workspace was materialized (1 or 0).",
        "workspace_materialized",
    ),
)


def render_prometheus(metrics: PipelineMetrics) -> str:
    """Render one metrics snapshot in Prometheus text exposition format."""

    labels = (
        f'workspace="{_escape_label(metrics.workspace)}",'
        f'stage="{_escape_label(metrics.stage)}"'
    )
    lines: list[str] = []
    for metric in _METRICS:
        value = getattr(metrics, metric.attribute)
        if value is None:
            continue
        lines.extend(
            (
                f"# HELP {metric.name} {metric.help}",
                f"# TYPE {metric.name} gauge",
                f"{metric.name}{{{labels}}} {_format_number(value)}",
            )
        )
    return "\n".join(lines) + "\n"


class PushgatewayClient:
    """Atomically replace or delete workspace/stage Pushgateway groups."""

    def __init__(
        self,
        base_url: str | None,
        *,
        job: str = "slackpipe",
        timeout_seconds: float = 10.0,
    ) -> None:
        normalized_url = base_url.strip().rstrip("/") if base_url else ""
        if normalized_url:
            parsed_url = urllib.parse.urlsplit(normalized_url)
            if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
                raise ValueError("base_url must be an absolute HTTP(S) URL")
        self._base_url = normalized_url or None
        _validate_label("job", job)
        _validate_nonnegative_number("timeout_seconds", timeout_seconds)
        if timeout_seconds == 0:
            raise ValueError("timeout_seconds must be greater than zero")
        self._job = job
        self._timeout_seconds = timeout_seconds

    @classmethod
    def from_env(
        cls, env: Mapping[str, str], **kwargs: object
    ) -> "PushgatewayClient":
        """Compatibility constructor from an explicitly supplied mapping."""

        return cls(env.get("SLACKPIPE_PUSHGATEWAY_URL"), **kwargs)

    @property
    def enabled(self) -> bool:
        return self._base_url is not None

    def push(
        self,
        metrics: PipelineMetrics,
        *,
        grouping_key: GroupingKey | None = None,
    ) -> bool:
        """Atomically replace a group using HTTP PUT; return false if disabled."""

        key = grouping_key or metrics.grouping_key
        if key != metrics.grouping_key:
            raise ValueError("grouping_key must match the metrics workspace and stage")
        return self._request("PUT", key, render_prometheus(metrics).encode("utf-8"))

    def delete(self, grouping_key: GroupingKey) -> bool:
        """Delete a workspace/stage group; return false if disabled."""

        return self._request("DELETE", grouping_key)

    def delete_stale_workspace_groups(
        self,
        *,
        active_workspaces: set[str],
        known_workspaces: set[str],
        stage: str,
    ) -> tuple[str, ...]:
        """Delete known groups absent from the active workspace set.

        Pushgateway has no safe delete-by-label discovery API, so callers pass
        the previously known workspace set.  The returned tuple is sorted and
        contains workspaces for which deletion was attempted while enabled.
        """

        stale = tuple(sorted(known_workspaces - active_workspaces))
        if not self.enabled:
            return ()
        for workspace in stale:
            self.delete(GroupingKey(workspace=workspace, stage=stage))
        return stale

    def _request(
        self,
        method: str,
        grouping_key: GroupingKey,
        body: bytes | None = None,
    ) -> bool:
        if not self.enabled:
            return False
        request = urllib.request.Request(
            self._group_url(grouping_key),
            data=body,
            headers={"Content-Type": _CONTENT_TYPE},
            method=method,
        )
        with urllib.request.urlopen(request, timeout=self._timeout_seconds):
            pass
        return True

    def _group_url(self, grouping_key: GroupingKey) -> str:
        parts = [self._base_url, "metrics", _encoded_path_label("job", self._job)]
        parts.extend(
            _encoded_path_label(name, value)
            for name, value in grouping_key.as_dict().items()
        )
        return "/".join(parts)


def _encoded_path_label(name: str, value: str) -> str:
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
    return f"{name}@base64/{encoded or '='}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_number(value: int | float | bool) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def _validate_label(name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    if "\x00" in value:
        raise ValueError(f"{name} must not contain a NUL byte")


def _validate_nonnegative_number(name: str, value: int | float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a non-negative finite number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a non-negative finite number")


__all__ = [
    "GroupingKey",
    "PipelineMetrics",
    "PushgatewayClient",
    "render_prometheus",
]
