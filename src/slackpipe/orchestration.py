from __future__ import annotations

import importlib
import re
import sqlite3
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dagster import (
    AssetCheckResult,
    AssetIn,
    AssetKey,
    AssetSelection,
    Backoff,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    Failure,
    Jitter,
    MaterializeResult,
    RunRequest,
    RetryPolicy,
    RunsFilter,
    ScheduleDefinition,
    SensorEvaluationContext,
    asset,
    asset_check,
    define_asset_job,
    multiprocess_executor,
    run_failure_sensor,
    run_status_sensor,
    sensor,
)

from slackpipe.resources import SlackpipeRuntimeResource
from slackpipe.slackdump import SlackdumpAuthenticationError
from slackpipe.workspaces import WorkspaceConfigurationError, WorkspaceSelectionError

SERIAL_POOL = "slackpipe_canonical"
ATTACHMENT_POOL = "slackpipe_attachments"
SERIAL_TAG = "slackpipe/serial"
MODE_TAG = "slackpipe/mode"
PHASE_TAG = "slackpipe/phase"
WORKSPACE_TAG = "slackpipe/workspace"
# Concurrency lane shared across code locations: every job must carry a
# `lane` tag (see deployment/dagster.yaml) so one team's runs can never
# starve another's out of the global run slots.
LANE_TAG = "lane"
LANE = "slackpipe"
ROLLOUT_ID_TAG = "slackpipe/rollout_id"
ATTACHMENT_ORDER_TAG = "slackpipe/attachment_order"
ATTACHMENT_INDEX_TAG = "slackpipe/attachment_index"
_SECRET_WORDS = ("cookie", "password", "secret", "token")
_SAFE_RESULT_METADATA = {
    "action",
    "archive_path",
    "archive_size_bytes",
    "attachment_backlog",
    "attachment_expected_bytes",
    "attachment_expected_count",
    "attachment_documented_variance_bytes",
    "attachment_documented_variance_count",
    "attachment_documented_variance_files",
    "attachment_retried_count",
    "attachment_accepted_variance_count",
    "attachment_accepted_variance_bytes",
    "attachment_accepted_variance_files",
    "attachment_missing_count",
    "attachment_present_bytes",
    "attachment_present_count",
    "attachment_size_mismatch_count",
    "attachment_skipped_count",
    "attachment_snippet_variance_bytes",
    "attachment_snippet_variance_count",
    "attachment_unsafe_count",
    "canonical_latest_message_at",
    "channel_count",
    "duration_seconds",
    "file_count",
    "lock_wait_seconds",
    "message_count",
    "mode",
    "rows_deleted",
    "rows_inserted",
    "rows_updated",
    "row_count",
    "source_latest_message_at",
    "table_count",
    "warehouse_path",
    "workspace_id",
    "metrics_pushed",
}

SLACK_EXTRACTION_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=5,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.FULL,
)


@dataclass(frozen=True)
class WorkspaceDescriptor:
    workspace_id: str
    url: str


class OrchestrationHelpers(Protocol):
    def extract_workspace(
        self,
        *,
        workspace: WorkspaceDescriptor,
        mode: str,
        run_id: str,
        runtime: SlackpipeRuntimeResource,
        status: Any = None,
    ) -> Any: ...

    def load_workspace_archive(
        self,
        *,
        workspace: WorkspaceDescriptor,
        archive: Any,
        mode: str,
        run_id: str,
        runtime: SlackpipeRuntimeResource,
    ) -> Any: ...

    def validate_archive(
        self, *, workspace: WorkspaceDescriptor, archive: Any
    ) -> Any: ...

    def validate_workspace(
        self, *, workspace: WorkspaceDescriptor, warehouse: Any, archive: Any
    ) -> Any: ...

    def backfill_attachments(
        self,
        *,
        workspace: WorkspaceDescriptor,
        warehouse: Any,
        run_id: str,
        runtime: SlackpipeRuntimeResource,
    ) -> Any: ...


class LazyModuleHelpers:
    """Load worker-owned extraction and warehouse modules only during a run."""

    def _module(self, module_name: str) -> Any:
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError as error:
            if error.name == module_name:
                raise RuntimeError(f"Dagster helper module {module_name} is not installed") from error
            raise

    def _workspace(
        self, descriptor: WorkspaceDescriptor, runtime: SlackpipeRuntimeResource
    ) -> Any:
        module = self._module("slackpipe.workspaces")
        return module.select_workspace(
            module.discover_workspaces(runtime.workspace_path), descriptor.workspace_id
        )

    def _runner(
        self,
        runtime: SlackpipeRuntimeResource,
        workspace: WorkspaceDescriptor,
        *,
        status: Any = None,
    ) -> Any:
        module = self._module("slackpipe.slackdump")
        workspaces = self._module("slackpipe.workspaces")
        attachments_enabled = workspaces.attachment_incremental_enabled(
            runtime.workspace_path
        )
        return module.SlackdumpRunner(
            runtime.slackdump_root,
            lock_path=_workspace_lock_path(runtime.slackdump_lock, workspace.workspace_id),
            attachment_incremental=attachments_enabled,
            status=status,
        )

    def extract_workspace(
        self,
        *,
        workspace: WorkspaceDescriptor,
        mode: str,
        run_id: str,
        runtime: SlackpipeRuntimeResource,
        status: Any = None,
    ) -> Any:
        del run_id
        runner = self._runner(runtime, workspace, status=status)
        selected_workspace = self._workspace(workspace, runtime)
        runner.preflight(selected_workspace)
        if mode == "full":
            run_full_backfill = getattr(runner, "run_full_backfill", None)
            if run_full_backfill is None:
                raise RuntimeError(
                    "all_workspaces_backfill requires "
                    "slackpipe.slackdump.SlackdumpRunner.run_full_backfill"
                )
            result = run_full_backfill(selected_workspace)
        elif mode == "incremental":
            # Steady-state incremental: skip entities dormant longer than
            # the configured staleness window (Slackdump -skip-stale-*).
            result = runner.run_incremental(
                selected_workspace, stale_after=runtime.incremental_stale_after
            )
        else:
            # Initial (and any other) ingestions sweep everything: no
            # stale skipping, so dormant channels/threads are captured.
            result = runner.run_incremental(selected_workspace)
        return {
            "archive_path": result.output_dir / "slackdump.sqlite",
            "workspace_id": workspace.workspace_id,
        }

    def load_workspace_archive(
        self,
        *,
        workspace: WorkspaceDescriptor,
        archive: Any,
        mode: str,
        run_id: str,
        runtime: SlackpipeRuntimeResource,
        status: Any = None,
    ) -> Any:
        module = self._module("slackpipe.transform")
        archive_path = archive.get("archive_path") if isinstance(archive, Mapping) else archive
        warehouse_path = Path(runtime.duckdb_path)
        transform_mode = {
            "acceptance": "development",
            "full": "backfill",
            "initial": "incremental",
            "incremental": "incremental",
        }.get(mode, "incremental")
        started = time.monotonic()
        result = module.ingest_slackdump(
            archive_path,
            warehouse_path,
            workspace_slug=workspace.workspace_id,
            mode=transform_mode,
            dagster_run_id=run_id,
            require_complete=True,
            lock_path=runtime.duckdb_lock,
            allow_lineage_bootstrap=runtime.lineage_bootstrap_enabled,
            status=status,
        )
        metrics_pushed = self._push_metrics(
            workspace=workspace,
            archive_path=Path(archive_path),
            warehouse_path=warehouse_path,
            result=result,
            duration=time.monotonic() - started,
            runtime=runtime,
        )
        return {
            "archive_path": Path(archive_path),
            "warehouse_path": warehouse_path,
            "workspace_id": result.workspace_id,
            "message_count": result.canonical_rows,
            "row_count": result.canonical_rows,
            "metrics_pushed": metrics_pushed,
        }

    def _push_metrics(
        self,
        *,
        workspace: WorkspaceDescriptor,
        archive_path: Path,
        warehouse_path: Path,
        result: Any,
        duration: float,
        runtime: SlackpipeRuntimeResource,
    ) -> bool:
        observability = self._module("slackpipe.observability")
        completed_at = time.time()
        with sqlite3.connect(f"file:{archive_path.as_posix()}?mode=ro", uri=True) as source:
            source_latest = source.execute(
                "SELECT max(ID) / 1000000.0 FROM MESSAGE"
            ).fetchone()[0]
        duckdb = self._module("duckdb")
        with duckdb.connect(str(warehouse_path), read_only=True) as target:
            canonical_latest, attachment_backlog = target.execute(
                """
                SELECT max(ts_us) / 1000000.0,
                       (SELECT count(*) FROM files
                        WHERE workspace_id = ? AND local_object_path IS NULL)
                FROM messages WHERE workspace_id = ?
                """,
                [result.workspace_id, result.workspace_id],
            ).fetchone()
        metrics = observability.PipelineMetrics(
            workspace=workspace.workspace_id,
            stage="canonicalize",
            run_succeeded=True,
            pipeline_completed_at=completed_at,
            observed_at=completed_at,
            source_latest_message_at=source_latest,
            canonical_latest_message_at=canonical_latest,
            source_row_count=result.source_rows,
            canonical_row_count=result.canonical_rows,
            rows_inserted=result.inserted_messages,
            rows_updated=result.updated_messages,
            run_duration_seconds=duration,
            archive_size_bytes=archive_path.stat().st_size,
            attachment_backlog=attachment_backlog,
        )
        return observability.PushgatewayClient(runtime.pushgateway_url).push(metrics)

    def validate_archive(
        self, *, workspace: WorkspaceDescriptor, archive: Any
    ) -> Any:
        del workspace
        archive_path = Path(
            archive.get("archive_path") if isinstance(archive, Mapping) else archive
        )
        if not archive_path.is_file():
            return {"passed": False, "row_count": 0}
        try:
            with sqlite3.connect(f"file:{archive_path.as_posix()}?mode=ro", uri=True) as db:
                integrity = db.execute("PRAGMA integrity_check").fetchone()
                tables = {
                    row[0].upper()
                    for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
                message_count = (
                    db.execute('SELECT count(*) FROM "MESSAGE"').fetchone()[0]
                    if "MESSAGE" in tables
                    else 0
                )
        except sqlite3.Error:
            return {"passed": False, "row_count": 0}
        required = {"CHANNEL", "MESSAGE", "S_USER", "WORKSPACE"}
        return {
            "passed": integrity == ("ok",) and required.issubset(tables),
            "row_count": message_count,
        }

    def validate_workspace(
        self, *, workspace: WorkspaceDescriptor, warehouse: Any, archive: Any
    ) -> Any:
        transform = self._module("slackpipe.transform")
        warehouse_path = Path(
            warehouse.get("warehouse_path")
            if isinstance(warehouse, Mapping)
            else warehouse
        )
        workspace_id = (
            warehouse.get("workspace_id", workspace.workspace_id)
            if isinstance(warehouse, Mapping)
            else workspace.workspace_id
        )
        archive_path = Path(
            archive.get("archive_path") if isinstance(archive, Mapping) else archive
        )
        validation = transform.validate_source_vs_canonical(
            archive_path,
            warehouse_path,
            workspace_id=workspace_id,
        )
        return validation.as_dict()

    def backfill_attachments(
        self,
        *,
        workspace: WorkspaceDescriptor,
        warehouse: Any,
        run_id: str,
        runtime: SlackpipeRuntimeResource,
    ) -> Any:
        del warehouse, run_id
        runner = self._runner(runtime, workspace)
        selected_workspace = self._workspace(workspace, runtime)
        runner.preflight(selected_workspace)
        result, reconciliation = runner.backfill_with_reconciliation(
            selected_workspace, accepted_sizes=runtime.accepted_attachment_sizes
        )
        return {
            "passed": reconciliation.passed,
            "archive_path": result.output_dir / "slackdump.sqlite",
            "attachment_expected_count": reconciliation.expected_count,
            "attachment_present_count": reconciliation.present_count,
            "attachment_skipped_count": reconciliation.skipped_count,
            "attachment_missing_count": reconciliation.missing_count,
            "attachment_size_mismatch_count": reconciliation.size_mismatch_count,
            "attachment_snippet_variance_count": reconciliation.snippet_variance_count,
            "attachment_snippet_variance_bytes": reconciliation.snippet_variance_bytes,
            "attachment_documented_variance_count": reconciliation.documented_variance_count,
            "attachment_documented_variance_bytes": reconciliation.documented_variance_bytes,
            "attachment_documented_variance_files": reconciliation.documented_variance_files,
            "attachment_retried_count": reconciliation.retried_count,
            "attachment_accepted_variance_count": reconciliation.accepted_variance_count,
            "attachment_accepted_variance_bytes": reconciliation.accepted_variance_bytes,
            "attachment_accepted_variance_files": reconciliation.accepted_variance_files,
            "attachment_unsafe_count": reconciliation.unsafe_count,
            "attachment_expected_bytes": reconciliation.expected_bytes,
            "attachment_present_bytes": reconciliation.present_bytes,
        }


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if not slug:
        raise ValueError("workspace id must contain a letter or number")
    if slug[0].isdigit():
        slug = f"workspace_{slug}"
    return slug


def _workspace_lock_path(base_path: str, workspace_id: str) -> str:
    return f"{base_path}.{_slug(workspace_id)}"


def _coerce_workspace(value: Any) -> WorkspaceDescriptor:
    if isinstance(value, WorkspaceDescriptor):
        return value
    if isinstance(value, Mapping):
        workspace_id = str(
            value.get("workspace_id")
            or value.get("WORKSPACE")
            or value.get("name")
            or value.get("id")
            or value.get("slug")
            or ""
        )
        return WorkspaceDescriptor(
            workspace_id=workspace_id,
            url=str(value.get("url") or f"https://{workspace_id}.slack.com"),
        )
    workspace_id = str(
        getattr(
            value,
            "workspace_id",
            getattr(
                value, "WORKSPACE", getattr(value, "name", getattr(value, "slug", getattr(value, "id", "")))
            ),
        )
    )
    return WorkspaceDescriptor(
        workspace_id=workspace_id,
        url=str(getattr(value, "url", f"https://{workspace_id}.slack.com")),
    )


def _external_workspace_discovery(
    environ: Mapping[str, str],
) -> Iterable[Any] | None:
    module_name = "slackpipe.workspaces"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as error:
        if error.name == module_name:
            return None
        raise
    discover = getattr(module, "discover_workspace_descriptors_from_env", None)
    return discover(environ=environ) if discover is not None else None


def discover_workspace_descriptors(
    environ: Mapping[str, str],
) -> tuple[WorkspaceDescriptor, ...]:
    discovered = _external_workspace_discovery(environ)
    if discovered is None:
        discovered = ()

    by_slug: dict[str, WorkspaceDescriptor] = {}
    for value in discovered:
        descriptor = _coerce_workspace(value)
        slug = _slug(descriptor.workspace_id)
        if slug in by_slug:
            raise ValueError(f"duplicate Dagster workspace slug: {slug}")
        by_slug[slug] = descriptor
    return tuple(by_slug[slug] for slug in sorted(by_slug))


def _safe_metadata(result: Any) -> dict[str, Any]:
    if isinstance(result, (str, Path)):
        return {"path": str(result)}
    if not isinstance(result, Mapping):
        return {}
    return {
        str(key): value
        for key, value in result.items()
        if str(key) in _SAFE_RESULT_METADATA
        and not any(word in str(key).lower() for word in _SECRET_WORDS)
        and isinstance(value, (bool, float, int, str, Path))
    }


def _check_result(result: Any, *, workspace: WorkspaceDescriptor) -> AssetCheckResult:
    if isinstance(result, AssetCheckResult):
        return result
    if isinstance(result, Mapping):
        passed = bool(result.get("passed", False))
        metadata = _safe_metadata(result)
    else:
        passed = bool(result)
        metadata = {}
    return AssetCheckResult(
        passed=passed,
        metadata={"workspace_id": workspace.workspace_id, **metadata},
    )


def _mode(context: Any) -> str:
    return context.run.tags.get(MODE_TAG, "incremental")


def _run_id(context: Any) -> str:
    return context.run.run_id


def _tee_status(context: Any) -> Any:
    """Emit progress to both the event log and the captured stdout tab."""
    def emit(message: str) -> None:
        context.log.info(message)
        print(message, flush=True)

    return emit


def _workspace_definitions(
    workspace: WorkspaceDescriptor,
    helpers: OrchestrationHelpers,
) -> tuple[list[Any], list[Any]]:
    slug = _slug(workspace.workspace_id)
    group_name = f"workspace_{slug}"
    raw_key = AssetKey([slug, "raw_slackdump"])
    canonical_key = AssetKey([slug, "canonical_duckdb"])
    raw_pool = f"slackpipe_raw_{slug}"

    @asset(
        key=raw_key,
        group_name=group_name,
        pool=raw_pool,
        description=f"Persistent resumable Slackdump archive for {workspace.workspace_id}.",
        metadata={"workspace_id": workspace.workspace_id},
        tags={"slackpipe/layer": "raw", WORKSPACE_TAG: slug},
        kinds={"slack", "sqlite"},
        code_version="1",
        retry_policy=SLACK_EXTRACTION_RETRY_POLICY,
    )
    def raw_slackdump(
        context, slackpipe_runtime: SlackpipeRuntimeResource
    ) -> MaterializeResult[Any]:
        if _mode(context) == "incremental":
            _require_initial_ingest_complete(context, slackpipe_runtime, workspace)
        try:
            result = helpers.extract_workspace(
                workspace=workspace,
                mode=_mode(context),
                run_id=_run_id(context),
                runtime=slackpipe_runtime,
                status=_tee_status(context),
            )
        except (
            SlackdumpAuthenticationError,
            WorkspaceConfigurationError,
            WorkspaceSelectionError,
        ) as error:
            raise Failure(
                description=f"{type(error).__name__}: {error}",
                allow_retries=False,
            ) from error
        return MaterializeResult(
            value=result,
            metadata={
                "workspace_id": workspace.workspace_id,
                "mode": _mode(context),
                **_safe_metadata(result),
            },
        )

    @asset(
        key=canonical_key,
        ins={"raw_archive": AssetIn(key=raw_key)},
        deps=[AssetKey([slug, "attachments"])],
        group_name=group_name,
        pool=SERIAL_POOL,
        description=f"Canonical DuckDB rows for {workspace.workspace_id}.",
        metadata={"workspace_id": workspace.workspace_id},
        tags={"slackpipe/layer": "canonical", WORKSPACE_TAG: slug},
        kinds={"duckdb"},
        code_version="1",
    )
    def canonical_duckdb(
        context,
        slackpipe_runtime: SlackpipeRuntimeResource,
        raw_archive: Any,
    ) -> MaterializeResult[Any]:
        if _mode(context) == "incremental":
            _require_initial_ingest_complete(context, slackpipe_runtime, workspace)
        result = helpers.load_workspace_archive(
            workspace=workspace,
            archive=raw_archive,
            mode=_mode(context),
            run_id=_run_id(context),
            runtime=slackpipe_runtime,
            status=_tee_status(context),
        )
        return MaterializeResult(
            value=result,
            metadata={
                "workspace_id": workspace.workspace_id,
                "mode": _mode(context),
                **_safe_metadata(result),
            },
        )

    @asset(
        key=AssetKey([slug, "attachments"]),
        deps=[raw_slackdump],
        group_name=group_name,
        pool=ATTACHMENT_POOL,
        description=f"Explicit attachment backfill for {workspace.workspace_id}.",
        metadata={"workspace_id": workspace.workspace_id},
        tags={"slackpipe/layer": "attachment", WORKSPACE_TAG: slug},
        kinds={"slack", "file"},
        code_version="1",
    )
    def attachments(
        context, slackpipe_runtime: SlackpipeRuntimeResource
    ) -> MaterializeResult[Any]:
        emit = _tee_status(context)
        emit(f"starting attachment backfill for {workspace.workspace_id}")
        result = helpers.backfill_attachments(
            workspace=workspace,
            warehouse=None,
            run_id=_run_id(context),
            runtime=slackpipe_runtime,
        )
        emit(f"finished attachment backfill for {workspace.workspace_id}")
        return MaterializeResult(
            value=result,
            metadata={
                "workspace_id": workspace.workspace_id,
                "mode": _mode(context),
                **_safe_metadata(result),
            }
        )

    @asset_check(
        asset=attachments,
        name="attachment_integrity",
        blocking=False,
        pool=ATTACHMENT_POOL,
        description=(
            "Advisory only: every downloadable attachment exists at the exact "
            "expected size. Size mismatches are redownloaded twice, then "
            "accepted with a warning; editable Slack snippets (mode=snippet, "
            "text/*) tolerate server-side normalization within a tight byte "
            "bound, and individually documented served-size variances pass on "
            "exact bytes only. All variance stays visible as metadata."
        ),
    )
    def attachment_integrity(attachments: Any) -> AssetCheckResult:
        return _check_result(attachments, workspace=workspace)

    @asset_check(
        asset=raw_slackdump,
        name="archive_valid",
        blocking=True,
        pool=raw_pool,
        description="The Slackdump archive exists and has the expected source structure.",
    )
    def archive_valid(raw_slackdump: Any) -> AssetCheckResult:
        return _check_result(
            helpers.validate_archive(workspace=workspace, archive=raw_slackdump),
            workspace=workspace,
        )

    @asset_check(
        asset=canonical_duckdb,
        name="canonical_integrity",
        blocking=True,
        pool=SERIAL_POOL,
        description="Canonical workspace rows satisfy warehouse uniqueness and integrity checks.",
    )
    def canonical_integrity(canonical_duckdb: Any) -> AssetCheckResult:
        return _check_result(
            helpers.validate_workspace(
                workspace=workspace,
                warehouse=canonical_duckdb,
                archive=canonical_duckdb,
            ),
            workspace=workspace,
        )

    return [raw_slackdump, canonical_duckdb, attachments], [
        archive_valid,
        canonical_integrity,
        attachment_integrity,
    ]


def _rollout_descriptors(
    descriptors: tuple[WorkspaceDescriptor, ...],
    solo_slugs: Iterable[str],
) -> tuple[WorkspaceDescriptor, ...]:
    """Workspaces covered by the all-workspaces rollout jobs.

    ``solo_slugs`` holds casefolded workspace IDs or Dagster slugs (see
    ``SLACKPIPE_SOLO_WORKSPACES``): solo workspaces run their own manual
    workflow instead. Unknown entries never match, so a typo cannot break
    code loading. Solo-ing everything falls back to the full set instead of
    defining jobs with empty selections.
    """
    solo = {str(token).casefold() for token in solo_slugs}
    if not solo:
        return descriptors
    included = tuple(
        descriptor
        for descriptor in descriptors
        if _slug(descriptor.workspace_id) not in solo
        and descriptor.workspace_id.casefold() not in solo
    )
    return included or descriptors


_INITIAL_TERMINAL_STATUSES = frozenset(
    {
        DagsterRunStatus.SUCCESS,
        DagsterRunStatus.FAILURE,
        DagsterRunStatus.CANCELED,
    }
)


def _initial_ingest_job_name(slug: str) -> str:
    return f"{slug}_ingest_once"


def _initial_ingest_attempted(instance, slug: str) -> bool:
    runs = instance.get_runs(
        filters=RunsFilter(job_name=_initial_ingest_job_name(slug)),
        limit=1,
    )
    return len(runs) > 0


def _initial_ingest_succeeded(instance, slug: str) -> bool:
    runs = instance.get_runs(
        filters=RunsFilter(
            job_name=_initial_ingest_job_name(slug),
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        limit=1,
    )
    return len(runs) > 0


def _has_succeeded_canonical_checkpoint(duckdb_path: str, workspace_id: str) -> bool:
    from pathlib import Path as _Path

    if not duckdb_path or not _Path(duckdb_path).exists():
        return False
    try:
        import duckdb
    except ImportError:
        return False
    try:
        connection = duckdb.connect(str(duckdb_path), read_only=True)
    except Exception:
        return False
    try:
        row = connection.execute(
            """
            SELECT 1
            FROM workspaces w
            JOIN ingestion_checkpoints c ON c.workspace_id = w.workspace_id
            JOIN ingestion_runs r ON r.run_id = c.successful_run_id
            WHERE lower(w.workspace_slug) = lower(?) AND r.status = 'succeeded'
            LIMIT 1
            """,
            [workspace_id],
        ).fetchone()
        return row is not None
    except Exception:
        return False
    finally:
        try:
            connection.close()
        except Exception:
            pass


def _initial_ingest_complete(instance, runtime, workspace) -> bool:
    slug = _slug(workspace.workspace_id)
    if _initial_ingest_succeeded(instance, slug):
        return True
    return _has_succeeded_canonical_checkpoint(runtime.duckdb_path, workspace.workspace_id)


def _require_initial_ingest_complete(context, runtime, workspace) -> None:
    if _initial_ingest_complete(context.instance, runtime, workspace):
        return
    slug = _slug(workspace.workspace_id)
    raise Failure(
        description=(
            f"initial ingestion for {workspace.workspace_id!r} has not completed; "
            f"run {_initial_ingest_job_name(slug)} first. Incremental runs are "
            "blocked until the initial full sweep succeeds."
        ),
        allow_retries=False,
    )


def _new_workspace_sensor(descriptors, runtime, *, default_status, jobs=()):
    ordered = tuple(sorted(descriptors, key=lambda item: item.workspace_id))

    @sensor(
        name="slackpipe_new_workspace_sensor",
        jobs=list(jobs),
        description=(
            "Starts {slug}_ingest_once for workspaces with no initial-ingest "
            "attempt and no succeeded warehouse checkpoint."
        ),
        default_status=default_status,
        minimum_interval_seconds=300,
    )
    def new_workspace_sensor(context: SensorEvaluationContext):
        for candidate in ordered:
            slug = _slug(candidate.workspace_id)
            if _initial_ingest_attempted(context.instance, slug):
                continue
            if _has_succeeded_canonical_checkpoint(
                runtime.duckdb_path, candidate.workspace_id
            ):
                continue
            context.log.info(f"launching initial ingest for new workspace {slug}")
            return RunRequest(
                run_key=f"auto-initial-{slug}",
                job_name=_initial_ingest_job_name(slug),
                tags={WORKSPACE_TAG: slug, MODE_TAG: "initial"},
            )
        context.log.info("no new workspaces without an initial ingest, skipping")
        return None

    return new_workspace_sensor


def _flow_selection(assets: list[Any]) -> AssetSelection:
    """Per-workspace pipeline order: raw extraction, attachments, canonical.

    The asset graph edges (attachments after raw, canonical after
    attachments) enforce the order inside any job selecting the flow.
    """
    flow_assets = [item for item in assets if item.key.path[-1] != "attachments"]
    attachment_assets = [item for item in assets if item.key.path[-1] == "attachments"]
    return (
        AssetSelection.assets(*flow_assets)
        | AssetSelection.checks_for_assets(*flow_assets)
        | AssetSelection.assets(*attachment_assets)
        | AssetSelection.checks_for_assets(*attachment_assets)
    )


def _raw_selection(assets: list[Any]) -> AssetSelection:
    raw_assets = [item for item in assets if item.key.path[-1] == "raw_slackdump"]
    return AssetSelection.assets(*raw_assets) | AssetSelection.checks_for_assets(*raw_assets)


def _canonical_selection(assets: list[Any]) -> AssetSelection:
    canonical_assets = [item for item in assets if item.key.path[-1] == "canonical_duckdb"]
    return AssetSelection.assets(*canonical_assets) | AssetSelection.checks_for_assets(
        *canonical_assets
    )


def _attachment_selection(assets: list[Any]) -> AssetSelection:
    attachment_assets = [item for item in assets if item.key.path[-1] == "attachments"]
    return AssetSelection.assets(*attachment_assets) | AssetSelection.checks_for_assets(
        *attachment_assets
    )


def _attachment_order(
    runtime: SlackpipeRuntimeResource, rollout_slugs: set[str]
) -> tuple[str, ...]:
    """Size-ordered workspace slugs from source archives, smallest first.

    Sized from the Slackdump archives (not DuckDB) because attachments run
    before canonicalization: a workspace joins the order as soon as its
    archive exists. Directories outside the rollout (e.g. solo workspaces)
    are skipped by slug without opening their databases, so an 8 GB solo
    archive never slows the sensor. Workspaces without an archive yet are
    skipped until their first successful extraction.
    """
    slackdump = importlib.import_module("slackpipe.slackdump")
    root = Path(runtime.slackdump_root)
    if not root.is_dir():
        return ()
    sized: list[tuple[int, str]] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir() or not (child / "slackdump.sqlite").is_file():
            continue
        slug = _slug(child.name)
        if not slug or slug not in rollout_slugs:
            continue
        try:
            expected, _, _ = slackdump.expected_attachments(child)
        except (OSError, ValueError):
            continue
        sized.append((sum(size for _, size, _, _ in expected.values()), slug))
    sized.sort()
    return tuple(slug for _, slug in sized if slug)


def _rollout_sensor(
    *,
    extract_job: Any,
    canonical_job: Any,
    attachment_jobs: list[Any],
    rollout_slugs: set[str],
    default_status: DefaultSensorStatus,
):
    rollout_attachments = [
        job
        for job in attachment_jobs
        if job.name.removesuffix("_attachments_once") in rollout_slugs
    ]
    monitored = [extract_job, canonical_job, *rollout_attachments]
    requested = [canonical_job, *rollout_attachments]

    @run_status_sensor(
        run_status=DagsterRunStatus.SUCCESS,
        name="slackpipe_attachments_and_duckdb_coordinator",
        description=(
            "Advances a successful extraction through size-ordered attachment "
            "backfills, then serialized canonicalization into DuckDB."
        ),
        monitored_jobs=monitored,
        request_jobs=requested,
        default_status=default_status,
        # Chained phases run minutes-to-hours each: polling every 2 minutes
        # adds negligible chain latency while cutting Postgres tick writes
        # ~75% versus the 30s default.
        minimum_interval_seconds=120,
    )
    def attachments_and_duckdb_coordinator(context, slackpipe_runtime: SlackpipeRuntimeResource):
        run = context.dagster_run
        rollout_id = run.tags.get(ROLLOUT_ID_TAG, run.run_id)
        if run.job_name == extract_job.name:
            order = _attachment_order(slackpipe_runtime, rollout_slugs)
            context.log.info(
                f"extraction {run.run_id[:8]} succeeded, attachment order: {list(order)}"
            )
            if not order:
                context.log.info("no workspaces with archives yet, rollout paused")
                return None
            context.log.info(f"launching attachment backfill for {order[0]}")
            return RunRequest(
                run_key=f"{rollout_id}:attachments:0",
                job_name=f"{_slug(order[0])}_attachments_once",
                tags={
                    ROLLOUT_ID_TAG: rollout_id,
                    ATTACHMENT_ORDER_TAG: ",".join(order),
                    ATTACHMENT_INDEX_TAG: "0",
                },
            )
        if run.job_name == canonical_job.name:
            context.log.info(
                f"canonical sweep {run.run_id[:8]} succeeded, rollout {rollout_id[:8]} complete"
            )
            return None
        if ATTACHMENT_ORDER_TAG not in run.tags:
            # Manual/uncoordinated attachment run: never advance the rollout.
            context.log.info(
                f"ignoring uncoordinated attachment run {run.run_id[:8]} "
                f"({run.job_name})"
            )
            return None
        order = tuple(item for item in run.tags.get(ATTACHMENT_ORDER_TAG, "").split(",") if item)
        index = int(run.tags.get(ATTACHMENT_INDEX_TAG, "-1")) + 1
        if order and index < len(order):
            context.log.info(
                f"attachment {run.run_id[:8]} succeeded, launching next: {order[index]}"
            )
            return RunRequest(
                run_key=f"{rollout_id}:attachments:{index}",
                job_name=f"{_slug(order[index])}_attachments_once",
                tags={
                    ROLLOUT_ID_TAG: rollout_id,
                    ATTACHMENT_ORDER_TAG: ",".join(order),
                    ATTACHMENT_INDEX_TAG: str(index),
                },
            )
        context.log.info(f"attachments complete, launching canonical sweep")
        return RunRequest(
            run_key=f"{rollout_id}:canonical",
            job_name=canonical_job.name,
            tags={ROLLOUT_ID_TAG: rollout_id},
        )

    return attachments_and_duckdb_coordinator


def _failure_sensor(default_status: DefaultSensorStatus):
    @run_failure_sensor(
        name="slackpipe_run_failure_metrics",
        description="Publishes a bounded Prometheus snapshot when a Slackpipe run fails.",
        default_status=default_status,
        minimum_interval_seconds=120,
    )
    def slackpipe_run_failure_metrics(
        context, slackpipe_runtime: SlackpipeRuntimeResource
    ) -> None:
        if not slackpipe_runtime.pushgateway_url:
            context.log.info("no pushgateway configured, skipping failure metrics")
            return
        context.log.info(
            f"publishing failure metrics for {context.dagster_run.job_name} "
            f"({context.dagster_run.run_id[:8]})"
        )
        observability = importlib.import_module("slackpipe.observability")
        completed_at = time.time()
        metrics = observability.PipelineMetrics(
            workspace=context.dagster_run.tags.get(WORKSPACE_TAG, "unknown"),
            stage=context.dagster_run.job_name,
            run_succeeded=False,
            pipeline_completed_at=completed_at,
            observed_at=completed_at,
            run_failures=1,
            workspace_materialized=False,
        )
        observability.PushgatewayClient(slackpipe_runtime.pushgateway_url).push(metrics)

    return slackpipe_run_failure_metrics


def build_definitions(
    *,
    environ: Mapping[str, str] | None = None,
    workspaces: Iterable[Any] | None = None,
    helpers: OrchestrationHelpers | None = None,
    runtime: SlackpipeRuntimeResource | None = None,
) -> Definitions:
    runtime = runtime or SlackpipeRuntimeResource.from_environ(environ)
    discovery_environ = (
        {"SLACKPIPE_WORKSPACE_FILE": runtime.workspace_file}
        if runtime.workspace_file
        else {}
    )
    descriptors = tuple(
        discover_workspace_descriptors(discovery_environ)
        if workspaces is None
        else (_coerce_workspace(value) for value in workspaces)
    )
    helpers = helpers or LazyModuleHelpers()
    assets: list[Any] = []
    checks: list[Any] = []
    workspace_assets: dict[str, list[Any]] = {}
    for descriptor in descriptors:
        slug = _slug(descriptor.workspace_id)
        generated_assets, generated_checks = _workspace_definitions(descriptor, helpers)
        assets.extend(generated_assets)
        checks.extend(generated_checks)
        workspace_assets[slug] = generated_assets

    serial_tags = {SERIAL_TAG: "true"}
    lane_tags = {LANE_TAG: LANE}
    jobs: list[Any] = []
    attachment_jobs: list[Any] = []
    extract_job = canonical_job = None
    rollout_descriptors = _rollout_descriptors(
        tuple(descriptors), runtime.solo_workspace_slugs
    )
    rollout_slugs = {_slug(descriptor.workspace_id) for descriptor in rollout_descriptors}
    rollout_assets = [
        item
        for slug, items in workspace_assets.items()
        if slug in rollout_slugs
        for item in items
    ]
    solo_ids = sorted(
        descriptor.workspace_id
        for descriptor in descriptors
        if _slug(descriptor.workspace_id) not in rollout_slugs
    )
    solo_note = (
        f" Solo workflow workspace(s), not in rollout jobs: {', '.join(solo_ids)}."
        if solo_ids
        else ""
    )
    if runtime.deferred_backfills_enabled and rollout_assets:
        extract_job = define_asset_job(
            "all_workspaces_extract",
            selection=_raw_selection(rollout_assets),
            description=(
                "Nightly steady-state raw extraction for all rollout workspaces "
                "(incremental resume with stale skipping; the rollout sensor "
                "chains size-ordered attachment backfills and a canonical "
                "sweep after it)." + solo_note
            ),
            tags={LANE_TAG: LANE, PHASE_TAG: "raw", MODE_TAG: "incremental"},
            executor_def=multiprocess_executor.configured({"max_concurrent": 2}),
        )
        canonical_job = define_asset_job(
            "all_workspaces_canonical",
            selection=_canonical_selection(rollout_assets),
            description=(
                "Serialized canonicalization for all discovered workspaces."
                + solo_note
            ),
            tags={LANE_TAG: LANE, **serial_tags, PHASE_TAG: "canonical", MODE_TAG: "full"},
        )
        jobs.extend([extract_job, canonical_job])
    schedules: list[ScheduleDefinition] = []
    for descriptor in descriptors:
        slug = _slug(descriptor.workspace_id)
        flow_selection = _flow_selection(workspace_assets[slug])
        # Solo workspaces run outside the rollout chain: their jobs skip
        # the serial run tag so a long solo run (e.g. leadership's 8 GB
        # resume) can never hold the rollout queue. Step-level safety
        # still comes from the op-granularity pools plus the OS locks
        # (per-workspace Slackdump lock, global DuckDB lock).
        job_tags = {**lane_tags, **(serial_tags if slug in rollout_slugs else {})}
        job = define_asset_job(
            f"{slug}_ingest_once",
            selection=flow_selection,
            description=(
                f"Initial full-sweep Slackdump-to-DuckDB flow for {descriptor.workspace_id} "
                "(no stale skipping)."
            ),
            tags={
                **job_tags,
                MODE_TAG: "initial",
                WORKSPACE_TAG: slug,
            },
        )
        jobs.append(job)
        incremental_job = define_asset_job(
            f"{slug}_incremental",
            selection=flow_selection,
            description=(
                f"Steady-state incremental flow for {descriptor.workspace_id} "
                "(skips entities dormant beyond the staleness window)."
            ),
            tags={
                **job_tags,
                MODE_TAG: "incremental",
                WORKSPACE_TAG: slug,
            },
        )
        jobs.append(incremental_job)
        jobs.append(
            define_asset_job(
                f"{slug}_acceptance",
                selection=flow_selection,
                description=f"Manual one-workspace acceptance flow for {descriptor.workspace_id}.",
                tags={
                    **job_tags,
                    MODE_TAG: "acceptance",
                    WORKSPACE_TAG: slug,
                },
            )
        )
        attachment_job = define_asset_job(
            f"{slug}_attachments_once",
            selection=_attachment_selection(workspace_assets[slug]),
            description=f"Deliberate one-off attachment backfill for {descriptor.workspace_id}.",
            tags={
                **job_tags,
                MODE_TAG: "attachments",
                WORKSPACE_TAG: slug,
            },
        )
        jobs.append(attachment_job)
        attachment_jobs.append(attachment_job)
        # No per-workspace schedules: the single nightly rollout schedule
        # below is the only recurring trigger. Per-workspace crons were
        # removed after they auto-queued 24 runs against one rollout and
        # every one had to be canceled (Sep-2026). Recurring per-workspace
        # runs can be relaunched manually via their *_incremental jobs.

    failure_sensor = _failure_sensor(
        DefaultSensorStatus.RUNNING
        if runtime.pushgateway_url
        else DefaultSensorStatus.STOPPED
    )
    sensors: list[Any] = [
        failure_sensor,
        _new_workspace_sensor(
            rollout_descriptors,
            runtime,
            default_status=DefaultSensorStatus.RUNNING,
            jobs=[job for job in jobs if job.name.endswith("_ingest_once")],
        ),
    ]
    if extract_job is not None and canonical_job is not None:
        # One sequential nightly rollout instead of per-workspace schedules:
        # the sensor advances extract -> attachments -> canonical, so runs
        # never overlap. Started deliberately from the UI/API; solo
        # workspaces stay manual.
        schedules.append(
            ScheduleDefinition(
                name="nightly_slackdump_incremental",
                cron_schedule="0 1 * * *",
                job=extract_job,
                default_status=DefaultScheduleStatus.STOPPED,
                description=(
                    "Nightly sequential rollout: incremental extraction, "
                    "then sensor-chained attachments and canonical sweep."
                ),
            )
        )
        sensors.append(
            _rollout_sensor(
                extract_job=extract_job,
                canonical_job=canonical_job,
                attachment_jobs=attachment_jobs,
                rollout_slugs=rollout_slugs,
                default_status=DefaultSensorStatus.RUNNING,
            )
        )
    return Definitions(
        assets=assets,
        asset_checks=checks,
        jobs=jobs,
        schedules=schedules,
        sensors=sensors,
        resources={"slackpipe_runtime": runtime},
    )
