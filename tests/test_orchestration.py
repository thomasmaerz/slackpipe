from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import (
    Backoff,
    DagsterInstance,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    Jitter,
    build_run_status_sensor_context,
)
from dagster._core.instance.config import dagster_instance_config

from slackpipe.orchestration import (
    ATTACHMENT_INDEX_TAG,
    ATTACHMENT_ORDER_TAG,
    ATTACHMENT_POOL,
    MODE_TAG,
    ROLLOUT_ID_TAG,
    SERIAL_POOL,
    SERIAL_TAG,
    WorkspaceDescriptor,
    _workspace_lock_path,
    build_definitions,
    discover_workspace_descriptors,
)
from slackpipe.definitions import defs as load_defs
from slackpipe.resources import SlackpipeRuntimeResource


class StubHelpers:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def extract_workspace(
        self, *, workspace: WorkspaceDescriptor, mode: str, run_id: str, runtime: Any,
        status: Any = None,
    ) -> dict[str, Any]:
        self.calls.append(("extract", workspace.workspace_id, mode))
        return {
            "archive_path": f"/synthetic/{workspace.workspace_id}/slackdump.sqlite",
            "message_count": 3,
            "token": "must-not-be-metadata",
        }

    def load_workspace_archive(
        self,
        *,
        workspace: WorkspaceDescriptor,
        archive: Any,
        mode: str,
        run_id: str,
        runtime: Any,
        status: Any = None,
    ) -> dict[str, Any]:
        self.calls.append(("load", workspace.workspace_id, mode))
        return {"warehouse_path": "/synthetic/slackpipe.duckdb", "message_count": 3}

    def validate_archive(
        self, *, workspace: WorkspaceDescriptor, archive: Any
    ) -> dict[str, Any]:
        self.calls.append(("check_archive", workspace.workspace_id, ""))
        return {"passed": True, "row_count": 3}

    def validate_workspace(
        self, *, workspace: WorkspaceDescriptor, warehouse: Any, archive: Any
    ) -> dict[str, Any]:
        self.calls.append(("check_warehouse", workspace.workspace_id, ""))
        return {"passed": True, "message_count": 3}

    def backfill_attachments(
        self, *, workspace: WorkspaceDescriptor, warehouse: Any, run_id: str, runtime: Any
    ) -> dict[str, Any]:
        self.calls.append(("attachments", workspace.workspace_id, ""))
        return {"passed": True, "file_count": 2}


def descriptor(workspace_id: str) -> WorkspaceDescriptor:
    return WorkspaceDescriptor(
        workspace_id=workspace_id,
        url=f"https://{workspace_id}.slack.com",
    )


def test_legacy_workspace_secret_environment_is_not_discovered() -> None:
    assert discover_workspace_descriptors(
        {
            "SLACKPIPE_WORKSPACES": "KMNR",
            "SLACKPIPE_WORKSPACE_KMNR_TOKEN": "xoxp-synthetic-secret",
            "SLACKPIPE_WORKSPACE_KMNR_COOKIE": "synthetic-cookie",
        }
    ) == ()


def test_code_location_discovers_workspace_at_definitions_load(monkeypatch) -> None:
    repository = load_defs().get_repository_def()

    assert "kmnr_acceptance" not in {job.name for job in repository.get_all_jobs()}
    assert repository.schedule_defs == []


def test_workspace_file_discovery_is_secret_free(tmp_path: Path) -> None:
    config = tmp_path / "workspaces.env"
    config.write_text(
        "WORKSPACE=KMNR 89.7 FM\n"
        "CHANNEL_URL=https://kmnr.slack.com\n"
        "SLACK_TOKEN=xoxp-synthetic-secret\n"
        "SLACK_COOKIE=synthetic-cookie\n",
        encoding="utf-8",
    )

    discovered = discover_workspace_descriptors(
        {"SLACKPIPE_WORKSPACE_FILE": str(config)}
    )

    assert [workspace.workspace_id for workspace in discovered] == ["KMNR 89.7 FM"]
    assert "synthetic-secret" not in repr(discovered)
    assert "synthetic-cookie" not in repr(discovered)


def test_definitions_have_per_workspace_assets_checks_jobs_and_no_schedules() -> None:
    loaded = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )

    Definitions.validate_loadable(loaded)
    repository = loaded.get_repository_def()
    asset_keys = {
        key.to_user_string()
        for assets_def in repository.asset_graph.assets_defs
        for key in assets_def.keys
    }
    check_keys = {key.to_user_string() for key in repository.asset_graph.asset_check_keys}
    job_names = {job.name for job in repository.get_all_jobs()}

    assert asset_keys == {
        "kmnr/raw_slackdump",
        "kmnr/canonical_duckdb",
        "kmnr/attachments",
        "other/raw_slackdump",
        "other/canonical_duckdb",
        "other/attachments",
    }
    assert check_keys == {
        "kmnr/raw_slackdump:archive_valid",
        "kmnr/canonical_duckdb:canonical_integrity",
        "other/raw_slackdump:archive_valid",
        "other/canonical_duckdb:canonical_integrity",
        "kmnr/attachments:attachment_integrity",
        "other/attachments:attachment_integrity",
    }
    assert {
        "kmnr_acceptance",
        "kmnr_ingest_once",
        "kmnr_attachments_once",
        "other_acceptance",
        "other_ingest_once",
        "other_attachments_once",
    }.issubset(job_names)
    assert "all_workspaces_extract" not in job_names
    assert "all_workspaces_canonical" not in job_names
    # No per-workspace schedules: the nightly rollout schedule is the only
    # recurring trigger (and only defined for deferred-backfill rollouts).
    assert repository.schedule_defs == []


def test_initial_and_incremental_workflow_types() -> None:
    loaded = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )
    repository = loaded.get_repository_def()
    jobs = {job.name: job for job in repository.get_all_jobs()}

    assert jobs["kmnr_ingest_once"].tags[MODE_TAG] == "initial"
    assert jobs["kmnr_incremental"].tags[MODE_TAG] == "incremental"


def test_solo_workspace_jobs_skip_serial_tag_but_rollout_jobs_keep_it() -> None:
    from slackpipe.orchestration import SERIAL_TAG

    loaded = build_definitions(
        environ={"SLACKPIPE_SOLO_WORKSPACES": "other"},
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )
    jobs = {job.name: job for job in loaded.get_repository_def().get_all_jobs()}

    for name in (
        "other_ingest_once",
        "other_incremental",
        "other_acceptance",
        "other_attachments_once",
    ):
        assert SERIAL_TAG not in jobs[name].tags
        assert jobs[name].tags["lane"] == "slackpipe"
    for name in (
        "kmnr_ingest_once",
        "kmnr_incremental",
        "kmnr_acceptance",
        "kmnr_attachments_once",
    ):
        assert jobs[name].tags[SERIAL_TAG] == "true"
        assert jobs[name].tags["lane"] == "slackpipe"


def test_accepted_attachment_sizes_parsing() -> None:
    import pytest

    assert SlackpipeRuntimeResource().accepted_attachment_sizes == {}
    parsed = SlackpipeRuntimeResource(
        attachment_size_accept="F0123456789=70095357, FMP4=7"
    ).accepted_attachment_sizes
    assert parsed == {"F0123456789": 70095357, "FMP4": 7}
    with pytest.raises(ValueError, match="F0123456789=12345"):
        SlackpipeRuntimeResource(attachment_size_accept="bogus").accepted_attachment_sizes
    with pytest.raises(ValueError, match="must be positive"):
        SlackpipeRuntimeResource(attachment_size_accept="F0123456789=0").accepted_attachment_sizes


def test_kmnr_acceptance_executes_only_kmnr_flow_and_both_checks() -> None:
    helpers = StubHelpers()
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=helpers,
    )

    result = loaded.resolve_job_def("kmnr_acceptance").execute_in_process()

    assert result.success
    assert {call[1] for call in helpers.calls} == {"kmnr"}
    assert ("extract", "kmnr", "acceptance") in helpers.calls
    assert ("load", "kmnr", "acceptance") in helpers.calls
    evaluations = [
        event.event_specific_data
        for event in result.all_events
        if event.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    assert len(evaluations) == 3
    assert all(evaluation.passed for evaluation in evaluations)
    materializations = result.asset_materializations_for_node("kmnr__raw_slackdump")
    assert "token" not in materializations[0].metadata


def test_rollout_jobs_split_raw_and_canonical_phases() -> None:
    helpers = StubHelpers()
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("kmnr")],
        helpers=helpers,
    )

    extract = loaded.resolve_job_def("all_workspaces_extract")
    canonical = loaded.resolve_job_def("all_workspaces_canonical")

    assert extract.tags == {"lane": "slackpipe", "slackpipe/phase": "raw", MODE_TAG: "incremental"}
    assert canonical.tags == {
        "lane": "slackpipe",
        SERIAL_TAG: "true",
        "slackpipe/phase": "canonical",
        MODE_TAG: "full",
    }


def test_nightly_rollout_schedule_targets_extract_and_starts_stopped() -> None:
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("kmnr")],
        helpers=StubHelpers(),
    )
    schedules = {
        schedule.name: schedule
        for schedule in loaded.get_repository_def().schedule_defs
    }

    assert set(schedules) == {"nightly_rollout_schedule"}
    nightly = schedules["nightly_rollout_schedule"]
    assert nightly.job_name == "all_workspaces_extract"
    assert nightly.cron_schedule == "0 1 * * *"
    assert nightly.default_status is DefaultScheduleStatus.STOPPED


def test_all_workspace_extract_runs_all_discovered_raw_assets() -> None:
    helpers = StubHelpers()
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=helpers,
    )

    instance = DagsterInstance.ephemeral()
    for workspace_id in ("kmnr", "other"):
        first = loaded.resolve_job_def(f"{workspace_id}_ingest_once").execute_in_process(
            instance=instance
        )
        assert first.success
    helpers.calls.clear()

    result = loaded.resolve_job_def("all_workspaces_extract").execute_in_process(
        instance=instance
    )

    assert result.success
    assert {
        (stage, workspace, mode)
        for stage, workspace, mode in helpers.calls
        if stage in {"extract", "load"}
    } == {
        ("extract", "kmnr", "incremental"),
        ("extract", "other", "incremental"),
    }


def _node_names(job: Any) -> set[str]:
    return {node.name for node in job.nodes}


def test_solo_workspaces_removes_workspace_from_extract_but_keeps_its_jobs() -> None:
    helpers = StubHelpers()
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
            "SLACKPIPE_SOLO_WORKSPACES": "other",
        },
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=helpers,
    )

    instance = DagsterInstance.ephemeral()
    first = loaded.resolve_job_def("kmnr_ingest_once").execute_in_process(
        instance=instance
    )
    assert first.success
    helpers.calls.clear()

    result = loaded.resolve_job_def("all_workspaces_extract").execute_in_process(
        instance=instance
    )

    assert result.success
    assert {
        (stage, workspace, mode)
        for stage, workspace, mode in helpers.calls
        if stage in {"extract", "load"}
    } == {("extract", "kmnr", "incremental")}
    job_names = {job.name for job in loaded.get_repository_def().get_all_jobs()}
    assert "other_ingest_once" in job_names
    assert "other_attachments_once" in job_names
    assert "other" in loaded.resolve_job_def("all_workspaces_extract").description


def test_solo_workspaces_removes_workspace_from_canonical_selection() -> None:
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
            "SLACKPIPE_SOLO_WORKSPACES": "other",
        },
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )

    canonical = loaded.resolve_job_def("all_workspaces_canonical")

    assert "kmnr__canonical_duckdb" in _node_names(canonical)
    assert "other__canonical_duckdb" not in _node_names(canonical)


def test_solo_workspaces_matches_ids_and_slugs_case_insensitively() -> None:
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
            "SLACKPIPE_SOLO_WORKSPACES": "KMNR 89.7 FM",
        },
        workspaces=[descriptor("KMNR 89.7 FM"), descriptor("other")],
        helpers=StubHelpers(),
    )

    extract = loaded.resolve_job_def("all_workspaces_extract")

    assert "kmnr_89_7_fm__raw_slackdump" not in _node_names(extract)
    assert "other__raw_slackdump" in _node_names(extract)


def test_solo_workspaces_ignores_unknown_slugs_and_falls_back_when_all_excluded() -> None:
    unknown = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
            "SLACKPIPE_SOLO_WORKSPACES": "no-such-workspace",
        },
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )
    assert "other__raw_slackdump" in _node_names(
        unknown.resolve_job_def("all_workspaces_extract")
    )

    exclude_all = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
            "SLACKPIPE_SOLO_WORKSPACES": "kmnr, other",
        },
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )
    assert {
        "kmnr__raw_slackdump",
        "other__raw_slackdump",
    }.issubset(
        _node_names(exclude_all.resolve_job_def("all_workspaces_extract"))
    )


def test_solo_workspaces_parses_comma_separated_values() -> None:
    runtime = SlackpipeRuntimeResource.from_environ(
        {"SLACKPIPE_SOLO_WORKSPACES": " Leadership , kmnr_89_7_fm,,"}
    )

    assert runtime.solo_workspace_slugs == ("leadership", "kmnr_89_7_fm")
    assert SlackpipeRuntimeResource.from_environ({}).solo_workspace_slugs == ()


def test_nightly_rollout_is_the_only_schedule() -> None:
    # Per-workspace incremental + full-sweep schedules were deleted: the
    # nightly rollout is the single recurring trigger.
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
            "SLACKPIPE_SOLO_WORKSPACES": "other",
        },
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )

    statuses = {
        schedule.name: schedule.default_status
        for schedule in loaded.get_repository_def().schedule_defs
    }

    assert statuses == {
        "nightly_rollout_schedule": DefaultScheduleStatus.STOPPED,
    }


def test_sensors_use_two_minute_minimum_interval() -> None:
    # Chained phases run minutes-to-hours: a 120s floor cuts Postgres tick
    # writes ~75% versus the 30s default at negligible chain latency.
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("kmnr")],
        helpers=StubHelpers(),
    )

    intervals = {
        sensor.name: sensor.minimum_interval_seconds for sensor in loaded.sensors
    }

    assert intervals == {
        "slackpipe_run_failure_metrics": 120,
        "slackpipe_rollout_coordinator": 120,
        "slackpipe_new_workspace_sensor": 300,
    }


def _seed_initial_ingests(loaded: Definitions, instance: DagsterInstance) -> None:
    for job in loaded.get_repository_def().get_all_jobs():
        if job.name.endswith("_ingest_once"):
            seeded = loaded.resolve_job_def(job.name).execute_in_process(
                instance=instance
            )
            assert seeded.success


def _successful_run_context(
    loaded: Definitions,
    job_name: str,
    *,
    instance: DagsterInstance,
    tags: dict[str, str] | None = None,
    runtime: SlackpipeRuntimeResource,
):
    _seed_initial_ingests(loaded, instance)
    result = loaded.resolve_job_def(job_name).execute_in_process(
        instance=instance,
        tags=tags,
    )
    assert result.success
    return build_run_status_sensor_context(
        sensor_name="slackpipe_rollout_coordinator",
        dagster_event=next(event for event in result.all_events if event.is_job_success),
        dagster_instance=instance,
        dagster_run=result.dagster_run,
        resources={"slackpipe_runtime": runtime},
        repository_def=loaded.get_repository_def(),
    )


def _rollout_sensor(loaded: Definitions):
    return next(
        sensor
        for sensor in loaded.sensors
        if sensor.name == "slackpipe_rollout_coordinator"
    )


def test_rollout_sensor_advances_extract_to_first_attachment(
    monkeypatch,
) -> None:
    runtime = SlackpipeRuntimeResource(
        deferred_backfills_enabled=True,
    )
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("kmnr")],
        helpers=StubHelpers(),
        runtime=runtime,
    )
    monkeypatch.setattr(
        "slackpipe.orchestration._attachment_order",
        lambda _runtime, _slugs: ("kmnr",),
    )
    context = _successful_run_context(
        loaded,
        "all_workspaces_extract",
        instance=DagsterInstance.ephemeral(),
        tags={ROLLOUT_ID_TAG: "rollout-1"},
        runtime=runtime,
    )

    request = _rollout_sensor(loaded)(context)

    assert request.job_name == "kmnr_attachments_once"
    assert request.run_key == "rollout-1:attachments:0"
    assert request.tags == {
        ROLLOUT_ID_TAG: "rollout-1",
        ATTACHMENT_ORDER_TAG: "kmnr",
        ATTACHMENT_INDEX_TAG: "0",
    }


def test_rollout_sensor_orders_attachments_and_advances_one_at_a_time(
    monkeypatch,
) -> None:
    runtime = SlackpipeRuntimeResource(
        deferred_backfills_enabled=True,
    )
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("small"), descriptor("large")],
        helpers=StubHelpers(),
        runtime=runtime,
    )
    monkeypatch.setattr(
        "slackpipe.orchestration._attachment_order",
        lambda _runtime, _slugs: ("small", "large"),
    )
    instance = DagsterInstance.ephemeral()
    _seed_initial_ingests(loaded, instance)
    extract_result = loaded.resolve_job_def("all_workspaces_extract").execute_in_process(
        instance=instance
    )
    assert extract_result.success
    extract_context = _successful_run_context(
        loaded,
        "all_workspaces_extract",
        instance=instance,
        tags={ROLLOUT_ID_TAG: "rollout-2"},
        runtime=runtime,
    )

    first = _rollout_sensor(loaded)(extract_context)

    assert first.job_name == "small_attachments_once"
    assert first.run_key == "rollout-2:attachments:0"
    assert first.tags == {
        ROLLOUT_ID_TAG: "rollout-2",
        ATTACHMENT_ORDER_TAG: "small,large",
        ATTACHMENT_INDEX_TAG: "0",
    }

    first_context = _successful_run_context(
        loaded,
        first.job_name,
        instance=instance,
        tags=first.tags,
        runtime=runtime,
    )
    second = _rollout_sensor(loaded)(first_context)

    assert second.job_name == "large_attachments_once"
    assert second.run_key == "rollout-2:attachments:1"
    assert second.tags == {
        ROLLOUT_ID_TAG: "rollout-2",
        ATTACHMENT_ORDER_TAG: "small,large",
        ATTACHMENT_INDEX_TAG: "1",
    }

    second_context = _successful_run_context(
        loaded,
        second.job_name,
        instance=instance,
        tags=second.tags,
        runtime=runtime,
    )
    canonical = _rollout_sensor(loaded)(second_context)

    assert canonical.job_name == "all_workspaces_canonical"
    assert canonical.run_key == "rollout-2:canonical"
    assert canonical.tags == {ROLLOUT_ID_TAG: "rollout-2"}

    canonical_context = _successful_run_context(
        loaded,
        "all_workspaces_canonical",
        instance=instance,
        tags={ROLLOUT_ID_TAG: "rollout-2"},
        runtime=runtime,
    )
    assert _rollout_sensor(loaded)(canonical_context) is None


def test_rollout_sensor_slug_maps_display_name_order_entries(monkeypatch) -> None:
    """Regression: Sep-2026 the sensor requested `YYJ Tech_attachments_once`
    (raw archive directory name) instead of `yyj_tech_attachments_once`,
    failing the tick. Order entries must be slug-mapped to job names on
    both the extract hop and the tag-readback hop."""
    runtime = SlackpipeRuntimeResource(
        deferred_backfills_enabled=True,
    )
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("YYJ Tech"), descriptor("CTO Craft")],
        helpers=StubHelpers(),
        runtime=runtime,
    )
    monkeypatch.setattr(
        "slackpipe.orchestration._attachment_order",
        lambda _runtime, _slugs: ("YYJ Tech", "CTO Craft"),
    )
    instance = DagsterInstance.ephemeral()
    context = _successful_run_context(
        loaded,
        "all_workspaces_extract",
        instance=instance,
        tags={ROLLOUT_ID_TAG: "rollout-3"},
        runtime=runtime,
    )

    first = _rollout_sensor(loaded)(context)

    assert first.job_name == "yyj_tech_attachments_once"
    assert first.run_key == "rollout-3:attachments:0"

    first_context = _successful_run_context(
        loaded,
        first.job_name,
        instance=instance,
        tags=first.tags,
        runtime=runtime,
    )
    second = _rollout_sensor(loaded)(first_context)

    assert second.job_name == "cto_craft_attachments_once"
    assert second.run_key == "rollout-3:attachments:1"


def test_rollout_sensor_ignores_uncoordinated_attachment_run() -> None:
    runtime = SlackpipeRuntimeResource(
        deferred_backfills_enabled=True,
    )
    loaded = build_definitions(
        environ={
            "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS": "true",
        },
        workspaces=[descriptor("kmnr")],
        helpers=StubHelpers(),
        runtime=runtime,
    )
    instance = DagsterInstance.ephemeral()
    acceptance = loaded.resolve_job_def("kmnr_acceptance").execute_in_process(
        instance=instance
    )
    assert acceptance.success
    context = _successful_run_context(
        loaded,
        "kmnr_attachments_once",
        instance=instance,
        runtime=runtime,
    )

    assert _rollout_sensor(loaded)(context) is None


def test_assets_have_safe_properties_retry_and_rich_materialization_metadata() -> None:
    helpers = StubHelpers()
    loaded = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr")],
        helpers=helpers,
    )
    repository = loaded.get_repository_def()
    raw = next(
        assets_def
        for assets_def in repository.asset_graph.assets_defs
        if any(key.to_user_string() == "kmnr/raw_slackdump" for key in assets_def.keys)
        and len(assets_def.keys) == 1
    )

    assert raw.descriptions_by_key[raw.key]
    assert raw.code_versions_by_key[raw.key] == "1"
    assert raw.tags_by_key[raw.key]["slackpipe/workspace"] == "kmnr"
    assert raw.node_def.retry_policy.max_retries == 3
    assert raw.node_def.retry_policy.backoff is Backoff.EXPONENTIAL
    assert raw.node_def.retry_policy.jitter is Jitter.FULL

    result = loaded.resolve_job_def("kmnr_acceptance").execute_in_process()
    materialization = result.asset_materializations_for_node("kmnr__raw_slackdump")[0]
    assert {"archive_path", "message_count", "mode", "workspace_id"}.issubset(
        materialization.metadata
    )
    assert "token" not in materialization.metadata


def test_failure_sensor_status_tracks_safe_pushgateway_configuration() -> None:
    disabled = build_definitions(
        workspaces=[descriptor("kmnr")], helpers=StubHelpers()
    ).get_repository_def()
    enabled = build_definitions(
        runtime=SlackpipeRuntimeResource(
            pushgateway_url="http://pushgateway:9091",
        ),
        workspaces=[descriptor("kmnr")],
        helpers=StubHelpers(),
    ).get_repository_def()

    def _failure_sensor(repository):
        return next(
            sensor
            for sensor in repository.sensor_defs
            if sensor.name == "slackpipe_run_failure_metrics"
        )

    assert _failure_sensor(disabled).default_status is DefaultSensorStatus.STOPPED
    assert _failure_sensor(enabled).default_status is DefaultSensorStatus.RUNNING


def test_assets_and_checks_use_phase_appropriate_pools() -> None:
    loaded = build_definitions(workspaces=[descriptor("kmnr")], helpers=StubHelpers())
    repository = loaded.get_repository_def()

    pools = {
        next(iter(assets_def.keys)).path[-1]: assets_def.node_def.pool
        for assets_def in repository.asset_graph.assets_defs
        if assets_def.is_executable and assets_def.keys
    }
    assert pools == {
        "raw_slackdump": "slackpipe_raw_kmnr",
        "canonical_duckdb": SERIAL_POOL,
        "attachments": ATTACHMENT_POOL,
    }
    check_pools = {checks_def.node_def.pool for checks_def in loaded.asset_checks or []}
    assert check_pools == {"slackpipe_raw_kmnr", SERIAL_POOL, ATTACHMENT_POOL}


def test_workspace_lock_path_is_stable_and_workspace_scoped() -> None:
    assert _workspace_lock_path("/var/lib/slackpipe/slackdump.lock", "Rands Leadership") == (
        "/var/lib/slackpipe/slackdump.lock.rands_leadership"
    )


def test_instance_config_serializes_and_cleans_stale_slots() -> None:
    config, custom_instance_class = dagster_instance_config(
        str(Path(__file__).parents[1] / "deployment")
    )

    assert custom_instance_class is None
    runs = config["concurrency"]["runs"]
    assert runs["max_concurrent_runs"] == 4
    assert runs["tag_concurrency_limits"] == [
        {"key": "slackpipe/phase", "value": "raw", "limit": 2},
        {"key": "slackpipe/phase", "value": "canonical", "limit": 1},
        {"key": SERIAL_TAG, "value": "true", "limit": 1},
        {"key": "lane", "value": "slackpipe", "limit": 3},
        {"key": "lane", "value": "slackquery", "limit": 1},
    ]
    assert config["concurrency"]["pools"] == {
        "default_limit": 1,
        "granularity": "op",
    }
    assert config["run_monitoring"]["enabled"] is True
    assert config["run_monitoring"]["free_slots_after_run_end_seconds"] == 60


class _FailingHelpers(StubHelpers):
    def extract_workspace(self, *, workspace, mode, run_id, runtime, status=None):
        raise RuntimeError("synthetic extract failure")


def _sensor_def(loaded):
    sensors = {
        sensor.name: sensor for sensor in loaded.get_repository_def().sensor_defs
    }
    return sensors["slackpipe_new_workspace_sensor"]


def test_new_workspace_sensor_fires_ingest_once_for_brand_new_workspace() -> None:
    from dagster import build_sensor_context

    loaded = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )
    result = _sensor_def(loaded).evaluate_tick(
        build_sensor_context(instance=DagsterInstance.ephemeral())
    )

    assert len(result.run_requests) == 1
    request = result.run_requests[0]
    assert request.job_name == "kmnr_ingest_once"
    assert request.run_key == "auto-initial-kmnr"


def test_new_workspace_sensor_never_refires_after_any_attempt() -> None:
    from dagster import build_sensor_context

    succeeded = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr")],
        helpers=StubHelpers(),
    )
    good_instance = DagsterInstance.ephemeral()
    first = succeeded.resolve_job_def("kmnr_ingest_once").execute_in_process(
        instance=good_instance
    )
    assert first.success
    assert (
        _sensor_def(succeeded).evaluate_tick(
            build_sensor_context(instance=good_instance)
        ).run_requests
        == []
    )

    failed = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr")],
        helpers=_FailingHelpers(),
    )
    bad_instance = DagsterInstance.ephemeral()
    attempt = failed.resolve_job_def("kmnr_ingest_once").execute_in_process(
        instance=bad_instance, raise_on_error=False
    )
    assert not attempt.success
    assert (
        _sensor_def(failed).evaluate_tick(
            build_sensor_context(instance=bad_instance)
        ).run_requests
        == []
    )


def test_new_workspace_sensor_skips_solo_workspaces() -> None:
    from dagster import build_sensor_context

    loaded = build_definitions(
        environ={"SLACKPIPE_SOLO_WORKSPACES": "other"},
        workspaces=[descriptor("kmnr"), descriptor("other")],
        helpers=StubHelpers(),
    )
    result = _sensor_def(loaded).evaluate_tick(
        build_sensor_context(instance=DagsterInstance.ephemeral())
    )

    assert [request.job_name for request in result.run_requests] == [
        "kmnr_ingest_once"
    ]


def test_incremental_blocked_until_initial_ingest_succeeds() -> None:
    helpers = StubHelpers()
    loaded = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr")],
        helpers=helpers,
    )
    instance = DagsterInstance.ephemeral()

    blocked = loaded.resolve_job_def("kmnr_incremental").execute_in_process(
        instance=instance, raise_on_error=False
    )
    assert not blocked.success
    assert helpers.calls == []

    first = loaded.resolve_job_def("kmnr_ingest_once").execute_in_process(
        instance=instance
    )
    assert first.success
    helpers.calls.clear()

    allowed = loaded.resolve_job_def("kmnr_incremental").execute_in_process(
        instance=instance
    )
    assert allowed.success
    assert ("extract", "kmnr", "incremental") in helpers.calls
    assert ("load", "kmnr", "incremental") in helpers.calls


def test_succeeded_checkpoint_grandfathers_legacy_workspace(tmp_path: Path) -> None:
    import duckdb

    from slackpipe.orchestration import _has_succeeded_canonical_checkpoint

    warehouse = tmp_path / "legacy.duckdb"
    connection = duckdb.connect(str(warehouse))
    connection.execute(
        "CREATE TABLE workspaces (workspace_id VARCHAR, workspace_slug VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE ingestion_checkpoints "
        "(workspace_id VARCHAR, successful_run_id VARCHAR)"
    )
    connection.execute("CREATE TABLE ingestion_runs (run_id VARCHAR, status VARCHAR)")
    connection.execute(
        "INSERT INTO workspaces VALUES (?, ?)", ["TLEGACY", "kmnr"]
    )
    connection.execute(
        "INSERT INTO ingestion_checkpoints VALUES (?, ?)", ["TLEGACY", "run-1"]
    )
    connection.execute(
        "INSERT INTO ingestion_runs VALUES (?, ?)", ["run-1", "succeeded"]
    )
    connection.close()

    assert _has_succeeded_canonical_checkpoint(str(warehouse), "kmnr") is True
    assert _has_succeeded_canonical_checkpoint(str(warehouse), "KMNR") is True
    assert _has_succeeded_canonical_checkpoint(str(warehouse), "other") is False
    missing = tmp_path / "missing.duckdb"
    assert _has_succeeded_canonical_checkpoint(str(missing), "kmnr") is False


class _AttachmentFailingHelpers(StubHelpers):
    def backfill_attachments(self, *, workspace, warehouse, run_id, runtime, status=None):
        self.calls.append(("attachments", workspace.workspace_id, ""))
        return {"passed": False, "file_count": 2}


def test_failing_attachment_check_does_not_fail_job() -> None:
    helpers = _AttachmentFailingHelpers()
    loaded = build_definitions(
        environ={},
        workspaces=[descriptor("kmnr")],
        helpers=helpers,
    )

    result = loaded.resolve_job_def("kmnr_attachments_once").execute_in_process()

    assert result.success
    evaluations = [
        event.event_specific_data
        for event in result.all_events
        if event.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    assert len(evaluations) == 1
    assert evaluations[0].passed is False
