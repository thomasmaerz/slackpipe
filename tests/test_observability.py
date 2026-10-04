from __future__ import annotations

import base64
from unittest.mock import patch

import pytest

from slackpipe.observability import (
    GroupingKey,
    PipelineMetrics,
    PushgatewayClient,
    render_prometheus,
)


def make_metrics(**overrides: object) -> PipelineMetrics:
    values: dict[str, object] = {
        "workspace": "acme",
        "stage": "canonicalize",
        "run_succeeded": True,
        "pipeline_completed_at": 1_700_000_000.0,
        "observed_at": 1_700_000_012.5,
        "source_latest_message_at": 1_699_999_900.0,
        "canonical_latest_message_at": 1_699_999_895.0,
        "source_row_count": 101,
        "canonical_row_count": 100,
        "rows_inserted": 3,
        "rows_updated": 2,
        "rows_deleted": 1,
        "run_duration_seconds": 8.25,
        "run_failures": 0,
        "lock_wait_seconds": 0.5,
        "archive_size_bytes": 4096,
        "attachment_backlog": 4,
        "workspace_discovered": True,
        "workspace_materialized": True,
    }
    values.update(overrides)
    return PipelineMetrics(**values)


def test_render_prometheus_includes_complete_bounded_snapshot() -> None:
    exposition = render_prometheus(make_metrics())

    expected_samples = {
        "slackpipe_run_success": "1",
        "slackpipe_last_success_timestamp_seconds": "1700000000",
        "slackpipe_pipeline_completed_timestamp_seconds": "1700000000",
        "slackpipe_pipeline_completion_lag_seconds": "12.5",
        "slackpipe_source_latest_message_timestamp_seconds": "1699999900",
        "slackpipe_canonical_latest_message_timestamp_seconds": "1699999895",
        "slackpipe_source_canonical_latest_message_lag_seconds": "5",
        "slackpipe_source_row_count": "101",
        "slackpipe_canonical_row_count": "100",
        "slackpipe_rows_inserted": "3",
        "slackpipe_rows_updated": "2",
        "slackpipe_rows_deleted": "1",
        "slackpipe_run_duration_seconds": "8.25",
        "slackpipe_run_failures": "0",
        "slackpipe_lock_wait_seconds": "0.5",
        "slackpipe_archive_size_bytes": "4096",
        "slackpipe_attachment_backlog": "4",
        "slackpipe_workspace_discovered": "1",
        "slackpipe_workspace_materialized": "1",
    }
    labels = '{workspace="acme",stage="canonicalize"}'
    for name, value in expected_samples.items():
        assert f"# TYPE {name} gauge" in exposition
        assert f"{name}{labels} {value}\n" in exposition

    sample_lines = [
        line for line in exposition.splitlines() if not line.startswith("#")
    ]
    assert all(line.count("=") == 2 for line in sample_lines)


def test_completion_lag_is_independent_of_message_age() -> None:
    metrics = make_metrics(
        pipeline_completed_at=2_000.0,
        observed_at=2_003.0,
        source_latest_message_at=100.0,
        canonical_latest_message_at=100.0,
    )

    assert metrics.pipeline_completion_lag_seconds == 3.0
    assert metrics.last_success_at == 2_000.0
    assert "slackpipe_pipeline_completion_lag_seconds" in render_prometheus(metrics)


def test_missing_message_timestamps_are_omitted() -> None:
    exposition = render_prometheus(
        make_metrics(
            source_latest_message_at=None,
            canonical_latest_message_at=None,
        )
    )

    assert "slackpipe_source_latest_message_timestamp_seconds" not in exposition
    assert "slackpipe_canonical_latest_message_timestamp_seconds" not in exposition
    assert "slackpipe_source_canonical_latest_message_lag_seconds" not in exposition
    assert "slackpipe_run_success" in exposition


def test_failed_run_preserves_explicit_previous_success() -> None:
    exposition = render_prometheus(
        make_metrics(
            run_succeeded=False,
            last_success_at=1_600_000_000.0,
            run_failures=1,
        )
    )

    labels = '{workspace="acme",stage="canonicalize"}'
    assert f"slackpipe_run_success{labels} 0" in exposition
    assert f"slackpipe_last_success_timestamp_seconds{labels} 1600000000" in exposition


def test_push_uses_atomic_put_and_base64_grouping_path() -> None:
    metrics = make_metrics(workspace="acme/team", stage="load prod")
    client = PushgatewayClient("https://push.example.test/root/")
    response = _context_manager_response()

    with patch(
        "slackpipe.observability.urllib.request.urlopen", return_value=response
    ) as urlopen:
        assert client.push(metrics) is True

    request = urlopen.call_args.args[0]
    expected_workspace = base64.urlsafe_b64encode(b"acme/team").decode("ascii")
    expected_stage = base64.urlsafe_b64encode(b"load prod").decode("ascii")
    assert request.method == "PUT"
    assert request.full_url == (
        "https://push.example.test/root/metrics/job@base64/c2xhY2twaXBl/"
        f"workspace@base64/{expected_workspace}/stage@base64/{expected_stage}"
    )
    assert request.get_header("Content-type") == (
        "text/plain; version=0.0.4; charset=utf-8"
    )
    assert request.data == render_prometheus(metrics).encode("utf-8")
    assert urlopen.call_args.kwargs == {"timeout": 10.0}


def test_unset_pushgateway_is_noop() -> None:
    client = PushgatewayClient.from_env({})

    with patch("slackpipe.observability.urllib.request.urlopen") as urlopen:
        assert client.enabled is False
        assert client.push(make_metrics()) is False
        assert client.delete(GroupingKey("acme", "canonicalize")) is False
        assert client.delete_stale_workspace_groups(
            active_workspaces={"acme"},
            known_workspaces={"acme", "old"},
            stage="canonicalize",
        ) == ()

    urlopen.assert_not_called()


def test_blank_pushgateway_url_is_noop() -> None:
    assert PushgatewayClient("  ").enabled is False


def test_invalid_pushgateway_url_is_rejected_without_exposing_it() -> None:
    secret_url = "file:///tmp/secret"

    with pytest.raises(ValueError, match="absolute HTTP\\(S\\) URL") as error:
        PushgatewayClient(secret_url)

    assert secret_url not in str(error.value)


def test_delete_stale_workspace_groups_uses_delete_per_stale_group() -> None:
    client = PushgatewayClient("http://pushgateway:9091")
    response = _context_manager_response()

    with patch(
        "slackpipe.observability.urllib.request.urlopen", return_value=response
    ) as urlopen:
        deleted = client.delete_stale_workspace_groups(
            active_workspaces={"current"},
            known_workspaces={"old-b", "current", "old-a"},
            stage="archive",
        )

    assert deleted == ("old-a", "old-b")
    assert [call.args[0].method for call in urlopen.call_args_list] == [
        "DELETE",
        "DELETE",
    ]
    assert all(call.args[0].data is None for call in urlopen.call_args_list)


def test_grouping_override_must_match_metric_labels() -> None:
    client = PushgatewayClient("http://pushgateway:9091")

    with pytest.raises(ValueError, match="grouping_key must match"):
        client.push(make_metrics(), grouping_key=GroupingKey("other", "canonicalize"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_row_count", -1),
        ("run_duration_seconds", float("nan")),
        ("pipeline_completed_at", float("inf")),
        ("run_succeeded", 1),
        ("workspace", ""),
    ],
)
def test_invalid_metric_values_are_rejected(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        make_metrics(**{field: value})


def _context_manager_response():
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    return Response()
