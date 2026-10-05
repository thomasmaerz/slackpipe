from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def _yaml(path: str) -> dict[str, object]:
    return yaml.safe_load((ROOT / path).read_text())


def test_workspace_loads_owned_code_locations() -> None:
    workspace = _yaml("deployment/workspace.yaml")
    locations = {
        entry["grpc_server"]["location_name"]: entry["grpc_server"]
        for entry in workspace["load_from"]
    }

    assert locations == {
        "slackpipe": {
            "host": "code-server",
            "port": 4000,
            "location_name": "slackpipe",
        },
        "slackquery": {
            "host": "slackquery-code-server",
            "port": 4001,
            "location_name": "slackquery",
        },
    }


def test_slackquery_code_server_compose_contract() -> None:
    compose = _yaml("compose.yaml")
    services = compose["services"]
    service = services["slackquery-code-server"]

    assert service["image"] == "${SLACKQUERY_IMAGE:-slackquery:local}"
    assert service["build"]["context"] == "${SLACKQUERY_BUILD_CONTEXT:-../slackquery}"
    assert service["env_file"] == [
        "${SLACKQUERY_ENV_FILE:-../slackquery/.env}"
    ]
    assert service["command"] == [
        "dagster",
        "code-server",
        "start",
        "-h",
        "0.0.0.0",
        "-p",
        "4001",
        "-m",
        "slackquery.definitions",
        "-a",
        "definitions",
    ]

    mounts = {volume["target"]: volume for volume in service["volumes"]}
    assert mounts["/var/lib/dagster"]["source"] == "dagster-data"
    assert mounts["/opt/slackpipe/data/slackpipe.duckdb"]["read_only"] is True
    assert "read_only" not in mounts["/opt/slackquery/state"]
    assert "read_only" not in mounts["/opt/slackquery/artifacts"]
    assert "read_only" not in mounts["/opt/slackquery/extensions"]
    assert mounts["/opt/slackquery/state"]["bind"]["create_host_path"] is True
    assert mounts["/opt/slackquery/extensions"]["bind"]["create_host_path"] is True
    assert mounts["/opt/slackquery/artifacts"]["bind"]["create_host_path"] is True
    assert service["read_only"] is True
    assert service["healthcheck"]["test"][-2:] == ["-p", "4001"]
    assert service["deploy"]["resources"]["limits"] == {
        "cpus": "2.0",
        "memory": "3G",
    }

    for dependent in ("webserver", "daemon"):
        assert services[dependent]["depends_on"]["slackquery-code-server"] == {
            "condition": "service_healthy"
        }
