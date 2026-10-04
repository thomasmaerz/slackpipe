from dagster import Definitions

from slackpipe.definitions import defs


def test_definitions_load_with_only_safe_failure_sensor() -> None:
    loaded = defs()

    Definitions.validate_loadable(loaded)
    repository = loaded.get_repository_def()
    assert repository.schedule_defs == []
    assert [sensor.name for sensor in repository.sensor_defs] == [
        "slackpipe_new_workspace_sensor",
        "slackpipe_run_failure_metrics",
    ]
