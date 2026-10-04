from dagster import Definitions, definitions

from slackpipe.orchestration import build_definitions


@definitions
def defs() -> Definitions:
    return build_definitions()
