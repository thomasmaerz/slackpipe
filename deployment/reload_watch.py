"""Poll the workspace credential file and reload the Dagster code location on change.

Runs as a lightweight compose sidecar. Watches ``SLACKPIPE_WORKSPACE_FILE``
(a read-only bind of the host .env) by content hash; when the hash changes it
POSTs a ``reloadCodeLocation`` GraphQL mutation to the webserver. New
``WORKSPACE=`` blocks therefore become assets/jobs/schedules without manual
intervention.

Only stdlib is used so the sidecar runs on the base image with no extra deps.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import urllib.request

LOG = logging.getLogger("slackpipe.reload_watch")

WORKSPACE_FILE = os.environ.get(
    "SLACKPIPE_WORKSPACE_FILE", "/run/secrets/slackpipe/workspaces.env"
)
WEBSERVER_URL = os.environ.get("SLACKPIPE_WEBSERVER_URL", "http://webserver:3000").rstrip("/")
LOCATION = os.environ.get("SLACKPIPE_LOCATION_NAME", "slackpipe")
INTERVAL = float(os.environ.get("SLACKPIPE_RELOAD_POLL_SECONDS", "60"))
TIMEOUT = float(os.environ.get("SLACKPIPE_RELOAD_HTTP_TIMEOUT_SECONDS", "10"))

_MUTATION = (
    "mutation { reloadRepositoryLocation(repositoryLocationName: %s) "
    "{ __typename } }"
)


def _file_hash(path: str) -> str | None:
    try:
        with open(path, "rb") as stream:
            return hashlib.sha256(stream.read()).hexdigest()
    except OSError:
        return None


def _reload() -> bool:
    payload = json.dumps({"query": _MUTATION % json.dumps(LOCATION)}).encode()
    request = urllib.request.Request(
        f"{WEBSERVER_URL}/graphql",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            body = response.read().decode("utf-8", "replace")
    except Exception as error:  # noqa: BLE001 - sidecar must never crash on transient failure
        LOG.warning("code-location reload request failed: %s", error)
        return False
    try:
        result = json.loads(body)
        typename = result["data"]["reloadRepositoryLocation"]["__typename"]
    except (KeyError, TypeError, ValueError):
        LOG.warning("code-location reload returned unexpected response: %s", body[:300])
        return False
    if typename not in {"RepositoryLocation", "WorkspaceLocationEntry"}:
        LOG.warning("code-location reload failed with result type %s", typename)
        return False
    LOG.info("reloaded code location %r after workspace file change", LOCATION)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    last = _file_hash(WORKSPACE_FILE)
    LOG.info("watching %s (poll %.0fs)", WORKSPACE_FILE, INTERVAL)
    backoff = INTERVAL
    while True:
        time.sleep(INTERVAL)
        current = _file_hash(WORKSPACE_FILE)
        if current is None or current == last:
            continue
        LOG.info("workspace file changed; requesting code-location reload")
        if _reload():
            last = current
            backoff = INTERVAL
        else:
            # Keep `last` unchanged so the next tick retries; sleep extra to
            # avoid hammering a struggling webserver.
            time.sleep(min(backoff, 600))
            backoff = min(backoff * 2, 600)


if __name__ == "__main__":
    main()
