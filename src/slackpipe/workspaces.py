"""Secure discovery of Slack workspace credentials from repeated env blocks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
import re
from urllib.parse import urlsplit


_FIELDS = ("WORKSPACE", "CHANNEL_URL", "SLACK_TOKEN", "SLACK_COOKIE")
_ATTACHMENTS_SETTING = "SLACKPIPE_ATTACHMENTS_INCREMENTAL"
_GLOBAL_SETTINGS = {_ATTACHMENTS_SETTING, "SLACKPIPE_ENABLE_DEFERRED_BACKFILLS"}
_ASSIGNMENT = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")


class WorkspaceConfigurationError(ValueError):
    """Raised when workspace configuration is incomplete or unsafe."""


class WorkspaceSelectionError(LookupError):
    """Raised when a workspace selector is absent or ambiguous."""


@dataclass(frozen=True)
class WorkspaceDescriptor:
    workspace_id: str
    url: str


@dataclass(frozen=True, repr=False)
class Workspace:
    workspace_id: str
    url: str
    token: str = field(repr=False)
    cookie: str = field(repr=False)

    @property
    def slug(self) -> str:
        """Backwards-compatible alias for the single workspace identity."""
        return self.workspace_id

    @property
    def name(self) -> str:
        """Backwards-compatible alias for the single workspace identity."""
        return self.workspace_id

    @property
    def descriptor(self) -> WorkspaceDescriptor:
        return WorkspaceDescriptor(workspace_id=self.workspace_id, url=self.url)

    def __repr__(self) -> str:
        return f"Workspace(workspace_id={self.workspace_id!r}, url={self.url!r})"


def discover_workspaces(path: str | Path) -> tuple[Workspace, ...]:
    """Read and parse workspace blocks from ``path`` without retaining the text."""
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as stream:
            return parse_workspace_blocks(stream.read())
    except OSError as error:
        raise WorkspaceConfigurationError(
            f"cannot read workspace configuration {source}"
        ) from error


def parse_workspace_blocks(content: str) -> tuple[Workspace, ...]:
    """Parse repeated WORKSPACE/URL and credential assignment blocks.

    ``WORKSPACE`` is the single identity: the real Slack workspace name used
    verbatim as the archive directory name, the Dagster ``workspace_id``, and
    the DuckDB ``workspace_slug``. Slackdump receives the lowercased form as
    its credential label (it lowercases on ``workspace new`` but resolves
    ``-workspace`` case-sensitively). Comment lines (starting with ``#``)
    are documentation only and never parsed.
    """
    blocks: list[dict[str, str]] = []
    current: dict[str, str] = {}

    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        match = _ASSIGNMENT.match(line)
        if not match:
            raise WorkspaceConfigurationError(
                f"invalid assignment at line {line_number}"
            )
        key, raw_value = match.groups()
        if key in _GLOBAL_SETTINGS:
            continue
        if key not in _FIELDS:
            continue
        if key == "WORKSPACE" and current:
            blocks.append(current)
            current = {}
        elif not current and key != "WORKSPACE":
            raise WorkspaceConfigurationError(
                f"{key} appears before WORKSPACE at line {line_number}"
            )
        if key in current:
            raise WorkspaceConfigurationError(
                f"duplicate {key} in block starting at line {line_number}"
            )
        current[key] = _parse_value(raw_value, line_number)

    if current:
        blocks.append(current)
    if not blocks:
        raise WorkspaceConfigurationError("no workspace blocks found")

    workspaces = tuple(_workspace_from_block(block, index) for index, block in enumerate(blocks, 1))
    _validate_unique_workspaces(workspaces)
    return workspaces


def workspace_descriptors(workspaces: tuple[Workspace, ...]) -> tuple[WorkspaceDescriptor, ...]:
    return tuple(workspace.descriptor for workspace in workspaces)


def discover_workspace_descriptors_from_env(
    environ: Mapping[str, str],
) -> tuple[WorkspaceDescriptor, ...] | None:
    config_path = environ.get("SLACKPIPE_WORKSPACE_FILE")
    if not config_path:
        return None
    return workspace_descriptors(discover_workspaces(config_path))


def attachment_incremental_enabled(path: str | Path) -> bool:
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as stream:
            values = []
            for raw_line in stream:
                match = _ASSIGNMENT.match(raw_line.strip())
                if match and match.group(1) == _ATTACHMENTS_SETTING:
                    values.append(_parse_value(match.group(2), 0).strip().lower())
    except OSError as error:
        raise WorkspaceConfigurationError(
            f"cannot read workspace configuration {source}"
        ) from error
    if not values:
        return False
    if len(values) != 1 or values[0] not in {"true", "false"}:
        raise WorkspaceConfigurationError(
            f"{_ATTACHMENTS_SETTING} must appear once with true or false"
        )
    return values[0] == "true"


def select_workspace(workspaces: tuple[Workspace, ...], selector: str) -> Workspace:
    """Select exactly one workspace by case-insensitive workspace id."""
    requested = selector.strip().casefold()
    if not requested:
        raise WorkspaceSelectionError("workspace selector must not be empty")
    matches = [
        workspace
        for workspace in workspaces
        if workspace.workspace_id.casefold() == requested
    ]
    if not matches:
        available = ", ".join(sorted(workspace.workspace_id for workspace in workspaces))
        raise WorkspaceSelectionError(
            f"workspace {selector!r} not found; available workspaces: {available}"
        )
    if len(matches) > 1:
        ids = ", ".join(sorted(workspace.workspace_id for workspace in matches))
        raise WorkspaceSelectionError(
            f"workspace selector {selector!r} is ambiguous; matches: {ids}"
        )
    return matches[0]


def _parse_value(raw_value: str, line_number: int) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    if value[0] not in ("'", '"'):
        value = re.split(r"\s+#", value, maxsplit=1)[0].rstrip()
        return value

    quote = value[0]
    result: list[str] = []
    escaped = False
    closing = None
    for index, char in enumerate(value[1:], start=1):
        if quote == '"' and escaped:
            result.append({"n": "\n", "r": "\r", "t": "\t"}.get(char, char))
            escaped = False
        elif quote == '"' and char == "\\":
            escaped = True
        elif char == quote:
            closing = index
            break
        else:
            result.append(char)
    if closing is None or escaped:
        raise WorkspaceConfigurationError(f"unterminated quoted value at line {line_number}")
    trailing = value[closing + 1 :].strip()
    if trailing and not trailing.startswith("#"):
        raise WorkspaceConfigurationError(f"unexpected text after value at line {line_number}")
    return "".join(result)


def _workspace_from_block(block: dict[str, str], index: int) -> Workspace:
    missing = [field for field in _FIELDS if not block.get(field)]
    if missing:
        raise WorkspaceConfigurationError(
            f"workspace block {index} is missing: {', '.join(missing)}"
        )

    workspace_id = block["WORKSPACE"].strip()
    url = block["CHANNEL_URL"].strip()
    token = block["SLACK_TOKEN"]
    cookie = block["SLACK_COOKIE"]
    _validate_workspace_id(workspace_id, index)
    _reject_control_characters(url, "CHANNEL_URL", index)
    _reject_secret_whitespace(token, "SLACK_TOKEN", index)
    _reject_secret_whitespace(cookie, "SLACK_COOKIE", index)

    parsed = urlsplit(url)
    hostname = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError as error:
        raise WorkspaceConfigurationError(
            f"workspace block {index} has an invalid Slack CHANNEL_URL"
        ) from error
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or not hostname.endswith(".slack.com")
    ):
        raise WorkspaceConfigurationError(
            f"workspace block {index} has an invalid Slack CHANNEL_URL"
        )
    return Workspace(workspace_id=workspace_id, url=url, token=token, cookie=cookie)


def _validate_workspace_id(value: str, index: int) -> None:
    if not value:
        raise WorkspaceConfigurationError(
            f"workspace block {index} has an empty WORKSPACE value"
        )
    if len(value) > 64:
        raise WorkspaceConfigurationError(
            f"workspace block {index} has a WORKSPACE value longer than 64 characters"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise WorkspaceConfigurationError(
            f"workspace block {index} has control characters in WORKSPACE"
        )
    if "/" in value or "\\" in value or "\0" in value:
        raise WorkspaceConfigurationError(
            f"workspace block {index} has a path separator in WORKSPACE"
        )
    if value in (".", ".."):
        raise WorkspaceConfigurationError(
            f"workspace block {index} has a reserved WORKSPACE value"
        )
    if value != value.strip():
        raise WorkspaceConfigurationError(
            f"workspace block {index} has leading or trailing whitespace in WORKSPACE"
        )
    if value.startswith("."):
        raise WorkspaceConfigurationError(
            f"workspace block {index} has a leading dot in WORKSPACE"
        )


def _reject_control_characters(value: str, field_name: str, index: int) -> None:
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise WorkspaceConfigurationError(
            f"workspace block {index} has control characters in {field_name}"
        )


def _reject_secret_whitespace(value: str, field_name: str, index: int) -> None:
    if any(char.isspace() or ord(char) == 127 for char in value):
        raise WorkspaceConfigurationError(
            f"workspace block {index} has invalid whitespace in {field_name}"
        )


def _validate_unique_workspaces(workspaces: tuple[Workspace, ...]) -> None:
    seen: set[str] = set()
    for workspace in workspaces:
        folded = workspace.workspace_id.casefold()
        if folded in seen:
            raise WorkspaceConfigurationError(
                f"duplicate workspace {workspace.workspace_id!r}"
            )
        seen.add(folded)
