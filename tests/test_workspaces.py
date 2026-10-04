from pathlib import Path

import pytest

from slackpipe.workspaces import (
    WorkspaceConfigurationError,
    WorkspaceSelectionError,
    attachment_incremental_enabled,
    discover_workspaces,
    discover_workspace_descriptors_from_env,
    parse_workspace_blocks,
    select_workspace,
    workspace_descriptors,
)


FAKE_CONFIG = """
# Synthetic credentials only. Comments are documentation, never parsed.
export WORKSPACE="KMNR 89.7 FM"
CHANNEL_URL=https://kmnr897fm.slack.com/client/T0001
SLACK_TOKEN='xoxc-fake-one'
SLACK_COOKIE=xoxd-fake-one

IGNORED_SETTING=safe
WORKSPACE=leadership  # inline comment is stripped for unquoted values
CHANNEL_URL="https://rands-leadership.slack.com/"
SLACK_TOKEN=xoxc-fake-two
SLACK_COOKIE='xoxd-fake-two'
"""


def test_discovers_all_blocks_and_exposes_secret_free_descriptors(tmp_path: Path) -> None:
    env_path = tmp_path / "synthetic.env"
    env_path.write_text(FAKE_CONFIG, encoding="utf-8")

    workspaces = discover_workspaces(env_path)
    descriptors = workspace_descriptors(workspaces)

    assert [item.workspace_id for item in workspaces] == ["KMNR 89.7 FM", "leadership"]
    assert descriptors[0].url == "https://kmnr897fm.slack.com/client/T0001"
    exposed = repr(descriptors) + repr(workspaces)
    assert "xoxc-fake" not in exposed
    assert "xoxd-fake" not in exposed


def test_workspace_identity_is_verbatim_no_slug_derivation() -> None:
    workspaces = parse_workspace_blocks(FAKE_CONFIG)

    # The real Slack name is preserved exactly; nothing is slugified.
    assert workspaces[0].workspace_id == "KMNR 89.7 FM"
    assert workspaces[0].slug == "KMNR 89.7 FM"
    assert workspaces[0].name == "KMNR 89.7 FM"


def test_comment_marker_lines_are_ignored(tmp_path: Path) -> None:
    workspaces = parse_workspace_blocks(
        "# KMNR 89.7 FM - https://kmnr897fm.slack.com\n"
        "WORKSPACE=KMNR 89.7 FM\n"
        "CHANNEL_URL=https://kmnr897fm.slack.com\n"
        "SLACK_TOKEN=xoxc-fake-one\n"
        "SLACK_COOKIE=xoxd-fake-one\n"
    )

    assert [item.workspace_id for item in workspaces] == ["KMNR 89.7 FM"]


def test_descriptor_discovery_uses_runtime_workspace_file(tmp_path: Path) -> None:
    env_path = tmp_path / "synthetic.env"
    env_path.write_text(FAKE_CONFIG, encoding="utf-8")

    descriptors = discover_workspace_descriptors_from_env(
        {"SLACKPIPE_WORKSPACE_FILE": str(env_path)}
    )

    assert [item.workspace_id for item in descriptors] == ["KMNR 89.7 FM", "leadership"]
    assert "xox" not in repr(descriptors)
    assert discover_workspace_descriptors_from_env({}) is None


def test_attachment_incremental_flag_defaults_off_and_is_explicit(tmp_path: Path) -> None:
    env_path = tmp_path / "synthetic.env"
    env_path.write_text(FAKE_CONFIG, encoding="utf-8")
    assert attachment_incremental_enabled(env_path) is False

    env_path.write_text(
        FAKE_CONFIG + "\nSLACKPIPE_ATTACHMENTS_INCREMENTAL=true\n",
        encoding="utf-8",
    )
    assert attachment_incremental_enabled(env_path) is True

    env_path.write_text(
        FAKE_CONFIG + "\nSLACKPIPE_ATTACHMENTS_INCREMENTAL=maybe\n",
        encoding="utf-8",
    )
    with pytest.raises(WorkspaceConfigurationError, match="true or false"):
        attachment_incremental_enabled(env_path)


@pytest.mark.parametrize("selector", ["KMNR 89.7 FM", "kmnr 89.7 fm", "LEADERSHIP"])
def test_selects_by_case_insensitive_workspace_id(selector: str) -> None:
    selected = select_workspace(parse_workspace_blocks(FAKE_CONFIG), selector)

    expected = "leadership" if selector.casefold() == "leadership" else "KMNR 89.7 FM"
    assert selected.workspace_id == expected


def test_selection_reports_only_non_secret_workspace_information() -> None:
    workspaces = parse_workspace_blocks(FAKE_CONFIG)

    with pytest.raises(WorkspaceSelectionError) as caught:
        select_workspace(workspaces, "missing")

    message = str(caught.value)
    assert "KMNR 89.7 FM" in message
    assert "leadership" in message
    assert "xox" not in message


@pytest.mark.parametrize(
    "content, expected",
    [
        (
            "WORKSPACE=Only\nCHANNEL_URL=https://only.slack.com\n"
            "SLACK_TOKEN=xoxc-fake\n",
            "SLACK_COOKIE",
        ),
        (
            "WORKSPACE=Only\nCHANNEL_URL=https://evil.example\n"
            "SLACK_TOKEN=xoxc-fake\nSLACK_COOKIE=xoxd-fake\n",
            "invalid Slack CHANNEL_URL",
        ),
        (
            "WORKSPACE=Only\nCHANNEL_URL=https://only.slack.com\n"
            "SLACK_TOKEN=xoxc-fake value\nSLACK_COOKIE=xoxd-fake\n",
            "invalid whitespace in SLACK_TOKEN",
        ),
        (
            "WORKSPACE=a/b\nCHANNEL_URL=https://only.slack.com\n"
            "SLACK_TOKEN=xoxc-fake\nSLACK_COOKIE=xoxd-fake\n",
            "path separator",
        ),
        (
            "WORKSPACE=..\nCHANNEL_URL=https://only.slack.com\n"
            "SLACK_TOKEN=xoxc-fake\nSLACK_COOKIE=xoxd-fake\n",
            "reserved WORKSPACE",
        ),
    ],
)
def test_rejects_incomplete_or_unsafe_blocks(content: str, expected: str) -> None:
    with pytest.raises(WorkspaceConfigurationError, match=expected):
        parse_workspace_blocks(content)


def test_duplicate_workspaces_case_insensitive_are_rejected_without_secrets() -> None:
    content = FAKE_CONFIG.replace("leadership  # inline comment is stripped for unquoted values", "KMNR 89.7 FM")

    with pytest.raises(WorkspaceConfigurationError) as caught:
        parse_workspace_blocks(content)

    assert "KMNR 89.7 FM" in str(caught.value)
    assert "xox" not in str(caught.value)


def test_key_before_workspace_is_rejected() -> None:
    with pytest.raises(WorkspaceConfigurationError, match="appears before WORKSPACE"):
        parse_workspace_blocks("SLACK_TOKEN=xoxc-fake\n")
