import multiprocessing
from pathlib import Path
import stat
import subprocess
import sys
import time
from typing import Any

import pytest

from slackpipe.slackdump import (
    GlobalFileLock,
    SlackdumpAuthenticationError,
    SlackdumpExecutionError,
    SlackdumpLockTimeout,
    SlackdumpRunner,
    netscape_cookie_file,
    reconcile_attachments,
    _run_streaming,
    _slackdump_label,
)
from slackpipe.workspaces import Workspace


FAKE_TOKEN = "xoxc-fake-token"
FAKE_COOKIE = "xoxd-fake-cookie"


def fake_workspace() -> Workspace:
    return Workspace(
        workspace_id="Example Team",
        url="https://example-team.slack.com",
        token=FAKE_TOKEN,
        cookie=FAKE_COOKIE,
    )


def test_cookie_file_is_netscape_private_and_deleted(tmp_path: Path) -> None:
    with netscape_cookie_file(FAKE_COOKIE, directory=tmp_path) as path:
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        content = path.read_text(encoding="utf-8")
        assert content.startswith("# Netscape HTTP Cookie File\n")
        assert f"\td\t{FAKE_COOKIE}\n" in content

    assert not path.exists()


def test_cookie_file_is_deleted_after_failure(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError):
        with netscape_cookie_file(FAKE_COOKIE, directory=tmp_path) as path:
            raise RuntimeError("synthetic failure")

    assert not path.exists()


def test_incremental_plan_uses_archive_then_resume_and_persistent_directory(
    tmp_path: Path,
) -> None:
    workspace = fake_workspace()
    runner = SlackdumpRunner(
        tmp_path / "archives", lock_path=tmp_path / "global.lock"
    )

    first = runner.plan_incremental(workspace)
    assert first.action == "archive"
    assert first.output_dir == tmp_path / "archives" / workspace.workspace_id
    assert first.command == (
        "slackdump",
        "archive",
        "-load-env",
        "-files=false",
        "-y",
        "-o",
        str(first.output_dir),
    )

    first.output_dir.mkdir(parents=True)
    (first.output_dir / "slackdump.sqlite").touch()
    resumed = runner.plan_incremental(workspace)
    assert resumed.action == "resume"
    assert resumed.output_dir == first.output_dir
    assert resumed.command == (
        "slackdump",
        "resume",
        "-load-env",
        "-files=false",
        "-refresh",
        "-threads",
        "-skip-complete-threads",
        "-dedupe",
        str(first.output_dir),
    )


def test_incremental_plan_applies_stale_window_only_to_resume(
    tmp_path: Path,
) -> None:
    workspace = fake_workspace()
    runner = SlackdumpRunner(
        tmp_path / "archives", lock_path=tmp_path / "global.lock"
    )

    # Fresh workspace takes the archive path: no stale flags possible.
    first = runner.plan_incremental(workspace, stale_after="p90d")
    assert first.action == "archive"
    assert "-skip-stale-threads" not in first.command

    first.output_dir.mkdir(parents=True)
    (first.output_dir / "slackdump.sqlite").touch()
    resumed = runner.plan_incremental(workspace, stale_after="p90d")
    assert resumed.action == "resume"
    # -dedupe prunes identical lookback overlap after successful resume;
    # safe because continuity is proven by key coverage in transform.
    assert "-dedupe" in resumed.command
    tail = resumed.command[-7:-1]
    assert tail == (
        "-skip-complete-threads",
        "-dedupe",
        "-skip-stale-threads",
        "p90d",
        "-skip-stale-channels",
        "p90d",
    )

    plain = runner.plan_incremental(workspace)
    assert "-skip-stale-threads" not in plain.command
    assert "-skip-stale-channels" not in plain.command

    with pytest.raises(SlackdumpExecutionError, match="ISO 8601"):
        runner.plan_incremental(workspace, stale_after="90-days")


def test_full_backfill_uses_same_persistent_archive_flow(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []

    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )

    result = runner.run_full_backfill(fake_workspace())

    assert result.action == "archive"
    assert calls[-1][1] == "archive"


def test_attachment_incremental_opt_in_and_explicit_backfill(tmp_path: Path) -> None:
    workspace = fake_workspace()
    output_dir = tmp_path / "archives" / workspace.workspace_id
    output_dir.mkdir(parents=True)
    (output_dir / "slackdump.sqlite").touch()
    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        attachment_incremental=True,
    )

    assert "-files=true" in runner.plan_incremental(workspace).command
    backfill = runner.plan_attachment_backfill(workspace)
    assert backfill.action == "attachment-backfill"
    assert backfill.command == (
        "slackdump",
        "tools",
        "redownload",
        "-load-env",
        str(output_dir),
    )


def test_backfill_requires_existing_archive(tmp_path: Path) -> None:
    runner = SlackdumpRunner(
        tmp_path / "archives", lock_path=tmp_path / "global.lock"
    )

    with pytest.raises(SlackdumpExecutionError, match="before an archive exists"):
        runner.plan_attachment_backfill(fake_workspace())


def test_run_uses_ephemeral_auth_no_secret_arguments_and_cleans_up(
    tmp_path: Path,
) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def executor(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        env = kwargs["env"]
        assert isinstance(env, dict)
        calls.append((tuple(command), env))
        assert env["SLACK_TOKEN"] == FAKE_TOKEN
        assert env["SLACK_COOKIE"] == FAKE_COOKIE
        assert FAKE_TOKEN not in command
        assert FAKE_COOKIE not in command
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )

    result = runner.run_incremental(fake_workspace())

    assert result.action == "archive"
    assert len(calls) == 3
    auth_command, auth_env = calls[0]
    select_command, _ = calls[1]
    archive_command, archive_env = calls[2]
    assert auth_command[1:3] == ("workspace", "new")
    assert auth_command[-1] == "example team"
    assert select_command[1:] == ("workspace", "select", "example team")
    assert archive_command[1] == "archive"
    assert "-cache-dir" not in archive_command
    assert archive_command[2:4] == ("-workspace", "example team")
    assert auth_env["SLACK_COOKIE"] == archive_env["SLACK_COOKIE"] == FAKE_COOKIE
    assert not Path(archive_env["HOME"]).exists()


def test_slackdump_label_lowercases_but_preserves_spaces_and_punctuation() -> None:
    assert _slackdump_label("KMNR 89.7 FM") == "kmnr 89.7 fm"
    assert _slackdump_label("CTO Craft") == "cto craft"
    assert _slackdump_label("eng-managers") == "eng-managers"
    assert _slackdump_label("leadership") == "leadership"


def test_status_and_result_are_secret_free(tmp_path: Path) -> None:
    messages: list[str] = []

    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        status=messages.append,
        cookie_temp_dir=tmp_path,
    )

    result = runner.run_incremental(fake_workspace())
    exposed = repr(result) + "".join(messages)
    assert FAKE_TOKEN not in exposed
    assert FAKE_COOKIE not in exposed


def test_preflight_authenticates_without_starting_archive(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []

    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )
    runner.preflight(fake_workspace())

    assert len(calls) == 1
    assert calls[0][1:3] == ("workspace", "import")
    assert not Path(calls[0][3]).exists()
    assert not (tmp_path / "archives" / "Example Team").exists()


def test_ansi_invalid_auth_is_non_retryable_authentication_error(tmp_path: Path) -> None:
    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[1:3] == ("workspace", "import"):
            return subprocess.CompletedProcess(
                command,
                5,
                stdout="",
                stderr="\x1b[31mERROR\x1b[0m invalid_auth.",
            )
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )

    with pytest.raises(SlackdumpAuthenticationError, match="failed with exit code 5"):
        runner.preflight(fake_workspace())


DNS_BLIP_STDERR = (
    '008 (Cache Error): authentication error: Post "https://slack.com/api/auth.test": '
    "dial tcp: lookup slack.com on 127.0.0.11:53: server misbehaving"
)


def _runner_with_stderr(
    tmp_path: Path, stderr: str, returncode: int = 8
) -> SlackdumpRunner:
    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command, returncode, stdout="", stderr=stderr
        )

    return SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )


def test_dns_blip_during_preflight_is_retryable_not_auth_error(
    tmp_path: Path,
) -> None:
    """Regression: Sep-2026 leadership run died when a transient DNS failure
    in the retry's auth.test was classified as credential death
    (allow_retries=False), discarding the remaining retries."""
    runner = _runner_with_stderr(tmp_path, DNS_BLIP_STDERR)

    with pytest.raises(SlackdumpExecutionError) as caught:
        runner.preflight(fake_workspace())

    assert not isinstance(caught.value, SlackdumpAuthenticationError)
    assert "server misbehaving" in str(caught.value)


def test_dns_blip_during_resume_is_retryable(tmp_path: Path) -> None:
    runner = _runner_with_stderr(
        tmp_path,
        'streaming error: Post "https://slack.com/api/conversations.replies": '
        "dial tcp: lookup slack.com on 127.0.0.11:53: server misbehaving",
        returncode=6,
    )

    with pytest.raises(SlackdumpExecutionError) as caught:
        runner.run_incremental(fake_workspace())

    assert not isinstance(caught.value, SlackdumpAuthenticationError)


def test_dns_blip_on_start_exception_is_retryable(tmp_path: Path) -> None:
    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise OSError("dial tcp: lookup slack.com on 127.0.0.11:53: server misbehaving")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )

    with pytest.raises(SlackdumpExecutionError) as caught:
        runner.preflight(fake_workspace())

    assert not isinstance(caught.value, SlackdumpAuthenticationError)


def test_genuine_invalid_auth_still_non_retryable(tmp_path: Path) -> None:
    runner = _runner_with_stderr(tmp_path, "callback error: invalid_auth.")

    with pytest.raises(SlackdumpAuthenticationError):
        runner.preflight(fake_workspace())


def test_attachment_reconciliation_requires_exact_size_and_safe_paths(tmp_path: Path) -> None:
    import json
    import sqlite3

    archive = tmp_path / "archive"
    archive.mkdir()
    with sqlite3.connect(archive / "slackdump.sqlite") as db:
        db.execute(
            "CREATE TABLE MESSAGE (CHANNEL_ID TEXT, TS TEXT, CHUNK_ID INTEGER, IDX INTEGER, DATA BLOB)"
        )
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, ?, ?, ?)",
            [
                "C1",
                "1.0",
                1,
                1,
                json.dumps({"files": [{"id": "F1", "name": "a?.txt", "size": 4, "mode": "hosted"}]}),
            ],
        )
    target = archive / "__uploads" / "F1" / "a_.txt"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"four")

    result = reconcile_attachments(archive)
    assert result.passed is True
    assert (result.expected_count, result.present_count, result.present_bytes) == (1, 1, 4)

    target.write_bytes(b"bad")
    mismatch = reconcile_attachments(archive)
    assert mismatch.passed is False
    assert mismatch.size_mismatch_count == 1


def _snippet_archive(
    tmp_path: Path,
    *,
    name: str = "note.csv",
    size: int,
    actual: bytes,
    mode: str = "snippet",
    mimetype: str = "text/csv",
) -> Path:
    import json
    import sqlite3

    archive = tmp_path / "archive"
    archive.mkdir(parents=True)
    with sqlite3.connect(archive / "slackdump.sqlite") as db:
        db.execute(
            "CREATE TABLE MESSAGE (CHANNEL_ID TEXT, TS TEXT, CHUNK_ID INTEGER, IDX INTEGER, DATA BLOB)"
        )
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, ?, ?, ?)",
            [
                "C1",
                "1.0",
                1,
                1,
                json.dumps(
                    {
                        "files": [
                            {
                                "id": "FSNIP",
                                "name": name,
                                "size": size,
                                "mode": mode,
                                "mimetype": mimetype,
                            }
                        ]
                    }
                ),
            ],
        )
    target = archive / "__uploads" / "FSNIP" / name
    target.parent.mkdir(parents=True)
    target.write_bytes(actual)
    return archive


def test_snippet_normalization_within_bound_passes_with_variance_recorded(
    tmp_path: Path,
) -> None:
    # Slack metadata says 10 bytes; Slack served 7 (3-byte normalization).
    archive = _snippet_archive(tmp_path, size=10, actual=b"1234567")

    result = reconcile_attachments(archive)
    assert result.passed is True
    assert result.size_mismatch_count == 0
    assert result.snippet_variance_count == 1
    assert result.snippet_variance_bytes == 3


def test_snippet_variance_bounds_stay_strict(tmp_path: Path) -> None:
    from slackpipe.slackdump import SNIPPET_VARIANCE_MAX_BYTES

    # Binary mode with the same delta still fails.
    binary = _snippet_archive(
        tmp_path / "binary", size=10, actual=b"1234567", mode="hosted",
        mimetype="application/octet-stream",
    )
    assert reconcile_attachments(binary).passed is False

    # Non-text snippet still fails.
    odd = _snippet_archive(
        tmp_path / "odd", size=10, actual=b"1234567", mimetype="image/png",
    )
    assert reconcile_attachments(odd).passed is False

    # Over-bound snippet delta still fails.
    big = _snippet_archive(
        tmp_path / "big",
        size=10 + SNIPPET_VARIANCE_MAX_BYTES + 1,
        actual=b"1234567",
    )
    failed = reconcile_attachments(big)
    assert failed.passed is False
    assert failed.size_mismatch_count == 1
    assert failed.snippet_variance_count == 0


def _hosted_archive(
    tmp_path: Path, *, size: int, actual: bytes, file_id: str = "FMP4"
) -> Path:
    import json
    import sqlite3

    archive = tmp_path / "archive"
    archive.mkdir(parents=True)
    with sqlite3.connect(archive / "slackdump.sqlite") as db:
        db.execute(
            "CREATE TABLE MESSAGE (CHANNEL_ID TEXT, TS TEXT, CHUNK_ID INTEGER, IDX INTEGER, DATA BLOB)"
        )
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, ?, ?, ?)",
            [
                "C1",
                "1.0",
                1,
                1,
                json.dumps(
                    {
                        "files": [
                            {
                                "id": file_id,
                                "name": "clip.mp4",
                                "size": size,
                                "mode": "hosted",
                                "mimetype": "video/mp4",
                            }
                        ]
                    }
                ),
            ],
        )
    target = archive / "__uploads" / file_id / "clip.mp4"
    target.parent.mkdir(parents=True)
    target.write_bytes(actual)
    return archive


def test_documented_variance_passes_only_on_exact_accepted_bytes(
    tmp_path: Path,
) -> None:
    from slackpipe.slackdump import DOCUMENTED_SIZE_VARIANCE

    assert DOCUMENTED_SIZE_VARIANCE["F0BU90K0EM9"] == 70095357

    # Unknown binary mismatch still fails.
    archive = _hosted_archive(tmp_path / "plain", size=10, actual=b"1234567")
    failed = reconcile_attachments(archive)
    assert failed.passed is False
    assert failed.size_mismatch_count == 1
    assert failed.documented_variance_count == 0

    # Accepted file passes only when on-disk bytes equal the recorded value.
    accepted = _hosted_archive(
        tmp_path / "ok", size=10, actual=b"1234567", file_id="FMP4"
    )
    passed = reconcile_attachments(accepted, accepted_sizes={"FMP4": 7})
    assert passed.passed is True
    assert passed.size_mismatch_count == 0
    assert passed.documented_variance_count == 1
    assert passed.documented_variance_bytes == 3
    assert passed.documented_variance_files == "FMP4=7"

    changed = reconcile_attachments(accepted, accepted_sizes={"FMP4": 8})
    assert changed.passed is False
    assert changed.size_mismatch_count == 1
    assert changed.documented_variance_count == 0


def test_streaming_executor_emits_output_and_redacts_credentials(
    capsys: pytest.CaptureFixture[str],
) -> None:
    command = (
        sys.executable,
        "-c",
        f"print('progress {FAKE_TOKEN} {FAKE_COOKIE}')",
    )
    completed = _run_streaming(command, env={}, workspace=fake_workspace())

    output = capsys.readouterr().out
    assert "progress [REDACTED] [REDACTED]" in output
    assert completed.returncode == 0
    assert FAKE_TOKEN not in completed.stdout
    assert FAKE_COOKIE not in completed.stdout


@pytest.mark.parametrize("failure_stage", [1, 2, 3])
def test_subprocess_errors_are_redacted_and_auth_files_removed(
    tmp_path: Path, failure_stage: int
) -> None:
    calls = 0
    cookie_paths: list[Path] = []

    def executor(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        env = kwargs["env"]
        assert isinstance(env, dict)
        cookie_paths.extend(tmp_path.glob("slackpipe-auth-*/slackpipe-cookie-*"))
        if calls == failure_stage:
            return subprocess.CompletedProcess(
                command,
                9,
                stdout="",
                stderr=f"auth failed for {FAKE_TOKEN} cookie={FAKE_COOKIE} xoxb-unknown",
            )
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )

    with pytest.raises(SlackdumpExecutionError) as caught:
        runner.run_incremental(fake_workspace())

    message = str(caught.value)
    assert "[REDACTED]" in message
    assert FAKE_TOKEN not in message
    assert FAKE_COOKIE not in message
    assert "xoxb-unknown" not in message
    assert all(not path.exists() for path in cookie_paths)


def test_authentication_failure_has_distinct_non_secret_type(tmp_path: Path) -> None:
    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            9,
            stdout="",
            stderr=f"invalid_auth {FAKE_TOKEN} {FAKE_COOKIE}",
        )

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )

    with pytest.raises(SlackdumpAuthenticationError) as caught:
        runner.run_incremental(fake_workspace())

    assert "invalid_auth" in str(caught.value)
    assert FAKE_TOKEN not in str(caught.value)
    assert FAKE_COOKIE not in str(caught.value)


def test_subprocess_start_exception_is_redacted(tmp_path: Path) -> None:
    def executor(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise OSError(f"cannot start with {FAKE_TOKEN} and {FAKE_COOKIE}")

    runner = SlackdumpRunner(
        tmp_path / "archives",
        lock_path=tmp_path / "global.lock",
        executor=executor,
        cookie_temp_dir=tmp_path,
    )

    with pytest.raises(SlackdumpExecutionError) as caught:
        runner.run_incremental(fake_workspace())

    assert "[REDACTED]" in str(caught.value)
    assert FAKE_TOKEN not in str(caught.value)
    assert FAKE_COOKIE not in str(caught.value)


def _hold_lock(path: str, ready: Any) -> None:
    with GlobalFileLock(path, timeout=1.0):
        ready.set()
        time.sleep(0.4)


def test_global_lock_reports_wait_and_times_out(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    ready = multiprocessing.Event()
    process = multiprocessing.Process(target=_hold_lock, args=(str(lock_path), ready))
    process.start()
    try:
        assert ready.wait(1.0)
        messages: list[str] = []
        with pytest.raises(SlackdumpLockTimeout, match="timed out"):
            with GlobalFileLock(
                lock_path,
                timeout=0.1,
                poll_interval=0.01,
                status=messages.append,
            ):
                pytest.fail("lock should not have been acquired")
        assert any("waiting for global Slackdump lock" in item for item in messages)
    finally:
        process.join(timeout=2.0)
        if process.is_alive():
            process.terminate()
            process.join()


def _mismatch_archive(tmp_path: Path, payload: bytes | None = b"bad"):
    import json
    import sqlite3

    archive = tmp_path / "Example Team"
    archive.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(archive / "slackdump.sqlite") as db:
        db.execute(
            "CREATE TABLE MESSAGE (CHANNEL_ID TEXT, TS TEXT, CHUNK_ID INTEGER, IDX INTEGER, DATA BLOB)"
        )
        db.execute(
            "INSERT INTO MESSAGE VALUES (?, ?, ?, ?, ?)",
            [
                "C1",
                "1.0",
                1,
                1,
                json.dumps({"files": [{"id": "F1", "name": "a?.txt", "size": 4, "mode": "hosted"}]}),
            ],
        )
    if payload is not None:
        target = archive / "__uploads" / "F1" / "a_.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    return archive


class _AlwaysOk:
    def __call__(self, command, *, env=None, capture_output=False, text=False, check=False):
        import subprocess

        return subprocess.CompletedProcess(
            args=tuple(command), returncode=0, stdout="", stderr=""
        )


class _HealOnSecondRedownload(_AlwaysOk):
    def __init__(self, archive: Path) -> None:
        self.archive = archive
        self.redownloads = 0

    def __call__(self, command, *, env=None, capture_output=False, text=False, check=False):
        import subprocess

        if "redownload" in tuple(command):
            self.redownloads += 1
            if self.redownloads >= 2:
                (self.archive / "__uploads" / "F1" / "a_.txt").write_bytes(b"four")
        return subprocess.CompletedProcess(
            args=tuple(command), returncode=0, stdout="", stderr=""
        )


def _test_runner(tmp_path: Path, executor, messages: list):
    return SlackdumpRunner(
        str(tmp_path),
        lock_path=str(tmp_path / "test.lock"),
        executor=executor,
        status=messages.append,
    )


def test_reconcile_records_mismatch_identities(tmp_path: Path) -> None:
    archive = _mismatch_archive(tmp_path)

    result = reconcile_attachments(archive)

    assert result.passed is False
    assert result.size_mismatch_count == 1
    assert len(result.mismatched_files) == 1
    file_id, _name, expected, served = result.mismatched_files[0]
    assert (file_id, expected, served) == ("F1", 4, 3)


def test_backfill_retries_heal_transient_mismatch(tmp_path: Path) -> None:
    archive = _mismatch_archive(tmp_path)
    messages: list = []
    runner = _test_runner(tmp_path, _HealOnSecondRedownload(archive), messages)

    _result, reconciliation = runner.backfill_with_reconciliation(fake_workspace())

    assert reconciliation.passed is True
    assert reconciliation.size_mismatch_count == 0
    assert reconciliation.accepted_variance_count == 0
    assert reconciliation.retried_count >= 1
    assert any("redownload retry" in message for message in messages)


def test_backfill_accepts_persistent_mismatch_with_warning(tmp_path: Path) -> None:
    archive = _mismatch_archive(tmp_path)
    messages: list = []
    runner = _test_runner(tmp_path, _AlwaysOk(), messages)

    _result, reconciliation = runner.backfill_with_reconciliation(fake_workspace())

    assert reconciliation.passed is True
    assert reconciliation.size_mismatch_count == 0
    assert reconciliation.accepted_variance_count == 1
    assert reconciliation.accepted_variance_bytes == 1
    assert reconciliation.accepted_variance_files == "F1=3"
    assert reconciliation.retried_count == 2
    assert any("WARNING" in message and "F1" in message for message in messages)


def test_backfill_missing_still_fails_after_retries(tmp_path: Path) -> None:
    _mismatch_archive(tmp_path, payload=None)
    messages: list = []
    runner = _test_runner(tmp_path, _AlwaysOk(), messages)

    _result, reconciliation = runner.backfill_with_reconciliation(fake_workspace())

    assert reconciliation.passed is False
    assert reconciliation.missing_count == 1
    assert reconciliation.accepted_variance_count == 0
    assert reconciliation.retried_count == 2
