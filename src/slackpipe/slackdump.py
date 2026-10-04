"""Secure command construction and execution for Slackdump archives."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Iterator, Protocol

from slackpipe.workspaces import Workspace, WorkspaceDescriptor


StatusCallback = Callable[[str], None]
_TOKEN_PATTERN = re.compile(r"\bxox[a-zA-Z]-[^\s'\"\\]+")
_ANSI_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_AUTH_FAILURE_PATTERN = re.compile(
    r"\b(?:invalid_auth|not_authed|account_inactive|token_revoked|invalid_cookie)\b",
    re.IGNORECASE,
)
# Transport-layer failures (DNS, TCP, TLS timeouts, 5xx) are transient and
# must stay retryable even when they surface during the authentication
# preflight. Without this, a seconds-long DNS blip in `workspace new` is
# misclassified as credential death, which sets allow_retries=False and
# discards the remaining extraction retries (observed Sep-2026: an 8h
# leadership resume died on `dial tcp: lookup slack.com ... server
# misbehaving` in the retry's auth.test). Checked before the auth pattern so
# a transport failure never reads as an auth failure.
_TRANSIENT_NETWORK_PATTERN = re.compile(
    r"(?:dial tcp|lookup .*?(?:no such host|server misbehaving|temporary failure)"
    r"|server misbehaving|no such host|temporary failure in name resolution"
    r"|network (?:is )?unreachable|connection (?:refused|reset|timed out)"
    r"|i/o timeout|context deadline exceeded|operation timed out"
    r"|TLS handshake timeout|unexpected EOF|status code (?:502|503|504)\b"
    r"|\b(?:502|503|504) (?:Bad Gateway|Service Unavailable|Gateway Timeout)\b)",
    re.IGNORECASE,
)


def _classify_subprocess_error(action: str, normalized_detail: str) -> type[SlackdumpExecutionError]:
    """Pick the error type for a failed slackdump invocation.

    Transport failures are always retryable (`SlackdumpExecutionError`), no
    matter which action surfaced them. Credential failures stay non-retryable
    (`SlackdumpAuthenticationError`).
    """
    if _TRANSIENT_NETWORK_PATTERN.search(normalized_detail):
        return SlackdumpExecutionError
    if action == "authentication" or _AUTH_FAILURE_PATTERN.search(normalized_detail):
        return SlackdumpAuthenticationError
    return SlackdumpExecutionError


class SlackdumpLockTimeout(TimeoutError):
    """Raised when the global Slackdump lock cannot be acquired in time."""


class SlackdumpExecutionError(RuntimeError):
    """A Slackdump failure whose message has had credentials removed."""


class SlackdumpAuthenticationError(SlackdumpExecutionError):
    """A non-secret Slack authentication failure."""


class Executor(Protocol):
    def __call__(
        self,
        command: Sequence[str],
        *,
        env: Mapping[str, str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]: ...


@dataclass(frozen=True)
class SlackdumpPlan:
    workspace: WorkspaceDescriptor
    action: str
    command: tuple[str, ...]
    output_dir: Path


@dataclass(frozen=True)
class SlackdumpResult:
    workspace: WorkspaceDescriptor
    action: str
    command: tuple[str, ...]
    output_dir: Path
    returncode: int


@dataclass(frozen=True)
class AttachmentReconciliation:
    expected_count: int
    present_count: int
    skipped_count: int
    missing_count: int
    size_mismatch_count: int
    unsafe_count: int
    expected_bytes: int
    present_bytes: int
    snippet_variance_count: int = 0
    snippet_variance_bytes: int = 0
    documented_variance_count: int = 0
    documented_variance_bytes: int = 0
    documented_variance_files: str = ""
    retried_count: int = 0
    accepted_variance_count: int = 0
    accepted_variance_bytes: int = 0
    accepted_variance_files: str = ""
    mismatched_files: tuple = ()

    @property
    def passed(self) -> bool:
        return not (self.missing_count or self.size_mismatch_count or self.unsafe_count)


#: Maximum byte delta still classified as Slack-side snippet normalization
#: rather than a hard size mismatch. Snippets are editable server-side
#: documents: Slack may serve a few normalized bytes (line endings,
#: whitespace) fewer or more than the size recorded in message metadata
#: (observed: 3 bytes on a 46,377-byte CSV snippet). Binaries and
#: over-bound deltas keep failing exactly as before.
SNIPPET_VARIANCE_MAX_BYTES = 64


#: Server-side rendition variances documented with byte-exact evidence.
#: Each entry maps a Slack file ID to the exact byte count Slack serves
#: for it, where that differs from the size in message metadata and the
#: served bytes are structurally complete. Fails closed: any other actual
#: size for a listed file is still a hard mismatch.
#:
#: - F0BU90K0EM9 (CTO Craft screenshare mp4): metadata says 70,980,927 in
#:   7 uniform occurrences; two independent downloads served exactly
#:   70,095,357 bytes; ftyp+moov+free+mdat tile exactly to EOF (no mid-box
#:   cut); served from Slack's files-tmb transcode host. Playability
#:   unverified — any byte change re-fails loudly.
DOCUMENTED_SIZE_VARIANCE: dict[str, int] = {
    "F0BU90K0EM9": 70095357,
}


class GlobalFileLock:
    """An OS-level exclusive lock shared by every Slackdump execution."""

    def __init__(
        self,
        path: str | Path,
        *,
        timeout: float,
        poll_interval: float = 0.1,
        status: StatusCallback | None = None,
    ) -> None:
        if timeout < 0:
            raise ValueError("lock timeout must be non-negative")
        if poll_interval <= 0:
            raise ValueError("lock poll interval must be positive")
        self.path = Path(path)
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.status = status or (lambda _message: None)
        self._fd: int | None = None

    def __enter__(self) -> GlobalFileLock:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as error:
            raise SlackdumpExecutionError(
                f"cannot open global Slackdump lock {self.path}"
            ) from error
        os.fchmod(fd, 0o600)
        started = time.monotonic()
        waiting_reported = False
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fd = fd
                if waiting_reported:
                    self.status(
                        f"acquired global Slackdump lock after {time.monotonic() - started:.1f}s"
                    )
                return self
            except BlockingIOError:
                elapsed = time.monotonic() - started
                if not waiting_reported:
                    self.status(
                        f"waiting for global Slackdump lock {self.path} "
                        f"(timeout {self.timeout:.1f}s)"
                    )
                    waiting_reported = True
                if elapsed >= self.timeout:
                    os.close(fd)
                    raise SlackdumpLockTimeout(
                        f"timed out after {self.timeout:.1f}s waiting for global "
                        f"Slackdump lock {self.path}"
                    )
                time.sleep(min(self.poll_interval, self.timeout - elapsed))

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


class SlackdumpRunner:
    def __init__(
        self,
        output_root: str | Path,
        *,
        lock_path: str | Path,
        executable: str = "slackdump",
        lock_timeout: float = 300.0,
        attachment_incremental: bool = False,
        executor: Executor = subprocess.run,
        status: StatusCallback | None = None,
        cookie_temp_dir: str | Path | None = None,
    ) -> None:
        self.output_root = Path(output_root)
        self.lock_path = Path(lock_path)
        self.executable = executable
        self.lock_timeout = lock_timeout
        self.attachment_incremental = attachment_incremental
        self.executor = executor
        self.status = status or (lambda _message: None)
        self.cookie_temp_dir = Path(cookie_temp_dir) if cookie_temp_dir else None

    def plan_incremental(
        self, workspace: Workspace, *, stale_after: str = ""
    ) -> SlackdumpPlan:
        output_dir = workspace_output_dir(self.output_root, workspace)
        has_archive = (output_dir / "slackdump.sqlite").is_file()
        files_flag = f"-files={str(self.attachment_incremental).lower()}"
        if has_archive:
            action = "resume"
            stale_flags: tuple[str, ...] = ()
            if stale_after:
                if not stale_after.startswith(("p", "P")):
                    raise SlackdumpExecutionError(
                        f"stale_after must be an ISO 8601 duration (e.g. p90d), got {stale_after!r}"
                    )
                stale_flags = (
                    "-skip-stale-threads",
                    stale_after,
                    "-skip-stale-channels",
                    stale_after,
                )
            command = (
                self.executable,
                "resume",
                "-load-env",
                files_flag,
                "-refresh",
                "-threads",
                # Skip threads the archive already holds in full (parent +
                # Slack's current reply_count); incomplete or new threads
                # still paginate. In-place edits/deletes inside complete
                # threads are not re-detected (upstream caveat).
                "-skip-complete-threads",
                # Prune identical lookback-overlap duplicates after a
                # successful resume so the archive does not grow
                # unboundedly. Safe: continuity is proven by key coverage
                # (equal-or-newer payloads), not rowid prefixes, and the
                # latest copy of each payload is kept — exactly what
                # canonicalization selects.
                "-dedupe",
                *stale_flags,
                str(output_dir),
            )
        else:
            action = "archive"
            command = (
                self.executable,
                "archive",
                "-load-env",
                files_flag,
                "-y",
                "-o",
                str(output_dir),
            )
        return SlackdumpPlan(workspace.descriptor, action, command, output_dir)

    def plan_attachment_backfill(self, workspace: Workspace) -> SlackdumpPlan:
        output_dir = workspace_output_dir(self.output_root, workspace)
        if not (output_dir / "slackdump.sqlite").is_file():
            raise SlackdumpExecutionError(
                f"cannot backfill attachments before an archive exists for {workspace.workspace_id}"
            )
        command = (
            self.executable,
            "tools",
            "redownload",
            "-load-env",
            str(output_dir),
        )
        return SlackdumpPlan(
            workspace.descriptor, "attachment-backfill", command, output_dir
        )

    def run_incremental(
        self, workspace: Workspace, *, stale_after: str = ""
    ) -> SlackdumpResult:
        return self._run(workspace, self.plan_incremental(workspace, stale_after=stale_after))

    def preflight(self, workspace: Workspace) -> None:
        """Perform Slackdump's real auth.test without starting archive work."""

        with GlobalFileLock(
            self.lock_path,
            timeout=self.lock_timeout,
            status=self.status,
        ):
            temp_parent = str(self.cookie_temp_dir) if self.cookie_temp_dir else None
            with tempfile.TemporaryDirectory(
                prefix="slackpipe-auth-", dir=temp_parent
            ) as auth_dir:
                os.chmod(auth_dir, 0o700)
                auth_path = Path(auth_dir) / "workspace.env"
                fd = os.open(
                    auth_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                )
                try:
                    os.write(
                        fd,
                        f"SLACK_TOKEN={workspace.token}\nSLACK_COOKIE={workspace.cookie}\n".encode(),
                    )
                finally:
                    os.close(fd)
                try:
                    child_env = _subprocess_environment(workspace.token, workspace.cookie)
                    child_env["HOME"] = auth_dir
                    self._execute(
                        (
                            self.executable,
                            "workspace",
                            "import",
                            str(auth_path),
                        ),
                        child_env,
                        workspace,
                        action="authentication",
                    )
                finally:
                    auth_path.unlink(missing_ok=True)
        self.status(f"Slackdump authentication preflight passed for {workspace.workspace_id}")

    def run_full_backfill(self, workspace: Workspace) -> SlackdumpResult:
        """Create the initial full archive, or finish/top off an existing one."""
        return self._run(workspace, self.plan_incremental(workspace))

    def run_attachment_backfill(self, workspace: Workspace) -> SlackdumpResult:
        return self._run(workspace, self.plan_attachment_backfill(workspace))

    def reconcile_attachments(
        self, workspace: Workspace, *, accepted_sizes: Mapping[str, int] | None = None
    ) -> AttachmentReconciliation:
        return reconcile_attachments(
            workspace_output_dir(self.output_root, workspace),
            accepted_sizes=accepted_sizes,
        )

    def backfill_with_reconciliation(
        self,
        workspace: Workspace,
        *,
        accepted_sizes: Mapping[str, int] | None = None,
        max_retries: int = 2,
    ) -> tuple[SlackdumpResult, AttachmentReconciliation]:
        result = self.run_attachment_backfill(workspace)
        reconciliation = self.reconcile_attachments(
            workspace, accepted_sizes=accepted_sizes
        )
        attempts = 0
        while attempts < max_retries and (
            reconciliation.size_mismatch_count or reconciliation.missing_count
        ):
            attempts += 1
            self.status(
                f"attachment reconcile found "
                f"{reconciliation.size_mismatch_count} size-mismatched and "
                f"{reconciliation.missing_count} missing files for workspace "
                f"{workspace.workspace_id}; redownload retry "
                f"{attempts}/{max_retries}"
            )
            result = self.run_attachment_backfill(workspace)
            reconciliation = self.reconcile_attachments(
                workspace, accepted_sizes=accepted_sizes
            )
        reconciliation = replace(reconciliation, retried_count=attempts)
        if reconciliation.size_mismatch_count:
            forgiven = [
                f"{file_id} served {served} bytes instead of {expected}"
                for file_id, _name, expected, served in reconciliation.mismatched_files
            ]
            self.status(
                f"WARNING: accepting "
                f"{reconciliation.size_mismatch_count} attachment size "
                f"mismatches for workspace {workspace.workspace_id} after "
                f"{attempts} retries: " + ", ".join(forgiven)
            )
            accepted_bytes = sum(
                abs(served - expected)
                for _file_id, _name, expected, served in reconciliation.mismatched_files
            )
            accepted_files = ",".join(
                sorted(
                    f"{file_id}={served}"
                    for file_id, _name, _expected, served in reconciliation.mismatched_files
                )
            )
            reconciliation = replace(
                reconciliation,
                size_mismatch_count=0,
                accepted_variance_count=len(reconciliation.mismatched_files),
                accepted_variance_bytes=accepted_bytes,
                accepted_variance_files=accepted_files,
                mismatched_files=(),
            )
        return result, reconciliation


    def _run(self, workspace: Workspace, plan: SlackdumpPlan) -> SlackdumpResult:
        plan.output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        if plan.output_dir.is_symlink() or not plan.output_dir.is_dir():
            raise SlackdumpExecutionError(
                f"workspace output path is not a directory: {plan.output_dir}"
            )
        self.status(f"starting Slackdump {plan.action} for workspace {workspace.workspace_id}")
        with GlobalFileLock(
            self.lock_path,
            timeout=self.lock_timeout,
            status=self.status,
        ):
            temp_parent = str(self.cookie_temp_dir) if self.cookie_temp_dir else None
            with tempfile.TemporaryDirectory(
                prefix="slackpipe-auth-", dir=temp_parent
            ) as auth_dir:
                os.chmod(auth_dir, 0o700)
                with netscape_cookie_file(
                    workspace.cookie,
                    directory=auth_dir,
                ) as cookie_path:
                    env = _subprocess_environment(workspace.token, workspace.cookie)
                    env["HOME"] = auth_dir
                    auth_command = (
                        self.executable,
                        "workspace",
                        "new",
                        "-load-env",
                        _slackdump_label(workspace.workspace_id),
                    )
                    self._execute(
                        auth_command,
                        env,
                        workspace,
                        action="authentication",
                    )
                    # Slackdump may report the Slack domain from auth.test but
                    # stores the credential under the explicit label supplied
                    # above. Select that label before running with
                    # ``-workspace`` so spaces/case in the real name remain a
                    # valid single identity throughout Slackpipe.
                    self._execute(
                        (
                            self.executable,
                            "workspace",
                            "select",
                            _slackdump_label(workspace.workspace_id),
                        ),
                        env,
                        workspace,
                        action="workspace-selection",
                    )
                    command = _with_workspace(
                        plan.command, _slackdump_label(workspace.workspace_id)
                    )
                    completed = self._execute(
                        command,
                        env,
                        workspace,
                        action=plan.action,
                    )
        self.status(f"finished Slackdump {plan.action} for workspace {workspace.workspace_id}")
        return SlackdumpResult(
            workspace=plan.workspace,
            action=plan.action,
            command=plan.command,
            output_dir=plan.output_dir,
            returncode=completed.returncode,
        )

    def _execute(
        self,
        command: tuple[str, ...],
        env: Mapping[str, str],
        workspace: Workspace,
        *,
        action: str,
    ) -> subprocess.CompletedProcess[str]:
        try:
            if self.executor is subprocess.run:
                completed = _run_streaming(
                    command,
                    env=env,
                    workspace=workspace,
                )
            else:
                completed = self.executor(
                    command,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
        except Exception as error:
            message = redact_subprocess_text(
                str(error), workspace.token, workspace.cookie
            )
            error_type = _classify_subprocess_error(
                action, _ANSI_PATTERN.sub("", message)
            )
            raise error_type(
                f"Slackdump {action} could not start: {message}"
            ) from error
        if completed.returncode != 0:
            detail = completed.stderr or completed.stdout or "no process output"
            detail = redact_subprocess_text(
                detail.strip(), workspace.token, workspace.cookie
            )
            normalized = _ANSI_PATTERN.sub("", detail)
            error_type = _classify_subprocess_error(action, normalized)
            raise error_type(
                f"Slackdump {action} failed with exit code "
                f"{completed.returncode}: {detail}"
            )
        return completed


def _run_streaming(
    command: tuple[str, ...],
    *,
    env: Mapping[str, str],
    workspace: Workspace,
) -> subprocess.CompletedProcess[str]:
    """Stream child output live while retaining redacted diagnostics for failures."""

    process = subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    captured: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        safe = redact_subprocess_text(line, workspace.token, workspace.cookie)
        captured.append(safe)
        sys.stdout.write(safe)
        sys.stdout.flush()
    returncode = process.wait()
    output = "".join(captured)
    return subprocess.CompletedProcess(command, returncode, stdout=output, stderr="")


def workspace_output_dir(output_root: str | Path, workspace: Workspace) -> Path:
    return Path(output_root) / workspace.workspace_id


_SAFE_FILE_ID = re.compile(r"^[A-Za-z0-9_-]+$")
_UNSAFE_FILENAME = re.compile(r'[<>:"/\\|?*]')
_RESERVED_FILENAME = re.compile(
    r"^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)", re.IGNORECASE
)


def expected_attachments(
    archive_dir: str | Path,
) -> tuple[dict[str, tuple[str, int, str, str]], int, int]:
    """Scan an archive for downloadable files without touching uploads.

    Returns ``(expected, skipped, unsafe)`` where expected maps file_id to
    ``(safe_name, expected_size, slack_mode, mimetype)``. Read-only: used by
    both reconciliation and rollout size-ordering.
    """

    import json
    import sqlite3

    root = Path(archive_dir).resolve(strict=True)
    db_path = root / "slackdump.sqlite"
    # file_id -> (safe_name, expected_size, slack_mode, mimetype)
    expected: dict[str, tuple[str, int, str, str]] = {}
    skipped = unsafe = 0
    with sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True) as db:
        rows = db.execute(
            """
            SELECT DATA FROM (
                SELECT DATA, row_number() OVER (
                    PARTITION BY CHANNEL_ID, TS
                    ORDER BY CHUNK_ID DESC, IDX DESC, rowid DESC
                ) AS newest_rank
                FROM MESSAGE
            ) WHERE newest_rank = 1
            """
        )
        for (raw,) in rows:
            payload = json.loads(bytes(raw).decode() if isinstance(raw, bytes) else str(raw))
            effective = (
                payload.get("message")
                if isinstance(payload.get("message"), dict)
                else payload
            )
            for item in effective.get("files") or []:
                if not isinstance(item, dict) or not item.get("id"):
                    continue
                if item.get("mode") in {"hidden_by_limit", "external", "tombstone"}:
                    skipped += 1
                    continue
                name = item.get("name")
                if not item.get("is_external") and not name:
                    skipped += 1
                    continue
                file_id = str(item["id"])
                if not _SAFE_FILE_ID.fullmatch(file_id):
                    unsafe += 1
                    continue
                record = (
                    _sanitize_filename(str(name or "unnamed_file")),
                    int(item.get("size") or 0),
                    str(item.get("mode") or ""),
                    str(item.get("mimetype") or ""),
                )
                if file_id in expected and expected[file_id] != record:
                    unsafe += 1
                    continue
                expected[file_id] = record
    return expected, skipped, unsafe


def reconcile_attachments(
    archive_dir: str | Path, *, accepted_sizes: Mapping[str, int] | None = None
) -> AttachmentReconciliation:
    """Strictly reconcile downloadable message files against Slackdump uploads.

    ``accepted_sizes`` extends the built-in documented-variance record with
    extra file_id -> served-bytes entries (e.g. from configuration). A
    mismatch whose on-disk size exactly equals the accepted value passes
    as documented variance; anything else stays a hard mismatch.
    """

    root = Path(archive_dir).resolve(strict=True)
    uploads = root / "__uploads"
    expected, skipped, unsafe = expected_attachments(root)

    present = missing = size_mismatch = expected_bytes = present_bytes = 0
    mismatches: list = []
    snippet_variance = snippet_variance_bytes = 0
    documented_variance = documented_variance_bytes = 0
    documented_files: list[str] = []
    accepted = {**DOCUMENTED_SIZE_VARIANCE, **dict(accepted_sizes or {})}
    for file_id, (name, expected_size, slack_mode, mimetype) in expected.items():
        expected_bytes += expected_size
        path = uploads / file_id / name
        try:
            if path.is_symlink() or path.parent.is_symlink():
                unsafe += 1
                continue
            stat = path.stat()
        except FileNotFoundError:
            missing += 1
            continue
        if not path.is_file() or not path.resolve().is_relative_to(root):
            unsafe += 1
            continue
        present += 1
        present_bytes += stat.st_size
        if expected_size > 0 and stat.st_size != expected_size:
            delta = abs(stat.st_size - expected_size)
            if (
                slack_mode == "snippet"
                and mimetype.startswith("text/")
                and delta <= SNIPPET_VARIANCE_MAX_BYTES
            ):
                snippet_variance += 1
                snippet_variance_bytes += delta
            elif accepted.get(file_id) == stat.st_size:
                documented_variance += 1
                documented_variance_bytes += delta
                documented_files.append(f"{file_id}={stat.st_size}")
            else:
                size_mismatch += 1
                mismatches.append((file_id, name, expected_size, stat.st_size))
    return AttachmentReconciliation(
        expected_count=len(expected),
        present_count=present,
        skipped_count=skipped,
        missing_count=missing,
        size_mismatch_count=size_mismatch,
        unsafe_count=unsafe,
        expected_bytes=expected_bytes,
        present_bytes=present_bytes,
        snippet_variance_count=snippet_variance,
        snippet_variance_bytes=snippet_variance_bytes,
        documented_variance_count=documented_variance,
        documented_variance_bytes=documented_variance_bytes,
        documented_variance_files=",".join(sorted(documented_files)),
        mismatched_files=tuple(mismatches),
    )


def _sanitize_filename(value: str) -> str:
    safe = _UNSAFE_FILENAME.sub("_", value).rstrip(" .") or "unnamed_file"
    return f"_{safe}" if _RESERVED_FILENAME.match(safe) else safe


@contextmanager
def netscape_cookie_file(
    cookie: str, *, directory: str | Path | None = None
) -> Iterator[Path]:
    """Create a private Netscape cookie file and unlink it on every exit path."""
    if not cookie or any(char.isspace() or ord(char) == 127 for char in cookie):
        raise ValueError("Slack cookie contains invalid whitespace")
    temp_directory = str(directory) if directory is not None else None
    fd, filename = tempfile.mkstemp(prefix="slackpipe-cookie-", dir=temp_directory)
    path = Path(filename)
    try:
        os.fchmod(fd, 0o600)
        content = (
            "# Netscape HTTP Cookie File\n"
            ".slack.com\tTRUE\t/\tTRUE\t2147483647\td\t"
            f"{cookie}\n"
        )
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        yield path
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def redact_subprocess_text(text: str, *secrets: str) -> str:
    redacted = text
    for secret in sorted((secret for secret in secrets if secret), key=len, reverse=True):
        redacted = redacted.replace(secret, "[REDACTED]")
    return _TOKEN_PATTERN.sub("[REDACTED]", redacted)


def _subprocess_environment(token: str, cookie: str) -> dict[str, str]:
    env = dict(os.environ)
    for name in tuple(env):
        if name in {"SLACK_TOKEN", "SLACK_COOKIE"} or re.fullmatch(
            r"SLACKPIPE_WORKSPACE_.*_(?:TOKEN|COOKIE)", name
        ):
            env.pop(name)
    env["SLACK_TOKEN"] = token
    env["SLACK_COOKIE"] = cookie
    return env


def _slackdump_label(workspace_id: str) -> str:
    """Return the credential label Slackdump stores for a workspace id.

    Slackdump lowercases the label on ``workspace new`` but resolves
    ``-workspace`` case-sensitively, so the exact lowercased form must be
    used for ``new``, ``select``, and ``-workspace`` alike. ``lower()``
    (not ``casefold()``) mirrors Go's ``strings.ToLower`` for this purpose.
    Spaces and punctuation are preserved; only case is normalized.
    """
    return workspace_id.lower()


def _with_workspace(command: tuple[str, ...], workspace_id: str) -> tuple[str, ...]:
    if command[1:3] == ("tools", "redownload"):
        return (*command[:3], "-workspace", workspace_id, *command[3:])
    return (*command[:2], "-workspace", workspace_id, *command[2:])
