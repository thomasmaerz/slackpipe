# Operations runbook

All paths and hosts below are examples. Keep installation-specific values in
ignored configuration files or a secret manager.

## Storage contracts

| Data | Access |
|---|---|
| Workspace credentials | Read-only secret mount, mode `0600` recommended |
| Raw Slackdump root | Writable by Slackpipe only |
| Canonical DuckDB directory | Single Slackpipe writer |
| Dagster storage | PostgreSQL plus durable compute-log/artifact volume |
| Slackquery canonical mount | Read-only |

Never copy a live SQLite archive directly. Slackpipe uses SQLite's backup API to
produce a coherent local snapshot before transformation.

## Initial rollout

1. Copy `.env.example` and `deployment.env.example` to ignored files.
2. Set strong database credentials and local bind paths.
3. Start PostgreSQL, Pushgateway, Dagster, and code servers.
4. Verify every code location loads.
5. Launch each `<workspace_slug>_ingest_once` job.
6. Confirm `archive_valid`, `attachment_integrity`, and
   `canonical_integrity` checks.
7. Start recurring schedules only after initial success.

Initial ingestion ignores `SLACKPIPE_INCREMENTAL_STALE_AFTER`. Steady-state
resume jobs may use the configured stale window to bound channel and thread
rescans.

## Routine monitoring

Check:

- Dagster daemon and code-location health;
- failed, canceled, queued, or unusually long runs;
- schedule and sensor ticks, including skipped ticks;
- latest asset checks per workspace;
- Pushgateway latest-run success and completion-lag gauges;
- free space for raw archives, attachments, snapshots, DuckDB, and logs.

## Failure handling

### Authentication failures

Validate credentials with Slackdump's real `auth.test`. Rotate the ignored
credential file, reload the code location, and retry. Never paste credentials
into Dagster run config or logs.

### Lock timeout

Identify the existing raw-archive or canonical writer. Do not delete a lock file
until confirming no process holds its advisory lock. Stale empty lock files are
harmless; the kernel lock is authoritative.

### Incomplete archive

Resume extraction. Slackpipe rejects incomplete initial archives and source
continuity regressions before canonical mutation.

### Attachment mismatch

Rerun the attachment stage and inspect source metadata versus local byte counts.
Use `SLACKPIPE_ATTACHMENT_SIZE_ACCEPT` only for reviewed, documented upstream
variance—not as a blanket bypass.

### Canonical validation failure

Preserve the source snapshot and failed run metadata. Because transformation is
transactional, the previous canonical state remains available. Diagnose key-set,
count, hash, timestamp, or lineage differences before retrying.

## Compaction

Compaction is offline and proof-gated:

1. stop extraction for the workspace;
2. compact to a new SQLite destination;
3. verify newest-row maps and semantic equivalence;
4. preserve the original archive;
5. replace only after successful verification;
6. rebase ingestion checkpoints to the compacted source identity.

## Backup and recovery

Back up together:

- raw Slackdump archives and attachments;
- canonical DuckDB after a checkpoint;
- Dagster PostgreSQL storage;
- ignored deployment configuration through a secure secret backup system.

Restore raw and canonical state consistently. Run source/canonical validation
before re-enabling schedules.

## Slackquery integration

[Slackquery](https://github.com/thomasmaerz/slackquery) is an independent code
location and repository. Mount the canonical DuckDB and attachment tree into its
writer read-only. Slackquery's state, artifacts, and embedding service remain
outside Slackpipe ownership.
