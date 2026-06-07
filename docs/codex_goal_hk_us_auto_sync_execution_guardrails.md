# Goal: HK/US Low-risk Auto-sync Execution Guardrails and Recovery

You are in autonomous iteration mode only after the user explicitly starts this
goal.

The user wants Codex to continue infrastructure work while the existing
A-share low-risk auto-sync may be running. Build guarded HK/US low-risk
auto-sync execution support, checkpoint recovery, retry semantics, command
generation, tests, and runbook updates. Do not start HK/US real execution
during this goal. Do not interfere with any active A-share low-risk auto-sync.

## Current State

- Durable mirror root: `/mnt/gw/TuShare`
- Backup root: `/mnt/gw/TuShare-backup`
- A-share low-risk auto-sync may be running as external production activity.
- A-share low-risk auto-sync execute support exists and is currently the only
  enabled auto-sync execute path.
- HK/US low-risk scopes exist:
  - `hk-low-risk`
  - `us-low-risk`
  - `global-equity-low-risk`
- HK/US executable low-risk endpoints have been probed and configured:
  - HK: `hk_basic`, `hk_tradecal`, `hk_daily`, `hk_daily_adj`, `hk_adjfactor`
  - US: `us_basic`, `us_tradecal`, `us_daily`, `us_daily_adj`, `us_adjfactor`
- HK/US plan-only or disabled endpoints remain excluded:
  - HK intraday/realtime: `hk_mins`, `rt_hk_k`
  - HK financial/PIT: `hk_income`, `hk_balancesheet`, `hk_cashflow`,
    `hk_fina_indicator`
  - US financial/PIT: `us_income`, `us_balancesheet`, `us_cashflow`,
    `us_fina_indicator`
- HK/US `mirror-auto-sync --execute` is currently blocked by design with:
  `mirror-auto-sync execute currently supports only scope a-share-low-risk`.
- Existing A-share baseline must remain intact.

## Hard Boundaries

- Do not execute HK/US auto-sync against durable roots.
- Do not execute HK/US `mirror-run`.
- Do not run generated command scripts.
- Do not fetch real HK/US historical data.
- Do not backfill HK/US ranges.
- Do not stop, restart, signal, or modify any running A-share auto-sync
  process.
- Do not mutate the existing A-share auto-sync state file.
- Do not write inside `/mnt/gw/TuShare` or `/mnt/gw/TuShare-backup` during
  durable checks.
- Tests may write only to temporary test roots.
- Diagnostic artifacts may write only to explicit `/tmp` output paths.
- Do not add minute, tick, realtime, order book, financial PIT, object/PDF,
  news/research download, PostgreSQL loader, remote backup, restore-into,
  compaction executor, scheduler daemon, or parallel execution.
- Do not enable `global-equity-low-risk` auto-sync execution in this goal.
- Do not output token plaintext.
- Commit each completed implementation phase separately.
- Do not commit empty commits.

## Important Concurrency Constraint

The currently running A-share process was launched from the code loaded at its
start time. Any new lock or coordination code added by this goal will not be
loaded by that already-running process.

Therefore this goal may build and test HK/US auto-sync execution support, but
must not authorize starting real HK/US execution while an older A-share
auto-sync process is still active against the same durable mirror and backup
roots. A later operator step can start HK/US only after:

- the active A-share run has finished, or
- A-share is restarted under a lock-aware version of the code, and
- the final HK/US gate reports no blockers.

This protects the shared catalog, shared raw/lake roots, and shared backup
target from uncoordinated concurrent writers.

## Deliverable Boundary

This goal is complete only when:

- HK/US auto-sync execute support is implemented for `hk-low-risk` and
  `us-low-risk` behind explicit guardrails.
- `global-equity-low-risk` remains dry-run/readiness only.
- `global-equity-low-risk` is a reporting composition of A-share, HK, and US
  low-risk scopes. It is intentionally not an auto-sync execution scope in this
  goal because it would combine multiple market calendars, checkpoints, retry
  domains, and stop conditions in one long-running writer.
- A mirror write-lock model exists and is covered by tests.
- HK/US checkpoint state supports crash/interruption recovery.
- Retry behavior is conservative, bounded, and error-class aware.
- HK/US daily-like execution stages market calendar dependencies before
  daily-like endpoints and never uses natural-day fallback.
- Existing A-share auto-sync behavior and tests remain unchanged except for
  lock-aware compatibility that is backward compatible.
- A final gate prevents HK/US execution while an incompatible A-share writer is
  active or while required locks cannot be acquired.
- Dry-run and fake-execution tests prove the new behavior without real
  Tushare requests.
- Durable checks are read-only or write only explicit `/tmp` artifacts.
- No real HK/US auto-sync has been started.

## Acceptance Criteria

Functional:

- `python3 -m tushare_mirror mirror-auto-sync --scope hk-low-risk ... --json`
  remains dry-run by default and shows:
  - planned windows
  - checkpoint path
  - executable endpoint list
  - excluded endpoint list
  - calendar dependency summary
  - retry policy
  - lock requirements
  - recovery status
- `python3 -m tushare_mirror mirror-auto-sync --scope us-low-risk ... --json`
  provides the same contract.
- HK/US execute mode requires all of:
  - `--execute`
  - `--confirm-auto-sync`
  - a new explicit HK/US execution confirmation control
  - `--state`
  - valid root and backup paths
  - valid writable state path outside mirror and backup roots
  - market scope in `hk-low-risk` or `us-low-risk`
  - `max-jobs-per-api <= 20`
  - `window-days <= max-jobs-per-api`
  - successful final preflight gate
- HK/US execute mode refuses:
  - `global-equity-low-risk`
  - missing market calendar dependency plan
  - disabled or plan-only endpoints
  - active incompatible mirror writer
  - unsafe state paths
  - missing token
  - missing catalog
  - backup mutation or restore-check blockers
  - schema quarantine or incompatible schema blockers
- Generated command previews are guarded with `USER_CONFIRMATION_REQUIRED`.
- JSON output contains `report_version` and stable field names.

Safety:

- No HK/US execute command is run during this goal.
- No durable root writes happen during durable verification.
- A-share test coverage remains green.
- A-share endpoint scope, A-share auto-sync state semantics, and A-share
  command syntax remain backward compatible.
- Token availability is reported only as true/false; token values are never
  printed.

Recovery:

- Checkpoint state records enough data to resume safely after interruption:
  - `state_version`
  - `scope`
  - `root`
  - `backup`
  - `from_date`
  - `to_date`
  - `resolved_to_date`
  - `window_days`
  - `max_jobs_per_api`
  - `completed_windows`
  - `in_progress_window`
  - `failed_windows`
  - `next_start_date`
  - `attempt_history`
  - `last_error_type`
  - `last_run_id`
  - `updated_at`
- State writes are atomic enough for process interruption:
  write temp file, flush, replace target.
- On resume, an interrupted window is treated conservatively:
  - inspect catalog/run summary when possible
  - if completion cannot be proven, retry the same bounded window
  - do not skip ahead from an unverified interrupted window
- A failed non-retryable window stops later windows.
- Retry attempts are bounded and visible in state and output.

## Suggested CLI Design

Keep existing A-share syntax valid:

```bash
python3 -m tushare_mirror mirror-auto-sync \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope a-share-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state "$AUTO_SYNC_STATE" \
  --max-attempts 3 \
  --retry-backoff-seconds 60 \
  --execute \
  --confirm-auto-sync \
  --json
```

Add a separate HK/US confirmation layer. Exact flag names may be adjusted
during implementation, but they must be explicit and backward compatible.

Do not require the operator to retype a long parameter-derived confirmation
phrase. The confirmation phrase should be generated by code from the parsed
arguments and shown in dry-run/status JSON for review. The execute command
should require a simple explicit control such as `--confirm-hk-us-auto-sync`.
That flag means "I reviewed the generated phrase and guarded command"; the
phrase itself is not a secret and is not a security boundary.

One acceptable HK design:

```bash
python3 -m tushare_mirror mirror-auto-sync \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope hk-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --max-attempts 3 \
  --retry-backoff-seconds 60 \
  --execute \
  --confirm-auto-sync \
  --confirm-hk-us-auto-sync \
  --json
```

For US:

```bash
python3 -m tushare_mirror mirror-auto-sync \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope us-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /mnt/gw/TuShare-us-auto-sync-state.json \
  --max-attempts 3 \
  --retry-backoff-seconds 60 \
  --execute \
  --confirm-auto-sync \
  --confirm-hk-us-auto-sync \
  --json
```

Dry-run and readiness output should include:

- `confirmation_phrase`
- `confirmation_reviewed=false` unless the explicit confirmation flag is
  supplied
- `do_not_run_automatically=true`

Example generated phrase:

```text
CONFIRM HK-LOW-RISK AUTO-SYNC 19900101-latest-trade-date MAXJOBS20
```

## Lock and Active Writer Model

Add a lock abstraction and a legacy active-writer detector used by auto-sync
execution paths. This belongs with the execution gate: the lock has no value if
the gate does not enforce it.

Required properties:

- Lock acquisition is required before any auto-sync execution window starts.
- Lock metadata records:
  - `lock_version`
  - `scope`
  - `root`
  - `backup`
  - `pid`
  - `hostname`
  - `started_at`
  - `command_kind`
  - `state_path`
- Lock path is either:
  - inside the explicit `--state` directory when available, or
  - an explicit `/tmp` or user-provided lock path for dry-run tests, or
  - a carefully documented mirror-root lock only during real execution after
    user confirmation.
- Lock must not be acquired during read-only reports.
- Stale lock detection must be conservative:
  - if the PID is alive on the same host, block
  - if liveness cannot be determined, block unless user supplies a future
    explicit stale-lock recovery command
  - do not delete stale locks automatically in this goal
- Existing A-share behavior must remain backward compatible. If A-share is
  restarted under the new code, it should also use the lock before execution.

Legacy active-writer detection:

- A currently running A-share process may have been started before this lock
  code existed and therefore may not hold a lock.
- Before allowing HK/US execute on durable roots, inspect for likely active
  writers even when no lock file exists.
- Signals should include:
  - same-host process command line matching `python3 -m tushare_mirror
    mirror-auto-sync` with `--execute` and the same `--root`
  - same-host process command line matching `python3 -m tushare_mirror
    mirror-run` with `--execute` and the same `--root`
  - recent write activity on catalog files such as `catalog.sqlite`,
    `catalog.sqlite-wal`, or `catalog.sqlite-shm`
  - recent raw/lake file writes under the mirror root
- If any signal indicates a possible active writer, block HK/US execute with a
  clear message and recommend waiting for the current run to finish or
  restarting A-share under lock-aware code later.
- Do not rely only on SQLite WAL existence, because WAL presence alone is not
  proof of a live writer.
- Do not delete stale locks automatically in this goal. If a stale lock blocks
  future work, report an explicit recovery recommendation; destructive cleanup
  must remain a separate user-confirmed action.

## Retry Policy

Retryable:

- `rate_limited`
- `network_error`
- `server_error`
- temporary `unknown_error` only when the underlying item is a fetch failure
  and validation/backup/restore-check have not failed

Non-retryable:

- `permission_denied`
- `invalid_params`
- `invalid_endpoint`
- schema incompatible
- quarantine
- validation failure
- backup failure
- restore-check failure
- unsafe path
- disabled or plan-only endpoint selected

Retry behavior:

- Retry only the current bounded window.
- Do not advance `next_start_date` until validation, backup, and restore-check
  pass.
- Use `max_attempts` and `retry_backoff_seconds`.
- Optionally add jitter, but keep deterministic tests.
- Include the final error type and per-attempt history in state and JSON.

## Phase 0: Baseline and A-share Protection Snapshot

Run:

```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
```

Read-only A-share checks only:

```bash
python3 -m tushare_mirror mirror-scope --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-auto-sync \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --scope a-share-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /tmp/tushare-a-share-auto-sync-dry-run-state.json \
  --json
```

Do not inspect or modify the live A-share state unless the user explicitly asks.

Implementation reconciliation:

- Review current HK/US commits and code before implementing.
- Known completed work to account for includes, at minimum:
  - `3f6177a` HK/US readiness report integration
  - `23f5cca` HK/US auto-sync planning guardrails
  - `21f06e0` HK/US pull command generation
  - `69b039c` HK/US mirror runbook
  - `7e01644` HK/US mirror orchestration support
- Treat completed pieces as baseline. Do not reimplement or churn them unless a
  test or acceptance criterion exposes a concrete gap.
- Record the remaining gap list in the Phase 0 notes before moving to Phase 1.

If baseline fails, fix and commit:

```text
fix: restore HK US auto-sync baseline health
```

## Phase 1: Finalize HK/US Auto-sync Readiness Model

Add a read-only readiness model for HK/US auto-sync execution.

Output fields:

- `report_version`
- `scope`
- `execute_supported`
- `execute_blocked_reason`
- `executable_endpoints`
- `excluded_endpoints`
- `calendar_api`
- `calendar_dependency_status`
- `pagination_summary`
- `state_path_status`
- `lock_status`
- `backup_status`
- `restore_check_status`
- `schema_status`
- `token_available`
- `warnings`
- `blocking_errors`

Tests:

- HK ready model with fake healthy root.
- US ready model with fake healthy root.
- Missing token blocks execute readiness without printing token.
- Missing catalog blocks.
- Plan-only endpoints remain excluded.
- JSON stable.
- No side effects.

Commit:

```text
feat: add HK US auto-sync readiness model
```

## Phase 2: Add Lock and Active Writer Detection Infrastructure

Implement a small lock manager and legacy active-writer detector for mirror
auto-sync execution.

Requirements:

- acquire/release context manager
- metadata JSON
- liveness checks
- conservative stale-lock behavior
- same-root active process detection
- recent catalog/raw/lake write signal reporting
- read-only status report
- fake-test support without durable roots

Tests:

- acquire and release lock.
- second writer blocked.
- same-host live PID blocks.
- unknown/stale lock blocks by default.
- active legacy A-share writer without lock blocks HK/US execute.
- recent catalog write signal blocks or warns according to gate policy.
- no lock during dry-run.
- A-share auto-sync fake execute still works under lock-aware code.

Commit:

```text
feat: add mirror auto-sync writer detection
```

## Phase 3: Checkpoint State V2 and Recovery

Enhance auto-sync checkpoint state without breaking existing A-share state V1.

Behavior:

- read V1 state and continue to support it
- write V2 for new runs when recovery fields are needed
- record in-progress window before execution starts
- atomically update completed/failed windows
- on resume, recover interrupted in-progress window conservatively
- never skip an unverified interrupted window

Tests:

- V1 A-share state still resumes.
- V2 HK state resumes.
- interrupted window retried.
- completed window not rerun.
- malformed state blocks with clear error.
- atomic write path tested with temporary files.

Commit:

```text
feat: add auto-sync checkpoint recovery state
```

## Phase 4: HK/US Execution Gate

Replace the current hardcoded HK/US execute block with a full gate.

Gate must pass before HK/US execute:

- scope is `hk-low-risk` or `us-low-risk`
- `global-equity-low-risk` is blocked
- explicit HK/US confirmation flag present
- confirmation phrase is generated from scope/date/max_jobs and included in
  output
- state path is safe
- lock can be acquired
- catalog exists
- token available
- market calendar dependency plan exists
- disabled and plan-only endpoints excluded
- max jobs and window size guardrails pass
- backup and restore-check readiness clear
- schema/quarantine clear
- no incompatible active writer detected

Tests:

- HK execute blocked without extra confirmation.
- US execute blocked without extra confirmation.
- generated phrase is stable for the same parsed arguments.
- generated phrase changes when scope, date range, or max jobs changes.
- global execute blocked.
- active lock blocked.
- active legacy A-share writer blocked.
- fake healthy HK execute gate passes.
- fake healthy US execute gate passes.
- A-share execute path still requires only existing confirmation controls.

Commit:

```text
feat: add guarded HK US auto-sync execution gate
```

## Phase 5: HK/US Window Execution Sequencing

Implement HK/US auto-sync fake execution using existing `MirrorOrchestrator`
support.

Required sequence per window:

1. acquire execution lock
2. write `in_progress_window`
3. call the existing `MirrorOrchestrator.run(..., mode="pilot", ...)` for that
   bounded window. This is not a new mode; it is the current bounded pilot
   mode already used by `mirror-run`.
4. fetch market calendar dependency through orchestrator before daily-like
   endpoints
5. run daily-like endpoints using local market calendar only
6. validate
7. backup
8. restore-check
9. mark window succeeded and advance checkpoint
10. release lock

Do not add parallel execution. Do not add stock loops. Do not include disabled
or plan-only endpoints.

Tests:

- HK fake window executes expected endpoints.
- US fake window executes expected endpoints.
- `hk_tradecal` precedes HK daily-like endpoints.
- `us_tradecal` precedes US daily-like endpoints.
- no natural-day fallback.
- plan-only endpoints excluded from execution summary.
- validation/backup/restore failure stops advancement.

Commit:

```text
feat: support guarded HK US auto-sync fake execution
```

## Phase 6: Retry and Stop Semantics

Finalize window-level retry behavior for HK/US auto-sync. This phase should use
the existing error classification where possible; it is not a request-level
retry loop and must not retry individual endpoint pages independently.

Tests:

- rate limit retry succeeds on later attempt.
- network failure retry stops after max attempts.
- permission denied stops immediately.
- invalid params stops immediately.
- schema/quarantine stops immediately.
- validation failure stops immediately.
- backup failure stops immediately.
- restore-check failure stops immediately.
- state records attempt history and last error.

Commit:

```text
feat: add HK US auto-sync retry stop semantics
```

## Phase 7: Command Preview and Script Generation

Update guarded command generation for HK/US auto-sync.

Add or update a read-only/file-output command if needed:

```bash
python3 -m tushare_mirror mirror-auto-sync-command \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --scope hk-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --output /tmp/tushare-hk-auto-sync-command \
  --json
```

Output files:

- `README.md`
- `commands.sh`
- `readiness.json`
- `request_estimate.json`
- `stop_policy.json`

Rules:

- output only to user-provided path
- refuse output inside mirror root
- refuse output inside backup root
- refuse existing output unless `--overwrite`
- generated commands are guarded and commented
- no script is executed
- no token plaintext

Tests:

- HK command bundle generated.
- US command bundle generated.
- unsafe output blocked.
- commands contain `USER_CONFIRMATION_REQUIRED`.
- command safety warning-only.
- no side effects.

Commit:

```text
feat: add guarded HK US auto-sync command bundle
```

## Phase 8: Read-only Status and Recovery Reports

Add read-only status reports for operators.

Suggested commands:

```bash
python3 -m tushare_mirror mirror-auto-sync-status \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --json

python3 -m tushare_mirror mirror-auto-sync-recovery-plan \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --scope hk-low-risk \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --json
```

Report:

- current state version
- scope/root/backup
- next window
- completed window count
- failed window count
- in-progress window
- last run id
- last error type
- recovery action
- whether execution may resume after user confirmation
- blocking errors
- warnings

Tests:

- clean state status.
- interrupted state recovery plan.
- failed non-retryable state blocks resume.
- malformed state blocks.
- JSON stable.
- read-only.

Commit:

```text
feat: add auto-sync status and recovery reports
```

## Phase 9: A-share Regression and Compatibility Suite

Add tests proving A-share low-risk baseline is not changed.

Cover:

- existing A-share dry-run contract.
- existing A-share execute confirmation requirements.
- A-share fake execute still works.
- A-share V1 state resumes.
- A-share command syntax remains valid.
- A-share endpoint list unchanged.
- A-share disabled/plan-only endpoint exclusions unchanged.
- A-share auto-sync does not require HK/US-specific confirmation controls.

Commit:

```text
test: protect A-share auto-sync baseline
```

## Phase 10: HK/US Auto-sync Read-only Contract Suite

Add comprehensive tests for all new HK/US commands.

Cover:

- no durable root writes during dry-run.
- no backup writes during dry-run.
- no generated script execution.
- no token plaintext in JSON or generated files.
- state path outside roots required.
- unsafe paths refused.
- lock not acquired during read-only reports.
- real Tushare client not required for dry-run.
- fake client only in execute tests.

Commit:

```text
test: add HK US auto-sync read-only contracts
```

## Phase 11: Runbook Update

Update `docs/hk_us_low_risk_pull_runbook.md` and
`docs/tushare_mirror_phase1_runbook.md`.

Document:

- HK/US auto-sync execution is now guarded, not automatic.
- A-share active run must not be disturbed.
- Why the already-running A-share process cannot be coordinated by newly added
  lock code until it is restarted.
- Required command order before any real HK/US execution.
- Confirmation phrase.
- State file recommendations:
  - `/mnt/gw/TuShare-hk-auto-sync-state.json`
  - `/mnt/gw/TuShare-us-auto-sync-state.json`
- Recovery commands.
- Retry semantics.
- Stop conditions.
- Backup and restore-check after every window.
- `global-equity-low-risk` remains not executable by auto-sync.

Commit:

```text
docs: add HK US auto-sync guardrail runbook
```

## Phase 12: Durable Read-only Checks

Run only read-only or `/tmp` output commands.

Environment:

```bash
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
```

Commands:

```bash
python3 -m tushare_mirror mirror-scope --scope hk-low-risk --json
python3 -m tushare_mirror mirror-scope --scope us-low-risk --json
python3 -m tushare_mirror mirror-scope --scope global-equity-low-risk --json

python3 -m tushare_mirror mirror-auto-sync \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope hk-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /tmp/tushare-hk-auto-sync-dry-run-state.json \
  --json

python3 -m tushare_mirror mirror-auto-sync \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope us-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /tmp/tushare-us-auto-sync-dry-run-state.json \
  --json
```

Generate command bundles in `/tmp` only:

```bash
rm -rf /tmp/tushare-hk-auto-sync-command /tmp/tushare-us-auto-sync-command

python3 -m tushare_mirror mirror-auto-sync-command \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope hk-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --output /tmp/tushare-hk-auto-sync-command \
  --json

python3 -m tushare_mirror mirror-auto-sync-command \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope us-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /mnt/gw/TuShare-us-auto-sync-state.json \
  --output /tmp/tushare-us-auto-sync-command \
  --json
```

Do not run generated commands.
Do not execute HK/US auto-sync.
Do not execute HK/US mirror-run.
Do not fetch real HK/US data.

Final tests:

```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
```

## Final Report

Report:

- commits
- HK auto-sync readiness status
- US auto-sync readiness status
- lock model status
- checkpoint recovery status
- retry and stop semantics
- command bundle status
- A-share baseline status
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next user-confirmed action

Stop after Phase 12.
Do not execute HK/US auto-sync.
Do not execute HK/US mirror-run.
Do not fetch real HK/US data.
