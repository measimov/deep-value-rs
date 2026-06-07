# HK/US Auto-sync Phase 0 Reconciliation

This note records the Phase 0 baseline for
`codex_goal_hk_us_auto_sync_execution_guardrails.md`.

## Baseline

Commands run:

- `git status --short`
- `python3 -m unittest discover tests/tushare_mirror -v`
- `python3 -m compileall tushare_mirror tests/tushare_mirror`
- `git diff --check`
- `python3 scripts/tushare_real_smoke.py --help`

Result:

- `python3 -m unittest discover tests/tushare_mirror -v`: 437 tests OK.
- `python3 -m compileall tushare_mirror tests/tushare_mirror`: OK.
- `git diff --check`: OK.
- `python3 scripts/tushare_real_smoke.py --help`: OK.
- `git status --short`: clean before implementation.

No real HK/US requests were executed. No HK/US `mirror-run` or
`mirror-auto-sync --execute` was executed.

## Completed HK/US Work To Preserve

Current history already includes HK/US low-risk infrastructure:

- `14705c4` defines HK/US low-risk mirror scopes.
- `2592445` adds HK/US low-risk endpoint configs and inventory.
- `ef1e6bb` adds HK/US low-risk planners.
- `7358cd5` adds fake coverage for HK/US low-risk endpoints.
- `7e01644` supports HK/US low-risk mirror orchestration.
- `3f6177a` integrates HK/US low-risk readiness reports.
- `21f06e0` adds HK/US low-risk pull command generation.
- `23f5cca` adds HK/US auto-sync planning guardrails.
- `69b039c` adds the HK/US low-risk mirror runbook.
- `5126466`, `3aa3884`, and `a0e773d` fix durable HK/US readiness and
  inventory/report contracts.

These are baseline capabilities and should not be reimplemented unless a
concrete test or acceptance criterion exposes a gap.

## Observed Current State

- `mirror-scope --scope a-share-low-risk --json` is read-only and reports the
  current A-share executable and disabled endpoint sets.
- `mirror-auto-sync` dry-run for `a-share-low-risk` against the durable root
  planned windows from `19900101` to `latest-trade-date` and did not execute.
- Existing HK/US tests prove current support for:
  - endpoint source maps and probe metadata
  - endpoint configs and inventory
  - fake fetch/write/read/validate fixtures
  - planner support
  - mirror orchestration support
  - report integration
  - guarded pull command generation
  - dry-run-only HK/US auto-sync planning

## Remaining Gaps

The active goal should focus on these gaps:

- HK/US auto-sync readiness model for execution, not just planning.
- Writer lock and legacy active-writer detection so HK/US cannot collide with
  a running A-share writer.
- Checkpoint state recovery with interrupted-window semantics.
- Replacement of the hardcoded HK/US execute block with a guarded gate for
  `hk-low-risk` and `us-low-risk`.
- Window-level retry and stop semantics for HK/US auto-sync.
- Guarded HK/US auto-sync command bundle generation.
- Read-only status and recovery reports.
- A-share regression tests proving the existing baseline remains compatible.
- Durable read-only checks only; no HK/US execution during this goal.
