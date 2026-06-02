# Goal: Bounded Code-date Matrix Planning Infrastructure

You are in autonomous iteration mode.

The user wants infrastructure only. Do not fetch real Tushare data, do not execute mirror-run, do not backfill new dates, and do not enable executable code loops.

Current state:
- Durable pilot exists at /mnt/gw/TuShare
- Backup exists at /mnt/gw/TuShare-backup
- Low-risk January 2025 pilot succeeded
- mirror-review, mirror-readiness, mirror-batch-plan exist
- api-infra-readiness exists
- endpoint taxonomy, planner registry, disabled inventory scaffolding, execution policy guardrails exist
- code-universe exists
- code-list-plan exists
- code_list / code_date_matrix execution remains blocked
- 12 enabled executable endpoints
- 22 disabled inventory-only endpoints
- Tests last passed: 144 OK
- Worktree should start clean

Hard boundaries:
- Do not execute mirror-run.
- Do not fetch real Tushare data.
- Do not backfill new dates.
- Do not add new executable endpoints.
- Do not loop over all stocks.
- Do not execute code-list or code-date jobs.
- Do not enable executable code loops.
- Do not touch minute/tick/order/realtime execution.
- Do not touch financial PIT execution.
- Do not implement PostgreSQL loader.
- Do not fetch PDFs/news/research/object data.
- Do not implement remote backup, restore-into, compaction executor, scheduler, or parallel execution.
- Do not output token plaintext.
- Commit each completed phase separately.
- Do not commit empty commits.

Baseline first:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

If baseline fails, fix and commit:
fix: restore code-date matrix planning baseline health

## Phase 1: Code-date matrix planner model

Add plan-only model classes for bounded code/date matrix planning.

A code-date matrix plan combines:
- local code universe
- explicit date list or bounded date range
- optional trading-days-only calendar filter
- endpoint params template
- max code limit
- max date limit
- max candidate jobs limit

Required hard limits in this phase:
- limit-codes required
- limit-codes <= 20
- max-dates <= 20
- max-candidate-jobs <= 100
- execution_allowed=false

No real execution.

Data structures should include:
- CodeDateMatrixPlan
- CodeDateMatrixItem
- CodeDateMatrixSummary

Each item:
- api_name
- ts_code
- date
- params
- job_key
- existing_status if available
- planned_action
- would_require_real_request=true
- execution_allowed=false
- blocked_reason

Summary:
- api_name
- universe
- source_snapshot_id
- total_codes
- planned_codes
- total_dates
- planned_dates
- candidate_jobs
- planned_jobs
- truncated_by_code_limit
- truncated_by_date_limit
- truncated_by_candidate_limit
- execution_allowed=false
- dry_run=true
- warnings
- blocking_errors

Tests:
- model serialization
- candidate limit calculation
- truncation flags
- JSON stable

Commit:
feat: add code-date matrix plan model

## Phase 2: Implement code-date-matrix-plan CLI

Add CLI:
python3 -m tushare_mirror code-date-matrix-plan --api stk_managers --universe a_share_listed --limit-codes 3 --dates 20250102,20250103
python3 -m tushare_mirror code-date-matrix-plan --api stk_managers --universe a_share_listed --limit-codes 3 --start-date 20250101 --end-date 20250110 --max-dates 5
python3 -m tushare_mirror code-date-matrix-plan --api stk_managers --universe a_share_listed --limit-codes 3 --start-date 20250101 --end-date 20250110 --trading-days-only --calendar-exchange SSE --max-dates 5 --json

Behavior:
- read local code universe only
- read local trade_cal only when --trading-days-only is used
- do not fetch trade_cal implicitly
- generate candidate job params only
- do not execute
- do not write catalog
- do not create validation_runs
- no Tushare requests
- no full stock loop

If local stock_basic is missing, block clearly.
If local trade_cal is missing and --trading-days-only is used, block clearly.
If endpoint is disabled inventory, return blocked unless policy allows planning only.
If endpoint planner_kind does not support code_date_matrix, return blocked.

For Phase 2, allow plan-only for:
- stk_managers
- namechange
- stk_rewards

Do not enable execution for them.

Tests:
- stk_managers explicit dates plan
- namechange explicit dates plan
- stk_rewards explicit dates plan
- trading-days-only uses local trade_cal
- missing trade_cal blocks
- missing stock_basic blocks
- limit-codes required
- limit-codes >20 blocked
- max-dates >20 blocked
- candidate jobs >100 blocked/truncated according to design
- no side effects
- JSON stable

Commit:
feat: add bounded code-date matrix planner CLI

## Phase 3: Planner registry and policy integration

Update PlannerRegistry so code_date_matrix resolves to the plan-only planner.

Update execution policy:
- code_date_matrix planning is allowed only in dry-run plan commands.
- code_date_matrix execution remains blocked everywhere.
- mirror-run must not execute code_date_matrix endpoints.
- direct fetch must not bypass policy.
- disabled inventory endpoints remain blocked for execution.
- existing low-risk executable endpoints remain unaffected.

Policy decision fields should include:
- requires_code_loop=true
- requires_date_loop=true
- execution_allowed=false
- user_confirmation_required=true
- blocked_reason
- missing_infrastructure

Tests:
- code-date-matrix-plan allowed as dry-run
- direct fetch blocked
- mirror-run excludes code-date endpoints
- disabled inventory cannot execute
- existing low-risk mirror tests unaffected

Commit:
feat: guard code-date matrix execution policy

## Phase 4: Existing-status and coverage integration

Enhance code-date-matrix-plan so it can report existing_status when local catalog already has matching jobs/files.

Do not write catalog.

Existing statuses:
- missing
- active_exists
- failed_exists
- quarantined_exists
- staged_exists
- unknown

For plan-only endpoints with no executed history, most items will be missing. That is fine.

If matching active data exists in fake tests, planned_action should be skip_existing.

Tests:
- active_exists item becomes skip_existing
- failed_exists item becomes retry_failed but execution_allowed=false
- quarantined_exists item becomes blocked_quarantined
- no side effects

Commit:
feat: add existing-status to code-date matrix plans

## Phase 5: Readiness report integration

Update api-infra-readiness to include:

- code_date_matrix_planner=plan_only
- code_date_matrix_existing_status=implemented
- executable_code_date_matrix=false
- max_safe_code_limit=20
- max_safe_date_limit=20
- max_safe_candidate_jobs=100
- missing_for_execution:
  - explicit endpoint enablement
  - per-endpoint fake tests
  - rate-limit policy
  - user confirmation
  - small real smoke
  - resume strategy for code/date loops
  - failure aggregation
  - coverage semantics

Tests:
- readiness JSON includes code-date matrix fields
- report remains read-only
- no side effects

Commit:
feat: report code-date matrix infrastructure readiness

## Phase 6: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add section:
Bounded code-date matrix planning infrastructure

Explain:
- code-date-matrix-plan is dry-run only
- code universe comes only from local stock_basic/hs_const
- trading-days-only uses local trade_cal only
- no implicit fetch
- limit-codes required
- max-dates required or safely bounded
- max candidate jobs capped
- execution remains blocked
- no full stock loop
- how future enablement should work:
  1. choose one endpoint
  2. enable config explicitly
  3. fake tests
  4. plan 1-3 codes and 1-3 dates
  5. user-confirmed real smoke
  6. coverage/report
  7. only then expand

Commit:
docs: add bounded code-date matrix planning runbook

## Phase 7: Real durable read-only checks

Run only read-only commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror code-date-matrix-plan --api stk_managers --universe a_share_listed --limit-codes 3 --dates 20250102,20250103 --json
python3 -m tushare_mirror code-date-matrix-plan --api namechange --universe a_share_listed --limit-codes 3 --dates 20250102,20250103 --json
python3 -m tushare_mirror code-date-matrix-plan --api stk_rewards --universe a_share_listed --limit-codes 3 --dates 20250102,20250103 --json
python3 -m tushare_mirror api-infra-readiness --json
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json

These must not fetch or write catalog.

Final tests:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

Final report:
Bounded Code-date Matrix Planning Result:
- commits
- code-date matrix model status
- code-date-matrix-plan CLI status
- planner registry/policy status
- existing-status support
- api-infra-readiness update
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next recommended phase

Stop after Phase 7.
Do not execute any code-date fetch.
Do not run real requests.
