# Goal: Period / Code-period / PIT Readiness Infrastructure

You are in autonomous iteration mode.

The user wants infrastructure only. Do not fetch real Tushare data, do not execute mirror-run, do not backfill new dates, do not enable financial execution, and do not enter full mirror.

Current state:
- Durable pilot exists at /mnt/gw/TuShare
- Backup exists at /mnt/gw/TuShare-backup
- Low-risk January 2025 pilot succeeded
- mirror-review, mirror-readiness, mirror-batch-plan exist
- api-infra-readiness exists
- endpoint taxonomy, planner registry, disabled inventory scaffolding, execution policy guardrails exist
- code-universe exists
- code-list-plan exists
- code-date-matrix-plan exists
- code_list / code_date_matrix execution remains blocked
- Tests last passed: 158 OK
- Worktree should start clean

Hard boundaries:
- Do not execute mirror-run.
- Do not fetch real Tushare data.
- Do not backfill new dates.
- Do not add new executable endpoints.
- Do not enable financial/PIT execution.
- Do not execute period or code-period jobs.
- Do not loop over all stocks.
- Do not touch minute/tick/order/realtime execution.
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
fix: restore period pit readiness baseline health

## Phase 1: Period and fiscal calendar utilities

Implement infrastructure-only period utilities.

Add helpers for:
- YYYYQ1 / YYYYQ2 / YYYYQ3 / YYYYQ4
- YYYY0331 / YYYY0630 / YYYY0930 / YYYY1231
- annual periods
- quarter end date normalization
- period_year extraction
- period ordering
- period range generation

Supported period forms:
- 2024Q1
- 2024Q2
- 2024Q3
- 2024Q4
- 20240331
- 20240630
- 20240930
- 20241231

Add a PeriodRangePlanner that can generate bounded period lists:
- --periods explicit
- --start-period / --end-period
- --period-frequency quarterly|annual

Limits:
- max_periods <= 20 in this phase
- no execution
- no Tushare requests

Tests:
- parse all period formats
- invalid period rejected
- quarterly range generation
- annual range generation
- max_periods enforced
- stable ordering
- JSON serialization if applicable

Commit:
feat: add period planning utilities

## Phase 2: PIT safety metadata model

Add PIT safety metadata structures for future financial endpoints.

Fields:
- pit_required
- period_field
- announcement_date_fields
- usable_after_field
- fallback_usable_after_policy
- allow_without_disclosure_date
- lookahead_risk
- strategy_safe_default
- blocked_reason

Recommended defaults:
- financial_statement: pit_required=true
- financial_indicator: pit_required=true
- usable_after_field should be derived from ann_date/f_ann_date/disclosure_date
- allow_without_disclosure_date=false
- strategy_safe_default=false until proven otherwise

Add validation rules:
- PIT endpoint must define period_field.
- PIT endpoint must define at least one announcement/disclosure date field.
- PIT endpoint without usable_after strategy is blocked.
- Unknown PIT safety should block execution.

No execution.

Tests:
- valid PIT metadata
- missing period_field blocks
- missing announcement date blocks
- allow_without_disclosure_date=false by default
- unknown PIT safety blocks
- JSON stable

Commit:
feat: add PIT safety metadata model

## Phase 3: Period planner registry

Add plan-only planner for planner_kind=period.

CLI:
python3 -m tushare_mirror period-plan --api income --periods 20240331,20240630 --json
python3 -m tushare_mirror period-plan --api fina_indicator --start-period 2024Q1 --end-period 2024Q4 --period-frequency quarterly --max-periods 4 --json

Behavior:
- plan only
- no execution
- no fetch
- no catalog writes
- no validation rows
- no Tushare requests
- execution_allowed=false
- disabled inventory endpoint can be planned only if inventory metadata allows plan_only
- if endpoint lacks period strategy, block clearly
- if endpoint requires PIT and PIT metadata is incomplete, block clearly

Output:
- api_name
- planner_kind
- periods
- period_count
- max_periods
- candidate_jobs
- execution_allowed=false
- pit_required
- pit_safety_status
- blocked_reason
- warnings

Tests:
- income period-plan blocked/plan-only according to metadata
- fina_indicator period-plan blocked/plan-only according to metadata
- max_periods enforced
- invalid period rejected
- no side effects
- no Tushare requests
- JSON stable

Commit:
feat: add bounded period planner

## Phase 4: Code-period matrix plan model and CLI

Implement plan-only code-period matrix planning.

CLI:
python3 -m tushare_mirror code-period-plan --api income --universe a_share_listed --limit-codes 3 --periods 20240331,20240630 --json
python3 -m tushare_mirror code-period-plan --api fina_indicator --universe a_share_listed --limit-codes 3 --start-period 2024Q1 --end-period 2024Q4 --period-frequency quarterly --max-periods 4 --json

Behavior:
- read local code universe only
- generate bounded code x period candidate jobs
- no execution
- no fetch
- no catalog writes
- no validation rows
- no Tushare requests
- execution_allowed=false

Hard limits:
- limit-codes required
- limit-codes <= 20
- max_periods <= 20
- max_candidate_jobs <= 100

Output item:
- api_name
- ts_code
- period
- params
- job_key
- existing_status if available
- planned_action
- pit_required
- pit_safety_status
- would_require_real_request=true
- execution_allowed=false
- blocked_reason

Summary:
- api_name
- universe
- source_snapshot_id
- total_codes
- planned_codes
- total_periods
- planned_periods
- candidate_jobs
- planned_jobs
- truncated_by_code_limit
- truncated_by_period_limit
- truncated_by_candidate_limit
- execution_allowed=false
- dry_run=true
- pit_required
- pit_safety_status
- warnings
- blocking_errors

Tests:
- income code-period plan with fake stock_basic
- fina_indicator code-period plan with fake stock_basic
- missing stock_basic blocks
- limit-codes required
- limit-codes >20 blocked
- max_periods >20 blocked
- candidate jobs >100 blocked/truncated according to design
- no side effects
- JSON stable

Commit:
feat: add bounded code-period planner

## Phase 5: Existing-status for period/code-period plans

Enhance period-plan and code-period-plan with existing_status support.

Existing statuses:
- missing
- active_exists
- failed_exists
- staged_exists
- quarantined_exists
- unknown

Planned actions:
- fetch
- skip_existing
- retry_failed
- blocked_quarantined
- blocked_staged
- blocked_policy

Execution still blocked.

Tests:
- active_exists -> skip_existing
- failed_exists -> retry_failed but execution_allowed=false
- quarantined_exists -> blocked_quarantined
- staged_exists -> blocked_staged
- no side effects
- JSON stable

Commit:
feat: add existing-status to period planners

## Phase 6: Execution policy integration

Update execution policy so:
- period planning is allowed only in dry-run plan commands.
- code_period_matrix planning is allowed only in dry-run plan commands.
- actual fetch for period/code-period endpoints is blocked.
- mirror-run cannot execute period/code-period endpoints.
- financial_statement and financial_indicator remain blocked without PIT execution infrastructure.
- disabled inventory endpoints remain non-executable.
- existing low-risk executable endpoints remain unaffected.

Policy fields:
- requires_period_loop
- requires_code_loop
- requires_pit
- execution_allowed
- user_confirmation_required
- blocked_reason
- missing_infrastructure

Tests:
- period-plan allowed as dry-run
- code-period-plan allowed as dry-run
- direct fetch blocked
- mirror-run excludes period/code-period endpoints
- low-risk mirror tests unaffected

Commit:
feat: guard period and code-period execution policy

## Phase 7: Financial inventory metadata refinement

Refine disabled inventory scaffolding for financial families.

Inventory-only endpoints should remain disabled, but metadata should be richer.

Include at least stubs for:
- income
- balancesheet
- cashflow
- fina_indicator
- forecast
- express
- dividend
- fina_audit
- fina_mainbz

For each:
- endpoint_kind
- planner_kind
- execution_status=disabled
- reason_disabled
- required_infra
- risk_level
- period_field
- announcement_date_fields
- usable_after_field if known
- pit_safety
- notes

Do not enable execution.

Tests:
- financial inventory parses
- all financial inventory endpoints disabled
- PIT metadata present where expected
- execution policy blocks them
- api-infra-readiness counts them correctly

Commit:
feat: refine financial endpoint inventory metadata

## Phase 8: PIT readiness report

Add CLI:
python3 -m tushare_mirror pit-readiness
python3 -m tushare_mirror pit-readiness --json

Read-only.

Report:
- financial_endpoint_count
- pit_metadata_complete_count
- pit_metadata_incomplete_count
- execution_enabled_count
- execution_blocked_count
- missing_period_field
- missing_announcement_date_fields
- missing_usable_after_strategy
- strategy_safe_count
- strategy_unsafe_count
- next_required_infra

No Tushare requests.
No catalog writes.

Tests:
- JSON fields complete
- financial inventory counted
- incomplete metadata reported
- no side effects

Commit:
feat: add PIT readiness report

## Phase 9: api-infra-readiness integration

Update api-infra-readiness with:
- period_planner=plan_only
- code_period_matrix_planner=plan_only
- pit_safety_metadata=implemented
- pit_readiness_report=implemented
- executable_period_loop=false
- executable_code_period_loop=false
- financial_execution=false
- max_safe_period_limit=20
- max_safe_code_limit=20
- max_safe_candidate_jobs=100
- missing_for_execution:
  - endpoint enablement
  - PIT safe usable_after generation
  - per-endpoint fake tests
  - small real smoke
  - rate-limit policy
  - resume/failure aggregation
  - strategy-safe derived layer

Tests:
- readiness JSON includes period/PIT fields
- no side effects

Commit:
feat: report period and PIT infrastructure readiness

## Phase 10: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add:
Period, code-period, and PIT readiness infrastructure

Explain:
- period-plan is dry-run only
- code-period-plan is dry-run only
- no financial execution yet
- why PIT matters
- usable_after concept
- why period is not tradable date
- how future enablement should work:
  1. choose one financial endpoint
  2. complete PIT metadata
  3. fake tests
  4. period-plan only
  5. code-period-plan 1-3 codes x 1-3 periods
  6. user-confirmed tiny real smoke
  7. PIT validation
  8. only then consider execution
- stop conditions:
  - missing PIT metadata
  - missing disclosure date
  - schema incompatible
  - future-data risk
  - rate limit unknown
  - no backup

Commit:
docs: add period and PIT readiness runbook

## Phase 11: Real durable read-only checks

Run only read-only commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror period-plan --api income --periods 20240331,20240630 --json
python3 -m tushare_mirror code-period-plan --api income --universe a_share_listed --limit-codes 3 --periods 20240331,20240630 --json
python3 -m tushare_mirror code-period-plan --api fina_indicator --universe a_share_listed --limit-codes 3 --start-period 2024Q1 --end-period 2024Q4 --period-frequency quarterly --max-periods 4 --json
python3 -m tushare_mirror pit-readiness --json
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
Period / Code-period / PIT Readiness Result:
- commits
- period utilities status
- PIT metadata status
- period-plan CLI status
- code-period-plan CLI status
- existing-status support
- execution policy guardrails
- financial inventory refinement
- pit-readiness status
- api-infra-readiness update
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next recommended phase

Stop after Phase 11.
Do not execute financial fetch.
Do not run real requests.
