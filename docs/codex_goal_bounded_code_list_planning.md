# Goal: Bounded Code-list Planning Infrastructure

You are in autonomous iteration mode.

The user wants infrastructure only. Do not fetch real Tushare data, do not execute mirror-run, do not backfill, and do not enable new executable endpoints.

Current state:
- Durable pilot exists at /mnt/gw/TuShare
- Backup exists at /mnt/gw/TuShare-backup
- Low-risk January 2025 pilot succeeded
- mirror-review, mirror-readiness, mirror-batch-plan exist
- all-API infra readiness exists
- endpoint taxonomy, planner registry, disabled inventory scaffolding, and execution policy guardrails exist
- 12 enabled executable endpoints
- 22 disabled inventory-only endpoints
- Tests last passed: 127 OK
- Worktree should start clean

Hard boundaries:
- Do not execute mirror-run.
- Do not fetch real Tushare data.
- Do not backfill new dates.
- Do not add new executable endpoints.
- Do not loop over all stocks.
- Do not execute code-list endpoints.
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
fix: restore code-list planning baseline health

## Phase 1: Local code universe provider

Implement a read-only local CodeUniverseProvider.

It must read only local lake/catalog data, mainly stock_basic latest snapshot.

Supported universe names:
- a_share_listed
- a_share_active
- a_share_mainboard
- a_share_sme
- a_share_chinext
- a_share_star
- hs_const_sh
- hs_const_sz

If required local source data is missing, return a clear blocked result.

Do not fetch stock_basic or hs_const automatically.

Fields in result:
- universe_name
- source_api
- source_snapshot_id
- source_record_count
- code_count
- codes_sample
- blocked_reason
- warnings

Add CLI:
python3 -m tushare_mirror code-universe --universe a_share_listed --limit 20
python3 -m tushare_mirror code-universe --universe a_share_listed --limit 20 --json

Read-only only:
- no catalog writes
- no validation_runs
- no Tushare requests

Tests:
- reads fake stock_basic latest
- missing stock_basic blocks
- filters by market where supported
- hs_const universe uses local hs_const
- limit works
- JSON stable
- no side effects

Commit:
feat: add local code universe provider

## Phase 2: Bounded code-list planner

Implement a planner for code_list and code_date_matrix planner kinds, but plan-only.

Add CLI:
python3 -m tushare_mirror code-list-plan --api namechange --universe a_share_listed --limit-codes 5
python3 -m tushare_mirror code-list-plan --api stk_managers --universe a_share_listed --limit-codes 5 --json

Optional date range support:
python3 -m tushare_mirror code-list-plan --api <api> --universe a_share_listed --limit-codes 5 --start-date 20250101 --end-date 20250131

Behavior:
- read local code universe only
- generate candidate job params
- do not execute
- do not fetch
- do not write catalog
- do not enable real execution
- enforce limit-codes required
- hard max limit-codes <= 20 in this phase
- no full stock loop
- if endpoint is disabled inventory, return blocked unless endpoint already enabled in current config
- if planner kind is unsupported for endpoint, return blocked

Output per item:
- api_name
- ts_code
- params
- job_key
- existing_status if available
- planned_action
- blocked_reason
- would_require_real_request

Summary:
- api_name
- universe
- source_snapshot_id
- total_codes
- planned_codes
- candidate_jobs
- blocked
- warnings
- dry_run=true
- execution_allowed=false

Tests:
- namechange plan with fake stock_basic
- stk_managers plan with fake stock_basic
- limit-codes required
- limit-codes >20 blocked
- missing universe source blocked
- disabled inventory endpoint blocked
- no side effects
- no Tushare requests
- JSON stable

Commit:
feat: add bounded code-list planner

## Phase 3: Execution policy integration

Update execution policy so:
- code_list and code_date_matrix remain plan-only by default
- any actual fetch using code-list planner is blocked
- mirror-run cannot execute code-list planners
- inventory endpoints cannot bypass policy
- existing low-risk executable endpoints still work

Add policy decision fields:
- requires_code_loop
- max_codes_required
- execution_allowed
- user_confirmation_required
- blocked_reason

Tests:
- direct fetch for disabled code-list endpoint blocked
- mirror-run excludes code-loop endpoints
- code-list-plan allowed as dry-run
- existing low-risk endpoints unaffected

Commit:
feat: guard code-list execution policy

## Phase 4: Readiness report integration

Update api-infra-readiness to include code-list infrastructure status.

Add report fields:
- code_universe_provider: implemented
- code_list_planner: plan_only
- code_date_matrix_planner: plan_only
- executable_code_loop: false
- max_safe_code_plan_limit: 20
- missing_for_execution:
  - explicit enablement workflow
  - per-endpoint tests
  - rate-limit policy
  - user confirmation
  - small real smoke

Tests:
- readiness JSON includes code-list status
- disabled inventory counted correctly
- no side effects

Commit:
feat: report code-list infrastructure readiness

## Phase 5: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add:
Bounded code-list planning infrastructure

Explain:
- code universe comes only from local stock_basic/hs_const
- no implicit fetch
- code-list-plan is dry-run only
- limit-codes is mandatory
- hard max 20 in this phase
- no full stock loop
- how future enablement should work:
  1. choose one endpoint
  2. enable config explicitly
  3. fake tests
  4. plan 1-5 codes
  5. user-confirmed smoke
  6. coverage/report
  7. only then expand

Commit:
docs: add bounded code-list planning runbook

## Phase 6: Real durable read-only check

Run only read-only commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror code-universe --universe a_share_listed --limit 20 --json
python3 -m tushare_mirror code-universe --universe hs_const_sh --limit 20 --json
python3 -m tushare_mirror code-list-plan --api namechange --universe a_share_listed --limit-codes 5 --json
python3 -m tushare_mirror code-list-plan --api stk_managers --universe a_share_listed --limit-codes 5 --json
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
Bounded Code-list Planning Result:
- commits
- code universe provider status
- code-list planner status
- execution policy guardrails
- api-infra-readiness update
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next recommended phase

Stop after Phase 6.
Do not execute any code-list fetch.
Do not run real requests.
