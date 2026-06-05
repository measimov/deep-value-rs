# Goal: A-share Low-risk Executable Mirror Completion

You are in autonomous iteration mode.

The user wants Codex to complete executable infrastructure for all A-share low-risk data. Codex should implement endpoint support, tests, fake fixtures, smoke plans, mirror scopes, coverage, backup/report integration, and command templates. The user will manually execute the final full pull later.

Do not execute full mirror. Do not run uncontrolled backfill. Do not run all-stock loops unless explicitly bounded in fake tests. Do not touch minute/tick/order, financial PIT execution, PDFs/news/research downloads, PostgreSQL loader, remote backup, restore-into, compaction executor, scheduler, or parallel execution.

Current durable mirror:
- MIRROR_ROOT=/mnt/gw/TuShare
- MIRROR_BACKUP=/mnt/gw/TuShare-backup
- January 2025 low-risk pilot succeeded.
- Existing executable endpoints include stock_basic, trade_cal, hs_const, daily, adj_factor, daily_basic, weekly, monthly, suspend_d.
- Some event/governance endpoints were smoke-tested but not promoted into full low-risk execution.
- Tests last reported above 300 OK.
- Worktree should start clean.

Hard boundaries:
- No final full pull.
- No historical all-range execution.
- No minute/tick/order.
- No financial PIT execution.
- No object/PDF/news/research download.
- No stock-loop full execution.
- No token plaintext.
- Commit each phase separately.
- No empty commits.

Baseline:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

If baseline fails, fix and commit:
fix: restore A-share low-risk baseline health

## Phase 1: Define A-share low-risk executable scope

Create a formal scope named:
a-share-low-risk

This is different from all Tushare APIs.

Include only A-share endpoints that are:
- low/medium volume
- non-intraday
- non-financial-PIT
- non-object-download
- non-realtime
- no unbounded stock loop execution

Scope categories:

1. Reference/snapshot:
- stock_basic
- stock_company
- trade_cal
- namechange
- hs_const

2. Daily/periodic market data:
- daily
- weekly
- monthly
- adj_factor
- daily_basic
- suspend_d

3. Basic event/governance, only if safely bounded by existing planners:
- stk_managers
- stk_rewards
- top10_holders
- top10_floatholders
- stk_holdernumber
- stk_holdertrade
- pledge_stat
- pledge_detail
- repurchase

4. Concept/industry/index-style A-share metadata if low risk:
- concept
- concept_detail
- index_basic
- index_daily
- index_weekly
- index_monthly
- index_weight
- index_member where supported
- ths_index / ths_member if metadata is clear
- index_classify where supported

If exact metadata is uncertain, add as disabled_inventory or plan_only with reason. Do not guess executable behavior.

Add scope report:
python3 -m tushare_mirror mirror-scope --scope a-share-low-risk --json

Output:
- endpoints_in_scope
- executable_now
- plan_only
- disabled
- blocked_reason
- missing_metadata
- next_enablement_step

Tests:
- scope exists
- high-risk families excluded
- minute/tick/financial/object excluded
- JSON stable
- no side effects

Commit:
feat: define A-share low-risk mirror scope

## Phase 2: Add endpoint configs and inventory for missing A-share low-risk endpoints

Add endpoint config or inventory entries for missing candidates.

For each endpoint define:
- api_name
- family
- domain
- endpoint_kind
- planner_kind
- execution_status
- volume_class
- partition_template
- primary_date_field
- supported_params
- default_fields if known
- probe params if safe
- probe fields if safe
- risk_level
- required_infra
- notes

Classification rules:
- single_snapshot endpoints can be executable if params/fields are clear.
- date_backfill/calendar_backfill endpoints can be executable if partition/date semantics are clear.
- code_list/code_date_matrix endpoints remain plan_only unless bounded execution rules are implemented.
- uncertain endpoints remain disabled_inventory.

Do not enable unsafe endpoints just to increase count.

Tests:
- all new configs load
- required metadata present
- unsafe endpoints not executable
- endpoint-inventory reports them

Commit:
feat: add A-share low-risk endpoint inventory and configs

## Phase 3: Planner and partition support for missing safe endpoints

For newly executable endpoints, map to existing planner kinds:
- single_snapshot
- explicit_dates
- date_backfill
- calendar_backfill
- event_year_month
- period_year where applicable
- constituent/snapshot partition where applicable

For plan-only endpoints, planner should return blocked/plan-only output with clear reason.

Do not create stock-loop execution.

Tests:
- planner resolves for each endpoint
- partition resolver works where executable
- plan-only endpoints do not execute
- no side effects

Commit:
feat: add planners for A-share low-risk endpoint batch

## Phase 4: Fake fixtures and executable tests

For each executable endpoint in a-share-low-risk scope, create fake response fixtures.

Tests must cover:
- probe or fake probe
- fetch through fake client
- raw JSONL.zst write
- lake Parquet write
- schema registry
- snapshot commit
- validation
- LakeReader latest
- list-files behavior

For plan-only endpoints:
- dry-run/plan tests
- direct fetch blocked
- mirror-run excluded

No real requests in unit tests.

Commit:
test: add fake coverage for A-share low-risk executable endpoints

## Phase 5: Real smoke command preparation

Do not run real smoke by default.

Add or update opt-in script support:
python3 scripts/tushare_real_smoke.py --a-share-low-risk-smoke --root /tmp/tushare-a-share-low-risk-smoke --reset-root

The script may include tiny per-endpoint smoke commands, but must require explicit flag and keep hard limits:
- max 1 request for snapshot endpoints
- max 1-3 dates for date endpoints
- max 1-3 codes for code-plan-only smoke if later enabled
- no full stock loop
- no full backfill

If implementation would be too risky, generate runbook commands only.

Tests:
- --help works
- command list generated
- no default real request

Commit:
feat: prepare A-share low-risk real smoke commands

## Phase 6: Mirror orchestrator support

Update mirror-plan / mirror-run to support:
--scope a-share-low-risk

Important:
- mirror-plan is read-only.
- mirror-run can execute only the safe executable subset.
- plan-only endpoints must appear as excluded/blocked, not silently ignored.
- mode smoke and pilot must work.
- full mode may be listed as user-confirmed future mode but must not be auto-executed.

Pilot for this scope should still require:
- start-date/end-date
- max_jobs_per_api <= 20
- backup-target
- trade_cal dependency
- no natural-day fallback

Tests:
- mirror-plan a-share-low-risk
- mirror-run fake smoke
- mirror-run fake pilot
- blocked endpoints remain excluded
- no stock loops
- JSON stable

Commit:
feat: support A-share low-risk mirror orchestration

## Phase 7: Coverage and readiness integration

Update:
- mirror-review
- mirror-readiness
- mirror-status
- mirror-coverage-matrix
- request-estimate
- mirror-next-batch
- mirror-batch-plan
- mirror-operator-checklist
- monthly-promotion-checklist
- mirror-ops-report
- api-infra-readiness

They must support scope:
a-share-low-risk

Reports should distinguish:
- executable endpoints
- plan-only endpoints
- excluded high-risk endpoints
- missing coverage
- next batch action
- manual pull command preview

Tests:
- readiness for a-share-low-risk
- batch plan for 202502
- request-estimate
- coverage matrix
- no side effects
- JSON report_version present

Commit:
feat: integrate A-share low-risk scope into readiness reports

## Phase 8: Final pull command generator

Add read-only command:
python3 -m tushare_mirror mirror-pull-command --scope a-share-low-risk --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --json

This generates the exact user-run command sequence but does not execute:
- mirror-review
- mirror-readiness
- mirror-batch-plan
- mirror-run --execute
- validate --no-record
- backup-inspect
- restore-check
- mirror-review after execution

Output:
- commands
- user_confirmation_required=true
- estimated_requests
- scope
- date_range
- warnings
- stop_conditions

Also support writing to /tmp:
--output /tmp/tushare-a-share-low-risk-pull-202502

Output files:
- commands.sh guarded
- README.md
- plan.json

Do not execute.

Tests:
- command generation
- output inside mirror root blocked
- output inside backup blocked
- command safety warning-only
- no side effects

Commit:
feat: add A-share low-risk pull command generator

## Phase 9: Runbook update

Update docs/tushare_mirror_phase1_runbook.md and optionally create:
docs/a_share_low_risk_full_pull_runbook.md

Document:
- what a-share-low-risk includes
- what it excludes
- endpoint table
- executable vs plan-only status
- how to run real smoke
- how user manually starts full pull
- monthly batch procedure
- stop conditions
- backup/restore-check after every batch
- how to continue after failure
- why Codex does not execute full pull

Commit:
docs: add A-share low-risk full pull runbook

## Phase 10: Durable read-only checks

Run read-only checks:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-scope --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-batch-plan --root "$MIRROR_ROOT" --scope a-share-low-risk --start-date 20250201 --end-date 20250228 --calendar-exchange SSE --max-jobs-per-api 20 --json
python3 -m tushare_mirror request-estimate --scope a-share-low-risk --start-date 20250201 --end-date 20250228 --root "$MIRROR_ROOT" --json
python3 -m tushare_mirror mirror-pull-command --scope a-share-low-risk --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --json

Generate output in /tmp only:
rm -rf /tmp/tushare-a-share-low-risk-pull-202502
python3 -m tushare_mirror mirror-pull-command --scope a-share-low-risk --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --output /tmp/tushare-a-share-low-risk-pull-202502 --json

Do not run generated commands.

Final tests:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

Final report:
A-share Low-risk Executable Completion Result:
- commits
- scope definition
- endpoints added
- executable endpoints
- plan-only endpoints
- blocked endpoints
- fake tests
- real smoke commands
- orchestrator support
- readiness integration
- pull command generator
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next user action: run generated pull command manually

Stop after Phase 10.
Do not execute full pull.
