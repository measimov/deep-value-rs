# Goal: HK/US Low-risk Executable Mirror Enablement

You are in autonomous iteration mode only after the user explicitly starts this
goal.

The user wants Codex to continue infrastructure work while the A-share
low-risk auto-sync is running. Build executable mirror support for low-risk
Hong Kong and US equity data, including endpoint inventory, real bounded
interface probes, pagination characterization, fake fixtures, planners,
reports, smoke commands, guarded pull command generation, and runbook updates.

Do not execute full HK/US pulls. Do not run uncontrolled backfills. Do not run
generated commands. Do not interfere with any running A-share auto-sync process.
Do not print token plaintext.

## Source Documentation Baseline

Use current Tushare documentation as source-of-truth during implementation.
Known relevant documentation pages:

- HK/US menu: https://tushare.pro/document/2?doc_id=199
- HK basic: https://tushare.pro/document/2?doc_id=191
- HK trade calendar: https://tushare.pro/document/2?doc_id=250
- HK daily: https://tushare.pro/document/2?doc_id=192
- HK daily adjusted: https://tushare.pro/document/2?doc_id=339
- US basic: https://tushare.pro/document/2?doc_id=252
- US trade calendar: https://tushare.pro/document/2?doc_id=253
- US daily: https://tushare.pro/document/2?doc_id=254
- US daily adjusted: https://tushare.pro/document/2?doc_id=338
- US adjustment factor: https://tushare.pro/document/2?doc_id=402

Document any current doc mismatch found during real probes.

## Current State

- Durable mirror root: /mnt/gw/TuShare
- Backup root: /mnt/gw/TuShare-backup
- A-share low-risk auto-sync may be running or resumable; treat it as external
  production activity.
- Existing A-share low-risk scope exists.
- Existing guarded auto-sync exists for A-share only.
- Existing executable A-share endpoints include stock_basic, trade_cal,
  hs_const, daily, adj_factor, daily_basic, weekly, monthly, suspend_d, and
  selected bounded metadata/event endpoints.
- Worktree should start clean.

## Hard Boundaries

- Do not execute full HK/US pull.
- Do not run generated command scripts.
- Do not execute mirror-run against durable roots for HK/US.
- Do not backfill HK/US historical ranges.
- Do not execute any all-symbol loop except bounded fake tests.
- Do not add minute, tick, realtime, order book, financial PIT, object/PDF,
  news/research download, PostgreSQL loader, remote backup, restore-into,
  compaction executor, scheduler daemon, or parallel execution.
- Do not modify or stop any A-share auto-sync process.
- Do not write inside /mnt/gw/TuShare except read-only checks.
- Do not write inside /mnt/gw/TuShare-backup except read-only checks.
- Real probes may only write explicit diagnostic outputs under /tmp.
- Real probes must use TUSHARE_TOKEN from the environment and must never print
  the token or token-derived plaintext.
- Commit each completed phase separately.
- Do not commit empty commits.

## Deliverable Boundary

This goal is complete only when:

- `hk-low-risk`, `us-low-risk`, and `global-equity-low-risk` scopes exist.
- Candidate endpoints are classified as executable, plan-only, or disabled with
  explicit reasons.
- Each executable endpoint has a config, partition strategy, planner support,
  fake fixture, fake fetch/write/read/validate coverage, report integration,
  request estimate support, and guarded command generation.
- Each candidate endpoint has a bounded real probe result that records actual
  returned fields, row counts, permission status, empty/non-empty behavior,
  update shape, and pagination/window strategy.
- Real probe artifacts are saved under /tmp and contain no token plaintext.
- No real HK/US full pull has been executed.
- Existing A-share tests and HK/US tests pass.
- Durable checks are read-only or /tmp-output only.

## Acceptance Criteria

Functional:

- `python3 -m tushare_mirror mirror-scope --scope hk-low-risk --json`
  reports endpoints, executable_now, plan_only, disabled, blocked_reason,
  missing_metadata, real_probe_status, and next_enablement_step.
- `python3 -m tushare_mirror mirror-scope --scope us-low-risk --json`
  reports the same contract.
- `python3 -m tushare_mirror mirror-scope --scope global-equity-low-risk --json`
  reports combined A/HK/US low-risk coverage without enabling unsupported
  endpoints.
- `mirror-plan` supports HK/US scopes in read-only mode.
- `mirror-run` supports HK/US scopes only for fake tests and future
  user-confirmed bounded execution.
- `mirror-auto-sync` remains A-share-only unless a separate guarded HK/US
  auto-sync design is implemented and explicitly tested as dry-run.
- `mirror-pull-command` can generate HK/US guarded command bundles without
  executing them.

Real probe:

- `scripts/tushare_real_smoke.py --hk-us-low-risk-probe --output /tmp/...`
  exists, requires explicit flag, and makes no requests by default.
- The real probe uses only TUSHARE_TOKEN from env.
- Each candidate endpoint receives at least one tiny live request.
- For endpoints with documented or observed pagination, the probe performs at
  most two tiny requests to characterize offset/limit behavior.
- The probe never writes to mirror root or backup root.
- Probe output includes endpoint, status, request_count, params_used, fields,
  row_count, page_count_tested, pagination_supported, recommended_planner_kind,
  recommended_partition_template, and safety_notes.
- Probe output redacts all token-like values.

Safety:

- Minute/realtime endpoints are excluded: hk_mins, rt_hk_k and any realtime
  US endpoint found in docs.
- Financial endpoints are excluded or plan-only: hk_income, hk_balancesheet,
  hk_cashflow, hk_fina_indicator, us_income, us_balancesheet, us_cashflow,
  us_fina_indicator.
- `global-equity-low-risk` must not mean "all Tushare APIs".
- No generated shell command is executable by default or unguarded.
- Generated commands include USER_CONFIRMATION_REQUIRED.
- Real smoke/probe commands are opt-in and bounded.

Testing:

- Unit tests include fake fetch/write/read/validate for every executable HK/US
  endpoint.
- Unit tests include plan-only/disabled behavior for every excluded candidate.
- Unit tests prove commands are read-only unless explicit /tmp output is
  supplied.
- Unit tests prove real smoke is inert without explicit flag.
- Final `python3 -m unittest discover tests/tushare_mirror -v` passes.
- Final `python3 -m compileall tushare_mirror tests/tushare_mirror` passes.
- Final `git diff --check` passes.
- Final `python3 scripts/tushare_real_smoke.py --help` passes.

## Candidate Endpoint Policy

Initial executable candidates:

HK:

- hk_basic
- hk_tradecal
- hk_daily
- hk_daily_adj

US:

- us_basic
- us_tradecal
- us_daily
- us_daily_adj
- us_adjfactor

Initial disabled or plan-only candidates:

- hk_mins: disabled, intraday/minute data.
- rt_hk_k: disabled, realtime data.
- hk_income, hk_balancesheet, hk_cashflow, hk_fina_indicator: plan-only or
  disabled, financial/PIT boundary.
- us_income, us_balancesheet, us_cashflow, us_fina_indicator: plan-only or
  disabled, financial/PIT boundary.
- Any endpoint discovered during documentation review whose pagination,
  permission, or parameter semantics are unclear: disabled_inventory until
  probed and explicitly enabled.

Do not promote an endpoint to executable only because it exists in docs. Promote
only when docs, real probe, planner semantics, fake tests, and safety policy all
agree.

## Baseline First

Run:

```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
```

If baseline fails, fix and commit:

```text
fix: restore HK/US low-risk baseline health
```

## Phase 1: Documentation and Endpoint Discovery

Review official Tushare docs for HK/US low-risk candidates.

Produce internal metadata:

- endpoint name
- doc URL
- category
- documented input params
- documented output fields
- documented row limits
- permission notes
- update cadence
- pagination hints
- excluded/plan-only/executable recommendation

Tests:

- source metadata loads
- all candidate endpoints have doc URL
- excluded realtime/minute/financial endpoints are not executable
- JSON stable
- no side effects

Commit:

```text
docs: document HK US low-risk endpoint source map
```

## Phase 2: Real Bounded Probe Harness

Add explicit opt-in real probe support:

```bash
python3 scripts/tushare_real_smoke.py \
  --hk-us-low-risk-probe \
  --output /tmp/tushare-hk-us-low-risk-probe.json \
  --max-requests-per-endpoint 2
```

Probe requirements:

- no default real request
- require TUSHARE_TOKEN env var
- never print token
- write only /tmp output path unless user supplies another safe non-root path
- refuse output inside mirror root or backup root
- for each candidate endpoint, run one tiny request
- for paginated endpoints, run at most one additional tiny page request
- record real fields and row counts
- record permission errors without failing the whole harness
- classify pagination strategy from observed behavior
- produce machine-readable JSON and concise table output

Candidate probe examples, bounded:

- hk_basic: one snapshot request, optional field subset if supported.
- hk_tradecal: small date window such as 20250101-20250110.
- hk_daily: one known trade_date or tiny date window.
- hk_daily_adj: one known trade_date or tiny date window.
- us_basic: limit/offset probe, at most two pages.
- us_tradecal: small date window such as 20250101-20250110.
- us_daily: one known trade_date or tiny date window.
- us_daily_adj: one known trade_date or tiny date window, plus offset/limit
  only if docs/probe show support.
- us_adjfactor: one known trade_date or tiny date window.

Tests:

- help works
- default script does not make requests
- missing token blocks only when probe flag is used
- token redaction works
- output path inside mirror root blocked
- output path inside backup root blocked
- fake client proves pagination characterization
- JSON stable

Commit:

```text
feat: add HK US low-risk real probe harness
```

## Phase 3: Run Real Bounded Probes

Run the real probe only if TUSHARE_TOKEN is present in the environment.

Command:

```bash
python3 scripts/tushare_real_smoke.py \
  --hk-us-low-risk-probe \
  --output /tmp/tushare-hk-us-low-risk-probe.json \
  --max-requests-per-endpoint 2
```

Rules:

- Do not run mirror-run.
- Do not write to durable mirror root.
- Do not write to backup root.
- Do not fetch historical ranges.
- Do not loop symbols.
- Do not print token.

If a candidate endpoint returns permission denied, keep it in scope metadata as
plan-only or blocked_by_permission, not executable.

If a candidate endpoint returns empty data with valid code, record it and use
docs plus fake tests for planner design, but keep executable promotion
conditional unless another tiny date is clearly justified.

Commit only code/test/doc changes, not /tmp probe output.

Commit if code/docs changed:

```text
chore: record HK US low-risk probe contract
```

## Phase 4: Define Mirror Scopes

Add formal scopes:

- hk-low-risk
- us-low-risk
- global-equity-low-risk

Scope output:

- scope
- endpoints_in_scope
- executable_now
- plan_only
- disabled
- blocked_reason
- missing_metadata
- real_probe_status
- pagination_strategy
- next_enablement_step

Tests:

- scopes exist
- A-share scope remains unchanged
- global scope includes A/HK/US low-risk only
- high-risk families excluded
- JSON stable
- no side effects

Commit:

```text
feat: define HK US low-risk mirror scopes
```

## Phase 5: Endpoint Configs and Inventory

Add endpoint configs or inventory entries for candidates.

For each endpoint define:

- api_name
- family
- market
- domain
- endpoint_kind
- planner_kind
- execution_status
- volume_class
- partition_template
- primary_date_field
- supported_params
- default_fields
- probe params
- probe fields
- page_size
- pagination_strategy
- risk_level
- required_infra
- doc_url
- real_probe_status
- notes

Promotion rules:

- snapshot/list endpoints can be executable only if snapshot or bounded paging
  is clear.
- daily endpoints can be executable only if trade-calendar/date partitioning is
  clear.
- paginated daily endpoints can be executable only if bounded pagination guard
  exists.
- uncertain endpoints remain plan-only or disabled_inventory.

Tests:

- configs load
- required metadata present
- unsafe endpoints not executable
- endpoint-inventory reports them
- no side effects

Commit:

```text
feat: add HK US low-risk endpoint inventory and configs
```

## Phase 6: Planner and Partition Support

Add planner support for executable HK/US endpoints.

Expected planner kinds:

- single_snapshot or paged_snapshot for hk_basic/us_basic
- date_backfill/calendar_backfill for hk_tradecal/us_tradecal
- explicit_dates or calendar_backfill for hk_daily/hk_daily_adj/us_daily/
  us_daily_adj/us_adjfactor

Requirements:

- no natural-day fallback for daily-like endpoints if a market-specific calendar
  is required
- missing hk_tradecal/us_tradecal dependency must produce explicit dependency
  stage
- no stock loops
- no full mode auto-execution
- pagination guards must bound requests per date/page

Tests:

- planner resolves executable endpoints
- plan-only endpoints return blocked output
- dependency stage reported for missing calendar
- pagination plan bounded
- no side effects

Commit:

```text
feat: add HK US low-risk planners
```

## Phase 7: Fake Fixtures and Executable Tests

For every executable endpoint, create fake response fixtures.

Tests must cover:

- fake probe
- fake fetch through client
- raw JSONL.zst write
- lake Parquet write
- schema registry
- snapshot commit
- validation
- LakeReader latest
- list-files behavior
- pagination behavior where applicable

For plan-only/disabled endpoints:

- dry-run/plan tests
- direct fetch blocked
- mirror-run excluded

No real requests in unit tests.

Commit:

```text
test: add fake coverage for HK US low-risk endpoints
```

## Phase 8: Mirror Orchestration Support

Update mirror-plan and mirror-run to support:

```bash
--scope hk-low-risk
--scope us-low-risk
--scope global-equity-low-risk
```

Rules:

- mirror-plan is read-only.
- mirror-run can execute only safe executable subset.
- fake smoke and fake pilot must work.
- real HK/US mirror-run remains user-confirmed future action only.
- plan-only and disabled endpoints appear as excluded/blocked, not silently
  ignored.
- no stock loops.
- no full mode auto-execution.
- max_jobs_per_api guard remains enforced.

Tests:

- mirror-plan hk-low-risk
- mirror-plan us-low-risk
- mirror-plan global-equity-low-risk
- mirror-run fake smoke
- mirror-run fake pilot
- blocked endpoints remain excluded
- JSON stable

Commit:

```text
feat: support HK US low-risk mirror orchestration
```

## Phase 9: Readiness, Coverage, and Estimate Integration

Update:

- mirror-review
- mirror-readiness
- mirror-status
- mirror-coverage-matrix
- request-estimate
- mirror-next-batch
- mirror-batch-plan
- mirror-operator-checklist
- monthly-promotion-checklist where applicable
- mirror-ops-report
- api-infra-readiness

They must support HK/US scopes.

Reports should distinguish:

- executable endpoints
- plan-only endpoints
- disabled high-risk endpoints
- missing market calendar coverage
- date coverage by market calendar
- pagination risk
- permission/probe blockers
- next safe action
- manual pull command preview

Tests:

- readiness for hk-low-risk
- readiness for us-low-risk
- request-estimate for HK/US date windows
- coverage matrix for HK/US daily-like endpoints
- missing calendar blocks daily-like execution
- JSON report_version present
- no side effects

Commit:

```text
feat: integrate HK US low-risk readiness reports
```

## Phase 10: Guarded Pull Command Generator

Extend or add command generation:

```bash
python3 -m tushare_mirror mirror-pull-command \
  --scope hk-low-risk \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --start-date 19900101 \
  --end-date latest-trade-date \
  --max-jobs-per-api 20 \
  --json
```

Also support:

```bash
python3 -m tushare_mirror mirror-pull-command \
  --scope us-low-risk \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --start-date 19900101 \
  --end-date latest-trade-date \
  --max-jobs-per-api 20 \
  --output /tmp/tushare-us-low-risk-pull \
  --json
```

Output:

- commands
- user_confirmation_required=true
- estimated_requests
- pagination_strategy_summary
- calendar_dependency_summary
- scope
- date_range
- warnings
- stop_conditions

Output bundle:

- README.md
- commands.sh guarded
- plan.json
- request_estimate.json
- stop_policy.json

Safety:

- no command executed
- output inside mirror root blocked
- output inside backup root blocked
- commands.sh contains USER_CONFIRMATION_REQUIRED
- no token plaintext

Tests:

- command generation for HK
- command generation for US
- command generation for global-equity-low-risk
- output path safety
- command safety warning-only
- no side effects

Commit:

```text
feat: add HK US low-risk pull command generation
```

## Phase 11: Optional Auto-sync Design, Dry-run Only

Design HK/US auto-sync support but do not execute it.

Either:

- keep mirror-auto-sync A-share-only and document why HK/US is not yet enabled,
  or
- add dry-run-only HK/US auto-sync planning with explicit future execution
  guard.

Acceptance:

- no real HK/US auto-sync starts
- dry-run shows checkpoint path, windows, calendar dependencies, endpoint list,
  and retry policy
- execute mode blocked unless all safety criteria and user confirmation are
  added in a later goal

Tests:

- dry-run no side effects
- execute blocked if not explicitly supported
- A-share auto-sync behavior unchanged

Commit:

```text
feat: add HK US auto-sync planning guardrails
```

## Phase 12: Runbook Update

Update docs/tushare_mirror_phase1_runbook.md and optionally create:

- docs/hk_us_low_risk_pull_runbook.md

Document:

- what HK/US low-risk includes
- what it excludes
- endpoint table
- real probe procedure
- how pagination was characterized
- executable vs plan-only status
- how to generate manual pull commands
- how to run future bounded HK/US smoke
- stop conditions
- backup/restore expectations
- how this coexists with A-share auto-sync
- why Codex does not execute full HK/US pull automatically

Commit:

```text
docs: add HK US low-risk mirror runbook
```

## Phase 13: Durable Read-only Checks

Run only read-only or /tmp-output commands:

```bash
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-scope --scope hk-low-risk --json
python3 -m tushare_mirror mirror-scope --scope us-low-risk --json
python3 -m tushare_mirror mirror-scope --scope global-equity-low-risk --json
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope hk-low-risk --json
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope us-low-risk --json
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope hk-low-risk --json
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope us-low-risk --json
python3 -m tushare_mirror request-estimate --scope hk-low-risk --start-date 19900101 --end-date latest-trade-date --root "$MIRROR_ROOT" --json
python3 -m tushare_mirror request-estimate --scope us-low-risk --start-date 19900101 --end-date latest-trade-date --root "$MIRROR_ROOT" --json
python3 -m tushare_mirror mirror-pull-command --scope hk-low-risk --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --start-date 19900101 --end-date latest-trade-date --max-jobs-per-api 20 --json
python3 -m tushare_mirror mirror-pull-command --scope us-low-risk --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --start-date 19900101 --end-date latest-trade-date --max-jobs-per-api 20 --json
```

Generate guarded command bundles under /tmp only:

```bash
rm -rf /tmp/tushare-hk-low-risk-pull
rm -rf /tmp/tushare-us-low-risk-pull
python3 -m tushare_mirror mirror-pull-command --scope hk-low-risk --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --start-date 19900101 --end-date latest-trade-date --max-jobs-per-api 20 --output /tmp/tushare-hk-low-risk-pull --json
python3 -m tushare_mirror mirror-pull-command --scope us-low-risk --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --start-date 19900101 --end-date latest-trade-date --max-jobs-per-api 20 --output /tmp/tushare-us-low-risk-pull --json
```

Do not run generated commands.

Commit if check-related code fixes were needed:

```text
fix: support durable HK US low-risk readiness checks
```

## Phase 14: Final Tests

Run:

```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
```

Final report:

```text
HK/US Low-risk Executable Enablement Result:
- commits
- official documentation reviewed
- real probe status
- real probe output path
- endpoints executable
- endpoints plan-only
- endpoints disabled
- pagination strategy by endpoint
- calendar dependency strategy
- fake tests
- orchestration support
- readiness/report integration
- pull command generator
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next user action: review generated HK/US pull commands and explicitly confirm any real execution
```

Stop after Phase 14.

Do not execute full HK/US pull.
Do not run generated commands.
Do not interfere with A-share auto-sync.
