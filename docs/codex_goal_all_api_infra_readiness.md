# Goal: All Tushare API Infrastructure Readiness

You are in autonomous iteration mode.

The user wants to continue infrastructure work, not data backfill.

The goal is to prepare the codebase so it can eventually support all Tushare API families safely, without executing full mirror, without fetching new real data, and without adding uncontrolled endpoint execution.

Current state:
- Durable pilot succeeded at /mnt/gw/TuShare
- Backup exists at /mnt/gw/TuShare-backup
- Low-risk A-share January 2025 pilot succeeded
- mirror-review, mirror-readiness, mirror-batch-plan exist
- backfill, coverage, backup, restore-check, validate --no-record exist
- The project is not yet doing full mirror

Hard boundaries:
- Do not execute mirror-run.
- Do not fetch real Tushare data.
- Do not backfill new dates.
- Do not execute full mirror.
- Do not loop over stocks.
- Do not add uncontrolled endpoint execution.
- Do not touch minute/tick/order execution.
- Do not touch financial PIT execution.
- Do not implement PostgreSQL loader.
- Do not fetch PDFs/news/research/object data.
- Do not implement remote backup or restore-into.
- Do not implement compaction executor.
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
fix: restore all api infra baseline health

## Phase 1: Endpoint taxonomy and capability model

Create or extend endpoint metadata structures so every future Tushare API can be classified without being executed.

Add docs and schema for endpoint capability fields:

- api_name
- family
- market
- domain
- endpoint_kind
- volume_class
- planner_kind
- permission_class
- partition_template
- primary_date_field
- supported_params
- default_fields
- probe params
- probe fields
- pagination_mode
- date_strategy
- code_strategy
- period_strategy
- object_strategy
- pit_safety
- execution_status

Define endpoint_kind values:

- reference_snapshot
- calendar
- daily_bar
- daily_metric
- event
- constituent
- company_governance
- financial_statement
- financial_indicator
- macro
- fund
- index
- futures
- option
- hk_us
- text_news
- object_document
- minute_bar
- tick
- realtime
- unknown

Define planner_kind values:

- single_snapshot
- date_backfill
- calendar_backfill
- explicit_dates
- code_list
- code_date_matrix
- period
- code_period_matrix
- object_index
- object_download
- bucketed_intraday
- realtime_poll
- unsupported

Do not add real execution for unsupported kinds.

Add tests validating metadata schema and allowed enums.

Commit:
feat: add tushare endpoint capability taxonomy

## Phase 2: Planner registry

Introduce a PlannerRegistry that maps planner_kind to planner implementation.

Existing planners should be registered:

- single_snapshot
- date_backfill
- calendar_backfill
- explicit_dates

Unsupported future planners should produce clear blocked plans, not execute.

Add blocked planner results for:

- code_list
- code_date_matrix
- period
- code_period_matrix
- object_index
- object_download
- bucketed_intraday
- realtime_poll

Blocked output must explain:
- why blocked
- what infrastructure is missing
- whether real requests would be needed
- whether user confirmation is required

Tests:
- supported planner kinds resolve
- unsupported planner kinds block safely
- no Tushare requests
- no catalog mutation
- JSON output stable

Commit:
feat: add planner registry for tushare endpoint kinds

## Phase 3: Endpoint inventory scaffolding

Create documentation and optional YAML stubs for broad Tushare API families.

Do not enable execution by default.

Add files such as:

- tushare_mirror/endpoint_configs/inventory/stock_reference.yaml
- tushare_mirror/endpoint_configs/inventory/stock_market_data.yaml
- tushare_mirror/endpoint_configs/inventory/stock_events.yaml
- tushare_mirror/endpoint_configs/inventory/financial.yaml
- tushare_mirror/endpoint_configs/inventory/index.yaml
- tushare_mirror/endpoint_configs/inventory/fund.yaml
- tushare_mirror/endpoint_configs/inventory/macro.yaml
- tushare_mirror/endpoint_configs/inventory/object_text.yaml
- tushare_mirror/endpoint_configs/inventory/intraday.yaml

These are inventory stubs only.

Fields:
- api_name
- endpoint_kind
- planner_kind
- execution_status: disabled
- reason_disabled
- required_infra
- risk_level
- notes

Do not wire disabled inventory endpoints into mirror-run execution.

Tests:
- disabled inventory endpoints do not appear in executable mirror scopes
- disabled endpoint cannot be fetched unless explicitly enabled
- endpoint loader can parse inventory stubs
- malformed inventory fails clearly

Commit:
feat: add disabled tushare endpoint inventory scaffolding

## Phase 4: Execution policy guardrails

Add an execution policy layer that checks whether an endpoint may execute.

Policy inputs:
- endpoint config
- execution_status
- planner_kind
- scope
- mode
- user command
- max_jobs
- requires_code_loop
- requires_real_requests
- requires_object_download
- requires_pit_handling

Policy decisions:
- allow
- dry_run_only
- blocked
- requires_user_confirmation
- unsupported

Ensure blocked classes cannot execute:

- financial_statement without PIT infra
- financial_indicator without PIT infra
- minute_bar without bucket/compaction infra
- tick without bucket/compaction infra
- object_document without object store/index infra
- text_news without text/object policy
- realtime without realtime policy
- code_date_matrix without explicit code-list guardrails

Tests:
- low-risk existing endpoints still execute in tests
- disabled/unsupported endpoints block
- no accidental execution from inventory
- JSON blocked reasons clear

Commit:
feat: add endpoint execution policy guardrails

## Phase 5: All-API readiness report

Add CLI:

python3 -m tushare_mirror api-infra-readiness --json
python3 -m tushare_mirror api-infra-readiness

This command is read-only.

It reports:
- supported endpoint kinds
- supported planner kinds
- blocked planner kinds
- disabled inventory endpoint count
- enabled executable endpoint count
- missing infrastructure by category
- next recommended infra phases

Categories:
- low_risk_ready
- needs_code_loop
- needs_period_planner
- needs_pit
- needs_object_store
- needs_intraday_bucket
- needs_compaction
- needs_realtime_policy
- unsupported

No real requests.
No catalog mutation.

Tests:
- readiness report stable
- JSON fields present
- no side effects
- disabled inventory included in blocked/missing infra summary

Commit:
feat: add all api infrastructure readiness report

## Phase 6: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add section:
All Tushare API Infrastructure Roadmap

Explain:
- current low-risk executable scope
- inventory-only endpoint families
- why disabled endpoints are not executable
- what infrastructure is required for:
  - financial PIT
  - code loops
  - period planners
  - object documents
  - intraday/minute/tick
  - compaction
  - realtime
- how to safely enable a new endpoint
- required tests before enabling
- no real fetch until explicit user confirmation
- difference between inventory, enabled config, planner support, execution policy

Commit:
docs: add all api infrastructure roadmap

## Phase 7: Real durable read-only check

Run only read-only commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror api-infra-readiness --json

Do not run mirror-run.
Do not fetch.
Do not backfill.

Final tests:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

Final report:
All API Infrastructure Readiness Result:
- commits
- endpoint taxonomy implemented
- planner registry implemented
- inventory scaffolding implemented
- execution policy guardrails implemented
- api-infra-readiness result
- runbook updates
- real durable read-only checks
- safety boundaries
- tests
- worktree status
- next recommended phase

Stop after Phase 7.
Do not execute new real requests.
Do not run full mirror.
