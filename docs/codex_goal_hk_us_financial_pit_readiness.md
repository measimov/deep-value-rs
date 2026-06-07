# Goal: HK/US Financial PIT Readiness and Raw Mirror Enablement

You are in autonomous iteration mode.

The user wants Codex to prepare HK/US financial statement and financial indicator mirror infrastructure. This is not a full financial pull. The work must first prove endpoint contracts, permission status, pagination behavior, and PIT disclosure-date safety before any endpoint is promoted from plan-only to guarded raw execution.

Do not execute any real HK/US financial full pull. Do not run uncontrolled stock loops. Do not backfill financial data. Do not interfere with any currently running A-share low-risk auto-sync. Do not merge HK/US financial endpoints into the existing `hk-low-risk` or `us-low-risk` auto-sync scopes.

Current state:
- Durable mirror root: `/mnt/gw/TuShare`
- Backup root: `/mnt/gw/TuShare-backup`
- A-share low-risk auto-sync may be running.
- HK/US low-risk market-data scopes are executable for:
  - HK: `hk_basic`, `hk_tradecal`, `hk_daily`, `hk_daily_adj`, `hk_adjfactor`
  - US: `us_basic`, `us_tradecal`, `us_daily`, `us_daily_adj`, `us_adjfactor`
- HK/US financial endpoints remain plan-only or disabled:
  - HK: `hk_income`, `hk_balancesheet`, `hk_cashflow`, `hk_fina_indicator`
  - US: `us_income`, `us_balancesheet`, `us_cashflow`, `us_fina_indicator`
- Existing period, code-period, and PIT readiness infrastructure exists, but execution is currently plan-only.
- Existing `code_period_planner` hard-bounds code and period planning and currently returns `execution_allowed=false`.

Critical current problem:
- Local inventory assumes PIT disclosure fields such as `ann_date` or `f_ann_date` for HK/US financial statements.
- Current Tushare documentation for HK/US income, balance sheet, and cashflow endpoints documents long-format output such as `end_date`, `name`, `ind_name`, and `ind_value`, with no documented `ann_date`, `f_ann_date`, or `notice_date`.
- HK/US financial indicator endpoints document `notice_date`-like fields and may be PIT-safe if real probes confirm them.
- Therefore raw mirror readiness and PIT-safe readiness must be evaluated separately. Raw executable does not imply PIT-safe.

Hard boundaries:
- Do not execute a real HK/US financial full pull.
- Do not execute uncontrolled stock loops.
- Do not backfill HK/US financial data.
- Do not execute `mirror-run` for HK/US financial endpoints against durable roots.
- Do not write inside `/mnt/gw/TuShare` or `/mnt/gw/TuShare-backup`.
- Do not stop, restart, signal, or otherwise interfere with the running A-share auto-sync process.
- Do not change A-share low-risk executable behavior.
- Do not enable A-share financial execution.
- Do not add HK minute, realtime, tick, order, PDF, object, news, research, PostgreSQL loader, remote backup, restore-into, compaction executor, scheduler, or parallel execution.
- Do not output token plaintext.
- Real Tushare probes are allowed only when explicitly bounded, token-redacted, and writing diagnostics under `/tmp`.
- Generated command bundles may write only to user-provided output paths outside mirror and backup roots.
- Commit each completed phase separately.
- Do not commit empty commits.

Baseline first:
```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
python3 -m tushare_mirror mirror-scope --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-scope --scope hk-low-risk --json
python3 -m tushare_mirror mirror-scope --scope us-low-risk --json
```

If baseline fails, fix and commit:
```text
fix: restore HK US financial PIT baseline health
```

## Phase 0: Baseline Snapshot And A-share Isolation

Record the current executable endpoint lists for:
- `a-share-low-risk`
- `hk-low-risk`
- `us-low-risk`

Add regression coverage ensuring this goal does not change:
- A-share low-risk executable endpoint list.
- A-share auto-sync confirmation behavior.
- Existing HK/US low-risk market-data executable endpoint list.

Acceptance:
- A-share low-risk executable endpoints are unchanged.
- HK/US market-data low-risk executable endpoints are unchanged.
- HK/US financial endpoints remain excluded from current low-risk auto-sync scopes.
- No durable root writes.

Commit:
```text
test: freeze low-risk scope baselines before HK US financial work
```

## Phase 1: Document Field And Source Map Alignment

Update HK/US financial source-map metadata for:
- `hk_income`
- `hk_balancesheet`
- `hk_cashflow`
- `hk_fina_indicator`
- `us_income`
- `us_balancesheet`
- `us_cashflow`
- `us_fina_indicator`

For each endpoint, record:
- `documented_params`
- `documented_output_fields`
- `documented_row_limit`
- `permission_notes`
- `documented_pagination_notes`
- `assumed_pit_fields`
- `pit_disclosure_fields_in_documented_output`
- `pit_disclosure_availability`
- `pit_disclosure_concern`
- `pagination_verification_status`
- `raw_mirror_candidate`
- `pit_safe_candidate`

Required classification:
- HK/US income, balance sheet, and cashflow endpoints must be marked `pit_disclosure_availability=uncertain` unless a documented disclosure field is present.
- HK/US financial indicator endpoints may be marked `notice_date_possible`, but not PIT-safe until real probes confirm the field.
- Fields from docs and fields assumed by local inventory must be clearly separated.

Acceptance:
- Source map explicitly captures the mismatch between documented financial statement fields and inventory PIT assumptions.
- No execution status is promoted in this phase.
- JSON/YAML loads cleanly.

Commit:
```text
docs: align HK US financial source map with PIT field uncertainty
```

## Phase 2: Bounded Real Financial Probe

Extend the opt-in real smoke script:
```bash
python3 scripts/tushare_real_smoke.py \
  --hk-us-financial-pit-probe \
  --output /tmp/tushare-hk-us-financial-pit-probe.json
```

Probe rules:
- Requires explicit `--hk-us-financial-pit-probe`.
- Requires `--output` under `/tmp`.
- Must not write mirror root or backup root.
- Must not print token plaintext.
- Default maximum: 1 request per endpoint.
- A second request is allowed only to characterize pagination or date slicing.
- A global max-request limit must prevent accidental broad pulls.
- Permission denied is a valid probe result, not a code failure.
- No stock universe loop is allowed.

Suggested probe examples:
- HK: `00700.HK`, period `20241231`
- US: `NVDA`, period `20241231` or another documented period confirmed by the endpoint contract

Probe output per endpoint:
- `api_name`
- `probe_status`: `passed | permission_denied | contract_changed | empty_but_authorized | failed`
- `request_count`
- `params_shape`
- `observed_fields`
- `observed_row_count`
- `observed_disclosure_fields`
- `observed_pagination_behavior`
- `error_type`
- `redaction_status`

Acceptance:
- All 8 endpoints produce a probe status.
- Artifact contains no token plaintext.
- Artifact contains field names and counts, not large raw payloads.
- Probe output is redacted and written only under `/tmp`.
- Running without the explicit flag does not call Tushare.

Commit:
```text
feat: add bounded HK US financial PIT real probe
```

## Phase 3: Probe Contract Difference Reporter

Add read-only CLI:
```bash
python3 -m tushare_mirror hk-us-financial-probe-report \
  --input /tmp/tushare-hk-us-financial-pit-probe.json \
  --json
```

The report must compare:
- Source-map documented fields.
- Real probe observed fields.
- Inventory assumed PIT fields.
- Required raw mirror fields.
- Candidate disclosure fields.

Output per endpoint:
- `api_name`
- `probe_status`
- `documented_fields`
- `observed_fields`
- `inventory_assumed_pit_fields`
- `missing_assumed_pit_fields`
- `observed_disclosure_fields`
- `raw_executable_candidate`
- `pit_safe_candidate`
- `pit_usable_after_status`
- `recommended_execution_status`
- `blocking_errors`
- `warnings`

Rules:
- If a financial statement endpoint has no observed disclosure field, `pit_safe_candidate=false`.
- If a financial indicator endpoint has `notice_date` or an equivalent verified field, it may become a PIT-safe candidate.
- Permission-denied endpoints must remain plan-only.
- Contract-changed endpoints must remain plan-only.

Acceptance:
- Reporter is read-only.
- JSON has stable `report_version`.
- Difference matrix clearly shows documented-vs-assumed PIT field mismatch.
- No token plaintext is emitted.

Commit:
```text
feat: add HK US financial probe contract report
```

## Phase 4: Raw Financial Scope Design

Add scopes:
- `hk-financial-raw`
- `us-financial-raw`

Do not add executable `global-financial` scope.
Do not add HK/US financial endpoints to `hk-low-risk` or `us-low-risk` auto-sync.

Scope report:
```bash
python3 -m tushare_mirror mirror-scope --scope hk-financial-raw --json
python3 -m tushare_mirror mirror-scope --scope us-financial-raw --json
```

Output:
- `endpoints_in_scope`
- `raw_executable_now`
- `pit_safe_now`
- `plan_only`
- `permission_blocked`
- `contract_blocked`
- `blocked_reason`
- `pit_usable_after_status`
- `next_enablement_step`

Acceptance:
- Only endpoints with `probe_status=passed` and stable contract can be raw executable candidates.
- PIT-safe status is independent from raw executable status.
- A-share financial endpoints remain disabled.
- Existing low-risk scopes are unchanged.

Commit:
```text
feat: add HK US financial raw mirror scopes
```

## Phase 5: Guarded Code-period Execution Gate

Extend planning for financial raw scopes while keeping default code-period planning safe.

Execution may be allowed only when all guardrails pass:
- Scope is `hk-financial-raw` or `us-financial-raw`.
- Endpoint is a raw executable candidate.
- `--limit-codes` is present and `<= 20`.
- Period input is explicit or bounded.
- `--max-periods <= 20`.
- `max_candidate_jobs <= 100`.
- Probe contract is passed.
- No permission or contract blocker exists.
- Generated command is guarded.

No unbounded stock loop is allowed.

Acceptance:
- Existing `code-period-plan` remains `execution_allowed=false` by default.
- Financial raw execution gate is explicit and test-covered.
- Missing `limit-codes` blocks.
- Excessive code/period/job limits block.
- Planner can produce candidate raw jobs without executing them.

Commit:
```text
feat: add guarded HK US financial code-period execution gate
```

## Phase 6: PIT Usable-after Status Model

Extend PIT readiness with explicit statuses:
- `not_required`
- `complete`
- `probe_pending`
- `permission_blocked`
- `blocked_missing_metadata`
- `blocked_without_disclosure_date`
- `contract_blocked`

Rules:
- `fallback_usable_after_policy=block_without_disclosure_date` must be enforced against observed fields, not only declared metadata.
- Missing disclosure fields block PIT-safe status.
- `strategy_safe_default=true` requires endpoint-specific tests and observed disclosure fields.
- Raw executable status must not imply PIT-safe status.

Acceptance:
- Financial statement endpoints without observed disclosure date are `blocked_without_disclosure_date`.
- Indicator endpoints with verified `notice_date` can be `complete`.
- JSON reports both raw and PIT-safe status.

Commit:
```text
feat: refine PIT usable-after status for HK US financial data
```

## Phase 7: Fake Financial Raw Fixtures And Execution Tests

Add fake fixtures for all 8 endpoints.

Fixture requirements:
- Financial statements use long format:
  - `ts_code`
  - `end_date`
  - `name`
  - `ind_name`
  - `ind_value`
  - optional `report_type` / `ind_type` for US where applicable
- Financial indicators use wide format and include tests with and without `notice_date`.

Tests must cover:
- Fake probe.
- Fake raw fetch.
- Raw JSONL.zst write.
- Lake Parquet write.
- Schema registry.
- Snapshot commit.
- Validation.
- LakeReader latest.
- List-files behavior.
- PIT-safe blocked when disclosure date is absent.
- Permission-denied fixture remains plan-only.

Acceptance:
- No real Tushare requests in unit tests.
- Plan-only endpoints cannot bypass the execution gate.
- Existing HK/US market-data fixture tests still pass.

Commit:
```text
test: add fake HK US financial raw mirror coverage
```

## Phase 8: Financial Readiness, Estimate, And Coverage Reports

Add or extend read-only reporting for financial scopes.

Commands may include:
```bash
python3 -m tushare_mirror financial-readiness --scope hk-financial-raw --root /mnt/gw/TuShare --json
python3 -m tushare_mirror financial-request-estimate --scope hk-financial-raw --from-period 1990Q1 --to-period latest --limit-codes 20 --json
python3 -m tushare_mirror financial-coverage-matrix --scope hk-financial-raw --root /mnt/gw/TuShare --periods 20241231 --limit-codes 20 --json
```

Reports must distinguish:
- `raw_ready`
- `pit_safe_ready`
- `permission_blocked`
- `contract_blocked`
- `coverage_by_code_period`
- `estimated_requests_by_api`
- `not_a_quota_guarantee=true`

Acceptance:
- No Tushare calls.
- No durable writes.
- Coverage is by `ts_code x period`, not by trading day.
- JSON has stable `report_version`.

Commit:
```text
feat: add HK US financial readiness and coverage reports
```

## Phase 9: Guarded Financial Pull Command Generator

Add read-only/file-output command:
```bash
python3 -m tushare_mirror financial-pull-command \
  --scope hk-financial-raw \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --from-period 1990Q1 \
  --to-period latest \
  --limit-codes 20 \
  --max-periods 20 \
  --output /tmp/tushare-hk-financial-raw-command \
  --json
```

Output bundle:
- `README.md`
- `plan.json`
- `readiness.json`
- `probe_contract.json`
- `commands.sh`

Rules:
- Generate only; do not execute.
- Refuse output inside mirror root or backup root.
- Refuse existing output unless `--overwrite`.
- `commands.sh` must contain `USER_CONFIRMATION_REQUIRED`.
- Commands must be commented or guarded.
- No token plaintext.

Acceptance:
- `command-safety-check` passes with at most warning-only status.
- Generated commands do not include unbounded loops.
- Generated commands require user confirmation.

Commit:
```text
feat: add guarded HK US financial pull command generator
```

## Phase 10: Read-only And Safety Regression Suite

Add focused regression tests covering:
- Source-map loads.
- Probe output redaction.
- Probe report read-only behavior.
- Raw financial scopes.
- PIT status distinctions.
- Code-period execution gate guardrails.
- Fake raw execution.
- Financial readiness and coverage reports.
- Financial command generator path safety.
- A-share baseline unchanged.
- Existing HK/US low-risk market-data baseline unchanged.

Acceptance:
- No catalog mutation from read-only commands.
- No raw/lake files created by read-only commands.
- No validation_runs created by read-only commands.
- Output commands write only to explicit output paths outside durable roots.

Commit:
```text
test: add HK US financial PIT guardrail regressions
```

## Phase 11: Runbook Update

Update or create docs explaining:
- HK/US financial raw mirror scope.
- Raw financial mirror vs PIT-safe data.
- Why financial endpoints are not part of current `hk-low-risk` / `us-low-risk` auto-sync.
- Why financial statement endpoints may lack disclosure dates.
- Why missing disclosure dates block PIT-safe use.
- How to run bounded real probes.
- How to read the probe contract difference report.
- How to generate a guarded pull command.
- How to manually run a small limited-code financial pull later.
- Stop conditions.

Acceptance:
- Runbook includes exact safe commands.
- Runbook states that Codex must not run the generated financial pull.
- Runbook states that raw executable does not imply PIT-safe.

Commit:
```text
docs: add HK US financial PIT readiness runbook
```

## Phase 12: Durable Read-only And File-output Checks

Run final tests:
```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
```

Run durable read-only/file-output checks:
```bash
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-scope --scope hk-financial-raw --json
python3 -m tushare_mirror mirror-scope --scope us-financial-raw --json
python3 -m tushare_mirror financial-readiness --scope hk-financial-raw --root "$MIRROR_ROOT" --json
python3 -m tushare_mirror financial-readiness --scope us-financial-raw --root "$MIRROR_ROOT" --json
python3 -m tushare_mirror financial-request-estimate --scope hk-financial-raw --from-period 1990Q1 --to-period latest --limit-codes 20 --json
python3 -m tushare_mirror financial-request-estimate --scope us-financial-raw --from-period 1990Q1 --to-period latest --limit-codes 20 --json
```

Generate command bundles under `/tmp` only:
```bash
rm -rf /tmp/tushare-hk-financial-raw-command
rm -rf /tmp/tushare-us-financial-raw-command

python3 -m tushare_mirror financial-pull-command \
  --scope hk-financial-raw \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --from-period 1990Q1 \
  --to-period latest \
  --limit-codes 20 \
  --max-periods 20 \
  --output /tmp/tushare-hk-financial-raw-command \
  --json

python3 -m tushare_mirror financial-pull-command \
  --scope us-financial-raw \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --from-period 1990Q1 \
  --to-period latest \
  --limit-codes 20 \
  --max-periods 20 \
  --output /tmp/tushare-us-financial-raw-command \
  --json

python3 -m tushare_mirror command-safety-check --file /tmp/tushare-hk-financial-raw-command/commands.sh --json
python3 -m tushare_mirror command-safety-check --file /tmp/tushare-us-financial-raw-command/commands.sh --json
```

Do not run generated commands.
Do not execute financial full pull.
Do not execute mirror-run against durable financial scopes.

Final report:
```text
HK/US Financial PIT Readiness Result:
- commits
- probe status by endpoint
- raw executable candidates
- PIT-safe candidates
- blocked_without_disclosure_date endpoints
- permission-blocked endpoints
- source-map/document/probe differences
- financial raw scopes
- code-period guardrails
- fake tests
- command bundles
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next user-confirmed action
```

Stop after Phase 12.
