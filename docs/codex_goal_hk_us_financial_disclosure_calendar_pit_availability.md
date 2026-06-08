# Goal: HK/US Financial Disclosure Calendar PIT Availability Layer

You are in autonomous iteration mode.

The user wants a practical mixed route for HK/US financial data:

1. Strict raw archive route: Tushare HK/US financial data may be stored only as raw financial history and must not enter backtest features unless a reliable disclosure-date layer exists.
2. External disclosure completion route: use SEC first, and HKEX only if safely automatable, to build an auditable disclosure calendar that can gate raw financial data by point-in-time availability.

This goal does not execute a HK/US financial full pull. It does not backfill HK/US financial data. It builds source metadata, schema, bounded probes, matching reports, PIT availability gates, fake tests, command bundles, and runbook documentation.

## Current State

- Durable mirror root: `/mnt/gw/TuShare`
- Backup root: `/mnt/gw/TuShare-backup`
- A-share low-risk auto-sync may be running.
- HK/US market-data low-risk scopes are executable separately from financial scopes.
- HK financial raw-ready endpoints:
  - `hk_income`
  - `hk_balancesheet`
  - `hk_cashflow`
  - `hk_fina_indicator`
- US financial raw/PIT candidate:
  - `us_fina_indicator`
- US financial statement endpoints remain plan-only / contract pending:
  - `us_income`
  - `us_balancesheet`
  - `us_cashflow`
- Existing bounded real probes showed:
  - HK financial endpoints can return raw rows but no observed disclosure-date field.
  - `us_fina_indicator` returned `notice_date`.
  - US statement probes were authorized but empty.
- Existing PIT readiness logic distinguishes raw readiness from PIT-safe readiness.

## Hard Boundaries

- Do not execute HK/US financial full pull.
- Do not execute `mirror-run`.
- Do not backfill HK/US financial data.
- Do not run uncontrolled stock loops.
- Do not write inside `/mnt/gw/TuShare`.
- Do not write inside `/mnt/gw/TuShare-backup`.
- Do not stop, restart, signal, or otherwise interfere with the running A-share auto-sync process.
- Do not change A-share low-risk executable behavior.
- Do not enable A-share financial execution.
- Do not merge HK/US financial endpoints into `hk-low-risk` or `us-low-risk` auto-sync.
- Do not enable minute, tick, order, realtime, PDF, object, news, research, PostgreSQL loader, remote backup, restore-into, compaction executor, scheduler, or parallel execution.
- Do not output token plaintext.
- SEC probes must be tiny, bounded, and write only to `/tmp`.
- Tushare cross-validation probes must be tiny, bounded, token-redacted, and write only to `/tmp`.
- HKEX automation must be treated as unsafe unless a stable, documented, low-volume metadata path is proven. Do not bulk crawl HKEX, do not download PDFs, and do not scrape at scale.
- Generated command bundles may write only to user-provided output paths outside mirror and backup roots.
- Commit each completed phase separately.
- Do not commit empty commits.

## PIT Strength Model

Define and use these statuses consistently:

- `raw_only`: raw values are archived or plannable, but unavailable for backtest features.
- `availability_only`: an external disclosure event proves a report was public by `disclosure_date`, but the Tushare values have not been verified against the filing values.
- `as_filed_verified`: the filing date and values have been reconciled against the external filing source.

Rules:

- `raw_only` must never enter feature generation.
- `availability_only` may gate use by `disclosure_date <= as_of_date`, but reports must clearly state values are not as-filed verified.
- `as_filed_verified` is the only strong PIT state.
- Do not upgrade HK financial endpoints to `availability_only` unless a reliable disclosure event match exists.
- Do not upgrade any endpoint to `as_filed_verified` without value-level reconciliation.

## Overall Acceptance Criteria

- Existing A-share low-risk behavior is unchanged.
- Existing HK/US market-data low-risk behavior is unchanged.
- Existing financial raw readiness is not weakened.
- New disclosure dataclasses and JSON schema exist, but no durable disclosure store is written.
- SEC source inventory and bounded probe tooling exist.
- `us_fina_indicator` is used as the SEC-to-Tushare golden-path cross-validation anchor.
- HKEX has a hard automation gate; if stable metadata is not proven, HK disclosure availability remains manual-audit-only.
- New CLI commands use the consistent `disclosure-*` namespace.
- Existing PIT reports expose availability/as-filed counters without changing raw execution semantics.
- Generated bundles are guarded and do not execute anything.
- Unit tests and safety regressions pass.
- Final durable checks are read-only or `/tmp` file-output only.

## Baseline First

```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
python3 -m tushare_mirror mirror-scope --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-scope --scope hk-low-risk --json
python3 -m tushare_mirror mirror-scope --scope us-low-risk --json
python3 -m tushare_mirror mirror-scope --scope hk-financial-raw --json
python3 -m tushare_mirror mirror-scope --scope us-financial-raw --json
```

If baseline fails, fix and commit:

```text
fix: restore HK US disclosure calendar baseline health
```

## Phase 0: Baseline Isolation And Integration Map

Record current behavior for:

- `a-share-low-risk`
- `hk-low-risk`
- `us-low-risk`
- `hk-financial-raw`
- `us-financial-raw`
- `pit-readiness`
- `financial-readiness`
- `code-period-plan`

Create an integration map documenting where new disclosure logic lives:

- `tushare_mirror/disclosure.py`: disclosure dataclasses, PIT strength enum, match status enum, validation helpers.
- `tushare_mirror/endpoint_configs/source_maps/disclosure_event_schema.yaml`: JSON/schema-level source-of-truth for disclosure event fields.
- `tushare_mirror/endpoint_configs/source_maps/financial_disclosure_sources.yaml`: SEC/HKEX/source inventory.
- `tushare_mirror/pit.py`: extend PIT result fields without breaking existing statuses.
- `tushare_mirror/financial_reports.py`: expose disclosure availability summaries where relevant.
- `tushare_mirror/code_period_planner.py`: keep raw execution gate separate from PIT feature gate.
- `scripts/tushare_real_smoke.py`: bounded SEC/Tushare cross-validation probes.

Add regression tests ensuring:

- A-share low-risk executable endpoint list is unchanged.
- HK/US market-data low-risk executable endpoint lists are unchanged.
- HK/US financial raw-ready endpoint lists are unchanged.
- Existing raw financial scopes do not require disclosure matches for raw-only planning.

Commit:

```text
test: freeze baseline before disclosure calendar work
```

## Phase 1: Disclosure Source Inventory And Event Schema

Add disclosure source inventory and event schema.

Disclosure event fields:

- `event_id`
- `market`
- `source`
- `source_status`
- `source_doc_id`
- `source_url`
- `ticker`
- `ts_code`
- `external_id`
- `cik`
- `period`
- `end_date`
- `report_type`
- `form_type`
- `filing_date`
- `accepted_at`
- `disclosure_date`
- `announcement_title`
- `language`
- `match_status`
- `match_confidence`
- `pit_strength`
- `as_filed_value_verified`
- `limitations`

Source inventory must include:

- SEC EDGAR submissions API
- SEC companyfacts API if useful for later value verification
- HKEXnews advanced search as tentative/manual-audit source unless stable metadata is proven
- Optional future vendor source placeholder

Do not implement durable storage.

Tests:

- Schema loads.
- Required fields are present.
- PIT strength enum accepts only `raw_only`, `availability_only`, `as_filed_verified`.
- HKEX default automation status is conservative.
- No side effects.

Commit:

```text
feat: add financial disclosure event schema
```

## Phase 2: SEC Golden Path Bounded Probe

Implement SEC-first bounded probe support.

Add script support:

```bash
python3 scripts/tushare_real_smoke.py \
  --sec-disclosure-probe \
  --ticker NVDA \
  --cik 0001045810 \
  --period 20241231 \
  --output /tmp/tushare-sec-disclosure-probe-nvda-20241231.json \
  --max-requests 3
```

Probe rules:

- Use SEC public JSON APIs only.
- Use a clear User-Agent.
- Respect SEC fair access behavior.
- Write only to `/tmp`.
- Do not download bulk archives.
- Do not download filing documents.
- Do not fetch more than the explicit request cap.

Output:

- `probe_version`
- `ticker`
- `cik`
- `period`
- `requests_made`
- `filings_considered`
- `selected_filing`
- `filing_date`
- `accepted_at`
- `accession_number`
- `form_type`
- `warnings`
- `blocking_errors`

Tests:

- SEC probe command appears in `--help`.
- Missing network or permission returns structured blocked output.
- Output is token-free.
- Request cap enforced.
- No durable writes.

Commit:

```text
feat: add bounded SEC disclosure probe
```

## Phase 3: SEC To Tushare Cross-validation Anchor

Use `us_fina_indicator` as the explicit SEC-to-Tushare golden-path anchor.

Add support:

```bash
python3 scripts/tushare_real_smoke.py \
  --sec-tushare-disclosure-cross-check \
  --api-name us_fina_indicator \
  --ts-code NVDA.US \
  --ticker NVDA \
  --cik 0001045810 \
  --period 20241231 \
  --output /tmp/tushare-sec-us-fina-indicator-cross-check-nvda-20241231.json \
  --max-sec-requests 3 \
  --max-tushare-requests 1
```

Behavior:

- If `TUSHARE_TOKEN` is missing, skip the Tushare side and return `tushare_status=blocked_token_missing`.
- If SEC is inaccessible, return `sec_status=blocked`.
- Do not fail the whole test suite for missing external access.
- Compare SEC `filing_date` with Tushare `notice_date`.
- Do not compare values yet.

Output:

- `sec_disclosure_date`
- `tushare_notice_date`
- `date_delta_days`
- `match_status`
- `match_confidence`
- `pit_strength_candidate`
- `limitations`

Match status:

- `exact`
- `near`
- `period_only`
- `candidate`
- `unmatched`
- `blocked`

Tests:

- Token missing is structured and token-safe.
- Exact/near/period-only/unmatched fixtures classify correctly.
- Real output redacts token-like values.
- No durable writes.

Commit:

```text
feat: add SEC Tushare disclosure cross-check probe
```

## Phase 4: Disclosure Contract Reporter

Add read-only CLI:

```bash
python3 -m tushare_mirror disclosure-contract-report \
  --sec-probe /tmp/tushare-sec-disclosure-probe-nvda-20241231.json \
  --cross-check /tmp/tushare-sec-us-fina-indicator-cross-check-nvda-20241231.json \
  --json
```

Output:

- `report_version`
- `source_status`
- `schema_status`
- `sec_status`
- `tushare_status`
- `match_status`
- `pit_strength_candidate`
- `can_mark_availability_only`
- `can_mark_as_filed_verified`
- `warnings`
- `blocking_errors`

Rules:

- `as_filed_verified` must be false until value reconciliation exists.
- `availability_only` requires at least a reliable disclosure date match.
- `candidate` does not enter feature layer.

Tests:

- Exact SEC/Tushare fixture can produce `availability_only`.
- Missing Tushare notice date blocks availability.
- Candidate match does not enter feature layer.
- JSON stable.
- Read-only.

Commit:

```text
feat: add disclosure contract report
```

## Phase 5: HKEX Automation Hard Gate

Assess HKEX only as a bounded metadata-source gate.

Optional bounded command:

```bash
python3 scripts/tushare_real_smoke.py \
  --hkex-disclosure-metadata-probe \
  --stock-code 00700 \
  --period 20241231 \
  --output /tmp/tushare-hkex-disclosure-probe-00700-20241231.json \
  --max-requests 2
```

Hard gate:

- If no stable documented metadata path is available, set `automation_status=manual_audit_only`.
- Do not bulk crawl HKEX.
- Do not download PDFs.
- Do not parse announcement documents.
- Do not mark HK financial endpoints `availability_only` from title heuristics alone.
- Operator-provided disclosure dates may be planned but not trusted automatically.

Output:

- `source_status`
- `automation_status`
- `manual_audit_required`
- `can_auto_match_disclosure_date`
- `limitations`
- `warnings`
- `blocking_errors`

Tests:

- Unstable/partial HKEX metadata keeps HK endpoints manual-audit-only.
- Title-only match is `candidate`, not `availability_only`.
- No network test runs by default.
- No durable writes.

Commit:

```text
feat: add HKEX disclosure automation gate
```

## Phase 6: Disclosure Matching Policy

Implement matching policy in `tushare_mirror/disclosure.py`.

Matching dimensions:

- market
- ticker / `ts_code`
- CIK or external identifier
- period / `end_date`
- report type / form type
- disclosure date
- accession number or source document id

Match grades:

- `exact`: identifier, period, report type, and disclosure date align.
- `near`: identifier and period align; disclosure date is close enough by configured tolerance.
- `period_only`: identifier and period align but report type/date confidence is low.
- `candidate`: plausible but not feature-eligible.
- `unmatched`: no useful match.
- `blocked`: source unavailable or unsafe.

PIT strength mapping:

- `exact` or approved `near` may become `availability_only`.
- `period_only` remains `candidate` unless explicitly operator-audited.
- No match can become `as_filed_verified` without value reconciliation.

Tests:

- Match grades classify fixtures correctly.
- Feature eligibility requires `availability_only` or stronger.
- HK title-only examples remain `candidate`.
- No side effects.

Commit:

```text
feat: add disclosure matching policy
```

## Phase 7: Disclosure Availability Reports

Add read-only CLI:

```bash
python3 -m tushare_mirror disclosure-source-report --json
python3 -m tushare_mirror disclosure-plan --scope us-financial-raw --from-period 2024Q4 --to-period 2024Q4 --limit-codes 1 --json
python3 -m tushare_mirror disclosure-availability --scope us-financial-raw --root /mnt/gw/TuShare --json
python3 -m tushare_mirror disclosure-gate --scope us-financial-raw --api-name us_fina_indicator --ts-code NVDA.US --period 20241231 --json
```

All commands are read-only.

Outputs must include:

- `report_version`
- `scope`
- `raw_only_count`
- `availability_only_count`
- `as_filed_verified_count`
- `candidate_count`
- `blocked_count`
- `feature_eligible_count`
- `warnings`
- `blocking_errors`

Integration:

- Extend existing `PITReadinessReporter.report()` with `availability_only_count` and `as_filed_verified_count`, initially zero unless disclosure matches exist.
- Extend financial readiness summaries to show disclosure state without changing raw readiness.
- Do not change raw financial code-period planning semantics.

Tests:

- Report commands return stable JSON.
- Existing PIT readiness remains backward compatible.
- Disclosure availability does not imply as-filed verification.
- No side effects.

Commit:

```text
feat: add disclosure availability reports
```

## Phase 8: PIT Feature Gate Integration

Add a feature-layer gate separate from raw execution.

Behavior:

- Raw financial endpoints may remain `guarded_raw`.
- Feature use requires `pit_strength >= availability_only`.
- Strong feature use requires `pit_strength=as_filed_verified`.
- `hk_income`, `hk_balancesheet`, `hk_cashflow`, and `hk_fina_indicator` remain blocked for feature use unless disclosure events are matched.
- `us_fina_indicator` may become `availability_only` only if SEC/Tushare cross-validation passes.

Do not implement feature generation.

Tests:

- Raw-only data is blocked from PIT feature gate.
- Availability-only data passes basic PIT feature gate with warning.
- As-filed-required mode rejects availability-only data.
- Existing code-period raw planning still works.
- No side effects.

Commit:

```text
feat: add disclosure PIT feature gate
```

## Phase 9: Disclosure Bundle Generator

Add read-only/file-output CLI:

```bash
python3 -m tushare_mirror disclosure-bundle \
  --scope us-financial-raw \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --from-period 2024Q4 \
  --to-period 2024Q4 \
  --output /tmp/tushare-us-financial-disclosure-bundle \
  --json
```

Bundle contents:

- `README.md`
- `source_report.json`
- `disclosure_plan.json`
- `availability.json`
- `gate.json`
- `limitations.md`
- `commands.sh`

Rules:

- Output cannot be inside mirror root.
- Output cannot be inside backup root.
- Refuse existing output unless `--overwrite`.
- `commands.sh` must be guarded and marked `USER_CONFIRMATION_REQUIRED`.
- Do not execute generated commands.
- Do not include token plaintext.

Tests:

- Bundle generated outside roots.
- Unsafe output paths blocked.
- Existing output refused without `--overwrite`.
- Commands guarded.
- Command safety passes.
- No durable side effects.

Commit:

```text
feat: add disclosure availability bundle generator
```

## Phase 10: Fake Fixtures And Regression Suite

Add tests and fixtures for:

- SEC disclosure event.
- SEC/Tushare `us_fina_indicator` cross-check.
- HKEX manual-audit-only gate.
- Exact/near/period-only/candidate/unmatched matching.
- PIT strength transitions.
- Disclosure reports.
- Bundle output safety.
- Read-only/no-mutation contracts.

Regression coverage:

- A-share low-risk scope unchanged.
- HK/US market-data scopes unchanged.
- HK/US financial raw scopes unchanged.
- US statement endpoints remain plan-only until non-empty contract is proven.
- HK endpoints remain non-PIT-safe unless disclosure events are matched.

Commit:

```text
test: add financial disclosure calendar regressions
```

## Phase 11: Runbook Update

Update `docs/tushare_mirror_phase1_runbook.md` and optionally add:

```text
docs/hk_us_financial_disclosure_calendar_runbook.md
```

Document:

- Strict raw route.
- External disclosure completion route.
- PIT strength taxonomy.
- Why disclosure date is not value verification.
- SEC-first workflow.
- `us_fina_indicator` golden path.
- HKEX manual-audit gate.
- Why HK title-only matching is not enough.
- CLI commands.
- Bundle generation.
- Safety boundaries.
- What remains prohibited.
- What is required before feature use.

Commit:

```text
docs: add HK US financial disclosure calendar runbook
```

## Phase 12: Durable Read-only And File-output Checks

Run final checks:

```bash
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help
```

Run read-only/file-output checks:

```bash
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror disclosure-source-report --json
python3 -m tushare_mirror disclosure-plan --scope us-financial-raw --from-period 2024Q4 --to-period 2024Q4 --limit-codes 1 --json
python3 -m tushare_mirror disclosure-availability --scope us-financial-raw --root "$MIRROR_ROOT" --json
python3 -m tushare_mirror disclosure-gate --scope us-financial-raw --api-name us_fina_indicator --ts-code NVDA.US --period 20241231 --json

rm -rf /tmp/tushare-us-financial-disclosure-bundle
python3 -m tushare_mirror disclosure-bundle \
  --scope us-financial-raw \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --from-period 2024Q4 \
  --to-period 2024Q4 \
  --output /tmp/tushare-us-financial-disclosure-bundle \
  --json
python3 -m tushare_mirror command-safety-check --file /tmp/tushare-us-financial-disclosure-bundle/commands.sh --json
```

Optional bounded probes if network and credentials are available:

```bash
python3 scripts/tushare_real_smoke.py \
  --sec-disclosure-probe \
  --ticker NVDA \
  --cik 0001045810 \
  --period 20241231 \
  --output /tmp/tushare-sec-disclosure-probe-nvda-20241231.json \
  --max-requests 3

python3 scripts/tushare_real_smoke.py \
  --sec-tushare-disclosure-cross-check \
  --api-name us_fina_indicator \
  --ts-code NVDA.US \
  --ticker NVDA \
  --cik 0001045810 \
  --period 20241231 \
  --output /tmp/tushare-sec-us-fina-indicator-cross-check-nvda-20241231.json \
  --max-sec-requests 3 \
  --max-tushare-requests 1
```

Do not run HK/US financial full pull.
Do not run `mirror-run`.
Do not backfill.
Do not write durable roots.

Final report:

```text
HK/US Financial Disclosure Calendar PIT Availability Result:
- commits
- disclosure schema status
- SEC probe status
- SEC/Tushare cross-check status
- HKEX automation gate status
- disclosure matching policy
- availability report status
- PIT feature gate status
- bundle status
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next recommended action
```

Stop after Phase 12.
