# HK/US Financial Disclosure Calendar Runbook

This runbook describes the disclosure-date layer for HK/US financial data. It
does not authorize a HK/US financial full pull, `mirror-run`, backfill, or
feature generation.

## Routes

Strict raw route:

- HK/US financial values may be archived as guarded raw financial data.
- `raw_only` data must not enter backtest features.
- Raw readiness is separate from PIT feature readiness.

External disclosure completion route:

- Use external disclosure metadata to decide when a financial report was public.
- SEC EDGAR is the first automated metadata source for US filings.
- HKEX remains manual-audit-only until a stable documented metadata path is
  proven.

## PIT Strength

- `raw_only`: stored or plannable raw values only; never feature-eligible.
- `availability_only`: disclosure date is matched, but values are not reconciled
  to the filing.
- `as_filed_verified`: disclosure date and values are reconciled against the
  external filing source.

Disclosure date is not value verification. An SEC filing date can prove that a
filing existed by a date, but it does not prove that each Tushare value equals
the filed value.

## SEC-First Workflow

Generate a bounded SEC metadata probe:

```bash
python3 scripts/tushare_real_smoke.py \
  --sec-disclosure-probe \
  --ticker NVDA \
  --cik 0001045810 \
  --period 20241231 \
  --output /tmp/tushare-sec-disclosure-probe-nvda-20241231.json \
  --max-requests 3
```

Cross-check the SEC filing date with Tushare `us_fina_indicator.notice_date`:

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

Review the contract:

```bash
python3 -m tushare_mirror disclosure-contract-report \
  --sec-probe /tmp/tushare-sec-disclosure-probe-nvda-20241231.json \
  --cross-check /tmp/tushare-sec-us-fina-indicator-cross-check-nvda-20241231.json \
  --json
```

`exact` or approved `near` matches may become `availability_only`. They are not
`as_filed_verified`.

## HKEX Gate

HKEX title/search metadata is not treated as stable automated evidence in this
layer. Title-only matches are `candidate` and are not feature-eligible.

```bash
python3 scripts/tushare_real_smoke.py \
  --hkex-disclosure-metadata-probe \
  --stock-code 00700 \
  --period 20241231 \
  --output /tmp/tushare-hkex-disclosure-probe-00700-20241231.json \
  --max-requests 2
```

The current expected result is manual-audit-only. Do not bulk crawl HKEX, do
not download PDFs, and do not infer PIT safety from announcement titles alone.

## Read-Only Reports

```bash
python3 -m tushare_mirror disclosure-source-report --json
python3 -m tushare_mirror disclosure-plan --scope us-financial-raw --from-period 2024Q4 --to-period 2024Q4 --limit-codes 1 --json
python3 -m tushare_mirror disclosure-availability --scope us-financial-raw --root /mnt/gw/TuShare --json
python3 -m tushare_mirror disclosure-gate --scope us-financial-raw --api-name us_fina_indicator --ts-code NVDA.US --period 20241231 --json
```

These commands are read-only. They do not call Tushare, fetch SEC, backfill,
write catalog state, or generate features.

## Bundle

Generate a review bundle outside the mirror and backup roots:

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

`commands.sh` is an echo-only guarded checklist. It is not an execution script.

## Prohibited

- Do not execute HK/US financial full pull from this runbook.
- Do not run `mirror-run` from disclosure commands.
- Do not write disclosure events into `/mnt/gw/TuShare` or
  `/mnt/gw/TuShare-backup`.
- Do not use HK title-only matches as PIT-safe evidence.
- Do not mark anything `as_filed_verified` without value-level reconciliation.

Before feature use, the data must have at least `availability_only` PIT strength
for the relevant issuer, period, and report type. Strong PIT usage requires
`as_filed_verified`.
