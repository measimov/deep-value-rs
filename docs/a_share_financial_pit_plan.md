# A-Share Financial PIT Plan

## Scope

This phase connects A-share financial raw data to the TuShare file lake. It does not enable value backtests directly. Strategy features remain blocked until strict point-in-time (PIT) availability is proven.

Initial endpoints:

- `income_vip`
- `balancesheet_vip`
- `cashflow_vip`
- `fina_indicator_vip`
- `disclosure_date`

The first value gate requires `income_vip`, `balancesheet_vip`, and `disclosure_date`.

## Execution Plan

1. Generate a read-only plan:

   ```bash
   python3 -m tushare_mirror --root /mnt/gw/TuShare a-share-financial-plan \
     --apis income_vip,balancesheet_vip,disclosure_date \
     --periods 2024Q4 \
     --max-jobs 3 --json
   ```

2. Execute only on the Linux file-lake host, not through the SMB mount:

   ```bash
   python3 -m tushare_mirror --root /mnt/gw/TuShare a-share-financial-run \
     --apis income_vip,balancesheet_vip,disclosure_date \
     --periods 2024Q4 \
     --max-jobs 3 --execute --json
   ```

3. Check PIT availability before any feature or backtest code uses the data:

   ```bash
   python3 -m tushare_mirror --root /mnt/gw/TuShare a-share-pit-availability \
     --periods 2024Q4 --json
   ```

## Strict Exit Conditions

- Current lake snapshots exist for every required API.
- Each requested period has active lake files for every required API.
- `disclosure_date` has rows for every requested period.
- Every `disclosure_date.actual_date` value is non-empty.
- Default `fetch` remains blocked for financial endpoints; only `financial-raw-fetch` with `a-share-financial-raw` is allowed.
- Fake-client execution tests pass and validate Parquet reads from the lake.
- A real read-only SMB check confirms current coverage state before feature work starts.

## Current Validation

On 2026-06-16, the SMB mount `/private/tmp/tushare-smb` was read-only and reachable. The 2024Q4 plan generated three missing jobs for `income_vip`, `balancesheet_vip`, and `disclosure_date`. The PIT availability gate correctly blocked feature use because no current snapshots exist for those APIs.

B-share value coverage is not proven by this phase. The current file lake samples showed no B-share rows in `stock_basic` or `daily_basic`; B-share support needs separate universe and market-data coverage validation before backtests include 200/900-prefixed codes.
