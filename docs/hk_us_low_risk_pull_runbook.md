# HK/US Low-risk Mirror Pull Runbook

This runbook prepares manual, user-confirmed HK/US low-risk mirror pulls. It
does not authorize Codex or automation to execute a full HK/US pull.

## Scope

Executable endpoints:

| Scope | Endpoint | Kind | Calendar dependency | Pagination |
| --- | --- | --- | --- | --- |
| `hk-low-risk` | `hk_basic` | snapshot/reference | none | none |
| `hk-low-risk` | `hk_tradecal` | market calendar | none | none |
| `hk-low-risk` | `hk_daily` | daily-like market data | `hk_tradecal` | none |
| `hk-low-risk` | `hk_daily_adj` | daily-like market data | `hk_tradecal` | offset/limit |
| `hk-low-risk` | `hk_adjfactor` | daily-like market data | `hk_tradecal` | none |
| `us-low-risk` | `us_basic` | snapshot/reference | none | offset/limit |
| `us-low-risk` | `us_tradecal` | market calendar | none | none |
| `us-low-risk` | `us_daily` | daily-like market data | `us_tradecal` | offset/limit |
| `us-low-risk` | `us_daily_adj` | daily-like market data | `us_tradecal` | offset/limit |
| `us-low-risk` | `us_adjfactor` | daily-like market data | `us_tradecal` | none |

`global-equity-low-risk` is an explicit composition of `a-share-low-risk`,
`hk-low-risk`, and `us-low-risk`. It is not a wildcard over all Tushare APIs.

Disabled or plan-only HK/US endpoints remain out of execution:

- Intraday/realtime: `hk_mins`, `rt_hk_k`
- HK financial/PIT: `hk_income`, `hk_balancesheet`, `hk_cashflow`,
  `hk_fina_indicator`
- US financial/PIT: `us_income`, `us_balancesheet`, `us_cashflow`,
  `us_fina_indicator`

Other prohibited families remain excluded: minute, tick, order book, realtime,
financial PIT execution, object/PDF/news/research download, PostgreSQL loader,
remote backup, restore-into, compaction executor, scheduler, and parallel
execution.

## Real Probe

HK/US interface probes are explicit and bounded:

```bash
python3 scripts/tushare_real_smoke.py \
  --hk-us-low-risk-probe \
  --output /tmp/tushare-hk-us-low-risk-probe.json \
  --max-requests-per-endpoint 2
```

The probe requires `TUSHARE_TOKEN`, writes only the redacted `/tmp` artifact,
and never writes to the durable mirror or backup roots. It is not a pull.

Observed pagination:

- No pagination: `hk_basic`, `hk_tradecal`, `hk_daily`, `hk_adjfactor`,
  `us_tradecal`, `us_adjfactor`
- Offset/limit pagination: `hk_daily_adj`, `us_basic`, `us_daily`,
  `us_daily_adj`

The probe result is also recorded in
`docs/hk_us_low_risk_endpoint_source_map.md` and
`tushare_mirror/endpoint_configs/source_maps/hk_us_low_risk.yaml`.

## Calendar Dependencies

HK daily-like endpoints use local `hk_tradecal`. US daily-like endpoints use
local `us_tradecal`. A-share remains on `trade_cal` with SSE behavior. There is
no natural-day fallback for daily-like HK/US planning.

If the target market calendar range is missing, reports should show a staged
dependency action such as `fetch_hk_tradecal_first` or
`fetch_us_tradecal_first`. That is a plan, not authorization to execute.

## Read-only Checks

```bash
export MIRROR_ROOT=/mnt/gw/TuShare
export MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-scope --scope hk-low-risk --json
python3 -m tushare_mirror mirror-scope --scope us-low-risk --json
python3 -m tushare_mirror mirror-scope --scope global-equity-low-risk --json

python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope hk-low-risk --json
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope hk-low-risk --json
python3 -m tushare_mirror mirror-batch-plan --root "$MIRROR_ROOT" --scope hk-low-risk --start-date 19900101 --end-date latest-trade-date --max-jobs-per-api 20 --json
python3 -m tushare_mirror request-estimate --scope hk-low-risk --start-date 19900101 --end-date latest-trade-date --root "$MIRROR_ROOT" --json
```

Repeat the same commands with `--scope us-low-risk` for US.

## Guarded Pull Commands

Preview HK commands without writing a bundle:

```bash
python3 -m tushare_mirror mirror-pull-command \
  --scope hk-low-risk \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --start-date 19900101 \
  --end-date latest-trade-date \
  --max-jobs-per-api 20 \
  --json
```

Generate a US guarded bundle outside mirror and backup roots:

```bash
python3 -m tushare_mirror mirror-pull-command \
  --scope us-low-risk \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --start-date 19900101 \
  --end-date latest-trade-date \
  --max-jobs-per-api 20 \
  --output /tmp/tushare-us-low-risk-pull \
  --json
```

Bundle contents:

- `README.md`
- `commands.sh`
- `plan.json`
- `request_estimate.json`
- `stop_policy.json`

`commands.sh` is commented and guarded. Any `mirror-run --execute` preview is
marked `USER_CONFIRMATION_REQUIRED`. Do not run generated commands
automatically.

## Guarded Auto-sync

HK/US auto-sync supports read-only planning, command bundle generation, status
inspection, and recovery planning. Real HK/US execution is guarded and must not
be run by Codex or automation in this goal. It requires both
`--confirm-auto-sync` and `--confirm-hk-us-auto-sync`, plus a user-selected
state file outside mirror and backup roots.

The active A-share auto-sync process, if already running, was launched from the
code loaded at its start time. New HK/US lock code cannot coordinate with that
already-running process until it is restarted under code that also uses the new
lock path. While A-share is active, do not stop, restart, or signal it as part
of HK/US preparation.

Preview HK windows without writing state:

```bash
python3 -m tushare_mirror mirror-auto-sync \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope hk-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --json
```

Recommended future state paths:

- HK: `/mnt/gw/TuShare-hk-auto-sync-state.json`
- US: `/mnt/gw/TuShare-us-auto-sync-state.json`

Generate a guarded HK auto-sync command bundle in `/tmp` only:

```bash
python3 -m tushare_mirror mirror-auto-sync-command \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope hk-low-risk \
  --from-date 19900101 \
  --to-date latest-trade-date \
  --window-days 20 \
  --max-jobs-per-api 20 \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --output /tmp/tushare-hk-auto-sync-command \
  --json
```

The generated `commands.sh` is commented, contains
`USER_CONFIRMATION_REQUIRED`, and must not be run automatically. The generated
confirmation phrase is operator friction, not a secret.

If a future user-confirmed HK/US auto-sync is interrupted, inspect state and
generate a recovery plan before retrying:

```bash
python3 -m tushare_mirror mirror-auto-sync-status \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --json

python3 -m tushare_mirror mirror-auto-sync-recovery-plan \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope hk-low-risk \
  --state /mnt/gw/TuShare-hk-auto-sync-state.json \
  --json
```

Retryable window failures include rate limits, network errors, server errors,
and unknown transient errors after the configured backoff. Permission denied,
invalid params, invalid endpoint, schema incompatible, validation failed,
backup failed, and restore-check failed are stop conditions requiring operator
review. `global-equity-low-risk` remains a reporting composition only; run HK
and US child scopes separately.

## Future Bounded HK/US Smoke

Future real HK/US smoke should use the same bounded probe command until a
separate smoke executor is explicitly designed:

```bash
python3 scripts/tushare_real_smoke.py \
  --hk-us-low-risk-probe \
  --output /tmp/tushare-hk-us-low-risk-probe.json \
  --max-requests-per-endpoint 2
```

This characterizes interface shape and pagination only. It does not replace
`mirror-pull-command`, and it does not start a durable mirror pull.

## Stop Conditions

Stop before any user-confirmed execution if:

- restore-check fails
- backup possible mutation is reported
- schema quarantine or incompatible schema appears
- validation fails
- token plaintext is detected
- a disabled or plan-only endpoint appears as executable
- a market calendar dependency is missing without a staged plan
- commands are unguarded or not marked `USER_CONFIRMATION_REQUIRED`

After any future user-executed HK/US batch, run validation with `--no-record`,
inspect backup, run restore-check, and review the mirror before planning the
next batch.

## Why Codex Does Not Execute The Full HK/US Pull

Codex leaves HK/US execution to a user-confirmed command because the historical
range can be large, HK/US calendar dependencies must be staged locally before
daily-like jobs, pagination behavior was characterized but still needs guarded
operator review under real quotas, and any active A-share writer must not be
disturbed by HK/US preparation. This work prepares infrastructure, status
reports, recovery reports, and command bundles; it does not authorize automation
to run HK/US `mirror-run --execute` or HK/US `mirror-auto-sync --execute`.
