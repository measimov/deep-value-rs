# A-share Low-risk Full Pull Runbook

This runbook prepares a manual, user-confirmed pull for `a-share-low-risk`.
It does not authorize Codex or automation to execute the pull.

## Scope

Executable infrastructure is available for bounded low-risk A-share endpoints:

- `stock_basic`, `stock_company`, `trade_cal`, `hs_const`
- `daily`, `weekly`, `monthly`, `adj_factor`, `daily_basic`, `suspend_d`
- `concept`, `index_basic`, `index_daily`, `index_weekly`, `index_monthly`
- `ths_index`, `index_classify`

Smoke-only bounded stock-code checks exist for `namechange`, `stk_managers`,
and `stk_rewards`.

Plan-only or disabled endpoints remain out of execution until separate bounded
loop guardrails exist: `top10_holders`, `top10_floatholders`,
`stk_holdernumber`, `stk_holdertrade`, `pledge_stat`, `pledge_detail`,
`repurchase`, `concept_detail`, `index_weight`, `index_member`, and
`ths_member`.

Excluded families remain prohibited: minute, tick, order book, realtime,
financial PIT, object/PDF/news/research downloads, PostgreSQL loader, remote
backup, restore-into, compaction executor, scheduler, and parallel execution.

## Real Smoke

Preview bounded smoke commands without sending requests:

```bash
python3 scripts/tushare_real_smoke.py \
  --a-share-low-risk-smoke \
  --root /tmp/tushare-a-share-low-risk-smoke \
  --print-commands
```

Only a human operator should remove `--print-commands` and provide
`TUSHARE_TOKEN`. The smoke set is bounded and is not a full pull.

## Monthly Pull Preparation

Run read-only checks:

```bash
python3 -m tushare_mirror mirror-scope --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope a-share-low-risk --json
python3 -m tushare_mirror mirror-batch-plan --root "$MIRROR_ROOT" --scope a-share-low-risk --start-date 20250201 --end-date 20250228 --calendar-exchange SSE --max-jobs-per-api 20 --json
python3 -m tushare_mirror request-estimate --scope a-share-low-risk --start-date 20250201 --end-date 20250228 --root "$MIRROR_ROOT" --json
```

Generate guarded commands:

```bash
python3 -m tushare_mirror mirror-pull-command \
  --scope a-share-low-risk \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --start-date 20250201 \
  --end-date 20250228 \
  --max-jobs-per-api 20 \
  --output /tmp/tushare-a-share-low-risk-pull-202502 \
  --json
```

Review `commands.sh`. It is a guarded preview, not an automation script.
Only after explicit user confirmation should the operator run the reviewed
`mirror-run --execute` command.

## Stop Conditions

Stop immediately if restore-check fails, backup mutation is detected, schema
quarantine appears, validation fails, token plaintext is detected, the request
estimate exceeds the reviewed guardrail, or an endpoint outside this scope
appears in the plan.

After each completed batch, run validation with `--no-record`, inspect backup,
run restore-check, and run post-batch review before planning the next month.
