# Goal: Full Mirror Readiness Infrastructure

You are in autonomous iteration mode. The user wants the project ready for controlled full backfill, but you must NOT execute full backfill.

Current durable pilot has succeeded:
- MIRROR_ROOT=/mnt/gw/TuShare
- MIRROR_BACKUP=/mnt/gw/TuShare-backup
- scope=low-risk-a-share
- mode=pilot
- range=20250101-20250131
- max_jobs_per_api=20
- latest durable pilot commit: 66311b4
- latest preflight/review work exists
- tests previously passed: 84+ tests
- worktree should start clean

Hard safety rules:
- Do not execute full mirror.
- Do not execute a new mirror-run --execute unless explicitly requested by user later.
- Do not fetch new real Tushare data.
- Do not backfill new dates.
- Do not add endpoints.
- Do not loop over stocks.
- Do not touch minute/tick/order/realtime.
- Do not touch financial statements/PIT/PostgreSQL loader.
- Do not touch PDF/news/research/object endpoints.
- Do not implement remote backup, restore-into, compaction, scheduler, parallel execution.
- Do not output token plaintext.
- Commit each completed phase separately.
- Never commit empty commits.

Run baseline first:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

If broken, fix baseline and commit:
fix: restore baseline test health

Phase 1: Mirror review command
Implement a read-only CLI:
python3 -m tushare_mirror mirror-review --root <root> --backup <backup> --scope low-risk-a-share [--mode pilot] [--start-date 20250101] [--end-date 20250131] [--calendar-exchange SSE] [--json]

It must:
- not fetch
- not execute backfill
- not write catalog
- not create validation_runs
- use validate --no-record
- use restore-check/backup-inspect
- use coverage/list-files/show-snapshots
- detect token plaintext presence as true/false only

Output:
root_status, backup_status, catalog_status, latest_snapshots, endpoint_summary, coverage_summary, backup_restore_check, backup_catalog_checksum_status, backup_possible_mutation, artifact_size, token_plaintext_found, ready_for_next_batch, warnings, blocking_errors.

Tests:
- ready fake pilot root
- backup missing
- mutated backup
- coverage gap
- JSON fields
- no side effects

Commit:
feat: add mirror pilot review command

Phase 2: Mirror readiness report
Implement read-only CLI:
python3 -m tushare_mirror mirror-readiness --root <root> --backup <backup> --scope low-risk-a-share [--json]

It must return readiness_status=ready|warning|blocked and ready_for_controlled_full_backfill=true/false.

Must pass:
- catalog opens
- supported schema
- latest snapshots exist
- trade_cal latest exists
- backup exists
- restore-check succeeds
- backup possible_mutation=false
- validate --snapshot latest --no-record succeeds
- pilot coverage complete for daily/adj_factor/daily_basic/suspend_d
- token plaintext not found
- backup not nested inside mirror root
- CLI guardrails exist

Warnings:
- only 2025-01 covered
- not full mirror
- event/company endpoints not stock-looped
- weekly/monthly not trading-days-only
- financial/PIT/minute/tick/object/postgres not covered
- no remote DR
- no compaction

Tests:
- ready pilot
- missing backup blocked
- mutated backup blocked
- missing trade_cal blocked
- incomplete coverage warning/blocking
- JSON fields
- no side effects

Commit:
feat: add mirror readiness report

Phase 3: Controlled batch planner
Implement read-only CLI:
python3 -m tushare_mirror mirror-batch-plan --root <root> --scope low-risk-a-share --start-date YYYYMMDD --end-date YYYYMMDD --calendar-exchange SSE --max-jobs-per-api 20 [--json]

It must not fetch, backfill, write catalog, or create validation.

It plans the next bounded batch, e.g. 20250201-20250228.
Daily-like endpoints:
- daily
- adj_factor
- daily_basic
- suspend_d

Weekly/monthly:
- bounded explicit date planning only
- no trading-days-only

Reference endpoints:
- stock_basic
- hs_const
- trade_cal
Show refresh strategy, do not refetch blindly.

Event/company endpoints:
- namechange
- stk_managers
- stk_rewards
Remain excluded/no_stock_loop.

Output:
batch_id, scope, start_date, end_date, calendar_exchange, max_jobs_per_api, endpoint_plans, total_candidate_jobs, total_planned_jobs, blocked_endpoints, warnings, estimated_request_count, requires_execute_confirmation.

Rules:
- If trade_cal range missing, plan trade_cal dependency first.
- Do not natural-day fallback for daily-like endpoints.
- If max_jobs_per_api truncates, show truncated=true.
- Unknown scope blocked.

Tests:
- 202502 batch plan
- missing trade_cal range planned as dependency
- daily-like blocked until trade_cal exists
- fake trade_cal exists -> daily-like planned by trading days
- max-jobs truncation
- no side effects
- JSON fields
- unknown scope blocked

Commit:
feat: add controlled mirror batch planner

Phase 4: Runbook
Update docs/tushare_mirror_phase1_runbook.md with:
- Controlled full backfill readiness
- Monthly batch planning
- User-confirmed monthly execution
- After-batch validation and backup
- Failure recovery
- Stop conditions

Recommended dry-run flow:
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share

python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share

python3 -m tushare_mirror mirror-batch-plan --root "$MIRROR_ROOT" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --calendar-exchange SSE --max-jobs-per-api 20 --json

Explicitly state: do not execute next batch until user confirms.

Stop conditions:
- readiness blocked
- restore-check failed
- backup possible_mutation true
- trade_cal dependency unresolved
- max_jobs_per_api > 20
- token missing
- severe disk warning
- unresolved validation failure
- schema quarantine exists
- inconsistent coverage

Commit:
docs: add controlled full backfill readiness runbook

Phase 5: Real durable root read-only checks
If Phases 1-4 pass, run only these read-only commands against real durable root:
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-batch-plan --root "$MIRROR_ROOT" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --calendar-exchange SSE --max-jobs-per-api 20 --json

These commands must not fetch, execute mirror-run, backfill, write catalog, or create validation_runs.

Final tests:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

Final report:
Goal Mode Result:
- commits
- mirror_review implemented/status
- mirror_readiness implemented/status
- mirror_batch_plan implemented/status
- runbook updated
- real durable read-only checks result
- next batch plan: 20250201-20250228, max_jobs_per_api=20, estimated_request_count, blocked endpoints, warnings
- safety: real_requests_executed=false, mirror_run_executed=false, full_mirror_executed=false, endpoints_added=false, stock_loop=false, minute/tick/order=false, financial/PIT=false, postgres=false
- tests
- worktree status
- next user-confirmed action
