# Goal: Full Pre-backfill Hardening Suite

You are in autonomous iteration mode.

The user wants a longer infrastructure-only run before entering controlled full backfill. Do not fetch real Tushare data, do not execute mirror-run, do not backfill new dates, do not add executable endpoints, and do not enter full mirror.

Current state:
- Durable mirror root: /mnt/gw/TuShare
- Backup root: /mnt/gw/TuShare-backup
- January 2025 low-risk pilot succeeded
- mirror-review, mirror-readiness, mirror-batch-plan exist
- api-infra-readiness exists
- code-universe, code-list-plan, code-date-matrix-plan exist
- period-plan, code-period-plan, PIT metadata, pit-readiness exist
- object-plan, intraday-plan, storage-estimate, compaction-plan, rate-policy, endpoint-enable-checklist exist
- object/text/intraday/financial execution remains blocked
- Tests last reported: 218 OK
- Worktree should start clean

Hard boundaries:
- Do not execute mirror-run.
- Do not fetch real Tushare data.
- Do not backfill new dates.
- Do not add executable endpoints.
- Do not execute stock loops.
- Do not enable financial/PIT/object/intraday/compaction execution.
- Do not implement PostgreSQL loader.
- Do not implement remote backup, restore-into, scheduler daemon, or parallel execution.
- Do not output token plaintext.
- Commit each completed phase separately.
- Do not commit empty commits.
- All new operational commands must be read-only unless explicitly documented as file-bundle output outside mirror/backup roots.
- Any command that writes outside mirror/backup roots must only write to a user-provided output path and must refuse unsafe paths.

Baseline first:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

If baseline fails, fix and commit:
fix: restore pre-backfill hardening baseline health

## Phase 1: Mirror status dashboard

Add read-only CLI:
python3 -m tushare_mirror mirror-status --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share
python3 -m tushare_mirror mirror-status --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share --json

Aggregate:
- catalog status
- latest snapshots
- mirror-review summary
- mirror-readiness summary
- backup-inspect summary
- restore-check summary
- api-infra-readiness summary
- daily-like coverage for the proven pilot range where available
- token plaintext scan result as true/false only

Output:
- report_version
- root
- backup
- catalog_status
- backup_status
- restore_check_status
- readiness_status
- ready_for_controlled_full_backfill
- latest_snapshot_count
- enabled_executable_endpoint_count
- disabled_inventory_endpoint_count
- daily_like_coverage_summary
- backup_possible_mutation
- token_plaintext_found
- warnings
- blocking_errors

Read-only:
- no fetch
- no mirror-run
- no backfill
- no catalog writes
- no validation_runs

Tests:
- healthy fake mirror
- missing backup
- mutated backup
- missing catalog
- JSON contract
- no side effects

Commit:
feat: add mirror status dashboard

## Phase 2: Mirror audit report

Add read-only CLI:
python3 -m tushare_mirror mirror-audit --root /mnt/gw/TuShare --scope low-risk-a-share
python3 -m tushare_mirror mirror-audit --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share --json

Support:
- --since YYYYMMDD
- --limit N

Summarize:
- run_count_by_type
- succeeded_run_count
- failed_run_count
- job_count_by_status
- validation_status_counts
- snapshot_count_by_api
- failed_jobs
- quarantined_count
- latest_run_id
- backup_summary if backup provided
- warnings
- blocking_errors

Read-only only.

Tests:
- fake catalog audit
- failed job appears
- quarantine appears
- validation failures appear
- JSON stable
- no side effects

Commit:
feat: add mirror audit report

## Phase 3: Next batch recommender

Add read-only CLI:
python3 -m tushare_mirror mirror-next-batch --root /mnt/gw/TuShare --scope low-risk-a-share
python3 -m tushare_mirror mirror-next-batch --root /mnt/gw/TuShare --scope low-risk-a-share --json

Behavior:
- inspect local trade_cal and coverage
- infer latest complete month for daily-like endpoints
- recommend next bounded month
- do not execute
- do not fetch
- do not write catalog

For current durable pilot, next month should likely be 20250201-20250228 unless already covered.

Output:
- report_version
- current_completed_months
- last_complete_month
- recommended_next_start_date
- recommended_next_end_date
- reason
- required_trade_cal_range
- estimated_request_count
- recommended_max_jobs_per_api
- plan_command_preview
- execute_command_preview marked USER_CONFIRMATION_REQUIRED
- warnings

Tests:
- no coverage
- January covered -> February
- February covered -> March
- partial coverage -> missing month/range
- JSON stable
- no side effects

Commit:
feat: add mirror next batch recommender

## Phase 4: Batch dry-run bundle generator

Add read-only/file-output CLI:
python3 -m tushare_mirror mirror-batch-bundle --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --output /tmp/tushare-batch-202502
python3 -m tushare_mirror mirror-batch-bundle ... --json

Bundle contents:
- README.md
- batch_plan.json
- readiness.json
- review.json
- status.json
- audit.json
- stop_policy.json
- commands.sh

commands.sh may include mirror-run --execute, but it must be commented or guarded by an obvious manual confirmation marker.

Rules:
- Do not create anything inside MIRROR_ROOT.
- Do not create anything inside MIRROR_BACKUP.
- Refuse output path inside mirror root.
- Refuse output path inside backup target.
- Refuse existing output path unless --overwrite.
- Do not execute commands.sh.
- Do not fetch.
- Do not write catalog.

Tests:
- bundle created outside roots
- commands generated but not executed
- existing output refused without overwrite
- output inside root blocked
- output inside backup blocked
- JSON stable
- no catalog side effects

Commit:
feat: add mirror batch dry-run bundle generator

## Phase 5: Operator checklist command

Add read-only CLI:
python3 -m tushare_mirror mirror-operator-checklist --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share --start-date 20250201 --end-date 20250228
python3 -m tushare_mirror mirror-operator-checklist ... --json

Output:
- report_version
- paths_valid
- backup_not_nested
- restore_check_passed
- backup_not_mutated
- readiness_not_blocked
- no_schema_quarantine
- no_failed_validation
- token_available true/false only
- max_jobs_guardrail
- batch_plan_available
- disk_space_warning
- stop_conditions
- exact_plan_command
- exact_execute_command marked USER_CONFIRMATION_REQUIRED

Read-only.

Tests:
- healthy checklist ready
- missing token warning/blocking without plaintext
- mutated backup blocks
- failed readiness blocks
- JSON stable
- no side effects

Commit:
feat: add mirror operator checklist

## Phase 6: Stop-condition policy report

Add read-only CLI:
python3 -m tushare_mirror stop-policy --scope low-risk-a-share --json
python3 -m tushare_mirror stop-policy --category financial --json
python3 -m tushare_mirror stop-policy --category intraday --json
python3 -m tushare_mirror stop-policy --category backup --json

Describe:
- stop immediately conditions
- continue with warning conditions
- retryable failures
- non-retryable failures
- backup-required conditions
- user-confirmation-required conditions

Categories:
- low-risk-a-share
- code-loop
- financial
- object-text
- intraday
- backup
- mirror-orchestrator

Tests:
- low-risk policy present
- financial policy blocks execution
- intraday policy blocks execution
- backup policy present
- JSON stable
- no side effects

Commit:
feat: add stop condition policy report

## Phase 7: Schema drift and quarantine status report

Add read-only CLI:
python3 -m tushare_mirror schema-status --root /mnt/gw/TuShare
python3 -m tushare_mirror schema-status --root /mnt/gw/TuShare --json

Report:
- schema_count_by_api
- latest_schema_by_api
- schema_change_count
- incompatible_schema_count
- pending_schema_change_count
- quarantine_count
- quarantined_apis
- warnings
- blocking_errors

Read-only only.

Tests:
- fake schema changes
- incompatible schema reported
- quarantine reported
- JSON stable
- no side effects

Commit:
feat: add schema and quarantine status report

## Phase 8: Backup history and mutation diagnostics

Add read-only CLI:
python3 -m tushare_mirror backup-status --backup /mnt/gw/TuShare-backup
python3 -m tushare_mirror backup-status --backup /mnt/gw/TuShare-backup --json

It should use backup-inspect and restore-check logic, but present operator-focused summary:
- manifest_valid
- backup_id
- created_at
- snapshot_scope
- file_count
- raw_file_count
- lake_file_count
- catalog_checksum_status
- possible_mutation
- restore_check_status
- recommended_action

Tests:
- clean backup
- mutated backup
- missing manifest
- bad manifest
- JSON stable
- no side effects

Commit:
feat: add backup status diagnostics

## Phase 9: Coverage matrix report

Add read-only CLI:
python3 -m tushare_mirror mirror-coverage-matrix --root /mnt/gw/TuShare --scope low-risk-a-share --start-date 20250101 --end-date 20250131
python3 -m tushare_mirror mirror-coverage-matrix --root /mnt/gw/TuShare --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --json

Report coverage for:
- daily
- adj_factor
- daily_basic
- suspend_d
- weekly
- monthly where supported

For daily-like:
- use trading-days-only with local SSE trade_cal

Output:
- api
- total_dates
- covered_dates
- missing_dates
- coverage_ratio
- missing_date_sample
- status

Read-only.

Tests:
- complete coverage
- partial coverage
- missing trade_cal
- JSON stable
- no side effects

Commit:
feat: add mirror coverage matrix report

## Phase 10: Request estimate and quota risk report

Add read-only CLI:
python3 -m tushare_mirror request-estimate --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --root /mnt/gw/TuShare --json

Report:
- estimated_requests_by_api
- estimated_total_requests
- planned_trade_cal_requests
- daily_like_requests
- weekly_monthly_requests
- reference_refresh_requests
- risk_level
- assumptions
- warnings
- not_a_quota_guarantee=true

Do not call Tushare.
Do not inspect token quota.
Do not fetch.

Tests:
- January/February estimate
- missing trade_cal range
- JSON stable
- no side effects

Commit:
feat: add request estimate report

## Phase 11: Read-only guardrail regression suite

Add tests ensuring read-only commands do not mutate catalog or create files.

Cover:
- mirror-review
- mirror-readiness
- mirror-batch-plan
- mirror-status
- mirror-audit
- mirror-next-batch
- mirror-batch-bundle
- mirror-operator-checklist
- stop-policy
- schema-status
- backup-status
- mirror-coverage-matrix
- request-estimate
- api-infra-readiness
- pit-readiness
- object-plan
- intraday-plan
- storage-estimate
- compaction-plan
- rate-policy
- endpoint-enable-checklist
- code-universe
- code-list-plan
- code-date-matrix-plan
- period-plan
- code-period-plan

Test style:
- record catalog counts before
- run command
- record counts after
- assert unchanged
- assert no raw/lake files created
- assert no validation_runs created

Commit:
test: expand read-only operational guardrails

## Phase 12: CLI help and JSON contract polish

Review help and JSON output for:
- mirror-status
- mirror-audit
- mirror-next-batch
- mirror-batch-bundle
- mirror-operator-checklist
- stop-policy
- schema-status
- backup-status
- mirror-coverage-matrix
- request-estimate

Ensure:
- help says read-only where relevant
- help says no real requests where relevant
- JSON includes report_version
- field names stable
- missing root/backup errors clear

Tests:
- --help works
- JSON report_version present
- missing root errors clear
- missing backup errors clear

Commit:
chore: polish pre-backfill operations CLI contracts

## Phase 13: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add section:
Pre-full-backfill operational hardening

Include:
- mirror-status
- mirror-audit
- mirror-next-batch
- mirror-batch-bundle
- mirror-operator-checklist
- stop-policy
- schema-status
- backup-status
- mirror-coverage-matrix
- request-estimate
- recommended operator workflow
- how to generate and review a batch bundle
- why commands.sh must not auto-execute
- what to report after each batch
- stop conditions
- why this is still not full mirror automation

Commit:
docs: add pre-full-backfill operations runbook

## Phase 14: Durable read-only checks

Run only read-only commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-status --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-audit --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-next-batch --root "$MIRROR_ROOT" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-operator-checklist --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --json
python3 -m tushare_mirror stop-policy --scope low-risk-a-share --json
python3 -m tushare_mirror schema-status --root "$MIRROR_ROOT" --json
python3 -m tushare_mirror backup-status --backup "$MIRROR_BACKUP" --json
python3 -m tushare_mirror mirror-coverage-matrix --root "$MIRROR_ROOT" --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --json
python3 -m tushare_mirror request-estimate --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --root "$MIRROR_ROOT" --json

Generate bundle in /tmp only:
rm -rf /tmp/tushare-mirror-batch-bundle-202502
python3 -m tushare_mirror mirror-batch-bundle --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --output /tmp/tushare-mirror-batch-bundle-202502 --json

Do not run commands.sh.
Do not execute mirror-run.
Do not fetch.

Final tests:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

Final report:
Pre-full-backfill Operational Hardening Result:
- commits
- mirror-status status
- mirror-audit status
- mirror-next-batch recommendation
- mirror-batch-bundle status
- operator checklist status
- stop-policy status
- schema-status status
- backup-status status
- coverage matrix status
- request-estimate status
- guardrail tests
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next recommended user-confirmed action

Stop after Phase 14.
Do not execute real requests.
