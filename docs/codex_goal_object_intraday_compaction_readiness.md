# Goal: Object / Text / Intraday / Compaction Infrastructure Readiness

You are in autonomous iteration mode.

The user wants infrastructure only. Do not fetch real Tushare data, do not execute mirror-run, do not backfill new dates, do not enable object/text/intraday execution, and do not enter full mirror.

Current state:
- Durable mirror root: /mnt/gw/TuShare
- Backup root: /mnt/gw/TuShare-backup
- January 2025 low-risk pilot succeeded
- mirror-review, mirror-readiness, mirror-batch-plan exist
- api-infra-readiness exists
- endpoint taxonomy / planner registry / execution policy exist
- code-universe, code-list-plan, code-date-matrix-plan exist
- period-plan, code-period-plan, PIT metadata, pit-readiness exist
- financial execution remains blocked
- Tests last passed: 186 OK
- Worktree should start clean

Hard boundaries:
- Do not execute mirror-run.
- Do not fetch real Tushare data.
- Do not backfill dates.
- Do not add executable endpoints.
- Do not enable object/text/intraday execution.
- Do not download PDF/news/research/object content.
- Do not execute minute/tick/order/realtime.
- Do not implement PostgreSQL loader.
- Do not implement remote backup, restore-into, real compaction execution, scheduler, or parallel execution.
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
fix: restore object intraday readiness baseline health

## Phase 1: Object/text metadata model

Add infrastructure-only object/text metadata models.

Object/text endpoint kinds:
- object_document
- text_news
- research_report
- announcement
- html_text
- unknown_object_text

Object strategy fields:
- object_index_required
- object_download_required
- content_addressed_storage
- sha256_dedup_required
- content_type_field
- source_url_field
- publish_time_fields
- title_fields
- object_id_fields
- metadata_lake_required
- binary_storage_layer
- execution_blocked_until_object_store_enabled

Do not implement object download.

Tests:
- valid object metadata
- missing object id/source fields warns or blocks
- download execution blocked
- JSON stable

Commit:
feat: add object text metadata model

## Phase 2: object-plan CLI

Add read-only plan-only CLI:
python3 -m tushare_mirror object-plan --api anns --start-date 20250101 --end-date 20250131 --json
python3 -m tushare_mirror object-plan --api news --start-date 20250101 --end-date 20250131 --json
python3 -m tushare_mirror object-plan --api report_rc --start-date 20250101 --end-date 20250131 --json

Behavior:
- plan-only
- no fetch
- no object download
- no catalog writes
- no validation rows
- execution_allowed=false
- blocked until object index/store policy exists

Output:
- api_name
- endpoint_kind
- planner_kind
- date_range
- object_strategy
- required_infra
- execution_allowed=false
- would_require_real_request=true
- would_download_objects=false
- blocked_reason
- warnings

Tests:
- anns plan blocked
- news plan blocked
- report/research plan blocked
- missing date range rejected
- invalid date rejected
- JSON stable
- no side effects

Commit:
feat: add object text planning CLI

## Phase 3: Intraday metadata and bucket strategy model

Add infrastructure-only intraday metadata models.

Intraday endpoint kinds:
- minute_bar
- tick
- order
- realtime

Required fields:
- freq
- bucket_strategy
- bucket_count
- partition_template
- target_file_size_mb
- max_file_size_mb
- compaction_required
- query_benchmark_required
- storage_estimate_required
- execution_blocked_until_bucket_policy_enabled

Default plan-only recommendations:
- minute: bucket_count 32 or 64
- tick/order: bucket_count 128
- target_file_size_mb 128-512
- max_file_size_mb 1024

No execution.

Tests:
- minute metadata valid
- tick metadata valid
- invalid bucket_count rejected
- compaction_required true
- execution blocked
- JSON stable

Commit:
feat: add intraday bucket metadata model

## Phase 4: intraday-plan CLI

Add read-only plan-only CLI:
python3 -m tushare_mirror intraday-plan --api stk_mins --freq 1min --start-date 20250102 --end-date 20250103 --bucket-count 64 --json
python3 -m tushare_mirror intraday-plan --api tick --start-date 20250102 --end-date 20250103 --bucket-count 128 --json

Behavior:
- plan-only
- no fetch
- no catalog writes
- no validation rows
- execution_allowed=false
- blocked until bucket partition + compaction + storage estimate + rate policy are implemented

Output:
- api_name
- freq
- date_range
- bucket_count
- estimated_partition_strategy
- required_infra
- execution_allowed=false
- blocked_reason
- warnings

Tests:
- minute plan blocked
- tick plan blocked
- bucket_count validation
- date range validation
- JSON stable
- no side effects

Commit:
feat: add intraday bucket planning CLI

## Phase 5: Storage estimate model

Add infrastructure-only storage estimate support.

CLI:
python3 -m tushare_mirror storage-estimate --scope low-risk-a-share --start-date 20250101 --end-date 20251231 --json
python3 -m tushare_mirror storage-estimate --category intraday --api stk_mins --freq 1min --start-date 20250102 --end-date 20250131 --bucket-count 64 --json

Behavior:
- estimate only
- no fetch
- no writes
- use existing pilot size as reference where possible
- for intraday, output very rough warning-level estimate, not authoritative

Output:
- scope/category
- date_range
- estimated_jobs
- estimated_raw_files
- estimated_lake_files
- estimated_size_class
- assumptions
- warnings
- confidence: low|medium|high

Tests:
- low-risk estimate from pilot reference
- intraday estimate warns low confidence
- no side effects
- JSON stable

Commit:
feat: add storage estimate reporting

## Phase 6: Compaction readiness and dry-run planner

Add plan-only compaction readiness.

CLI:
python3 -m tushare_mirror compaction-plan --root /mnt/gw/TuShare --api daily_basic --json

Behavior:
- read local catalog
- identify candidate partitions by file counts / sizes
- do not rewrite files
- do not create snapshots
- do not modify catalog
- execution_allowed=false in this phase

Output:
- api_name
- partitions_checked
- candidate_partitions
- small_file_count
- oversized_file_count
- estimated_actions
- execution_allowed=false
- required_infra
- warnings

Tests:
- no candidates
- fake small file candidates
- oversized file candidates
- no side effects
- JSON stable

Commit:
feat: add compaction readiness planner

## Phase 7: Rate-limit and failure policy model

Add infrastructure-only rate-limit/failure policy model.

Fields:
- endpoint/api family
- max_requests_per_batch
- retryable_errors
- non_retryable_errors
- backoff_strategy
- stop_conditions
- batch_abort_conditions
- permission_denied_policy
- rate_limited_policy
- schema_incompatible_policy
- quarantine_policy

CLI:
python3 -m tushare_mirror rate-policy --scope low-risk-a-share --json
python3 -m tushare_mirror rate-policy --category intraday --json
python3 -m tushare_mirror rate-policy --category financial --json

No execution.

Tests:
- low-risk policy present
- financial policy blocks execution
- intraday policy blocks execution
- JSON stable
- no side effects

Commit:
feat: add rate limit and failure policy report

## Phase 8: Endpoint enablement checklist

Add read-only CLI:
python3 -m tushare_mirror endpoint-enable-checklist --api fina_indicator --json
python3 -m tushare_mirror endpoint-enable-checklist --api anns --json
python3 -m tushare_mirror endpoint-enable-checklist --api stk_mins --json
python3 -m tushare_mirror endpoint-enable-checklist --api tick --json

Output:
- api_name
- endpoint_kind
- planner_kind
- current_execution_status
- required_infra
- required_tests
- required_smoke_steps
- allowed_next_action
- forbidden_actions
- risk_level
- execution_allowed=false unless already enabled low-risk

Tests:
- financial checklist
- object checklist
- intraday checklist
- unknown api clear error
- low-risk enabled endpoint checklist
- JSON stable
- no side effects

Commit:
feat: add endpoint enablement checklist

## Phase 9: Execution policy integration

Update execution policy to cover:
- object_index
- object_download
- text_news
- bucketed_intraday
- compaction execution
- realtime_poll

All remain plan-only or blocked for execution.

Existing low-risk endpoints must remain unaffected.

Tests:
- object execution blocked
- news execution blocked
- intraday execution blocked
- compaction execution blocked
- realtime execution blocked
- mirror-run cannot execute them
- direct fetch cannot bypass policy
- existing low-risk tests still pass

Commit:
feat: guard object intraday compaction execution policies

## Phase 10: api-infra-readiness update

Update api-infra-readiness to report:
- object_text_planner=plan_only
- object_download_execution=false
- intraday_bucket_planner=plan_only
- intraday_execution=false
- compaction_planner=plan_only
- compaction_execution=false
- storage_estimate=implemented
- rate_policy_report=implemented
- endpoint_enable_checklist=implemented

Missing infra groups:
- object_store_index
- object_download_validation
- text_dedup_policy
- intraday_bucket_partition
- compaction_executor
- query_benchmark
- storage_capacity_plan
- remote_disaster_recovery
- realtime_policy
- postgres_derived_layer

Tests:
- readiness JSON fields present
- disabled inventory counts unchanged
- no side effects

Commit:
feat: report object intraday readiness

## Phase 11: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add:
Object, text, intraday, and compaction readiness roadmap

Explain:
- object-plan is plan-only
- intraday-plan is plan-only
- compaction-plan is plan-only
- storage-estimate is approximate
- rate-policy is advisory
- endpoint-enable-checklist is required before enabling
- why no object/intraday execution is allowed yet
- future enablement steps
- stop conditions

Commit:
docs: add object intraday readiness roadmap

## Phase 12: Durable read-only checks

Run only read-only commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror object-plan --api anns --start-date 20250101 --end-date 20250131 --json
python3 -m tushare_mirror object-plan --api news --start-date 20250101 --end-date 20250131 --json
python3 -m tushare_mirror intraday-plan --api stk_mins --freq 1min --start-date 20250102 --end-date 20250103 --bucket-count 64 --json
python3 -m tushare_mirror storage-estimate --scope low-risk-a-share --start-date 20250101 --end-date 20251231 --json
python3 -m tushare_mirror compaction-plan --root "$MIRROR_ROOT" --api daily_basic --json
python3 -m tushare_mirror rate-policy --scope low-risk-a-share --json
python3 -m tushare_mirror endpoint-enable-checklist --api fina_indicator --json
python3 -m tushare_mirror endpoint-enable-checklist --api anns --json
python3 -m tushare_mirror endpoint-enable-checklist --api stk_mins --json
python3 -m tushare_mirror api-infra-readiness --json
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json

These must not fetch or write catalog.

Final tests:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

Final report:
Object / Text / Intraday / Compaction Readiness Result:
- commits
- object/text model status
- object-plan status
- intraday model status
- intraday-plan status
- storage-estimate status
- compaction-plan status
- rate-policy status
- endpoint-enable-checklist status
- execution policy status
- api-infra-readiness update
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next recommended phase

Stop after Phase 12.
Do not execute real requests.
