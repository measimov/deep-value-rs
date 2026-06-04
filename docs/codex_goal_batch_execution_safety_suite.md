# Goal: Controlled Batch Execution Safety Suite

You are in autonomous iteration mode.

The user wants another long infrastructure-only run before executing the February batch. Do not fetch real Tushare data, do not execute mirror-run, do not backfill dates, do not add executable endpoints, and do not enter full mirror.

Current state:
- Durable mirror root: /mnt/gw/TuShare
- Backup root: /mnt/gw/TuShare-backup
- January 2025 low-risk pilot succeeded
- February 2025 bundle exists at /tmp/tushare-mirror-batch-bundle-202502
- mirror-status, mirror-audit, mirror-next-batch, mirror-batch-bundle, mirror-operator-checklist, stop-policy, schema-status, backup-status, mirror-coverage-matrix, request-estimate exist
- api-infra-readiness exists
- object/text/intraday/financial execution remains blocked
- Latest tests passed: 268 OK
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
- Any new command must be read-only unless it writes only to a user-provided output path outside mirror root and backup root.
- Do not run commands.sh from any generated bundle.

Baseline first:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

If baseline fails, fix and commit:
fix: restore batch safety baseline health

## Phase 1: Batch bundle manifest schema

Enhance mirror-batch-bundle to write a bundle manifest file:

/tmp/tushare-mirror-batch-bundle-202502/bundle_manifest.json

Manifest fields:
- manifest_version
- bundle_id
- created_at
- source_root
- backup_root
- scope
- start_date
- end_date
- max_jobs_per_api
- generated_by
- input_reports
- files
- commands
- safety_boundaries
- requires_user_confirmation
- execute_command_present
- commands_guarded
- token_plaintext_found

Each files[] item:
- relative_path
- size_bytes
- sha256
- file_kind
- required

Each commands[] item:
- command_name
- command_text
- would_execute_real_requests
- requires_user_confirmation
- guarded
- allowed_in_bundle

Bundle manifest must not include token plaintext.

Tests:
- bundle manifest generated
- file hashes correct
- commands recorded and guarded
- no token plaintext
- existing bundle generation still works
- output path safety preserved

Commit:
feat: add batch bundle manifest schema

## Phase 2: Bundle verification CLI

Add read-only CLI:
python3 -m tushare_mirror mirror-batch-bundle-verify --bundle /tmp/tushare-mirror-batch-bundle-202502
python3 -m tushare_mirror mirror-batch-bundle-verify --bundle /tmp/tushare-mirror-batch-bundle-202502 --json

It must verify:
- bundle_manifest.json exists
- manifest_version supported
- required files exist
- size/sha256 match
- README.md exists
- commands.sh exists
- commands.sh contains USER_CONFIRMATION_REQUIRED
- commands.sh is not executable by default if practical, or warning if executable
- batch_plan.json valid
- readiness.json valid
- review.json valid
- status.json valid
- audit.json valid
- stop_policy.json valid
- no token plaintext in bundle
- no command auto-execution occurred

Output:
- status: passed|warning|blocked
- bundle_id
- file_count
- checked_file_count
- missing_file_count
- checksum_failure_count
- command_guard_status
- token_plaintext_found
- warnings
- blocking_errors

Tests:
- valid bundle passed
- missing file blocked
- checksum mismatch blocked
- unguarded commands.sh blocked
- token plaintext fixture blocked
- JSON stable
- read-only

Commit:
feat: add mirror batch bundle verification

## Phase 3: Command safety analyzer

Add read-only CLI:
python3 -m tushare_mirror command-safety-check --file /tmp/tushare-mirror-batch-bundle-202502/commands.sh
python3 -m tushare_mirror command-safety-check --file /tmp/tushare-mirror-batch-bundle-202502/commands.sh --json

Analyze shell command files without executing.

Detect:
- mirror-run --execute
- backfill --execute
- backfill-missing --execute
- rm -rf unsafe paths
- output path inside mirror root
- backup path inside mirror root
- missing USER_CONFIRMATION_REQUIRED marker
- token-like strings
- curl/wget/http calls
- python commands that would fetch
- unknown high-risk commands

Do not execute commands.

Output:
- file
- status
- execute_commands_found
- guarded_execute_commands
- unguarded_execute_commands
- destructive_commands_found
- network_commands_found
- token_plaintext_found
- warnings
- blocking_errors

Tests:
- guarded mirror-run accepted with warning
- unguarded mirror-run blocked
- destructive rm -rf blocked
- token text blocked
- JSON stable
- no execution

Commit:
feat: add command safety analyzer

## Phase 4: Batch rehearsal simulator

Add read-only CLI:
python3 -m tushare_mirror mirror-batch-rehearse --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --bundle /tmp/tushare-mirror-batch-bundle-202502
python3 -m tushare_mirror mirror-batch-rehearse ... --json

It should simulate the execution sequence without running commands:
1. preflight
2. review
3. readiness
4. batch-plan
5. operator checklist
6. execute command would run
7. validate --no-record would run
8. backup would run
9. restore-check would run
10. post-batch review would run

Do not call mirror-run.
Do not fetch.
Do not backfill.
Do not write catalog.
Do not write backup.

Output:
- rehearsal_status
- steps
- would_execute_real_requests
- estimated_request_count
- blocked_by
- warnings
- user_confirmation_required
- next_safe_action

Tests:
- valid bundle rehearsal passed
- missing bundle blocked
- unverified bundle warning/blocking
- readiness blocked -> rehearsal blocked
- JSON stable
- no side effects

Commit:
feat: add mirror batch rehearsal simulator

## Phase 5: Batch execution ledger model

Add infrastructure-only ledger model for future batch executions.

No actual execution.

Add CLI:
python3 -m tushare_mirror mirror-batch-ledger --root /mnt/gw/TuShare --scope low-risk-a-share
python3 -m tushare_mirror mirror-batch-ledger --root /mnt/gw/TuShare --scope low-risk-a-share --json

It should read catalog/run summaries and infer batch history:
- known pilot batches
- date ranges covered
- executed endpoints
- backups associated if known
- validation status
- coverage status
- next recommended batch

If no explicit batch ledger exists, infer from mirror runs and coverage. Do not write.

Output:
- ledger_status
- batches
- inferred_batches
- latest_completed_batch
- next_recommended_batch
- warnings

Tests:
- infer January pilot
- incomplete month detected
- no batches case
- JSON stable
- no side effects

Commit:
feat: add mirror batch ledger report

## Phase 6: Batch completion certificate generator

Add read-only/file-output CLI:
python3 -m tushare_mirror mirror-batch-certificate --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --output /tmp/tushare-batch-cert-202501
python3 -m tushare_mirror mirror-batch-certificate ... --json

This writes a human-readable certificate bundle outside mirror/backup roots.

Contents:
- certificate.json
- certificate.md

Certificate fields:
- certificate_version
- root
- backup
- scope
- date_range
- coverage_summary
- snapshot_summary
- validation_status
- backup_status
- restore_check_status
- token_plaintext_found
- generated_at
- limitations
- not_a_full_mirror=true

Path safety:
- output cannot be inside mirror root
- output cannot be inside backup root
- refuse existing output unless --overwrite

Tests:
- certificate generated
- output inside root blocked
- missing backup blocked/warning
- JSON stable
- no catalog side effects

Commit:
feat: add mirror batch completion certificate

## Phase 7: Failure drill simulator

Add read-only CLI:
python3 -m tushare_mirror mirror-failure-drill --scenario rate_limited --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-failure-drill --scenario backup_failed --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-failure-drill --scenario schema_incompatible --scope low-risk-a-share --json

Supported scenarios:
- rate_limited
- permission_denied
- invalid_params
- schema_incompatible
- validation_failed
- backup_failed
- restore_check_failed
- trade_cal_missing
- token_missing
- disk_space_low

Output:
- scenario
- severity
- stop_condition
- retry_allowed
- continue_allowed
- required_operator_action
- commands_to_inspect
- commands_not_to_run
- recovery_steps
- escalation_notes

No real failure injection into catalog.

Tests:
- all scenarios produce output
- unknown scenario rejected
- JSON stable
- no side effects

Commit:
feat: add mirror failure drill simulator

## Phase 8: Disk and path diagnostics

Add read-only CLI:
python3 -m tushare_mirror path-diagnostics --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --json

Report:
- root_exists
- backup_exists
- root_size
- backup_size
- root_file_count
- backup_file_count
- parent_free_bytes
- backup_inside_root
- root_inside_backup
- same_device if available
- warnings

Do not write.

Tests:
- normal paths
- backup inside root warning/blocking
- missing path
- JSON stable
- no side effects

Commit:
feat: add mirror path diagnostics

## Phase 9: Token hygiene scanner

Add CLI:
python3 -m tushare_mirror token-hygiene --path /mnt/gw/TuShare --json
python3 -m tushare_mirror token-hygiene --path /mnt/gw/TuShare-backup --json

Behavior:
- scan text-like files and SQLite fields carefully enough to detect obvious token plaintext
- do not print token values
- report counts/paths only
- skip binary/raw parquet content where impractical, but scan manifest/json/yaml/sqlite text fields
- do not mutate files

Output:
- path
- scanned_file_count
- skipped_file_count
- suspicious_match_count
- suspicious_paths
- token_plaintext_found
- warnings

Tests:
- token fixture detected but not printed
- clean tree passed
- binary skipped
- JSON stable
- no side effects

Commit:
feat: add token hygiene scanner

## Phase 10: Monthly batch promotion checklist

Add read-only CLI:
python3 -m tushare_mirror monthly-promotion-checklist --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share --from-month 202501 --to-month 202502 --json

Purpose: decide whether it is safe to promote from January pilot to February controlled batch.

Checks:
- source month coverage complete
- backup valid
- no backup mutation
- no schema/quarantine blockers
- next batch plan exists
- request estimate low/medium
- operator checklist ready
- bundle verified if provided
- explicit user confirmation required

Output:
- from_month
- to_month
- ready_to_promote
- blocking_errors
- warnings
- required_user_confirmation
- next_commands

No execution.

Tests:
- ready promotion
- incomplete source month blocked
- mutated backup blocked
- missing next plan warning/block
- JSON stable
- no side effects

Commit:
feat: add monthly batch promotion checklist

## Phase 11: Operations report aggregator

Add read-only CLI:
python3 -m tushare_mirror mirror-ops-report --root /mnt/gw/TuShare --backup /mnt/gw/TuShare-backup --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --next-start-date 20250201 --next-end-date 20250228 --json

Aggregate:
- mirror-status
- mirror-audit
- mirror-next-batch
- backup-status
- schema-status
- coverage matrix
- request estimate
- operator checklist
- stop policy summary
- path diagnostics
- token hygiene summary
- promotion checklist

Output:
- overall_status
- ready_for_next_user_confirmed_batch
- sections
- warnings
- blocking_errors
- recommended_next_action

No writes.

Tests:
- aggregate healthy fake mirror
- one blocking section propagates
- JSON stable
- no side effects

Commit:
feat: add mirror operations aggregate report

## Phase 12: Documentation and command bundle update

Update runbook.

Add:
Batch execution safety suite

Document:
- bundle verification
- command safety check
- rehearsal
- ledger
- certificate
- failure drills
- path diagnostics
- token hygiene
- monthly promotion checklist
- operations aggregate report
- exact recommended operator flow

Recommended operator flow:
1. mirror-status
2. mirror-audit
3. mirror-next-batch
4. mirror-batch-bundle
5. mirror-batch-bundle-verify
6. command-safety-check
7. mirror-batch-rehearse
8. mirror-operator-checklist
9. monthly-promotion-checklist
10. user confirms
11. only then mirror-run --execute

Commit:
docs: add batch execution safety suite runbook

## Phase 13: Read-only command contract regression

Add a single comprehensive test module ensuring all newly added commands are read-only unless writing to explicit output path:
- mirror-batch-bundle-verify
- command-safety-check
- mirror-batch-rehearse
- mirror-batch-ledger
- mirror-batch-certificate
- mirror-failure-drill
- path-diagnostics
- token-hygiene
- monthly-promotion-checklist
- mirror-ops-report

Commit:
test: add batch execution safety read-only contracts

## Phase 14: Durable read-only checks

Run only read-only commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
BUNDLE=/tmp/tushare-mirror-batch-bundle-202502

If BUNDLE exists:
python3 -m tushare_mirror mirror-batch-bundle-verify --bundle "$BUNDLE" --json
python3 -m tushare_mirror command-safety-check --file "$BUNDLE/commands.sh" --json
python3 -m tushare_mirror mirror-batch-rehearse --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --json

Run:
python3 -m tushare_mirror mirror-batch-ledger --root "$MIRROR_ROOT" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-failure-drill --scenario rate_limited --scope low-risk-a-share --json
python3 -m tushare_mirror path-diagnostics --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --json
python3 -m tushare_mirror token-hygiene --path "$MIRROR_ROOT" --json
python3 -m tushare_mirror token-hygiene --path "$MIRROR_BACKUP" --json
python3 -m tushare_mirror monthly-promotion-checklist --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --from-month 202501 --to-month 202502 --json
python3 -m tushare_mirror mirror-ops-report --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --next-start-date 20250201 --next-end-date 20250228 --json

Generate certificate in /tmp only:
rm -rf /tmp/tushare-batch-cert-202501
python3 -m tushare_mirror mirror-batch-certificate --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --output /tmp/tushare-batch-cert-202501 --json

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
Batch Execution Safety Suite Result:
- commits
- bundle manifest/verify status
- command safety status
- rehearsal status
- ledger status
- certificate status
- failure drill status
- path diagnostics status
- token hygiene status
- promotion checklist status
- ops report status
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next user-confirmed action

Stop after Phase 14.
Do not execute real requests.
