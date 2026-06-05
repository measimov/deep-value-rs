# Goal: February Execution Wrapper and Final Safety Gate

You are in autonomous iteration mode.

The user has NOT authorized execution of the February batch yet. This is still infrastructure-only work. Do not fetch real Tushare data, do not execute mirror-run, do not run commands.sh, do not backfill dates, and do not enter full mirror.

Current state:
- Durable mirror root: /mnt/gw/TuShare
- Backup root: /mnt/gw/TuShare-backup
- January 2025 low-risk pilot succeeded.
- February bundle exists at /tmp/tushare-mirror-batch-bundle-202502 and now has bundle_manifest.json.
- Bundle verification passed.
- Command safety has warning only because guarded mirror-run --execute preview exists.
- Rehearsal passed with estimated requests=6.
- Promotion checklist is staged, no hard blockers, ready_for_dependency_stage=true, ready_to_promote=false.
- February trade_cal 20250201-20250228 is missing locally.
- Daily-like endpoints are blocked_until_trade_cal.
- natural_day_fallback=false.
- No real February fetch has happened.
- Tests last passed: 327 OK.
- Worktree should start clean.

Hard boundaries:
- Do not execute mirror-run.
- Do not run commands.sh.
- Do not fetch real Tushare data.
- Do not backfill dates.
- Do not write inside /mnt/gw/TuShare.
- Do not write inside /mnt/gw/TuShare-backup.
- Do not add executable endpoints.
- Do not run stock loops.
- Do not enable financial/PIT/object/intraday/compaction execution.
- Do not implement PostgreSQL loader.
- Do not implement remote backup, restore-into, scheduler daemon, or parallel execution.
- Do not output token plaintext.
- You may write only docs, tests, source code, and explicit /tmp diagnostic artifacts.
- Commit each completed phase separately.
- Do not commit empty commits.

Baseline first:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

If baseline fails, fix and commit:
fix: restore February final gate baseline health

## Phase 1: Final gate model

Add a read-only final gate model for a user-confirmed batch execution.

This is not execution. It decides whether the user may safely run the already-planned command.

Data structures:
- FinalGateResult
- FinalGateCheck
- FinalGateCommandPreview

Gate checks:
- bundle exists
- bundle verified
- command safety passed or warning-only
- rehearsal passed
- promotion checklist has no hard blockers
- ops report has no hard blockers
- backup restore-check passed
- backup possible_mutation=false
- schema-status has no incompatible/quarantine blockers
- token available true/false only
- requested date range bounded
- max_jobs_per_api <= 20
- scope low-risk-a-share
- no full mirror
- no stock loop
- no prohibited API category
- no commands executed yet

Statuses:
- passed
- warning
- blocked

Output fields:
- gate_status
- ready_for_user_confirmed_execute
- ready_for_dependency_stage
- ready_for_full_batch_after_dependency
- blocking_errors
- warnings
- command_preview
- do_not_run_automatically=true

Tests:
- healthy staged February gate warning/passed according to dependency state
- missing bundle blocked
- invalid bundle blocked
- unguarded command blocked
- backup mutation blocked
- token missing warning/blocking according to existing policy
- JSON stable
- no side effects

Commit:
feat: add February final gate model

## Phase 2: final-gate CLI

Add read-only CLI:
python3 -m tushare_mirror mirror-final-gate \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --bundle /tmp/tushare-mirror-batch-bundle-202502 \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --max-jobs-per-api 20

Also:
python3 -m tushare_mirror mirror-final-gate ... --json

Behavior:
- read-only
- no fetch
- no mirror-run
- no commands.sh execution
- no backfill
- no catalog writes
- no validation_runs
- no backup mutation

It should aggregate:
- bundle verification
- command safety
- rehearsal
- promotion checklist
- ops report
- backup status
- schema status
- path diagnostics
- token hygiene
- stop policy

Output:
- report_version
- gate_status
- ready_for_user_confirmed_execute
- ready_for_dependency_stage
- ready_for_full_batch_after_dependency
- requested_range
- max_jobs_per_api
- estimated_request_count
- dependency_stage
- final_command_preview
- blocking_errors
- warnings
- safety_boundaries

Tests:
- CLI table output
- CLI JSON output
- missing root error clear
- missing backup error clear
- missing bundle error clear
- no side effects

Commit:
feat: add mirror final gate CLI

## Phase 3: User-confirmation token design

Design a future confirmation mechanism but do not require it yet for existing mirror-run.

Add infrastructure for generating a non-secret confirmation phrase.

Example:
python3 -m tushare_mirror mirror-final-gate ... --json

Output includes:
confirmation_phrase:
CONFIRM LOW-RISK-A-SHARE 20250201-20250228 MAXJOBS20

This phrase is not a token and not security. It is operator friction.

Do not modify mirror-run execution behavior in this phase unless trivial and fully backward-compatible.

If adding optional future support is safe, add:
--require-confirmation-phrase
to a wrapper only, not to mirror-run.

Preferred: add only to final-gate output and docs.

Tests:
- phrase stable for same scope/date/max_jobs
- phrase changes when date range changes
- phrase contains no token
- JSON stable

Commit:
feat: add mirror execution confirmation phrase

## Phase 4: Dry-run execution wrapper script generator

Add read-only/file-output CLI:
python3 -m tushare_mirror mirror-execute-script \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --bundle /tmp/tushare-mirror-batch-bundle-202502 \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --max-jobs-per-api 20 \
  --output /tmp/tushare-mirror-execute-202502.sh

Behavior:
- generates a script only
- does not execute it
- refuses output inside mirror root
- refuses output inside backup root
- refuses overwrite unless --overwrite
- script includes:
  - comments explaining risk
  - confirmation phrase
  - pre-run final gate command
  - mirror-run --execute command
  - post-run validate --no-record
  - backup-inspect
  - restore-check
  - mirror-review
  - mirror-next-batch
- script must not include token plaintext
- script should require human editing or uncommenting before actual execution, unless existing convention uses guarded commands
- commands must be clearly marked USER_CONFIRMATION_REQUIRED

Tests:
- script generated outside roots
- output inside root blocked
- output inside backup blocked
- overwrite behavior
- command safety check passes warning-only
- no side effects

Commit:
feat: add guarded mirror execute script generator

## Phase 5: Final gate bundle integration

Update mirror-batch-bundle to optionally include final gate outputs.

Add files to bundle when possible:
- final_gate.json
- execute_script_preview.sh
- final_operator_summary.md

Do not execute.

Ensure bundle_manifest includes them.

Update bundle verification to check these optional files when present.

Tests:
- bundle includes final gate files
- manifest includes final gate files
- verify passes
- command safety warning-only
- no side effects

Commit:
feat: add final gate artifacts to batch bundle

## Phase 6: February dry-run bundle regeneration

Regenerate February bundle in /tmp only with overwrite:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
BUNDLE=/tmp/tushare-mirror-batch-bundle-202502

python3 -m tushare_mirror mirror-batch-bundle \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --max-jobs-per-api 20 \
  --output "$BUNDLE" \
  --overwrite \
  --json

Then run:
python3 -m tushare_mirror mirror-batch-bundle-verify --bundle "$BUNDLE" --json
python3 -m tushare_mirror command-safety-check --file "$BUNDLE/commands.sh" --json
python3 -m tushare_mirror mirror-batch-rehearse --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --json
python3 -m tushare_mirror mirror-final-gate --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --json

Do not run commands.sh.
Do not execute mirror-run.
Do not fetch.

If bugs appear, fix, test, commit:
fix: support February final gate bundle

## Phase 7: Final readiness report command

Add read-only CLI:
python3 -m tushare_mirror mirror-execute-readiness \
  --root /mnt/gw/TuShare \
  --backup /mnt/gw/TuShare-backup \
  --bundle /tmp/tushare-mirror-batch-bundle-202502 \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --max-jobs-per-api 20 \
  --json

This is a high-level wrapper over final-gate.

Output:
- execute_readiness_status
- may_execute_after_user_confirmation
- must_not_execute_automatically=true
- final_gate_status
- bundle_status
- command_safety_status
- rehearsal_status
- promotion_status
- backup_status
- token_hygiene_status
- estimated_request_count
- confirmation_phrase
- exact_user_confirmed_command

Tests:
- healthy readiness
- bundle blocked propagates
- backup mutated propagates
- JSON stable
- no side effects

Commit:
feat: add mirror execute readiness report

## Phase 8: Read-only mutation regression

Add regression tests ensuring final-gate/readiness/script/bundle commands do not mutate durable roots or test catalogs.

Cover:
- mirror-final-gate
- mirror-execute-script
- mirror-batch-bundle with output outside root
- mirror-execute-readiness
- bundle verify
- command safety
- rehearsal

Check:
- catalog counts unchanged
- validation_count unchanged
- no raw/lake files created
- backup catalog checksum unchanged where applicable

Commit:
test: add final gate read-only regressions

## Phase 9: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add section:
Final user-confirmed execution gate

Explain:
- mirror-final-gate
- confirmation phrase
- mirror-execute-readiness
- execute script generator
- February bundle regeneration
- exact order before execution:
  1. mirror-status
  2. backup-status
  3. mirror-batch-bundle --overwrite
  4. mirror-batch-bundle-verify
  5. command-safety-check
  6. mirror-batch-rehearse
  7. monthly-promotion-checklist
  8. mirror-ops-report
  9. mirror-final-gate
  10. mirror-execute-readiness
  11. user manually confirms
  12. only then run mirror-run --execute

Explicitly state:
- This still does not execute February.
- Codex/automation must not run mirror-run without explicit user confirmation.
- Confirmation phrase is friction, not security.

Commit:
docs: add final execution gate runbook

## Phase 10: Durable final read-only checks

Run only read-only/file-output commands:

MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
BUNDLE=/tmp/tushare-mirror-batch-bundle-202502
SCRIPT=/tmp/tushare-mirror-execute-202502.sh

python3 -m tushare_mirror mirror-status --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-final-gate --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --json
python3 -m tushare_mirror mirror-execute-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --json

rm -f "$SCRIPT"
python3 -m tushare_mirror mirror-execute-script --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --output "$SCRIPT" --json
python3 -m tushare_mirror command-safety-check --file "$SCRIPT" --json

Do not run the script.
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
February Final Execution Gate Result:
- commits
- final gate status
- confirmation phrase
- execute readiness status
- execute script generated
- bundle regenerated/verified
- command safety status
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next user-confirmed action

Stop after Phase 10.
Do not execute real requests.
