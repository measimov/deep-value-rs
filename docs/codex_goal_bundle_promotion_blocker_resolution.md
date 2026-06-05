# Goal: Bundle and Promotion Blocker Resolution Before February Execute

You are in autonomous iteration mode.

The user wants another long infrastructure-only run. Do not fetch real Tushare data, do not execute mirror-run, do not execute backfill, do not add executable endpoints, and do not enter full mirror.

Current state:
- Durable mirror root: /mnt/gw/TuShare
- Backup root: /mnt/gw/TuShare-backup
- January 2025 low-risk pilot succeeded
- Backup restore-check succeeded
- Operational hardening suite is complete
- Existing February bundle path: /tmp/tushare-mirror-batch-bundle-202502
- Last result found blockers:
  1. Existing /tmp/tushare-mirror-batch-bundle-202502 is pre-manifest and lacks bundle_manifest.json.
  2. Rehearsal blocked by bundle verification.
  3. Promotion checklist blocked by incomplete January weekly/monthly coverage.
  4. Promotion checklist blocked because February trade_cal is missing locally, so daily-like February endpoints are blocked.
  5. Ops report blocked by same promotion blockers.
- Tests last passed: 314 OK
- Worktree should start clean

Hard boundaries:
- Do not execute mirror-run.
- Do not fetch real Tushare data.
- Do not backfill dates.
- Do not run commands.sh.
- Do not execute February batch.
- Do not add executable endpoints.
- Do not execute stock loops.
- Do not enable financial/PIT/object/intraday/compaction execution.
- Do not implement PostgreSQL loader.
- Do not implement remote backup, restore-into, scheduler daemon, or parallel execution.
- Do not output token plaintext.
- You may write only explicit /tmp diagnostic/bundle/certificate outputs.
- Do not write inside /mnt/gw/TuShare except read-only checks.
- Do not write inside /mnt/gw/TuShare-backup except read-only checks.
- Commit each completed phase separately.
- Do not commit empty commits.

Baseline first:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

If baseline fails, fix and commit:
fix: restore bundle promotion baseline health

## Phase 1: Bundle regeneration and migration safety

Improve mirror-batch-bundle behavior for existing pre-manifest bundle directories.

Current issue:
- Existing /tmp/tushare-mirror-batch-bundle-202502 lacks bundle_manifest.json and blocks verification.
- User needs a safe way to regenerate the bundle with manifest without manually deleting directories.

Implement safe behavior:
- mirror-batch-bundle should detect existing output directory without bundle_manifest.json.
- Without --overwrite, it should refuse with clear message:
  "existing bundle output is not a valid manifest-bearing bundle; rerun with --overwrite or choose another output path"
- With --overwrite, it should replace the old bundle safely.
- It must never delete mirror root or backup root.
- It must continue to refuse output paths inside mirror root or backup root.
- It must not execute any generated command.

Add tests:
- existing pre-manifest bundle refused without --overwrite
- existing pre-manifest bundle replaced with --overwrite
- valid existing bundle refused without --overwrite
- output inside mirror root blocked
- output inside backup blocked
- no catalog side effects

Commit:
fix: support regenerating pre-manifest batch bundles

## Phase 2: Bundle verification resilience

Improve mirror-batch-bundle-verify diagnostics for invalid/pre-manifest bundles.

If bundle_manifest.json is missing:
- status should be blocked
- blocking_errors should include missing bundle_manifest.json
- recommended_action should say:
  "Regenerate bundle with mirror-batch-bundle --overwrite"
- It should still optionally inspect README.md / commands.sh if present, but not pretend bundle is valid.

If bundle_manifest.json exists but required files missing:
- report missing files clearly
- return non-zero

If commands.sh exists but manifest missing:
- command safety can still be run separately, but verification remains blocked.

Add JSON fields:
- manifest_present
- manifest_valid
- pre_manifest_bundle_detected
- recommended_action

Tests:
- pre-manifest bundle gives clear diagnostics
- valid bundle passes
- invalid manifest blocks
- JSON stable
- read-only

Commit:
fix: clarify batch bundle verification diagnostics

## Phase 3: Weekly/monthly coverage semantics

Investigate and fix January weekly/monthly promotion blockers.

Current reported blocker:
- weekly coverage partial: 4/5, missing 20250131
- monthly coverage partial: 0/1, missing 20250131

But pilot executed:
- weekly jobs=5
- monthly jobs=1

Possible issue:
- coverage matrix may be using naive calendar/month-end dates instead of actual endpoint-covered trade dates.
- January 2025 monthly data may use 20250127 rather than 20250131 due to market holiday/calendar.
- weekly coverage semantics may need to derive expected weekly dates from local data or trade_cal, not naive Fridays.

Required behavior:
- Do not fetch anything.
- Do not backfill.
- Do not change stored data.
- Improve coverage semantics so weekly/monthly pilot coverage is assessed correctly and does not block promotion if the executed pilot dates match planned pilot dates.

Implement one of these safe approaches:

Preferred:
- mirror-coverage-matrix should distinguish:
  - daily_like coverage
  - weekly_monthly pilot planned-date coverage
- weekly/monthly coverage should compare against planned pilot dates or actual planned endpoint dates from mirror run summary when available.
- For promotion checklist, incomplete weekly/monthly should be warning unless the configured pilot plan explicitly expected those missing dates.

Alternative acceptable:
- monthly/weekly coverage remains reported as partial, but promotion checklist should not block low-risk monthly promotion solely on weekly/monthly coverage if daily-like coverage is complete and weekly/monthly are advisory pilot endpoints.

Do not hide the warning. Reclassify correctly:
- daily-like missing coverage -> blocking
- trade_cal dependency missing for target batch -> blocking until planned/fetched
- weekly/monthly ambiguous coverage -> warning unless explicitly required

Add tests:
- daily-like missing blocks
- weekly/monthly partial warning, not blocking, when pilot planned dates were executed
- monthly 20250127 accepted if that was planned/executed date
- JSON includes coverage_class=daily_like|weekly_monthly
- no side effects

Commit:
fix: clarify weekly monthly coverage promotion semantics

## Phase 4: Trade calendar dependency staging for next batch

Current February daily-like endpoints are blocked because local trade_cal does not cover February.

Improve planning/reporting so this blocker becomes an actionable staged plan, not an opaque failure.

Update mirror-batch-plan / promotion checklist / ops report:
- If target date range requires trade_cal and local trade_cal is missing, report:
  - dependency_status=missing
  - dependency_action=fetch_trade_cal_first
  - trade_cal_params={exchange:SSE,start_date:20250201,end_date:20250228}
  - daily_like_status=blocked_until_trade_cal
  - natural_day_fallback=false
- estimated_request_count should separate:
  - dependency_requests
  - executable_after_dependency_requests
  - currently_unblocked_requests
- Do not fetch trade_cal.
- Do not change catalog.

Add tests:
- missing trade_cal creates dependency stage
- daily-like endpoints blocked until dependency
- no natural day fallback
- request estimate separates dependency vs deferred requests
- JSON stable
- no side effects

Commit:
feat: clarify trade calendar dependency staging

## Phase 5: Two-stage batch bundle

Enhance mirror-batch-bundle to support staged execution documentation for February.

Generated bundle should contain:
- README.md
- bundle_manifest.json
- status.json
- audit.json
- review.json
- readiness.json
- batch_plan.json
- stop_policy.json
- operator_checklist.json
- command_safety.json if generated
- commands.sh

commands.sh should show stages, all guarded:

Stage 1:
- fetch/execute trade_cal dependency through mirror-run or planned orchestrator command, guarded with USER_CONFIRMATION_REQUIRED

Stage 2:
- rerun mirror-batch-plan after trade_cal
- execute daily-like endpoints only after trade_cal is local
- validate --no-record
- backup
- restore-check
- review

If current system cannot execute only Stage 1 separately, commands.sh should not invent unsafe commands. It should instead say:
"Run the single monthly mirror-run only after user confirms; orchestrator will fetch trade_cal before daily-like endpoints."

The bundle must make clear:
- no command has been executed
- February batch has not started
- bundle is a plan artifact only

Tests:
- commands.sh contains USER_CONFIRMATION_REQUIRED
- stage sections present
- no unguarded mirror-run
- no token plaintext
- bundle manifest includes all files
- verification passes

Commit:
feat: add staged monthly batch bundle output

## Phase 6: Promotion checklist refinement

Update monthly-promotion-checklist.

It should classify blockers:

Hard blockers:
- backup restore-check failed
- backup possible_mutation true
- token missing
- daily-like current month coverage incomplete for source month if source month is required baseline
- target trade_cal missing and no dependency plan present
- schema incompatible/quarantine
- max_jobs_per_api too high
- output path unsafe
- command bundle invalid

Warnings:
- current artifact covers only pilot month
- weekly/monthly advisory coverage partial
- no remote disaster recovery
- no compaction
- disabled high-risk API families
- target trade_cal missing but dependency plan is present and safe

For February promotion:
- ready_to_promote should be false if it means "execute all daily-like now without trade_cal"
- ready_for_dependency_stage may be true
- ready_for_full_batch_after_dependency may be pending
- output should clearly say next safe action:
  "Regenerate verified bundle; user may confirm the bounded February command that first fetches trade_cal and then proceeds under orchestrator control."

Add JSON fields:
- hard_blockers
- warnings
- dependency_stage
- ready_for_dependency_stage
- ready_for_batch_after_dependency
- next_safe_action

Tests:
- February trade_cal missing -> dependency stage, not vague failure
- weekly/monthly partial -> warning not hard blocker
- mutated backup -> hard blocker
- invalid bundle -> hard blocker
- JSON stable

Commit:
feat: refine monthly promotion checklist stages

## Phase 7: Ops report refinement

Update mirror-ops-report to reflect staged promotion semantics.

It should show:
- overall_status
- hard_blockers
- warnings
- dependency_stage
- next_safe_action
- bundle_status
- promotion_status
- daily_like_coverage
- weekly_monthly_advisory_coverage
- backup_status
- token_hygiene
- schema_status

For current February state, expected:
- not ready for immediate full February daily-like execution until trade_cal dependency is handled
- ready for user-confirmed bounded monthly orchestrator command if all safety checks pass and bundle is verified
- no full mirror

Tests:
- staged dependency state reported
- weekly/monthly advisory warning
- backup blocker propagates
- JSON stable
- no side effects

Commit:
feat: refine mirror ops report for staged promotion

## Phase 8: Certificate and ledger alignment

Update mirror-batch-certificate and mirror-batch-ledger so they can distinguish:
- completed pilot batch
- planned future batch
- dependency-stage planned batch
- not executed batch

Certificate for 202501 should remain completion certificate.
For 202502, if certificate requested before execution, it should either:
- refuse and recommend bundle instead, or
- generate a "plan certificate" clearly marked not executed.

Preferred:
- completion certificate only for completed ranges
- plan certificate is a separate type if implemented
- do not mark future batch complete

Tests:
- January completion certificate works
- February completion certificate before execution blocks
- ledger shows February planned but not executed if bundle exists
- JSON stable

Commit:
feat: align batch ledger and certificate states

## Phase 9: Bundle regeneration durable check

Run read-only/file-output checks on durable environment.

Commands:
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
BUNDLE=/tmp/tushare-mirror-batch-bundle-202502

Regenerate bundle with overwrite:
python3 -m tushare_mirror mirror-batch-bundle --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --start-date 20250201 --end-date 20250228 --max-jobs-per-api 20 --output "$BUNDLE" --overwrite --json

Verify:
python3 -m tushare_mirror mirror-batch-bundle-verify --bundle "$BUNDLE" --json
python3 -m tushare_mirror command-safety-check --file "$BUNDLE/commands.sh" --json
python3 -m tushare_mirror mirror-batch-rehearse --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --json
python3 -m tushare_mirror monthly-promotion-checklist --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --from-month 202501 --to-month 202502 --bundle "$BUNDLE" --json
python3 -m tushare_mirror mirror-ops-report --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --next-start-date 20250201 --next-end-date 20250228 --json

Do not run commands.sh.
Do not execute mirror-run.
Do not fetch.

If durable check exposes code bug, fix, test, commit:
fix: support verified February bundle readiness

## Phase 10: Read-only contract and regression tests

Add focused regression tests for all blockers fixed:
- pre-manifest bundle regeneration
- bundle verify pre-manifest diagnostics
- weekly/monthly advisory coverage
- trade_cal dependency staging
- staged bundle commands
- promotion checklist staged state
- ops report staged state
- certificate/ledger not-executed semantics
- read-only no catalog mutations

Commit:
test: add February bundle readiness regressions

## Phase 11: Runbook update

Update docs/tushare_mirror_phase1_runbook.md.

Add:
February batch readiness blocker resolution

Explain:
- pre-manifest bundle issue
- how to regenerate bundle with --overwrite
- how to verify bundle
- why weekly/monthly pilot coverage may be advisory
- why trade_cal dependency blocks daily-like endpoints
- two-stage mental model
- what is safe to execute only after user confirmation
- what remains prohibited
- exact recommended sequence:
  1. mirror-status
  2. backup-status
  3. mirror-batch-bundle --overwrite
  4. mirror-batch-bundle-verify
  5. command-safety-check
  6. mirror-batch-rehearse
  7. monthly-promotion-checklist
  8. mirror-ops-report
  9. user confirms
  10. only then mirror-run --execute

Commit:
docs: add February batch readiness blocker resolution

## Phase 12: Final durable read-only checks

Run:
git status --short
python3 -m unittest discover tests/tushare_mirror -v
python3 -m compileall tushare_mirror tests/tushare_mirror
git diff --check
python3 scripts/tushare_real_smoke.py --help

Then run durable read-only/file-output checks:
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
BUNDLE=/tmp/tushare-mirror-batch-bundle-202502

python3 -m tushare_mirror mirror-status --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --json
python3 -m tushare_mirror mirror-batch-bundle-verify --bundle "$BUNDLE" --json
python3 -m tushare_mirror command-safety-check --file "$BUNDLE/commands.sh" --json
python3 -m tushare_mirror mirror-batch-rehearse --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --bundle "$BUNDLE" --json
python3 -m tushare_mirror monthly-promotion-checklist --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --from-month 202501 --to-month 202502 --bundle "$BUNDLE" --json
python3 -m tushare_mirror mirror-ops-report --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share --start-date 20250101 --end-date 20250131 --next-start-date 20250201 --next-end-date 20250228 --json

Do not execute real requests.

Final report:
February Bundle Readiness Blocker Resolution Result:
- commits
- bundle regeneration status
- bundle verification status
- command safety status
- rehearsal status
- promotion checklist status
- ops report status
- weekly/monthly coverage classification
- trade_cal dependency staging
- certificate/ledger state
- durable read-only checks
- safety boundaries
- tests
- worktree status
- next user-confirmed action

Stop after Phase 12.
Do not execute mirror-run.
Do not fetch.
