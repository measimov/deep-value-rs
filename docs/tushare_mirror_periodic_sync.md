# TuShare Mirror Periodic Incremental Sync

This runbook configures one user-level systemd timer that updates the existing
A-share, Hong Kong, and US low-risk mirror checkpoints sequentially. It does not
bootstrap a historical pull and does not include financial/PIT, intraday,
realtime, object, or plan-only endpoints.

## Execution contract

The coordinator runs these scopes in this order:

1. `a-share-low-risk`
2. `hk-low-risk`
3. `us-low-risk`

All three scopes share `/mnt/gw/TuShare`, its catalog, and
`/mnt/gw/TuShare-backup`, so they must not execute concurrently. The coordinator
also takes a runtime lock to prevent overlapping scheduled runs.

The following existing checkpoints are mandatory:

```text
/mnt/gw/TuShare-auto-sync-state.json
/mnt/gw/TuShare-hk-low-risk-auto-sync-state.json
/mnt/gw/TuShare-us-low-risk-auto-sync-state.json
```

If any checkpoint is missing, invalid, or points at another scope/root/backup,
the scheduled run stops before making a request. This prevents a deleted state
file from silently restarting a pull at `19900101`.

## Validate without execution

```bash
python3 scripts/tushare_mirror_periodic_sync.py --json
```

Planning mode validates all paths and checkpoint contracts, prints guarded
command previews, and does not call Tushare or mutate state.

## Install the timer

The installer uses the untracked repository `.env` as the systemd environment
file and changes its mode to `600`. It never copies or prints the token.

```bash
scripts/install_tushare_mirror_periodic_sync.sh --start-now
```

The timer runs daily at 09:15 Asia/Shanghai with up to ten minutes of randomized
delay. `Persistent=true` means a missed run is started after the host and user
systemd manager come back. User lingering must remain enabled.

The service uses these existing guardrails:

- 20-calendar-day windows
- at most 20 jobs per API
- three attempts per window
- 60-second retry backoff
- explicit A/HK/US checkpoint resume
- mirror and backup writer locks
- validation, backup, and restore checks performed by `mirror-auto-sync`

## Observe and operate

```bash
systemctl --user status tushare-mirror-periodic-sync.timer
systemctl --user status tushare-mirror-periodic-sync.service
systemctl --user list-timers tushare-mirror-periodic-sync.timer
journalctl --user -u tushare-mirror-periodic-sync.service -n 200 --no-pager
journalctl --user -u tushare-mirror-periodic-sync.service -f
```

Read each checkpoint independently:

```bash
python3 -m tushare_mirror mirror-auto-sync-status \
  --state /mnt/gw/TuShare-auto-sync-state.json --json
python3 -m tushare_mirror mirror-auto-sync-status \
  --state /mnt/gw/TuShare-hk-low-risk-auto-sync-state.json --json
python3 -m tushare_mirror mirror-auto-sync-status \
  --state /mnt/gw/TuShare-us-low-risk-auto-sync-state.json --json
```

The status report keeps the total historical failure count for audit but does
not mark the checkpoint failed when every failed range was later covered by a
successful retry. Check `covered_failed_window_count` and
`uncovered_failed_window_count` when investigating an alert.

## Manual controls

Run one market through the coordinator:

```bash
python3 scripts/tushare_mirror_periodic_sync.py \
  --scope hk-low-risk \
  --execute \
  --confirm-periodic-sync \
  --json
```

Stop the current service without disabling future runs:

```bash
systemctl --user stop tushare-mirror-periodic-sync.service
```

Disable scheduled runs:

```bash
systemctl --user disable --now tushare-mirror-periodic-sync.timer
```

Re-enable them:

```bash
systemctl --user enable --now tushare-mirror-periodic-sync.timer
```

Do not launch a separate `mirror-auto-sync` or `mirror-run` writer while the
service is active. The lower-level locks should block it, but avoiding competing
operators keeps recovery and monitoring unambiguous.
