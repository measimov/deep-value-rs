from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import GLOBAL_EQUITY_LOW_RISK_SCOPE, MirrorAutoSyncReporter
from tushare_mirror.mirror import MirrorActiveWriterDetector, MirrorAutoSyncLock


class HKUSAutoSyncGuardrailTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.root.mkdir()
        self.backup.mkdir()
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        with sqlite3.connect(self.catalog.db_path) as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def test_hk_auto_sync_readiness_fields_are_stable_and_read_only(self):
        before = self.counts()
        state = self.base / "hk-state.json"
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=False,
        )
        payload = result.to_dict()
        self.assertEqual(result.status, "planned")
        self.assertEqual(payload["report_version"], "mirror-auto-sync/v1")
        self.assertTrue(payload["execute_supported"])
        self.assertEqual(payload["calendar_api"], "hk_tradecal")
        self.assertEqual(payload["calendar_dependency_status"], "required")
        self.assertEqual(payload["state_path_status"], "safe")
        self.assertEqual(payload["lock_status"]["status"], "not_required_dry_run")
        self.assertEqual(payload["schema_status"], "clear")
        self.assertFalse(payload["token_available"])
        self.assertIn("hk_daily_adj", payload["pagination_summary"])
        self.assertIn("hk_income", payload["excluded_endpoints"])
        self.assertIn("hk_daily", payload["executable_endpoints"])
        self.assertEqual(
            payload["confirmation_phrase"],
            "CONFIRM HK-LOW-RISK AUTO-SYNC 20250101-20250110 MAXJOBS20",
        )
        self.assertFalse(payload["confirmation_reviewed"])
        self.assertTrue(payload["do_not_run_automatically"])
        self.assertFalse(state.exists())
        self.assertEqual(before, self.counts())
        self.assertNotIn("fake-token", json.dumps(payload))

    def test_us_auto_sync_readiness_reports_calendar_and_pagination(self):
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="us-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "us-state.json",
            token_available=True,
        )
        payload = result.to_dict()
        self.assertEqual(payload["calendar_api"], "us_tradecal")
        self.assertEqual(payload["pagination_summary"]["us_daily"], "offset_limit")
        self.assertTrue(payload["token_available"])
        self.assertIn("us_income", payload["excluded_endpoints"])
        self.assertEqual(payload["backup_status"], "exists")

    def test_global_auto_sync_readiness_is_not_executable(self):
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope=GLOBAL_EQUITY_LOW_RISK_SCOPE,
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "global-state.json",
            token_available=True,
        )
        payload = result.to_dict()
        self.assertFalse(payload["execute_supported"])
        self.assertIn("not an auto-sync execution scope", payload["execute_blocked_reason"])
        self.assertEqual(payload["calendar_dependency_status"], "composed_scope_read_only")

    def test_auto_sync_lock_acquire_release_and_second_writer_block(self):
        lock_path = self.base / "auto-sync.lock"
        first = MirrorAutoSyncLock(
            lock_path=lock_path,
            scope="hk-low-risk",
            root=self.root,
            backup=self.backup,
            state_path=self.base / "hk-state.json",
            pid=123,
            hostname="host-a",
            pid_alive=lambda pid: pid == 123,
        )
        acquired = first.acquire()
        self.assertEqual(acquired["status"], "acquired")
        self.assertTrue(lock_path.exists())

        second = MirrorAutoSyncLock(
            lock_path=lock_path,
            scope="us-low-risk",
            root=self.root,
            backup=self.backup,
            state_path=self.base / "us-state.json",
            pid=456,
            hostname="host-a",
            pid_alive=lambda pid: pid == 123,
        )
        blocked = second.acquire()
        self.assertEqual(blocked["status"], "locked")
        self.assertIn("active auto-sync lock exists", blocked["blocking_errors"])

        released = first.release()
        self.assertEqual(released["status"], "released")
        self.assertFalse(lock_path.exists())

    def test_stale_or_unknown_lock_blocks_by_default(self):
        lock_path = self.base / "stale.lock"
        lock_path.write_text(
            json.dumps(
                {
                    "lock_version": "mirror-auto-sync-lock/v1",
                    "scope": "hk-low-risk",
                    "root": str(self.root),
                    "backup": str(self.backup),
                    "pid": 999999,
                    "hostname": "host-a",
                    "started_at": "2026-06-07T00:00:00Z",
                    "command_kind": "mirror-auto-sync",
                    "state_path": str(self.base / "hk-state.json"),
                }
            )
        )
        lock = MirrorAutoSyncLock(
            lock_path=lock_path,
            scope="us-low-risk",
            root=self.root,
            backup=self.backup,
            state_path=self.base / "us-state.json",
            hostname="host-a",
            pid_alive=lambda pid: False,
        )
        status = lock.status()
        self.assertEqual(status["status"], "stale_or_unknown")
        self.assertIn("stale or unknown", status["blocking_errors"][0])

    def test_legacy_active_writer_detection_blocks_same_root_execute_process(self):
        detector = MirrorActiveWriterDetector(
            process_entries=[
                {
                    "pid": 778009,
                    "cmdline": (
                        f"python3 -m tushare_mirror mirror-auto-sync --root {self.root} "
                        "--backup /tmp/backup --scope a-share-low-risk --execute"
                    ),
                },
                {"pid": 1, "cmdline": "python3 -m tushare_mirror mirror-auto-sync --root /other --execute"},
            ],
            now=lambda: 1000.0,
        )
        status = detector.report(root=self.root)
        self.assertEqual(status["status"], "active_writer_possible")
        self.assertTrue(status["active_writer_detected"])
        self.assertEqual(status["process_matches"][0]["pid"], 778009)

    def test_recent_catalog_write_is_reported_as_active_writer_signal(self):
        detector = MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 1)
        status = detector.report(root=self.root)
        self.assertEqual(status["status"], "active_writer_possible")
        self.assertTrue(any(signal["path"].endswith("catalog.sqlite") for signal in status["recent_file_signals"]))

    def test_v1_a_share_state_still_resumes_from_next_start_date(self):
        state = self.base / "a-share-state.json"
        state.write_text(
            json.dumps(
                {
                    "state_version": "mirror-auto-sync-state/v1",
                    "root": str(self.root),
                    "backup": str(self.backup),
                    "scope": "a-share-low-risk",
                    "from_date": "20250101",
                    "to_date": "20250120",
                    "resolved_to_date": "20250120",
                    "window_days": 10,
                    "max_jobs_per_api": 20,
                    "completed_windows": [],
                    "next_start_date": "20250111",
                }
            )
        )
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="a-share-low-risk",
            from_date="20250101",
            to_date="20250120",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=True,
        )
        self.assertTrue(result.resume_from_state)
        self.assertEqual(result.windows[0]["start_date"], "20250111")

    def test_v2_hk_interrupted_window_is_retried_conservatively(self):
        state = self.base / "hk-state.json"
        state.write_text(
            json.dumps(
                {
                    "state_version": "mirror-auto-sync-state/v2",
                    "root": str(self.root),
                    "backup": str(self.backup),
                    "scope": "hk-low-risk",
                    "from_date": "20250101",
                    "to_date": "20250120",
                    "resolved_to_date": "20250120",
                    "window_days": 10,
                    "max_jobs_per_api": 20,
                    "completed_windows": [],
                    "failed_windows": [],
                    "in_progress_window": {
                        "start_date": "20250105",
                        "end_date": "20250114",
                        "started_at": "2026-06-07T00:00:00Z",
                    },
                    "next_start_date": "20250115",
                    "attempt_history": [],
                }
            )
        )
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250120",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=True,
        )
        self.assertTrue(result.resume_from_state)
        self.assertEqual(result.windows[0]["start_date"], "20250105")
        self.assertEqual(result.next_start_date, "20250105")

    def test_malformed_state_blocks_with_clear_error(self):
        state = self.base / "bad-state.json"
        state.write_text(json.dumps({"state_version": "mirror-auto-sync-state/v99"}))
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250120",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=True,
        )
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("unsupported auto-sync state file version" in error for error in result.blocking_errors))
        self.assertEqual(result.windows, [])

    def test_atomic_state_write_replaces_file_without_tmp_residue(self):
        state = self.base / "atomic-state.json"
        payload = {
            "state_version": "mirror-auto-sync-state/v2",
            "scope": "hk-low-risk",
            "completed_windows": [],
            "failed_windows": [],
        }
        MirrorAutoSyncReporter()._atomic_write_state(state, payload)
        self.assertEqual(json.loads(state.read_text())["scope"], "hk-low-risk")
        self.assertFalse((self.base / ".atomic-state.json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
