from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import GLOBAL_EQUITY_LOW_RISK_SCOPE, MirrorAutoSyncReporter


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


if __name__ == "__main__":
    unittest.main()
