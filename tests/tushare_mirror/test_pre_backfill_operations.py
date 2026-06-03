from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backup import BackupExecutor, BackupPlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import MirrorOrchestrator, MirrorStatusReporter
from tushare_mirror.validation import Validator


class PreBackfillFakeClient:
    token = "fake-token-for-hash-only"

    def request(self, api_name, params, fields=None):
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(api_name, params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
        fields_list = list(fields or [])
        if api_name == "trade_cal":
            fields_list = ["exchange", "cal_date", "is_open", "pretrade_date"]
            items = [
                ["SSE", "20250101", 0, "20241231"],
                ["SSE", "20250102", 1, "20241231"],
                ["SSE", "20250103", 1, "20250102"],
                ["SSE", "20250104", 0, "20250103"],
                ["SSE", "20250105", 0, "20250103"],
                ["SSE", "20250106", 1, "20250103"],
                ["SSE", "20250107", 1, "20250106"],
                ["SSE", "20250108", 1, "20250107"],
                ["SSE", "20250109", 1, "20250108"],
                ["SSE", "20250110", 1, "20250109"],
            ]
        else:
            items = [self._row(api_name, params, fields_list)]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": fields_list, "items": items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=fields_list, items=items)

    def _row(self, api_name, params, fields):
        values = []
        for field in fields:
            if field == "ts_code":
                values.append("000001.SZ")
            elif field in {"trade_date", "ann_date", "end_date", "start_date", "cal_date", "in_date", "out_date", "list_date"}:
                values.append(params.get(field) or params.get("trade_date") or params.get("end_date") or "20250102")
            elif field == "exchange":
                values.append(params.get("exchange") or "SSE")
            elif field == "is_open":
                values.append(1)
            elif field == "hs_type":
                values.append(params.get("hs_type") or "SH")
            elif field == "is_new":
                values.append(str(params.get("is_new") or "1"))
            elif field in {"name", "symbol", "area", "industry", "market", "title", "gender", "lev", "edu", "national", "birthday", "begin_date", "resume", "change_reason", "suspend_timing", "suspend_type"}:
                values.append("x")
            else:
                values.append(1.0)
        return values


class MirrorStatusDashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self, root: Path | None = None):
        path = (root or self.root) / "_catalog" / "catalog.sqlite"
        with sqlite3.connect(path) as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def run_cli(self, *args, check=True, token="fake-status-token"):
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = token
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=check,
        )

    def build_pilot(self):
        result = MirrorOrchestrator(self.root, self.catalog, PreBackfillFakeClient(), sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="pilot",
            start_date="20250101",
            end_date="20250110",
            max_jobs_per_api=20,
            backup_target=str(self.backup),
        )
        self.assertEqual(result.status, "succeeded")

    def rebuild_backup(self):
        plan = BackupPlanner(self.root, self.catalog).plan(self.backup)
        BackupExecutor(self.root, self.catalog).backup(plan)

    def test_healthy_fake_mirror_status_is_read_only(self):
        self.build_pilot()
        before = self.counts()
        report = MirrorStatusReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "mirror-status/v1")
        self.assertEqual(report.catalog_status["schema_version"], 2)
        self.assertEqual(report.backup_status, "succeeded")
        self.assertEqual(report.restore_check_status, "succeeded")
        self.assertEqual(report.readiness_status, "warning")
        self.assertTrue(report.ready_for_controlled_full_backfill)
        self.assertGreater(report.latest_snapshot_count, 0)
        self.assertFalse(report.backup_possible_mutation)
        self.assertFalse(report.token_plaintext_found)
        self.assertFalse(report.blocking_errors)
        self.assertEqual({row["api_name"] for row in report.daily_like_coverage_summary}, {"daily", "adj_factor", "daily_basic", "suspend_d"})

    def test_missing_backup_blocks_status(self):
        self.build_pilot()
        missing = self.base / "missing-backup"
        report = MirrorStatusReporter().report(root=self.root, backup=missing, scope="low-risk-a-share")
        self.assertEqual(report.backup_status, "missing")
        self.assertEqual(report.restore_check_status, "not_checked")
        self.assertFalse(report.ready_for_controlled_full_backfill)
        self.assertTrue(any("backup not found" in error for error in report.blocking_errors))

    def test_mutated_backup_blocks_status(self):
        self.build_pilot()
        Validator(self.backup, CatalogStore(self.backup)).validate_latest_snapshots(record=True)
        report = MirrorStatusReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(report.backup_status, "succeeded")
        self.assertEqual(report.restore_check_status, "failed")
        self.assertTrue(report.backup_possible_mutation)
        self.assertFalse(report.ready_for_controlled_full_backfill)

    def test_missing_catalog_is_reported(self):
        self.build_pilot()
        missing_root = self.base / "missing-root"
        report = MirrorStatusReporter().report(root=missing_root, backup=self.backup, scope="low-risk-a-share")
        self.assertFalse(report.catalog_status["present"])
        self.assertFalse(report.ready_for_controlled_full_backfill)
        self.assertTrue(any("catalog not found" in error for error in report.blocking_errors))

    def test_cli_json_contract_and_no_side_effects(self):
        self.build_pilot()
        before = self.counts()
        result = self.run_cli(
            "mirror-status",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "low-risk-a-share",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "root",
            "backup",
            "catalog_status",
            "backup_status",
            "restore_check_status",
            "readiness_status",
            "ready_for_controlled_full_backfill",
            "latest_snapshot_count",
            "enabled_executable_endpoint_count",
            "disabled_inventory_endpoint_count",
            "daily_like_coverage_summary",
            "backup_possible_mutation",
            "token_plaintext_found",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-status/v1")
        self.assertNotIn("fake-status-token", result.stdout)


if __name__ == "__main__":
    unittest.main()
