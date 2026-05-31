from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backup import RestoreChecker
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import MirrorOrchestrator, MirrorPlanner


class MirrorFakeClient:
    token = "fake-token-for-hash-only"

    def __init__(self, deny: set[str] | None = None):
        self.deny = deny or set()
        self.request_calls: list[str] = []
        self.query_calls: list[tuple[str, dict]] = []

    def request(self, api_name, params, fields=None):
        self.request_calls.append(api_name)
        if api_name in self.deny:
            return {"code": -2001, "msg": "权限不足", "data": {"fields": list(fields or []), "items": []}}
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(api_name, params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.query_calls.append((api_name, dict(params)))
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


class Phase30MirrorOrchestratorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
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

    def run_cli(self, *args, check=True):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def test_mirror_plan_smoke_is_read_only_and_blocks_calendar_without_trade_cal(self):
        before = self.counts()
        plan = MirrorPlanner(self.root, self.catalog).plan(scope="low-risk-a-share", mode="smoke", max_jobs_per_api=3)
        self.assertEqual(self.counts(), before)
        self.assertEqual(plan.endpoint_count, 12)
        blocked = {item.endpoint: item.blocked_reason for item in plan.items if item.plan_status == "blocked"}
        self.assertEqual(blocked["daily"], "missing_trade_cal_snapshot")
        self.assertEqual(blocked["daily_basic"], "missing_trade_cal_snapshot")
        payload = json.loads(self.run_cli("mirror-plan", "--scope", "low-risk-a-share", "--mode", "smoke", "--max-jobs-per-api", "3", "--json").stdout)
        self.assertEqual(payload["endpoint_count"], 12)
        self.assertEqual(self.counts(), before)

    def test_mirror_run_without_execute_is_dry_run_only(self):
        before = self.counts()
        result = self.run_cli("mirror-run", "--scope", "low-risk-a-share", "--mode", "smoke", "--max-jobs-per-api", "3")
        self.assertIn("dry-run only", result.stdout)
        self.assertEqual(self.counts(), before)

    def test_unknown_scope_mode_and_max_jobs_are_rejected(self):
        with self.assertRaises(ValueError):
            MirrorPlanner(self.root, self.catalog).plan(scope="all", mode="smoke", max_jobs_per_api=3)
        with self.assertRaises(ValueError):
            MirrorPlanner(self.root, self.catalog).plan(scope="low-risk-a-share", mode="full", max_jobs_per_api=3)
        with self.assertRaises(ValueError):
            MirrorPlanner(self.root, self.catalog).plan(scope="low-risk-a-share", mode="smoke", max_jobs_per_api=4)

    def test_execute_smoke_runs_fixed_scope_and_backup_restore_check(self):
        backup_target = Path(self.tmp.name) / "backup"
        client = MirrorFakeClient()
        result = MirrorOrchestrator(self.root, self.catalog, client, sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="smoke",
            max_jobs_per_api=3,
            backup_target=str(backup_target),
        )
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(len(client.request_calls), 12)
        self.assertLessEqual(max(result.summary["max_jobs_per_api"], 0), 3)
        self.assertEqual(result.summary["backup_status"], "succeeded")
        self.assertEqual(result.summary["restore_check_status"], "succeeded")
        self.assertEqual(RestoreChecker().check(backup_target).status, "succeeded")
        endpoints = {item["endpoint"]: item for item in result.summary["items"]}
        self.assertEqual(endpoints["daily_basic"]["executed_jobs"], 3)
        self.assertEqual(endpoints["weekly"]["executed_jobs"], 2)
        self.assertEqual(endpoints["monthly"]["executed_jobs"], 2)
        self.assertNotIn("fake-token-for-hash-only", json.dumps(result.to_dict()))
        run = self.catalog.get_run(result.run_id)
        self.assertEqual(run["run_type"], "mirror")
        show = self.run_cli("show-run", "--run-id", result.run_id).stdout
        self.assertIn("daily_basic", show)

    def test_permission_denied_non_dependency_is_blocked_without_global_failure(self):
        client = MirrorFakeClient(deny={"hs_const"})
        result = MirrorOrchestrator(self.root, self.catalog, client, sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="smoke",
            max_jobs_per_api=3,
        )
        self.assertEqual(result.status, "succeeded")
        item = next(row for row in result.summary["items"] if row["endpoint"] == "hs_const")
        self.assertEqual(item["status"], "blocked")
        self.assertEqual(item["blocked_reason"], "permission_denied")

    def test_trade_cal_permission_denied_blocks_calendar_and_fails_dependency(self):
        client = MirrorFakeClient(deny={"trade_cal"})
        result = MirrorOrchestrator(self.root, self.catalog, client, sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="smoke",
            max_jobs_per_api=3,
        )
        self.assertEqual(result.status, "failed")
        self.assertTrue(result.summary["critical_dependency_failed"])
        daily = next(row for row in result.summary["items"] if row["endpoint"] == "daily")
        self.assertEqual(daily["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
