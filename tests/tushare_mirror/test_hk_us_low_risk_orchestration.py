from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import MirrorOrchestrator, MirrorPlanner


class HKUSMirrorFakeClient:
    token = "fake-token-for-hash-only"

    def __init__(self):
        self.request_calls: list[str] = []
        self.query_calls: list[tuple[str, dict, int | None]] = []

    def request(self, api_name, params, fields=None):
        self.request_calls.append(api_name)
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(api_name, params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.query_calls.append((api_name, dict(params), page_size))
        fields_list = list(fields or [])
        if api_name in {"hk_tradecal", "us_tradecal"}:
            fields_list = ["cal_date", "is_open", "pretrade_date"]
            items = [
                ["20250101", "0", "20241231"],
                ["20250102", "1", "20241231"],
                ["20250103", "1", "20250102"],
                ["20250104", "0", "20250103"],
                ["20250105", "0", "20250103"],
                ["20250106", "1", "20250103"],
                ["20250107", "1", "20250106"],
                ["20250108", "1", "20250107"],
                ["20250109", "1", "20250108"],
                ["20250110", "1", "20250109"],
            ]
        elif api_name == "trade_cal":
            fields_list = ["exchange", "cal_date", "is_open", "pretrade_date"]
            items = [
                ["SSE", "20250101", "0", "20241231"],
                ["SSE", "20250102", "1", "20241231"],
                ["SSE", "20250103", "1", "20250102"],
                ["SSE", "20250106", "1", "20250103"],
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
                values.append("AAPL" if api_name.startswith("us_") else ("00001.HK" if api_name.startswith("hk_") else "000001.SZ"))
            elif field in {"trade_date", "cal_date", "start_date", "end_date", "list_date", "delist_date"}:
                values.append(params.get(field) or params.get("trade_date") or "20250102")
            elif field == "is_open":
                values.append("1")
            elif field == "pretrade_date":
                values.append("20241231")
            elif field == "exchange":
                values.append(params.get("exchange") or ("NAS" if api_name.startswith("us_") else "SSE"))
            elif field in {"name", "fullname", "enname", "cn_spell", "market", "list_status", "isin", "curr_type", "classify", "symbol", "area", "industry", "hs_type", "is_new", "src", "type", "level", "publisher", "index_type", "category", "weight_rule", "desc"}:
                values.append("x")
            else:
                values.append(1.0)
        return values


class HKUSLowRiskOrchestrationTests(unittest.TestCase):
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

    def test_mirror_plan_hk_us_scopes_are_read_only_and_market_calendar_aware(self):
        before = self.counts()
        hk = MirrorPlanner(self.root, self.catalog).plan(scope="hk-low-risk", mode="smoke", max_jobs_per_api=3)
        us = MirrorPlanner(self.root, self.catalog).plan(scope="us-low-risk", mode="pilot", start_date="20250101", end_date="20250110", max_jobs_per_api=20)
        self.assertEqual(self.counts(), before)
        hk_items = {item.endpoint: item for item in hk.items}
        us_items = {item.endpoint: item for item in us.items}
        self.assertEqual(hk_items["hk_tradecal"].category, "calendar_dependency")
        self.assertEqual(hk_items["hk_daily"].blocked_reason, "missing_hk_tradecal_snapshot")
        self.assertEqual(us_items["us_tradecal"].params, {"start_date": "20250101", "end_date": "20250110", "is_open": "1"})
        self.assertEqual(us_items["us_daily"].blocked_reason, "missing_us_tradecal_snapshot")
        self.assertEqual(hk_items["hk_mins"].plan_status, "plan_only_no_execution")
        self.assertEqual(us_items["us_income"].plan_status, "plan_only_no_execution")

    def test_mirror_plan_global_scope_is_explicit_composition(self):
        plan = MirrorPlanner(self.root, self.catalog).plan(scope="global-equity-low-risk", mode="smoke", max_jobs_per_api=3)
        endpoints = {item.endpoint for item in plan.items}
        self.assertIn("daily", endpoints)
        self.assertIn("hk_daily", endpoints)
        self.assertIn("us_daily", endpoints)
        self.assertNotIn("hk_mins", {item.endpoint for item in plan.items if item.will_execute})

    def test_cli_plan_and_run_dry_run_do_not_mutate(self):
        before = self.counts()
        payload = json.loads(self.run_cli("mirror-plan", "--scope", "hk-low-risk", "--mode", "smoke", "--max-jobs-per-api", "3", "--json").stdout)
        self.assertEqual(payload["scope"], "hk-low-risk")
        self.assertIn("hk_daily", {item["endpoint"] for item in payload["items"]})
        dry_run = self.run_cli("mirror-run", "--scope", "us-low-risk", "--mode", "smoke", "--max-jobs-per-api", "3")
        self.assertIn("dry-run only", dry_run.stdout)
        self.assertEqual(self.counts(), before)

    def test_fake_hk_smoke_executes_safe_subset_and_excludes_disabled(self):
        client = HKUSMirrorFakeClient()
        result = MirrorOrchestrator(self.root, self.catalog, client, sleep=lambda _: None).run(
            scope="hk-low-risk",
            mode="smoke",
            max_jobs_per_api=3,
        )
        self.assertEqual(result.status, "succeeded")
        endpoints = {item["endpoint"]: item for item in result.summary["items"]}
        self.assertEqual(endpoints["hk_daily"]["executed_jobs"], 3)
        self.assertEqual(endpoints["hk_mins"]["status"], "excluded")
        called = set(client.request_calls) | {api for api, _, _ in client.query_calls}
        self.assertNotIn("trade_cal", called)
        self.assertIn("hk_tradecal", called)
        self.assertFalse({"hk_mins", "rt_hk_k", "hk_income"} & called)
        self.assertNotIn("fake-token-for-hash-only", json.dumps(result.to_dict()))

    def test_fake_us_pilot_executes_market_calendar_backfills(self):
        client = HKUSMirrorFakeClient()
        result = MirrorOrchestrator(self.root, self.catalog, client, sleep=lambda _: None).run(
            scope="us-low-risk",
            mode="pilot",
            start_date="20250101",
            end_date="20250110",
            max_jobs_per_api=20,
        )
        self.assertEqual(result.status, "succeeded")
        endpoints = {item["endpoint"]: item for item in result.summary["items"]}
        self.assertEqual(endpoints["us_daily"]["executed_jobs"], 7)
        self.assertEqual(endpoints["us_daily_adj"]["executed_jobs"], 7)
        self.assertEqual(endpoints["us_adjfactor"]["executed_jobs"], 7)
        called = set(client.request_calls) | {api for api, _, _ in client.query_calls}
        self.assertNotIn("trade_cal", called)
        self.assertIn("us_tradecal", called)
        self.assertFalse({"us_income", "us_balancesheet"} & called)


if __name__ == "__main__":
    unittest.main()
