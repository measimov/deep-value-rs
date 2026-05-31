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
from tushare_mirror.planner import JobPlanner
from tushare_mirror.reader import LakeReader
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


PHASE2 = ["weekly", "monthly", "suspend_d", "namechange", "hs_const", "stk_managers", "stk_rewards"]

FIXTURES = {
    "weekly": (["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"], [["000001.SZ", "20250103", 10.0, 12.0, 9.8, 11.1, 10.1, 1.0, 9.9, 1000.0, 11000.0]], {"trade_date": "20250103"}),
    "monthly": (["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"], [["000001.SZ", "20250127", 10.0, 12.0, 9.8, 11.1, 10.1, 1.0, 9.9, 1000.0, 11000.0]], {"trade_date": "20250127"}),
    "suspend_d": (["ts_code", "trade_date", "suspend_timing", "suspend_type"], [["000001.SZ", "20250102", "09:30", "S"]], {"trade_date": "20250102"}),
    "namechange": (["ts_code", "name", "start_date", "end_date", "ann_date", "change_reason"], [["000001.SZ", "Ping An Bank", "20250101", None, "20250102", "rename"]], {"ts_code": "000001.SZ"}),
    "hs_const": (["ts_code", "hs_type", "in_date", "out_date", "is_new"], [["000001.SZ", "SH", "20250102", None, "1"]], {"hs_type": "SH", "is_new": "1"}),
    "stk_managers": (["ts_code", "ann_date", "name", "gender", "lev", "title", "edu", "national", "birthday", "begin_date", "end_date", "resume"], [["000001.SZ", "20250102", "Alice", "F", "1", "CEO", "MBA", "CN", "19800101", "20240101", None, "resume"]], {"ts_code": "000001.SZ"}),
    "stk_rewards": (["ts_code", "ann_date", "end_date", "name", "title", "reward", "hold_vol"], [["000001.SZ", "20250401", "20241231", "Alice", "CEO", 100.0, 10.0]], {"ts_code": "000001.SZ", "end_date": "20241231"}),
}


class ApiFakeClient:
    def __init__(self, fields, items):
        self.fields = fields
        self.items = items

    def query_paginated(self, api_name, params, fields, page_size=None):
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": self.fields, "items": self.items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=self.fields, items=self.items)


class Phase2LowRiskEndpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

    def test_phase2_endpoint_configs_load(self):
        loaded = {row["api_name"] for row in self.catalog.list_endpoints()}
        self.assertTrue(set(PHASE2) <= loaded)
        for api_name in PHASE2:
            cfg = self.catalog.get_endpoint_config(api_name)
            for key in ["volume_class", "partition_template", "supported_params", "default_fields", "probe"]:
                self.assertTrue(cfg.get(key), f"{api_name}:{key}")
            self.assertTrue(cfg["probe"].get("params"), api_name)
            self.assertTrue(cfg["probe"].get("fields"), api_name)
            self.assertTrue(cfg.get("enabled"), api_name)

    def test_phase2_planner_paths_and_no_dry_run_side_effects(self):
        planner = JobPlanner(self.root, self.catalog)
        expectations = {
            "weekly": "api=weekly/year=2025/month=01",
            "monthly": "api=monthly/year=2025/month=01",
            "suspend_d": "api=suspend_d/year=2025/month=01",
            "namechange": "api=namechange/year=2026/month=05",
            "hs_const": "api=hs_const/hs_type=SH/snapshot_date=20260531",
            "stk_managers": "api=stk_managers/year=2026/month=05",
            "stk_rewards": "api=stk_rewards/period_year=2024",
        }
        for api_name in PHASE2:
            fields, items, params = FIXTURES[api_name]
            plan = planner.plan_single_fetch(api_name, params)
            self.assertIn(expectations[api_name], plan.lake_path)
            self.assertIn(f"raw/api={api_name}/", plan.raw_path)
            self.assertEqual(plan.planned_actions, ["request_tushare", "write_raw_jsonl_zst", "write_lake_parquet", "validate", "commit_snapshot"])
        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from jobs").fetchone()[0], 0)
            self.assertEqual(conn.execute("select count(*) from files").fetchone()[0], 0)
            self.assertEqual(conn.execute("select count(*) from snapshots").fetchone()[0], 0)

    def test_phase2_partition_fallback_for_empty_params(self):
        planner = JobPlanner(self.root, self.catalog)
        self.assertEqual(planner.plan_single_fetch("suspend_d", {}).partition_values["event_date"], "20260531")
        self.assertEqual(planner.plan_single_fetch("namechange", {}).partition_values["event_date"], "20260531")
        self.assertEqual(planner.plan_single_fetch("hs_const", {}).partition_values["snapshot_date"], "20260531")
        self.assertEqual(planner.plan_single_fetch("stk_rewards", {}).partition_values["period_date"], "20260531")

    def test_phase2_fake_fetch_validate_and_reader(self):
        store = FileLakeStore(self.root, self.catalog)
        for api_name in PHASE2:
            fields, items, params = FIXTURES[api_name]
            result = store.fetch(api_name, params, ApiFakeClient(fields, items))
            self.assertIsNotNone(result.snapshot_id, api_name)
            ok, _ = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, api_name)
            self.assertTrue(ok, api_name)
            table = LakeReader(self.root, self.catalog).scan_api(api_name)
            self.assertEqual(table.num_rows, len(items), api_name)
            files = self.catalog.files_for_snapshot(result.snapshot_id)
            self.assertEqual(len([f for f in files if f["content_type"] == "raw"]), 1, api_name)
            self.assertEqual(len([f for f in files if f["content_type"] == "lake"]), 1, api_name)
            lake_file = next(f for f in files if f["content_type"] == "lake")
            self.assertTrue(lake_file["schema_id"], api_name)

    def test_suspend_empty_result_commits_empty_snapshot(self):
        fields, _, params = FIXTURES["suspend_d"]
        result = FileLakeStore(self.root, self.catalog).fetch("suspend_d", params, ApiFakeClient(fields, []))
        self.assertIsNotNone(result.snapshot_id)
        self.assertEqual(result.record_count, 0)
        ok, _ = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, "suspend_d")
        self.assertTrue(ok)
        table = LakeReader(self.root, self.catalog).scan_api("suspend_d")
        self.assertEqual(table.num_rows, 0)

    def test_phase2_cli_weekly_smoke_surface(self):
        parsed = self.run_cli("fetch", "--api", "weekly", "--params", '{"trade_date":"20250103"}', "--dry-run", "--json")
        plan = json.loads(parsed.stdout)
        self.assertEqual(plan["api_name"], "weekly")
        self.assertIn("api=weekly/year=2025/month=01", plan["lake_path"])
        self.run_cli("probe", "--api", "weekly", "--help")
        fields, items, params = FIXTURES["weekly"]
        result = FileLakeStore(self.root, self.catalog).fetch("weekly", params, ApiFakeClient(fields, items))
        listed = json.loads(self.run_cli("list-files", "--api", "weekly", "--snapshot", "latest", "--json").stdout)
        self.assertEqual(len(listed), 1)
        snaps = json.loads(self.run_cli("show-snapshots", "--api", "weekly", "--latest", "--json").stdout)
        self.assertEqual(snaps[0]["snapshot_id"], result.snapshot_id)
        validation = json.loads(self.run_cli("validate", "--api", "weekly", "--snapshot", "latest", "--json").stdout)
        self.assertEqual(validation["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
