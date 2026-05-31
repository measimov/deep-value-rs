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


FIXTURES = {
    "daily": (
        ["ts_code", "trade_date", "close"],
        [["000001.SZ", "20250102", 11.1]],
        {"trade_date": "20250102"},
    ),
    "stock_basic": (
        ["ts_code", "symbol", "name", "area", "industry", "market", "list_date"],
        [["000001.SZ", "000001", "平安银行", "深圳", "银行", "主板", "19910403"]],
        {"list_status": "L"},
    ),
    "trade_cal": (
        ["exchange", "cal_date", "is_open", "pretrade_date"],
        [["SSE", "20250102", 1, "20241231"]],
        {"exchange": "SSE", "start_date": "20250101", "end_date": "20250131"},
    ),
    "adj_factor": (
        ["ts_code", "trade_date", "adj_factor"],
        [["000001.SZ", "20250102", 123.45]],
        {"trade_date": "20250102"},
    ),
    "daily_basic": (
        ["ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"],
        [["000001.SZ", "20250102", 11.1, 1.2, 1.3, 0.9, 8.1, 7.8, 0.8, 2.1, 2.0, 1.1, 1.0, 1000.0, 800.0, 700.0, 11000.0, 9000.0]],
        {"trade_date": "20250102"},
    ),
}


class ApiFakeClient:
    def __init__(self, fields, items):
        self.fields = fields
        self.items = items
        self.calls = 0

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": self.fields, "items": self.items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=self.fields, items=self.items)


class Phase12EndpointTests(unittest.TestCase):
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

    def test_endpoint_configs_load_for_phase12_endpoints(self):
        expected = {"daily", "stock_basic", "trade_cal", "adj_factor", "daily_basic"}
        loaded = {row["api_name"] for row in self.catalog.list_endpoints()}
        self.assertTrue(expected <= loaded)
        for api_name in expected:
            cfg = self.catalog.get_endpoint_config(api_name)
            self.assertTrue(cfg.get("volume_class"), api_name)
            self.assertTrue(cfg.get("default_fields"), api_name)
            probe = cfg.get("probe") or {}
            self.assertTrue(probe.get("params"), api_name)
            self.assertTrue(probe.get("fields"), api_name)

    def test_planner_partitions_and_existing_no_op(self):
        planner = JobPlanner(self.root, self.catalog)
        daily = planner.plan_single_fetch("daily", {"trade_date": "20250102"})
        self.assertIn("year=2025/month=01", daily.lake_path)
        self.assertEqual(daily.partition_values["trade_date"], "20250102")

        stock = planner.plan_single_fetch("stock_basic", {"list_status": "L"})
        self.assertIn("snapshot_date=", stock.lake_path)
        self.assertEqual(stock.partition_values["snapshot_date"], stock.lake_path.split("snapshot_date=")[1].split("/")[0])

        cal = planner.plan_single_fetch("trade_cal", {"exchange": "SSE", "start_date": "20250101", "end_date": "20250131"})
        self.assertIn("domain=calendar/api=trade_cal/exchange=SSE/year=2025", cal.lake_path)
        self.assertEqual(cal.partition_values["exchange"], "SSE")

        adj = planner.plan_single_fetch("adj_factor", {"trade_date": "20250102"})
        self.assertIn("api=adj_factor/year=2025/month=01", adj.lake_path)

        basic = planner.plan_single_fetch("daily_basic", {"trade_date": "20250102"})
        self.assertIn("api=daily_basic/year=2025/month=01", basic.lake_path)

        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from jobs").fetchone()[0], 0)
            self.assertEqual(conn.execute("select count(*) from files").fetchone()[0], 0)
            self.assertEqual(conn.execute("select count(*) from snapshots").fetchone()[0], 0)

        fields, items, params = FIXTURES["stock_basic"]
        first = FileLakeStore(self.root, self.catalog).fetch("stock_basic", params, ApiFakeClient(fields, items))
        self.assertIsNotNone(first.snapshot_id)
        no_op = planner.plan_single_fetch("stock_basic", params)
        self.assertTrue(no_op.existing_active_data)
        self.assertEqual(no_op.planned_actions, ["no_op_existing_active_data"])

    def test_fake_fetch_validate_and_reader_for_low_volume_endpoints(self):
        store = FileLakeStore(self.root, self.catalog)
        for api_name in ["stock_basic", "trade_cal", "adj_factor", "daily_basic"]:
            fields, items, params = FIXTURES[api_name]
            result = store.fetch(api_name, params, ApiFakeClient(fields, items))
            self.assertIsNotNone(result.snapshot_id, api_name)
            self.assertEqual(result.record_count, len(items), api_name)
            ok, _ = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, api_name)
            self.assertTrue(ok, api_name)
            table = LakeReader(self.root, self.catalog).scan_api(api_name)
            self.assertEqual(table.num_rows, len(items), api_name)
            files = self.catalog.files_for_snapshot(result.snapshot_id)
            self.assertEqual(len([f for f in files if f["content_type"] == "raw"]), 1)
            self.assertEqual(len([f for f in files if f["content_type"] == "lake"]), 1)

    def test_stock_basic_add_column_compatible(self):
        store = FileLakeStore(self.root, self.catalog)
        fields, items, params = FIXTURES["stock_basic"]
        store.fetch("stock_basic", params, ApiFakeClient(fields, items))
        added_fields = fields + ["fullname"]
        added_items = [items[0] + ["平安银行股份有限公司"]]
        result = store.fetch("stock_basic", {"list_status": "D"}, ApiFakeClient(added_fields, added_items))
        self.assertIsNotNone(result.snapshot_id)
        table = LakeReader(self.root, self.catalog).scan_api("stock_basic", columns=["ts_code", "fullname"])
        self.assertEqual(table.num_rows, 2)
        self.assertIn("fullname", table.column_names)

    def test_daily_basic_type_change_quarantined(self):
        store = FileLakeStore(self.root, self.catalog)
        fields, items, params = FIXTURES["daily_basic"]
        store.fetch("daily_basic", params, ApiFakeClient(fields, items))
        bad_items = [["000001.SZ", "20250103", "bad-close", 1.2, 1.3, 0.9, 8.1, 7.8, 0.8, 2.1, 2.0, 1.1, 1.0, 1000.0, 800.0, 700.0, 11000.0, 9000.0]]
        bad = store.fetch("daily_basic", {"trade_date": "20250103"}, ApiFakeClient(fields, bad_items))
        self.assertIsNone(bad.snapshot_id)
        self.assertTrue(any((self.root / "_quarantine").rglob("*")))

    def test_trade_cal_empty_result_commits_empty_snapshot(self):
        fields = FIXTURES["trade_cal"][0]
        result = FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20250101", "end_date": "20250101"},
            ApiFakeClient(fields, []),
        )
        self.assertIsNotNone(result.snapshot_id)
        self.assertEqual(result.record_count, 0)
        ok, _ = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, "trade_cal")
        self.assertTrue(ok)

    def test_cli_dry_run_show_permissions_and_list_files_for_stock_basic(self):
        self.catalog.record_probe(
            "stock_basic",
            "hashed-token",
            "accessible",
            {"list_status": "L"},
            ["ts_code"],
            "2099-01-01T00:00:00Z",
            row_count=1,
        )
        parsed = self.run_cli("fetch", "--api", "stock_basic", "--params", '{"list_status":"L"}', "--dry-run", "--json")
        plan = json.loads(parsed.stdout)
        self.assertEqual(plan["api_name"], "stock_basic")
        self.assertEqual(plan["permission_status"], "accessible")
        permissions = json.loads(self.run_cli("show-permissions", "--api", "stock_basic", "--json").stdout)
        self.assertEqual(permissions[0]["status"], "accessible")

        fields, items, params = FIXTURES["stock_basic"]
        FileLakeStore(self.root, self.catalog).fetch("stock_basic", params, ApiFakeClient(fields, items))
        listed = json.loads(self.run_cli("list-files", "--api", "stock_basic", "--snapshot", "latest", "--json").stdout)
        self.assertEqual(len(listed), 1)

    def test_probe_parser_accepts_stock_basic(self):
        from tushare_mirror.cli import build_parser

        args = build_parser().parse_args(["--root", str(self.root), "probe", "--api", "stock_basic"])
        self.assertEqual(args.api, "stock_basic")


if __name__ == "__main__":
    unittest.main()
