from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.errors import MirrorError
from tushare_mirror.planner import JobPlanner
from tushare_mirror.reader import LakeReader
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


HK_US_EXECUTABLE_FIXTURES: dict[str, tuple[list[str], list[list[object]], dict[str, object]]] = {
    "hk_basic": (
        ["ts_code", "name", "fullname", "enname", "market", "list_status", "list_date", "delist_date"],
        [["00001.HK", "HK Co", "Hong Kong Company", "HK Company", "MAIN", "L", "20000101", ""]],
        {"list_status": "L"},
    ),
    "hk_tradecal": (
        ["cal_date", "is_open", "pretrade_date"],
        [["20250102", "1", "20241231"]],
        {"start_date": "20250102", "end_date": "20250102", "is_open": "1"},
    ),
    "hk_daily": (
        ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
        [["00001.HK", "20250102", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0, 1000.0, 10000.0]],
        {"trade_date": "20250102"},
    ),
    "hk_daily_adj": (
        ["ts_code", "trade_date", "close", "open", "high", "low", "pre_close", "change", "pct_change", "vol", "amount", "vwap", "adj_factor", "turnover_ratio", "free_share", "total_share", "free_mv", "total_mv"],
        [["00001.HK", "20250102", 10.2, 10.0, 10.5, 9.8, 10.0, 0.2, 2.0, 1000.0, 10000.0, 10.1, 1.0, 0.1, 100000.0, 200000.0, 1000000.0, 2000000.0]],
        {"trade_date": "20250102"},
    ),
    "hk_adjfactor": (
        ["ts_code", "trade_date", "cum_adjfactor", "close_price"],
        [["00001.HK", "20250102", 1.0, 10.2]],
        {"trade_date": "20250102"},
    ),
    "us_basic": (
        ["ts_code", "name", "enname", "classify", "list_date", "delist_date"],
        [["AAPL", "Apple", "Apple Inc.", "EQ", "19801212", ""]],
        {"classify": "EQ"},
    ),
    "us_tradecal": (
        ["cal_date", "is_open", "pretrade_date"],
        [["20250102", "1", "20241231"]],
        {"start_date": "20250102", "end_date": "20250102", "is_open": "1"},
    ),
    "us_daily": (
        ["ts_code", "trade_date", "close", "open", "high", "low", "pre_close", "change", "pct_change", "vol", "amount", "vwap", "turnover_ratio", "total_mv", "pe", "pb"],
        [["AAPL", "20250102", 100.0, 99.0, 101.0, 98.5, 98.0, 2.0, 2.04, 1000.0, 100000.0, 100.1, 0.2, 3000000.0, 25.0, 8.0]],
        {"trade_date": "20250102"},
    ),
    "us_daily_adj": (
        ["ts_code", "trade_date", "close", "open", "high", "low", "pre_close", "change", "pct_change", "vol", "amount", "vwap", "adj_factor", "turnover_ratio", "free_share", "total_share", "free_mv", "total_mv", "exchange"],
        [["AAPL", "20250102", 100.0, 99.0, 101.0, 98.5, 98.0, 2.0, 2.04, 1000.0, 100000.0, 100.1, 1.0, 0.2, 1000000.0, 2000000.0, 100000000.0, 200000000.0, "NAS"]],
        {"trade_date": "20250102", "exchange": "NAS"},
    ),
    "us_adjfactor": (
        ["ts_code", "trade_date", "exchange", "cum_adjfactor", "close_price"],
        [["AAPL", "20250102", "NAS", 1.0, 100.0]],
        {"trade_date": "20250102", "exchange": "NAS"},
    ),
}

HK_US_PLAN_ONLY_OR_DISABLED = [
    "hk_mins",
    "rt_hk_k",
    "hk_income",
    "hk_balancesheet",
    "hk_cashflow",
    "hk_fina_indicator",
    "us_income",
    "us_balancesheet",
    "us_cashflow",
    "us_fina_indicator",
]


class HKUSFixtureClient:
    def __init__(self, fields: list[str], items: list[list[object]]):
        self.fields = fields
        self.items = items
        self.calls: list[tuple[str, dict[str, object], list[str], int | None]] = []

    def query_paginated(self, api_name, params, fields, page_size=None):
        fields_list = list(fields or [])
        self.calls.append((api_name, dict(params), fields_list, page_size))
        if page_size is None:
            events = [self._event(params, 0, self.items, False)]
            return QueryResult(events=events, fields=self.fields, items=self.items)
        first_items = self.items
        second_items = [list(row) for row in self.items]
        events = [
            self._event({**dict(params), "limit": page_size, "offset": 0}, 0, first_items, True),
            self._event({**dict(params), "limit": page_size, "offset": page_size}, 1, second_items, False),
        ]
        return QueryResult(events=events, fields=self.fields, items=[*first_items, *second_items])

    def _event(self, params: dict[str, object], page_index: int, items: list[list[object]], has_more: bool) -> dict[str, object]:
        return {
            "code": 0,
            "msg": None,
            "data": {"fields": self.fields, "items": items, "has_more": has_more},
            "_http_status": 200,
            "_page_index": page_index,
            "_request_params": dict(params),
        }


class HKUSLowRiskFakeFixtureTests(unittest.TestCase):
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

    def test_executable_hk_us_endpoints_fetch_validate_reader_and_list_files(self):
        store = FileLakeStore(self.root, self.catalog)
        planner = JobPlanner(self.root, self.catalog)
        for api_name, (fields, items, params) in HK_US_EXECUTABLE_FIXTURES.items():
            probe = planner.plan_probe(api_name)
            self.assertTrue(probe.fields, api_name)
            client = HKUSFixtureClient(fields, items)
            result = store.fetch(api_name, params, client)
            expected_rows = len(items) * (2 if client.calls[0][3] is not None else 1)
            self.assertIsNotNone(result.snapshot_id, api_name)
            self.assertEqual(result.record_count, expected_rows, api_name)

            ok, _ = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, api_name)
            self.assertTrue(ok, api_name)
            table = LakeReader(self.root, self.catalog).scan_api(api_name)
            self.assertEqual(table.num_rows, expected_rows, api_name)

            files = self.catalog.files_for_snapshot(str(result.snapshot_id))
            raw_files = [row for row in files if row["content_type"] == "raw"]
            lake_files = [row for row in files if row["content_type"] == "lake"]
            self.assertEqual(len(raw_files), 1, api_name)
            self.assertEqual(len(lake_files), 1, api_name)
            self.assertGreaterEqual(int(raw_files[0]["raw_event_count"]), 1, api_name)
            self.assertTrue(lake_files[0]["schema_id"], api_name)

            listed = json.loads(self.run_cli("list-files", "--api", api_name, "--snapshot", "latest", "--json").stdout)
            self.assertEqual(len(listed), 1, api_name)
            self.assertEqual(listed[0]["api_name"], api_name)

    def test_offset_paginated_fixtures_are_bounded_by_configured_page_size(self):
        store = FileLakeStore(self.root, self.catalog)
        for api_name in ["hk_daily_adj", "us_basic", "us_daily", "us_daily_adj"]:
            fields, items, params = HK_US_EXECUTABLE_FIXTURES[api_name]
            client = HKUSFixtureClient(fields, items)
            result = store.fetch(api_name, params, client)
            self.assertIsNotNone(result.snapshot_id, api_name)
            self.assertIsNotNone(client.calls[0][3], api_name)
            raw_file = next(row for row in self.catalog.files_for_snapshot(str(result.snapshot_id)) if row["content_type"] == "raw")
            self.assertEqual(int(raw_file["raw_event_count"]), 2, api_name)

    def test_plan_only_and_disabled_candidates_do_not_fetch(self):
        for api_name in HK_US_PLAN_ONLY_OR_DISABLED:
            with self.subTest(api_name=api_name):
                with self.assertRaises((KeyError, MirrorError)):
                    FileLakeStore(self.root, self.catalog).fetch(api_name, {}, HKUSFixtureClient([], []), max_attempts=1)


if __name__ == "__main__":
    unittest.main()
