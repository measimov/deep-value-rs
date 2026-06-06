from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backfill import BackfillPlanner, DatePlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.planner import JobPlanner
from tushare_mirror.planner_registry import PlannerRegistry, PlannerRegistryRequest
from tushare_mirror.store import FileLakeStore


class CaptureClient:
    def __init__(self):
        self.calls: list[tuple[str, dict, list[str], int | None]] = []

    def query_paginated(self, api_name, params, fields, page_size=None):
        fields_list = list(fields or [])
        self.calls.append((api_name, dict(params), fields_list, page_size))
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
                values.append("AAPL" if api_name.startswith("us_") else "00001.HK")
            elif field in {"trade_date", "cal_date", "start_date", "end_date", "list_date", "delist_date"}:
                values.append(params.get(field) or params.get("trade_date") or "20250102")
            elif field == "is_open":
                values.append(1)
            elif field == "pretrade_date":
                values.append("20241231")
            elif field == "exchange":
                values.append(params.get("exchange") or ("NAS" if api_name.startswith("us_") else "HK"))
            elif field in {"name", "enname", "classify", "list_status"}:
                values.append("x")
            else:
                values.append(1.0)
        return values


class CalendarClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls: list[tuple[str, int | None]] = []

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls.append((api_name, page_size))
        fields_list = list(fields or [])
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": fields_list, "items": self.rows, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=fields_list, items=self.rows)


class HKUSLowRiskPlannerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def test_partition_resolver_for_hk_us_endpoints(self):
        planner = JobPlanner(self.root, self.catalog)
        hk = planner.plan_single_fetch("hk_daily", {"trade_date": "20250102"})
        us = planner.plan_single_fetch("us_daily_adj", {"trade_date": "20250102", "exchange": "NAS"})
        self.assertIn("lake/market=hk/domain=stock/api=hk_daily/year=2025/month=01", hk.lake_path)
        self.assertIn("lake/market=us/domain=stock/api=us_daily_adj/year=2025/month=01", us.lake_path)
        self.assertEqual(hk.partition_values["trade_date"], "20250102")
        self.assertEqual(us.partition_values["trade_date"], "20250102")

    def test_store_respects_pagination_mode(self):
        client = CaptureClient()
        store = FileLakeStore(self.root, self.catalog)
        store.fetch("hk_daily", {"trade_date": "20250102"}, client)
        store.fetch("us_daily_adj", {"trade_date": "20250102", "exchange": "NAS"}, client)
        by_api = {api_name: page_size for api_name, _, _, page_size in client.calls}
        self.assertIsNone(by_api["hk_daily"])
        self.assertEqual(by_api["us_daily_adj"], 8000)

    def test_hk_us_trade_calendar_filters_without_exchange_column(self):
        store = FileLakeStore(self.root, self.catalog)
        store.fetch(
            "hk_tradecal",
            {"start_date": "20250101", "end_date": "20250104", "is_open": "1"},
            CalendarClient([
                ["20250101", "0", "20241231"],
                ["20250102", "1", "20241231"],
                ["20250103", "1", "20250102"],
                ["20250104", "0", "20250103"],
            ]),
        )
        store.fetch(
            "us_tradecal",
            {"start_date": "20250101", "end_date": "20250104", "is_open": "1"},
            CalendarClient([
                ["20250101", 0, "20241231"],
                ["20250102", 1, "20241231"],
                ["20250103", 1, "20250102"],
                ["20250104", 0, "20250103"],
            ]),
        )
        planner = DatePlanner(self.root, self.catalog)
        hk_dates, hk_meta = planner.plan_dates_with_metadata(
            start_date="20250101",
            end_date="20250104",
            trading_days_only=True,
            calendar_exchange="HKEX",
        )
        us_dates, us_meta = planner.plan_dates_with_metadata(
            start_date="20250101",
            end_date="20250104",
            trading_days_only=True,
            calendar_exchange="NASDAQ",
        )
        self.assertEqual(hk_dates, ["20250102", "20250103"])
        self.assertEqual(us_dates, ["20250102", "20250103"])
        self.assertEqual(hk_meta["calendar_api"], "hk_tradecal")
        self.assertEqual(us_meta["calendar_api"], "us_tradecal")

    def test_backfill_and_registry_support_hk_us_daily_like_plans(self):
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(
            "hk_daily_adj",
            ["20250102", "20250103"],
            max_jobs=20,
        )
        self.assertEqual(plan.date_field, "trade_date")
        self.assertEqual(plan.total_candidate_jobs, 2)
        result = PlannerRegistry(self.root, self.catalog).plan(
            PlannerRegistryRequest(
                api_name="us_adjfactor",
                planner_kind="calendar_backfill",
                dates=["20250102"],
                max_jobs=20,
            )
        )
        self.assertEqual(result.status, "supported")
        self.assertEqual(result.planned_jobs, 1)
