from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tushare_mirror.a_share_financial import (
    AShareFinancialExecutor,
    AShareFinancialPlanner,
    ASharePitAvailabilityReporter,
)
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.errors import MirrorError
from tushare_mirror.pit import validate_pit_safety
from tushare_mirror.reader import LakeReader
from tushare_mirror.store import FileLakeStore


class AShareFinancialFixtureClient:
    def __init__(self, *, missing_actual_date: bool = False):
        self.missing_actual_date = missing_actual_date
        self.calls: list[tuple[str, dict[str, Any], list[str], int | None]] = []

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls.append((api_name, dict(params), list(fields), page_size))
        period = str(params.get("period") or params.get("end_date"))
        rows = self._rows(api_name, period)
        items = [[row.get(field) for field in fields] for row in rows]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": list(fields), "items": items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=list(fields), items=items)

    def _rows(self, api_name: str, period: str) -> list[dict[str, Any]]:
        base = [
            {
                "ts_code": "000001.SZ",
                "ann_date": "20250315",
                "f_ann_date": "20250315",
                "end_date": period,
                "report_type": "1",
            },
            {
                "ts_code": "600000.SH",
                "ann_date": "20250320",
                "f_ann_date": "20250320",
                "end_date": period,
                "report_type": "1",
            },
        ]
        if api_name == "income_vip":
            return [
                {**row, "basic_eps": 1.2, "total_revenue": 100.0, "revenue": 95.0, "n_income": 10.0, "n_income_attr_p": 9.0, "update_flag": "0"}
                for row in base
            ]
        if api_name == "balancesheet_vip":
            return [
                {**row, "total_share": 1000.0, "total_assets": 500.0, "total_liab": 250.0, "total_hldr_eqy_exc_min_int": 240.0, "update_flag": "0"}
                for row in base
            ]
        if api_name == "cashflow_vip":
            return [{**row, "net_profit": 10.0, "n_cashflow_act": 12.0, "c_free_cashflow": 8.0, "update_flag": "0"} for row in base]
        if api_name == "fina_indicator_vip":
            return [{**row, "eps": 1.2, "bps": 5.0, "roe": 12.0, "debt_to_assets": 50.0, "update_flag": "0"} for row in base]
        if api_name == "disclosure_date":
            actual_dates = ["20250316", None if self.missing_actual_date else "20250321"]
            return [
                {
                    "ts_code": row["ts_code"],
                    "end_date": period,
                    "ann_date": row["ann_date"],
                    "actual_date": actual_dates[idx],
                    "pre_date": row["ann_date"],
                    "modify_date": None,
                }
                for idx, row in enumerate(base)
            ]
        raise AssertionError(f"unexpected api: {api_name}")


class AShareFinancialLakeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        with sqlite3.connect(self.root / "_catalog" / "catalog.sqlite") as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def run_cli(self, *args):
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = "secret-token-should-not-appear"
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=False,
        )

    def test_bundled_a_share_financial_configs_have_strict_pit_metadata(self):
        for api_name in ["income_vip", "balancesheet_vip", "cashflow_vip", "fina_indicator_vip", "disclosure_date"]:
            with self.subTest(api_name=api_name):
                cfg = self.catalog.get_endpoint_config(api_name)
                self.assertEqual(cfg["market"], "a")
                self.assertEqual(cfg["planner_kind"], "period")
                self.assertEqual(cfg["execution_status"], "enabled")
                self.assertEqual(validate_pit_safety(cfg).status, "complete")
                self.assertFalse(cfg["pit_safety"]["allow_without_disclosure_date"])
                self.assertFalse(cfg["pit_safety"]["strategy_safe_default"])

    def test_plan_is_read_only_bounded_and_uses_period_params(self):
        before = self.counts()
        plan = AShareFinancialPlanner(self.root, CatalogStore(self.root, read_only=True)).plan(
            apis="income_vip,disclosure_date",
            periods="2024Q4",
            max_jobs=2,
        )
        after = self.counts()
        self.assertEqual(before, after)
        self.assertFalse(plan.blocked)
        self.assertEqual(plan.planned_jobs, 2)
        by_api = {item.api_name: item for item in plan.items}
        self.assertEqual(by_api["income_vip"].params["period"], "20241231")
        self.assertEqual(by_api["income_vip"].params["report_type"], "1")
        self.assertEqual(by_api["disclosure_date"].params, {"end_date": "20241231"})
        self.assertTrue(by_api["income_vip"].execution_allowed)
        self.assertTrue(by_api["disclosure_date"].execution_allowed)

    def test_plain_fetch_remains_blocked_for_a_share_financial(self):
        with self.assertRaises(MirrorError) as ctx:
            FileLakeStore(self.root, self.catalog).fetch(
                "income_vip",
                {"period": "20241231"},
                AShareFinancialFixtureClient(),
                max_attempts=1,
            )
        self.assertIn("endpoint execution blocked", str(ctx.exception))

    def test_guarded_fake_execution_writes_lake_and_opens_pit_gate(self):
        plan = AShareFinancialPlanner(self.root, self.catalog).plan(
            apis="income_vip,balancesheet_vip,disclosure_date",
            periods="2024Q4",
            max_jobs=3,
        )
        self.assertFalse(plan.blocked)
        result = AShareFinancialExecutor(self.root, self.catalog).execute(
            plan,
            AShareFinancialFixtureClient(),
            max_attempts=1,
        )
        self.assertTrue(result.succeeded, result.errors)
        self.assertEqual(result.executed_jobs, 3)

        report = ASharePitAvailabilityReporter(self.root, self.catalog).report(periods="2024Q4")
        self.assertTrue(report.feature_layer_allowed, report.to_dict())
        self.assertEqual(report.pit_safe_periods, ["20241231"])
        self.assertEqual(report.missing_apis, [])

        income = LakeReader(self.root, self.catalog).scan_api("income_vip")
        disclosure = LakeReader(self.root, self.catalog).scan_api("disclosure_date")
        self.assertEqual(income.num_rows, 2)
        self.assertEqual(disclosure.num_rows, 2)

    def test_missing_actual_date_blocks_pit_feature_gate(self):
        plan = AShareFinancialPlanner(self.root, self.catalog).plan(
            apis="income_vip,balancesheet_vip,disclosure_date",
            periods="2024Q4",
            max_jobs=3,
        )
        result = AShareFinancialExecutor(self.root, self.catalog).execute(
            plan,
            AShareFinancialFixtureClient(missing_actual_date=True),
            max_attempts=1,
        )
        self.assertTrue(result.succeeded, result.errors)

        report = ASharePitAvailabilityReporter(self.root, self.catalog).report(periods="2024Q4")
        self.assertFalse(report.feature_layer_allowed)
        self.assertEqual(report.blocked_periods, ["20241231"])
        self.assertEqual(report.periods_detail[0].blocked_reason, "missing_disclosure_actual_date")
        self.assertEqual(report.periods_detail[0].disclosure_missing_actual_date_rows, 1)

    def test_cli_plan_and_availability_do_not_leak_token_or_write(self):
        before = self.counts()
        plan = self.run_cli(
            "a-share-financial-plan",
            "--apis", "income_vip,disclosure_date",
            "--periods", "2024Q4",
            "--max-jobs", "2",
            "--json",
        )
        availability = self.run_cli(
            "a-share-pit-availability",
            "--periods", "2024Q4",
            "--json",
        )
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(plan.returncode, 0, plan.stderr)
        self.assertEqual(availability.returncode, 1)
        payload = json.loads(plan.stdout)
        self.assertEqual(payload["planned_jobs"], 2)
        self.assertNotIn("secret-token-should-not-appear", plan.stdout + plan.stderr)
        self.assertNotIn("secret-token-should-not-appear", availability.stdout + availability.stderr)


if __name__ == "__main__":
    unittest.main()
