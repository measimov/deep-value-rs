from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.code_period_planner import CodePeriodPlanner
from tushare_mirror.code_universe import CodeUniverseProvider
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.store import FileLakeStore


class FixtureClient:
    def __init__(self, fields: list[str], items: list[list[object]]):
        self.fields = fields
        self.items = items

    def query_paginated(self, api_name, params, fields, page_size=None):
        return QueryResult(
            events=[
                {
                    "code": 0,
                    "msg": None,
                    "data": {"fields": self.fields, "items": self.items},
                    "_http_status": 200,
                    "_request_params": dict(params),
                }
            ],
            fields=self.fields,
            items=self.items,
        )


class HKUSFinancialCodePeriodGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def seed_hk_basic(self):
        return FileLakeStore(self.root, self.catalog).fetch(
            "hk_basic",
            {"list_status": "L"},
            FixtureClient(
                ["ts_code", "name", "list_status", "list_date"],
                [
                    ["00001.HK", "HK One", "L", "20000101"],
                    ["00002.HK", "HK Two", "L", "20010101"],
                    ["00003.HK", "HK Three", "D", "20020101"],
                ],
            ),
        )

    def seed_us_basic(self):
        return FileLakeStore(self.root, self.catalog).fetch(
            "us_basic",
            {"classify": "EQ"},
            FixtureClient(
                ["ts_code", "name", "classify", "list_date"],
                [
                    ["AAPL", "Apple", "EQ", "19801212"],
                    ["NVDA", "Nvidia", "EQ", "19990122"],
                    ["SPY", "SPDR", "ETF", "19930122"],
                ],
            ),
        )

    def run_cli(self, *args, check: bool = False):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def test_hk_and_us_reference_universes_read_local_snapshots(self):
        self.seed_hk_basic()
        self.seed_us_basic()
        hk = CodeUniverseProvider(self.root, self.catalog).get("hk_listed", limit=10)
        us = CodeUniverseProvider(self.root, self.catalog).get("us_equity", limit=10)
        self.assertFalse(hk.blocked)
        self.assertEqual(hk.source_api, "hk_basic")
        self.assertEqual(hk.codes, ["00001.HK", "00002.HK"])
        self.assertFalse(us.blocked)
        self.assertEqual(us.source_api, "us_basic")
        self.assertEqual(us.codes, ["AAPL", "NVDA"])

    def test_hk_financial_raw_scope_marks_verified_raw_jobs_guarded_allowed(self):
        self.seed_hk_basic()
        plan = CodePeriodPlanner(self.root, self.catalog).plan(
            api_name="hk_income",
            scope="hk-financial-raw",
            universe="hk_listed",
            limit_codes=2,
            periods="20241231",
        )
        payload = plan.to_dict()
        self.assertFalse(plan.blocked)
        self.assertEqual(payload["scope"], "hk-financial-raw")
        self.assertEqual(payload["execution_gate_status"], "ready_for_guarded_command")
        self.assertTrue(payload["raw_financial_scope"])
        self.assertTrue(payload["raw_execution_allowed"])
        self.assertTrue(payload["execution_allowed"])
        self.assertFalse(payload["pit_safe_execution_allowed"])
        self.assertTrue(payload["requires_guarded_command"])
        self.assertEqual(payload["planned_jobs"], 2)
        self.assertTrue(all(item["execution_allowed"] for item in payload["items"]))
        self.assertIn("financial_raw_not_pit_safe:hk_income", payload["warnings"])

    def test_code_period_without_financial_scope_remains_plan_only(self):
        self.seed_hk_basic()
        plan = CodePeriodPlanner(self.root, self.catalog).plan(
            api_name="hk_income",
            universe="hk_listed",
            limit_codes=1,
            periods="20241231",
        )
        payload = plan.to_dict()
        self.assertFalse(plan.blocked)
        self.assertEqual(payload["execution_gate_status"], "not_requested")
        self.assertFalse(payload["execution_allowed"])
        self.assertFalse(payload["raw_execution_allowed"])
        self.assertFalse(payload["items"][0]["execution_allowed"])
        self.assertEqual(payload["items"][0]["blocked_reason"], "guarded_financial_raw_scope_required")

    def test_us_fina_indicator_is_raw_and_pit_safe_candidate(self):
        self.seed_us_basic()
        result = self.run_cli(
            "code-period-plan",
            "--scope", "us-financial-raw",
            "--api", "us_fina_indicator",
            "--universe", "us_equity",
            "--limit-codes", "1",
            "--periods", "20241231",
            "--json",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(payload["execution_gate_status"], "ready_for_guarded_command")
        self.assertTrue(payload["raw_execution_allowed"])
        self.assertTrue(payload["pit_safe_execution_allowed"])
        self.assertEqual(payload["planned_jobs"], 1)

    def test_unverified_us_statement_and_market_mismatch_block(self):
        self.seed_hk_basic()
        self.seed_us_basic()
        unverified = self.run_cli(
            "code-period-plan",
            "--scope", "us-financial-raw",
            "--api", "us_income",
            "--universe", "us_equity",
            "--limit-codes", "1",
            "--periods", "20241231",
            "--json",
        )
        mismatch = self.run_cli(
            "code-period-plan",
            "--scope", "us-financial-raw",
            "--api", "hk_income",
            "--universe", "hk_listed",
            "--limit-codes", "1",
            "--periods", "20241231",
            "--json",
        )
        unverified_payload = json.loads(unverified.stdout)
        mismatch_payload = json.loads(mismatch.stdout)
        self.assertEqual(unverified.returncode, 1)
        self.assertIn("financial_raw_candidate_not_verified:us_income:empty_but_accessible", unverified_payload["blocking_errors"])
        self.assertEqual(mismatch.returncode, 1)
        self.assertIn("financial_raw_market_mismatch:hk_income:hk:us", mismatch_payload["blocking_errors"])

    def test_financial_raw_scope_does_not_bypass_bounded_limits(self):
        self.seed_hk_basic()
        result = self.run_cli(
            "code-period-plan",
            "--scope", "hk-financial-raw",
            "--api", "hk_income",
            "--universe", "hk_listed",
            "--limit-codes", "21",
            "--periods", "20241231",
            "--json",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 1)
        self.assertIn("limit_codes_exceeds_phase_limit:20", payload["blocking_errors"])
        self.assertFalse(payload["execution_allowed"])


if __name__ == "__main__":
    unittest.main()
