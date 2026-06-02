from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.code_period_planner import CodePeriodPlanner
from tushare_mirror.endpoints import enrich_endpoint_config
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.period_planner import PeriodPlanner
from tushare_mirror.periods import (
    MAX_PERIODS,
    PeriodRangePlanner,
    compare_periods,
    normalize_period,
    normalize_period_list,
    period_sort_key,
    period_year,
)
from tushare_mirror.pit import pit_metadata_from_config, validate_pit_safety
from tushare_mirror.store import FileLakeStore


class PeriodPlanningUtilityTests(unittest.TestCase):
    def test_parse_supported_quarter_and_end_date_forms(self):
        cases = {
            "2024Q1": "20240331",
            "2024Q2": "20240630",
            "2024Q3": "20240930",
            "2024Q4": "20241231",
            "20240331": "20240331",
            "20240630": "20240630",
            "20240930": "20240930",
            "20241231": "20241231",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(normalize_period(raw), expected)
                self.assertEqual(period_year(raw), 2024)

    def test_invalid_periods_are_rejected(self):
        for raw in ["2024Q5", "20240101", "2024", "Q12024", "", "20241331"]:
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    normalize_period(raw)

    def test_quarterly_range_generation_is_ordered_and_bounded(self):
        plan = PeriodRangePlanner().plan(
            start_period="2024Q1",
            end_period="2024Q4",
            period_frequency="quarterly",
            max_periods=4,
        )
        self.assertEqual(plan.periods, ["20240331", "20240630", "20240930", "20241231"])
        self.assertEqual(plan.total_periods, 4)
        self.assertEqual(plan.planned_periods, 4)
        self.assertFalse(plan.truncated_by_max_periods)

    def test_annual_range_generation_uses_year_end_periods(self):
        plan = PeriodRangePlanner().plan(
            start_period="2022Q1",
            end_period="2024Q4",
            period_frequency="annual",
            max_periods=3,
        )
        self.assertEqual(plan.periods, ["20221231", "20231231", "20241231"])
        self.assertEqual(plan.frequency, "annual")

    def test_max_periods_limit_and_truncation(self):
        plan = PeriodRangePlanner().plan(
            start_period="2020Q1",
            end_period="2026Q4",
            period_frequency="annual",
            max_periods=3,
        )
        self.assertEqual(plan.total_periods, 7)
        self.assertEqual(plan.planned_periods, 3)
        self.assertEqual(plan.periods, ["20201231", "20211231", "20221231"])
        self.assertTrue(plan.truncated_by_max_periods)
        with self.assertRaises(ValueError):
            PeriodRangePlanner().plan(
                start_period="2024Q1",
                end_period="2024Q4",
                max_periods=MAX_PERIODS + 1,
            )

    def test_explicit_period_list_dedupes_and_sorts_stably(self):
        self.assertEqual(
            normalize_period_list("2024Q4,20240331,2024Q2,20240331"),
            ["20240331", "20240630", "20241231"],
        )
        self.assertLess(compare_periods("2024Q1", "2024Q2"), 0)
        self.assertEqual(period_sort_key("2024Q3"), (2024, 3))

    def test_period_plan_json_serialization_is_stable(self):
        payload = PeriodRangePlanner().plan(
            periods="2024Q2,2024Q1",
            max_periods=2,
        ).to_dict()
        rendered = json.dumps(payload, sort_keys=True)
        self.assertEqual(payload["periods"], ["20240331", "20240630"])
        self.assertIn('"frequency": "explicit"', rendered)


class PITSafetyMetadataTests(unittest.TestCase):
    def test_valid_pit_metadata_is_complete_and_json_stable(self):
        cfg = {
            "api_name": "income",
            "endpoint_kind": "financial_statement",
            "pit_safety": {
                "pit_required": True,
                "period_field": "period",
                "announcement_date_fields": ["ann_date", "f_ann_date"],
                "usable_after_field": "ann_date",
                "fallback_usable_after_policy": "block_without_disclosure_date",
                "allow_without_disclosure_date": False,
                "lookahead_risk": True,
                "strategy_safe_default": False,
            },
        }
        result = validate_pit_safety(cfg)
        self.assertEqual(result.status, "complete")
        self.assertTrue(result.pit_required)
        self.assertFalse(result.metadata.allow_without_disclosure_date)
        rendered = json.dumps(result.to_dict(), sort_keys=True)
        self.assertIn('"period_field": "period"', rendered)
        self.assertIn('"usable_after_field": "ann_date"', rendered)

    def test_missing_period_field_blocks(self):
        result = validate_pit_safety(
            {
                "api_name": "income",
                "endpoint_kind": "financial_statement",
                "pit_safety": {
                    "pit_required": True,
                    "announcement_date_fields": ["ann_date"],
                    "usable_after_field": "ann_date",
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("missing_period_field", result.errors)

    def test_missing_announcement_date_blocks(self):
        result = validate_pit_safety(
            {
                "api_name": "fina_indicator",
                "endpoint_kind": "financial_indicator",
                "pit_safety": {
                    "pit_required": True,
                    "period_field": "period",
                    "usable_after_field": "ann_date",
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("missing_announcement_date_fields", result.errors)

    def test_default_allow_without_disclosure_date_is_false(self):
        metadata = pit_metadata_from_config(
            {
                "api_name": "income",
                "endpoint_kind": "financial_statement",
                "pit_safety": {
                    "pit_required": True,
                    "period_field": "period",
                    "announcement_date_fields": ["ann_date"],
                    "usable_after_field": "ann_date",
                },
            }
        )
        self.assertFalse(metadata.allow_without_disclosure_date)
        self.assertFalse(metadata.strategy_safe_default)

    def test_unknown_pit_safety_blocks_financial_endpoint(self):
        result = validate_pit_safety(
            {
                "api_name": "income",
                "endpoint_kind": "financial_statement",
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("unknown_pit_safety", result.errors)
        self.assertIn("missing_period_field", result.errors)
        self.assertIn("missing_announcement_date_fields", result.errors)
        self.assertIn("missing_usable_after_strategy", result.errors)


class PeriodPlannerCliTests(unittest.TestCase):
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

    def seed_stock_basic(self, count: int = 4):
        class FakeClient:
            def query_paginated(self, api_name, params, fields, page_size=None):
                source_fields = ["ts_code", "symbol", "name", "area", "industry", "market", "list_date"]
                base_codes = ["000001.SZ", "000002.SZ", "000004.SZ", "000006.SZ"]
                codes = base_codes + [f"{300000 + idx:06d}.SZ" for idx in range(max(0, count - len(base_codes)))]
                items = [
                    [code, code.split(".")[0], f"name{idx}", "area", "industry", "主板", "20200101"]
                    for idx, code in enumerate(codes[:count])
                ]
                event = {
                    "code": 0,
                    "msg": None,
                    "data": {"fields": source_fields, "items": items, "has_more": False},
                    "_http_status": 200,
                    "_page_index": 0,
                    "_request_params": dict(params),
                }
                return QueryResult(events=[event], fields=source_fields, items=items)

        return FileLakeStore(self.root, self.catalog).fetch("stock_basic", {"list_status": "L"}, FakeClient())

    def upsert_financial_endpoint(self, api_name: str, endpoint_kind: str = "financial_statement"):
        cfg = {
            "api_name": api_name,
            "family": "stock_financial",
            "market": "a",
            "domain": "financial",
            "namespace": "tushare.financial",
            "volume_class": "F1_FINANCIAL",
            "endpoint_kind": endpoint_kind,
            "planner_kind": "code_period_matrix",
            "execution_status": "enabled",
            "partition_template": "period_year",
            "primary_date_field": "period",
            "period_field": "period",
            "supported_params": ["ts_code", "period"],
            "default_fields": ["ts_code", "period", "ann_date"],
            "probe": {"params": {"ts_code": "000001.SZ", "period": "20240331"}, "fields": ["ts_code", "period"]},
            "pit_safety": {
                "pit_required": True,
                "period_field": "period",
                "announcement_date_fields": ["ann_date", "f_ann_date"],
                "usable_after_field": "ann_date",
                "fallback_usable_after_policy": "block_without_disclosure_date",
                "allow_without_disclosure_date": False,
                "lookahead_risk": True,
                "strategy_safe_default": False,
            },
        }
        enriched, table_id, partition_spec_id = enrich_endpoint_config(cfg)
        self.catalog.upsert_endpoint(enriched, table_id, partition_spec_id)

    def run_cli(self, *args, check=False):
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = "secret-token-should-not-appear"
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=check,
        )

    def test_income_period_plan_blocks_on_incomplete_pit_metadata_without_side_effects(self):
        before = self.counts()
        result = self.run_cli(
            "period-plan",
            "--api", "income",
            "--periods", "20240331,20240630",
            "--json",
        )
        after = self.counts()
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(before, after)
        self.assertTrue(payload["blocked"])
        self.assertFalse(payload["execution_allowed"])
        self.assertEqual(payload["periods"], ["20240331", "20240630"])
        self.assertEqual(payload["candidate_jobs"], 2)
        self.assertTrue(payload["pit_required"])
        self.assertEqual(payload["pit_safety_status"], "blocked")
        self.assertIn("pit:unknown_pit_safety", payload["blocking_errors"])
        self.assertNotIn("secret-token-should-not-appear", result.stdout)
        self.assertNotIn("secret-token-should-not-appear", result.stderr)

    def test_fina_indicator_period_range_blocks_but_plans_bounded_periods(self):
        plan = PeriodPlanner(self.root, self.catalog).plan(
            api_name="fina_indicator",
            start_period="2024Q1",
            end_period="2024Q4",
            period_frequency="quarterly",
            max_periods=4,
        )
        self.assertTrue(plan.blocked)
        self.assertEqual(plan.periods, ["20240331", "20240630", "20240930", "20241231"])
        self.assertEqual(plan.period_count, 4)
        self.assertEqual(plan.max_periods, 4)
        self.assertIn("pit:unknown_pit_safety", plan.blocking_errors)

    def test_max_periods_and_invalid_period_are_rejected(self):
        too_many = self.run_cli(
            "period-plan",
            "--api", "income",
            "--start-period", "2024Q1",
            "--end-period", "2024Q4",
            "--max-periods", "21",
            "--json",
        )
        invalid = self.run_cli(
            "period-plan",
            "--api", "income",
            "--periods", "20240101",
            "--json",
        )
        self.assertEqual(too_many.returncode, 1)
        self.assertIn("max_periods exceeds phase limit", too_many.stdout)
        self.assertEqual(invalid.returncode, 1)
        self.assertIn("unsupported period end date", invalid.stdout)

    def test_non_period_endpoint_blocks_clearly(self):
        plan = PeriodPlanner(self.root, self.catalog).plan(
            api_name="daily",
            periods="20240331",
        )
        self.assertTrue(plan.blocked)
        self.assertIn("planner_kind_not_period_compatible:calendar_backfill", plan.blocking_errors)
        self.assertFalse(plan.pit_required)

    def test_income_code_period_plan_with_fake_stock_basic_is_plan_only(self):
        self.seed_stock_basic()
        self.upsert_financial_endpoint("income", "financial_statement")
        before = self.counts()
        result = self.run_cli(
            "code-period-plan",
            "--api", "income",
            "--universe", "a_share_listed",
            "--limit-codes", "3",
            "--periods", "20240331,20240630",
            "--json",
        )
        after = self.counts()
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(before, after)
        self.assertFalse(payload["blocked"])
        self.assertFalse(payload["execution_allowed"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["planned_codes"], 3)
        self.assertEqual(payload["planned_periods"], 2)
        self.assertEqual(payload["planned_jobs"], 6)
        self.assertEqual(payload["items"][0]["params"]["period"], "20240331")
        self.assertTrue(payload["items"][0]["pit_required"])
        self.assertEqual(payload["items"][0]["pit_safety_status"], "complete")
        self.assertFalse(payload["items"][0]["execution_allowed"])

    def test_fina_indicator_code_period_range_with_fake_stock_basic(self):
        self.seed_stock_basic()
        self.upsert_financial_endpoint("fina_indicator", "financial_indicator")
        plan = CodePeriodPlanner(self.root, self.catalog).plan(
            api_name="fina_indicator",
            universe="a_share_listed",
            limit_codes=3,
            start_period="2024Q1",
            end_period="2024Q4",
            period_frequency="quarterly",
            max_periods=4,
        )
        self.assertFalse(plan.blocked)
        self.assertEqual(plan.summary.planned_codes, 3)
        self.assertEqual(plan.summary.planned_periods, 4)
        self.assertEqual(plan.summary.candidate_jobs, 16)
        self.assertEqual(plan.summary.planned_jobs, 12)
        self.assertEqual(sorted({item.period for item in plan.items}), ["20240331", "20240630", "20240930", "20241231"])

    def test_code_period_missing_stock_basic_and_limit_errors_block(self):
        self.upsert_financial_endpoint("income", "financial_statement")
        missing_source = self.run_cli(
            "code-period-plan",
            "--api", "income",
            "--universe", "a_share_listed",
            "--limit-codes", "3",
            "--periods", "20240331",
            "--json",
        )
        too_many_codes = self.run_cli(
            "code-period-plan",
            "--api", "income",
            "--universe", "a_share_listed",
            "--limit-codes", "21",
            "--periods", "20240331",
            "--json",
        )
        too_many_periods = self.run_cli(
            "code-period-plan",
            "--api", "income",
            "--universe", "a_share_listed",
            "--limit-codes", "3",
            "--periods", "20240331",
            "--max-periods", "21",
            "--json",
        )
        self.assertEqual(missing_source.returncode, 1)
        self.assertIn("missing_stock_basic_latest_snapshot", missing_source.stdout)
        self.assertEqual(too_many_codes.returncode, 1)
        self.assertIn("limit_codes_exceeds_phase_limit:20", too_many_codes.stdout)
        self.assertEqual(too_many_periods.returncode, 1)
        self.assertIn("max_periods_exceeds_phase_limit:20", too_many_periods.stdout)

    def test_code_period_candidate_jobs_above_phase_limit_are_truncated(self):
        self.seed_stock_basic(count=30)
        self.upsert_financial_endpoint("income", "financial_statement")
        plan = CodePeriodPlanner(self.root, self.catalog).plan(
            api_name="income",
            universe="a_share_listed",
            limit_codes=20,
            start_period="2020Q1",
            end_period="2026Q4",
            period_frequency="quarterly",
            max_periods=20,
        )
        self.assertFalse(plan.blocked)
        self.assertEqual(plan.summary.candidate_jobs, 840)
        self.assertEqual(plan.summary.planned_codes, 20)
        self.assertEqual(plan.summary.planned_periods, 5)
        self.assertEqual(plan.summary.planned_jobs, 100)
        self.assertTrue(plan.summary.truncated_by_code_limit)
        self.assertTrue(plan.summary.truncated_by_period_limit)
        self.assertTrue(plan.summary.truncated_by_candidate_limit)

    def test_inventory_income_code_period_blocks_on_incomplete_pit_metadata(self):
        self.seed_stock_basic()
        plan = CodePeriodPlanner(self.root, self.catalog).plan(
            api_name="income",
            universe="a_share_listed",
            limit_codes=2,
            periods="20240331",
        )
        self.assertTrue(plan.blocked)
        self.assertIn("pit:unknown_pit_safety", plan.summary.blocking_errors)
        self.assertEqual(plan.items, [])


if __name__ == "__main__":
    unittest.main()
