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
from tushare_mirror.endpoints import enrich_endpoint_config, load_into_catalog
from tushare_mirror.financial_reports import FinancialCoverageMatrixReporter, FinancialReadinessReporter, FinancialRequestEstimateReporter
from tushare_mirror.store import FileLakeStore


class ReportFixtureClient:
    def __init__(self, fields: list[str], items: list[list[object]]):
        self.fields = fields
        self.items = items

    def query_paginated(self, api_name, params, fields, page_size=None):
        return QueryResult(
            events=[
                {
                    "code": 0,
                    "msg": None,
                    "data": {"fields": self.fields, "items": self.items, "has_more": False},
                    "_http_status": 200,
                    "_request_params": dict(params),
                }
            ],
            fields=self.fields,
            items=self.items,
        )


class HKUSFinancialReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
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
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def seed_us_basic(self):
        FileLakeStore(self.root, self.catalog).fetch(
            "us_basic",
            {"classify": "EQ"},
            ReportFixtureClient(
                ["ts_code", "name", "classify", "list_date"],
                [["AAPL", "Apple", "EQ", "19801212"], ["NVDA", "Nvidia", "EQ", "19990122"]],
            ),
            max_attempts=1,
        )

    def upsert_us_fina_indicator(self):
        fields = [
            "ts_code",
            "end_date",
            "ind_type",
            "security_name_abbr",
            "accounting_standards",
            "notice_date",
            "start_date",
            "std_report_date",
            "financial_date",
            "currency",
            "report_type",
        ]
        cfg = {
            "api_name": "us_fina_indicator",
            "family": "us_financial",
            "market": "us",
            "domain": "financial",
            "namespace": "tushare.us_financial",
            "volume_class": "F1_FINANCIAL_BOUNDED_RAW",
            "endpoint_kind": "financial_indicator",
            "planner_kind": "code_period_matrix",
            "execution_status": "enabled",
            "partition_template": "period_year",
            "primary_date_field": "period",
            "period_field": "period",
            "supported_params": ["ts_code", "period"],
            "default_fields": fields,
            "probe": {"params": {"ts_code": "NVDA", "period": "20241231"}, "fields": fields},
            "pit_safety": {
                "pit_required": True,
                "period_field": "period",
                "announcement_date_fields": ["notice_date"],
                "usable_after_field": "notice_date",
                "fallback_usable_after_policy": "block_without_disclosure_date",
                "allow_without_disclosure_date": False,
                "lookahead_risk": True,
                "strategy_safe_default": False,
            },
        }
        enriched, table_id, partition_spec_id = enrich_endpoint_config(cfg)
        self.catalog.upsert_endpoint(enriched, table_id, partition_spec_id)
        return fields

    def seed_us_fina_indicator_coverage(self):
        self.seed_us_basic()
        fields = self.upsert_us_fina_indicator()
        FileLakeStore(self.root, self.catalog).fetch(
            "us_fina_indicator",
            {"ts_code": "NVDA", "period": "20241231"},
            ReportFixtureClient(fields, [["NVDA", "20241231", "Q", "Nvidia", "US GAAP", "20250226", "20240101", "20241231", "20241231", "USD", "Q4"]]),
            max_attempts=1,
            run_type="financial-raw-fetch",
            scope="us-financial-raw",
            max_codes_required=1,
            requires_pit_handling=False,
        )

    def test_financial_readiness_distinguishes_raw_and_pit_safe_status(self):
        hk = FinancialReadinessReporter().report(scope="hk-financial-raw", root=self.root).to_dict()
        us = FinancialReadinessReporter().report(scope="us-financial-raw", root=self.root).to_dict()
        self.assertEqual(hk["report_version"], "financial-readiness/v1")
        self.assertEqual(hk["raw_ready_count"], 4)
        self.assertEqual(hk["pit_safe_ready_count"], 0)
        self.assertEqual(us["raw_ready_count"], 1)
        self.assertEqual(us["pit_safe_ready"], ["us_fina_indicator"])
        self.assertEqual(us["contract_blocked_count"], 3)
        hk_by_api = {item["api_name"]: item for item in hk["items"]}
        self.assertEqual(hk_by_api["hk_fina_indicator"]["observed_disclosure_fields"], [])
        self.assertEqual(hk_by_api["hk_fina_indicator"]["pit_usable_after_status"], "blocked_without_disclosure_date")
        by_api = {item["api_name"]: item for item in us["items"]}
        self.assertEqual(by_api["us_fina_indicator"]["observed_disclosure_fields"], ["notice_date"])
        self.assertTrue(by_api["us_fina_indicator"]["pit_safe_ready"])
        self.assertTrue(by_api["us_income"]["contract_blocked"])

    def test_financial_request_estimate_is_code_period_based_and_bounded(self):
        report = FinancialRequestEstimateReporter().report(
            scope="us-financial-raw",
            from_period="2024Q4",
            to_period="2024Q4",
            limit_codes=2,
            max_periods=1,
        ).to_dict()
        self.assertEqual(report["report_version"], "financial-request-estimate/v1")
        self.assertEqual(report["planned_periods"], 1)
        self.assertEqual(report["estimated_requests_by_api"]["us_fina_indicator"], 2)
        self.assertEqual(report["estimated_total_requests"], 2)
        self.assertTrue(report["not_a_quota_guarantee"])

        blocked = FinancialRequestEstimateReporter().report(
            scope="hk-financial-raw",
            from_period="2024Q4",
            to_period="2024Q4",
            limit_codes=21,
        ).to_dict()
        self.assertIn("limit_codes_exceeds_guarded_limit:20", blocked["blocking_errors"])

    def test_financial_coverage_matrix_counts_code_period_coverage_and_is_read_only(self):
        self.seed_us_fina_indicator_coverage()
        before = self.counts()
        report = FinancialCoverageMatrixReporter().report(
            root=self.root,
            scope="us-financial-raw",
            periods="20241231",
            limit_codes=2,
            universe="us_equity",
        ).to_dict()
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(report["report_version"], "financial-coverage-matrix/v1")
        self.assertTrue(report["coverage_by_code_period"])
        by_api = {item["api_name"]: item for item in report["items"]}
        self.assertEqual(by_api["us_fina_indicator"]["total_code_periods"], 2)
        self.assertEqual(by_api["us_fina_indicator"]["covered_code_periods"], 1)
        self.assertEqual(by_api["us_fina_indicator"]["missing_code_periods"], 1)
        self.assertEqual(by_api["us_income"]["status"], "not_raw_ready")

    def test_financial_report_clis_are_json_stable_and_read_only(self):
        self.seed_us_fina_indicator_coverage()
        before = self.counts()
        commands = [
            (
                "financial-readiness",
                "--scope", "us-financial-raw",
                "--root", str(self.root),
                "--json",
            ),
            (
                "financial-request-estimate",
                "--scope", "us-financial-raw",
                "--from-period", "2024Q4",
                "--to-period", "2024Q4",
                "--limit-codes", "2",
                "--max-periods", "1",
                "--json",
            ),
            (
                "financial-coverage-matrix",
                "--root", str(self.root),
                "--scope", "us-financial-raw",
                "--periods", "20241231",
                "--limit-codes", "2",
                "--universe", "us_equity",
                "--json",
            ),
        ]
        versions = [
            "financial-readiness/v1",
            "financial-request-estimate/v1",
            "financial-coverage-matrix/v1",
        ]
        for command, version in zip(commands, versions):
            with self.subTest(command=command[0]):
                result = self.run_cli(*command)
                payload = json.loads(result.stdout)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(payload["report_version"], version)
        self.assertEqual(before, self.counts())


if __name__ == "__main__":
    unittest.main()
