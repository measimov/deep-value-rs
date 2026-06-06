from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.api_infra import ApiInfrastructureReadinessReporter
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import (
    MirrorBatchPlanner,
    MirrorCoverageMatrixReporter,
    MirrorNextBatchReporter,
    MirrorReadinessReporter,
    MirrorReviewer,
    MirrorStatusReporter,
    RequestEstimateReporter,
)


class HKUSLowRiskReportIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.backup = Path(self.tmp.name) / "backup"
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
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def test_readiness_and_status_accept_hk_us_scopes_without_side_effects(self):
        before = self.counts()
        hk = MirrorReadinessReporter().report(root=self.root, backup=self.backup, scope="hk-low-risk")
        us = MirrorStatusReporter().report(root=self.root, backup=self.backup, scope="us-low-risk")
        self.assertEqual(self.counts(), before)
        self.assertEqual(hk.scope, "hk-low-risk")
        self.assertIn("hk_tradecal", json.dumps(hk.to_dict()))
        self.assertEqual(us.scope, "us-low-risk")
        self.assertIn("us_tradecal", json.dumps(us.to_dict()))
        self.assertEqual(us.report_version, "mirror-status/v1")

        hk_review = MirrorReviewer().review(root=self.root, backup=self.backup, scope="hk-low-risk")
        us_review = MirrorReviewer().review(root=self.root, backup=self.backup, scope="us-low-risk")
        self.assertEqual(hk_review.calendar_exchange, "HKEX")
        self.assertEqual(us_review.calendar_exchange, "NASDAQ")

    def test_batch_plan_stages_market_calendar_dependency(self):
        before = self.counts()
        hk_plan = MirrorBatchPlanner(self.root, self.catalog).plan(
            scope="hk-low-risk",
            start_date="20250201",
            end_date="20250228",
            calendar_exchange="SSE",
            max_jobs_per_api=20,
        )
        self.assertEqual(self.counts(), before)
        by_endpoint = {item.endpoint: item for item in hk_plan.endpoint_plans}
        self.assertEqual(hk_plan.trade_cal_params, {"start_date": "20250201", "end_date": "20250228", "is_open": "1"})
        self.assertEqual(hk_plan.dependency_action, "fetch_hk_tradecal_first")
        self.assertEqual(by_endpoint["hk_tradecal"].category, "calendar_dependency")
        self.assertEqual(by_endpoint["hk_daily"].plan_status, "blocked_until_trade_cal")
        self.assertNotIn("weekly", by_endpoint)
        self.assertEqual(by_endpoint["hk_mins"].plan_status, "excluded_no_stock_loop")

    def test_coverage_matrix_and_request_estimate_use_market_calendar(self):
        before = self.counts()
        coverage = MirrorCoverageMatrixReporter().report(
            root=self.root,
            scope="us-low-risk",
            start_date="20250201",
            end_date="20250228",
        )
        estimate = RequestEstimateReporter().report(
            root=self.root,
            scope="us-low-risk",
            start_date="20250201",
            end_date="20250228",
        )
        self.assertEqual(self.counts(), before)
        self.assertEqual(coverage.report_version, "mirror-coverage-matrix/v1")
        self.assertTrue(all(item["coverage_class"] == "daily_like" for item in coverage.items))
        self.assertTrue(all("us_tradecal" in (item.get("reason") or "") for item in coverage.items))
        self.assertEqual(estimate.report_version, "request-estimate/v1")
        self.assertEqual(estimate.planned_trade_cal_requests, 1)
        self.assertEqual(estimate.trade_cal_params, {"start_date": "20250201", "end_date": "20250228", "is_open": "1"})
        self.assertEqual(estimate.dependency_action, "fetch_us_tradecal_first")

    def test_next_batch_and_api_infra_support_new_scopes(self):
        before = self.counts()
        next_batch = MirrorNextBatchReporter().report(root=self.root, scope="hk-low-risk")
        hk_infra = ApiInfrastructureReadinessReporter().report(scope="hk-low-risk")
        us_infra = ApiInfrastructureReadinessReporter().report(scope="us-low-risk")
        global_infra = ApiInfrastructureReadinessReporter().report(scope="global-equity-low-risk")
        self.assertEqual(self.counts(), before)
        self.assertEqual(next_batch.report_version, "mirror-next-batch/v1")
        self.assertEqual((next_batch.required_trade_cal_range or {}).get("calendar_api"), "hk_tradecal")
        self.assertIn("hk_daily", hk_infra.executable_api_names)
        self.assertIn("us_daily", us_infra.executable_api_names)
        self.assertIn("hk_daily", global_infra.executable_api_names)
        self.assertIn("us_daily", global_infra.executable_api_names)

    def test_cli_json_contracts_include_report_versions(self):
        before = self.counts()
        commands = [
            ("mirror-coverage-matrix", "--root", str(self.root), "--scope", "hk-low-risk", "--start-date", "20250201", "--end-date", "20250228", "--json"),
            ("request-estimate", "--root", str(self.root), "--scope", "us-low-risk", "--start-date", "20250201", "--end-date", "20250228", "--json"),
            ("mirror-batch-plan", "--root", str(self.root), "--scope", "hk-low-risk", "--start-date", "20250201", "--end-date", "20250228", "--max-jobs-per-api", "20", "--json"),
            ("api-infra-readiness", "--scope", "global-equity-low-risk", "--json"),
        ]
        payloads = [json.loads(self.run_cli(*command).stdout) for command in commands]
        self.assertEqual(self.counts(), before)
        self.assertEqual(payloads[0]["report_version"], "mirror-coverage-matrix/v1")
        self.assertEqual(payloads[1]["report_version"], "request-estimate/v1")
        self.assertEqual(payloads[2]["scope"], "hk-low-risk")
        self.assertEqual(payloads[2]["dependency_action"], "fetch_hk_tradecal_first")
        self.assertEqual(payloads[3]["scope"], "global-equity-low-risk")


if __name__ == "__main__":
    unittest.main()
