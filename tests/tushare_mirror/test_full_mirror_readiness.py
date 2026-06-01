from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backup import BackupExecutor, BackupPlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import MirrorBatchPlanner, MirrorOrchestrator, MirrorReadinessReporter, MirrorReviewer
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


class ReadinessFakeClient:
    token = "fake-token-for-hash-only"

    def __init__(self):
        self.request_calls: list[str] = []
        self.query_calls: list[tuple[str, dict]] = []

    def request(self, api_name, params, fields=None):
        self.request_calls.append(api_name)
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(api_name, params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.query_calls.append((api_name, dict(params)))
        fields_list = list(fields or [])
        if api_name == "trade_cal":
            fields_list = ["exchange", "cal_date", "is_open", "pretrade_date"]
            items = [
                ["SSE", "20250101", 0, "20241231"],
                ["SSE", "20250102", 1, "20241231"],
                ["SSE", "20250103", 1, "20250102"],
                ["SSE", "20250104", 0, "20250103"],
                ["SSE", "20250105", 0, "20250103"],
                ["SSE", "20250106", 1, "20250103"],
                ["SSE", "20250107", 1, "20250106"],
                ["SSE", "20250108", 1, "20250107"],
                ["SSE", "20250109", 1, "20250108"],
                ["SSE", "20250110", 1, "20250109"],
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
                values.append("000001.SZ")
            elif field in {"trade_date", "ann_date", "end_date", "start_date", "cal_date", "in_date", "out_date", "list_date"}:
                values.append(params.get(field) or params.get("trade_date") or params.get("end_date") or "20250102")
            elif field == "exchange":
                values.append(params.get("exchange") or "SSE")
            elif field == "is_open":
                values.append(1)
            elif field == "hs_type":
                values.append(params.get("hs_type") or "SH")
            elif field == "is_new":
                values.append(str(params.get("is_new") or "1"))
            elif field in {"name", "symbol", "area", "industry", "market", "title", "gender", "lev", "edu", "national", "birthday", "begin_date", "resume", "change_reason", "suspend_timing", "suspend_type"}:
                values.append("x")
            else:
                values.append(1.0)
        return values


class FebruaryCalendarClient(ReadinessFakeClient):
    def query_paginated(self, api_name, params, fields, page_size=None):
        if api_name != "trade_cal":
            return super().query_paginated(api_name, params, fields, page_size)
        start = params.get("start_date", "20250201")
        end = params.get("end_date", "20250228")
        fields_list = ["exchange", "cal_date", "is_open", "pretrade_date"]
        from datetime import datetime, timedelta

        current = datetime.strptime(start, "%Y%m%d")
        stop = datetime.strptime(end, "%Y%m%d")
        rows = []
        previous_open = "20250127"
        while current <= stop:
            date = current.strftime("%Y%m%d")
            is_open = 1 if current.weekday() < 5 else 0
            rows.append(["SSE", date, is_open, previous_open])
            if is_open:
                previous_open = date
            current += timedelta(days=1)
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": fields_list, "items": rows, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=fields_list, items=rows)


class FullMirrorReadinessReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self, root: Path | None = None):
        path = (root or self.root) / "_catalog" / "catalog.sqlite"
        with sqlite3.connect(path) as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def run_cli(self, *args, check=True, token="fake-review-token"):
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = token
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=check,
        )

    def build_pilot(self, *, max_jobs: int = 20):
        result = MirrorOrchestrator(self.root, self.catalog, ReadinessFakeClient(), sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="pilot",
            start_date="20250101",
            end_date="20250110",
            max_jobs_per_api=max_jobs,
            backup_target=str(self.backup),
        )
        self.assertEqual(result.status, "succeeded")

    def test_ready_fake_pilot_root_review_is_read_only(self):
        self.build_pilot()
        before = self.counts()
        review = MirrorReviewer().review(root=self.root, backup=self.backup, scope="low-risk-a-share", start_date="20250101", end_date="20250110")
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(review.root_status, "existing_catalog")
        self.assertEqual(review.backup_status, "present")
        self.assertEqual(review.validation_status, "succeeded")
        self.assertEqual(review.backup_restore_check["status"], "succeeded")
        self.assertTrue(review.ready_for_next_batch)
        coverage = {row["api_name"]: row for row in review.coverage_summary}
        self.assertEqual(coverage["daily_basic"]["coverage_ratio"], 1.0)
        self.assertFalse(review.token_plaintext_found)

    def test_backup_missing_blocks_review(self):
        self.build_pilot()
        missing = self.base / "missing-backup"
        review = MirrorReviewer().review(root=self.root, backup=missing, scope="low-risk-a-share", start_date="20250101", end_date="20250110")
        self.assertFalse(review.ready_for_next_batch)
        self.assertTrue(any("backup not found" in error for error in review.blocking_errors))

    def test_mutated_backup_is_detected(self):
        self.build_pilot()
        Validator(self.backup, CatalogStore(self.backup)).validate_latest_snapshots(record=True)
        review = MirrorReviewer().review(root=self.root, backup=self.backup, scope="low-risk-a-share", start_date="20250101", end_date="20250110")
        self.assertFalse(review.ready_for_next_batch)
        self.assertEqual(review.backup_catalog_checksum_status, "mismatch")
        self.assertTrue(review.backup_possible_mutation)

    def test_coverage_gap_warns_and_prevents_next_batch_ready(self):
        result = MirrorOrchestrator(self.root, self.catalog, ReadinessFakeClient(), sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="smoke",
            max_jobs_per_api=3,
        )
        self.assertEqual(result.status, "succeeded")
        plan = BackupPlanner(self.root, self.catalog).plan(self.backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        review = MirrorReviewer().review(root=self.root, backup=self.backup, scope="low-risk-a-share", start_date="20250101", end_date="20250110")
        daily_basic = next(row for row in review.coverage_summary if row["api_name"] == "daily_basic")
        self.assertEqual(daily_basic["covered_dates"], 3)
        self.assertEqual(daily_basic["missing_dates"], 4)
        self.assertFalse(review.ready_for_next_batch)
        self.assertTrue(review.warnings)

    def test_cli_json_fields_and_no_side_effects(self):
        self.build_pilot()
        before = self.counts()
        result = self.run_cli(
            "mirror-review",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "low-risk-a-share",
            "--start-date", "20250101",
            "--end-date", "20250110",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "root_status",
            "backup_status",
            "catalog_status",
            "latest_snapshots",
            "endpoint_summary",
            "coverage_summary",
            "backup_restore_check",
            "backup_catalog_checksum_status",
            "backup_possible_mutation",
            "artifact_size",
            "token_plaintext_found",
            "ready_for_next_batch",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertNotIn("fake-review-token", result.stdout)


class FullMirrorReadinessReportTests(FullMirrorReadinessReviewTests):
    def test_ready_pilot_returns_warning_status_and_true_readiness(self):
        self.build_pilot()
        before = self.counts()
        report = MirrorReadinessReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(report.readiness_status, "warning")
        self.assertTrue(report.ready_for_controlled_full_backfill)
        self.assertTrue(report.checks["catalog_opens"]["passed"])
        self.assertTrue(report.checks["trade_cal_latest_exists"]["passed"])
        self.assertTrue(report.checks["restore_check_succeeds"]["passed"])
        self.assertTrue(report.checks["pilot_coverage_complete"]["passed"])
        self.assertTrue(any("not a full mirror" in warning for warning in report.warnings))

    def test_missing_backup_blocks_readiness(self):
        self.build_pilot()
        report = MirrorReadinessReporter().report(root=self.root, backup=self.base / "missing", scope="low-risk-a-share")
        self.assertEqual(report.readiness_status, "blocked")
        self.assertFalse(report.ready_for_controlled_full_backfill)
        self.assertFalse(report.checks["backup_exists"]["passed"])

    def test_mutated_backup_blocks_readiness(self):
        self.build_pilot()
        Validator(self.backup, CatalogStore(self.backup)).validate_latest_snapshots(record=True)
        report = MirrorReadinessReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(report.readiness_status, "blocked")
        self.assertFalse(report.checks["backup_possible_mutation_false"]["passed"])

    def test_missing_trade_cal_blocks_readiness(self):
        self.build_pilot()
        with sqlite3.connect(self.catalog.db_path) as conn:
            conn.execute("update snapshots set status='superseded' where api_name='trade_cal'")
        report = MirrorReadinessReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(report.readiness_status, "blocked")
        self.assertFalse(report.checks["trade_cal_latest_exists"]["passed"])

    def test_incomplete_coverage_blocks_readiness(self):
        result = MirrorOrchestrator(self.root, self.catalog, ReadinessFakeClient(), sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="smoke",
            max_jobs_per_api=3,
        )
        self.assertEqual(result.status, "succeeded")
        plan = BackupPlanner(self.root, self.catalog).plan(self.backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        report = MirrorReadinessReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(report.readiness_status, "blocked")
        self.assertFalse(report.checks["pilot_coverage_complete"]["passed"])

    def test_cli_json_fields_and_no_side_effects_for_readiness(self):
        self.build_pilot()
        before = self.counts()
        result = self.run_cli(
            "mirror-readiness",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "low-risk-a-share",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "readiness_status",
            "ready_for_controlled_full_backfill",
            "checks",
            "warnings",
            "blocking_errors",
            "review",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["readiness_status"], "warning")
        self.assertTrue(payload["ready_for_controlled_full_backfill"])
        self.assertNotIn("fake-review-token", result.stdout)


class FullMirrorBatchPlanTests(FullMirrorReadinessReviewTests):
    def fetch_feb_trade_cal(self):
        result = FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20250201", "end_date": "20250228"},
            FebruaryCalendarClient(),
        )
        self.assertTrue(result.snapshot_id)

    def test_202502_batch_plan_blocks_daily_like_until_trade_cal_range_exists(self):
        self.build_pilot()
        before = self.counts()
        plan = MirrorBatchPlanner(self.root, self.catalog).plan(
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            calendar_exchange="SSE",
            max_jobs_per_api=20,
        )
        self.assertEqual(self.counts(), before)
        self.assertEqual(plan.trade_cal_dependency_status, "missing_range")
        by_endpoint = {item.endpoint: item for item in plan.endpoint_plans}
        self.assertEqual(by_endpoint["trade_cal"].planned_action, "fetch_calendar_range")
        for endpoint in ["daily", "adj_factor", "daily_basic", "suspend_d"]:
            self.assertEqual(by_endpoint[endpoint].plan_status, "blocked_until_trade_cal")
            self.assertEqual(by_endpoint[endpoint].blocked_reason, "missing_trade_cal_range")
        self.assertEqual(by_endpoint["namechange"].plan_status, "excluded_no_stock_loop")

    def test_fake_trade_cal_range_plans_daily_like_by_trading_days(self):
        self.fetch_feb_trade_cal()
        plan = MirrorBatchPlanner(self.root, self.catalog).plan(
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            calendar_exchange="SSE",
            max_jobs_per_api=20,
        )
        self.assertEqual(plan.trade_cal_dependency_status, "covered")
        by_endpoint = {item.endpoint: item for item in plan.endpoint_plans}
        self.assertEqual(by_endpoint["trade_cal"].planned_jobs, 0)
        self.assertEqual(by_endpoint["daily"].total_candidate_jobs, 20)
        self.assertEqual(by_endpoint["daily"].planned_jobs, 20)
        self.assertEqual(by_endpoint["daily"].missing_jobs, 20)
        self.assertFalse(by_endpoint["daily"].truncated)
        self.assertEqual(by_endpoint["weekly"].dates, ["20250207", "20250214", "20250221", "20250228"])
        self.assertEqual(by_endpoint["monthly"].dates, ["20250228"])

    def test_max_jobs_truncation_and_json_output(self):
        self.fetch_feb_trade_cal()
        before = self.counts()
        result = self.run_cli(
            "mirror-batch-plan",
            "--root", str(self.root),
            "--scope", "low-risk-a-share",
            "--start-date", "20250201",
            "--end-date", "20250228",
            "--calendar-exchange", "SSE",
            "--max-jobs-per-api", "2",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "batch_id",
            "scope",
            "start_date",
            "end_date",
            "calendar_exchange",
            "max_jobs_per_api",
            "endpoint_plans",
            "total_candidate_jobs",
            "total_planned_jobs",
            "blocked_endpoints",
            "warnings",
            "estimated_request_count",
            "requires_execute_confirmation",
        ]:
            self.assertIn(key, payload)
        daily = next(item for item in payload["endpoint_plans"] if item["endpoint"] == "daily")
        self.assertTrue(daily["truncated"])
        self.assertEqual(daily["planned_jobs"], 2)

    def test_unknown_scope_is_blocked(self):
        with self.assertRaises(ValueError):
            MirrorBatchPlanner(self.root, self.catalog).plan(
                scope="all",
                start_date="20250201",
                end_date="20250228",
                calendar_exchange="SSE",
                max_jobs_per_api=20,
            )


if __name__ == "__main__":
    unittest.main()
