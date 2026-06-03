from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backfill import BackfillExecutor, BackfillPlanner, DatePlanner
from tushare_mirror.backup import BackupExecutor, BackupPlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import BackupStatusReporter, DAILY_LIKE_MIRROR_APIS, MirrorAuditReporter, MirrorBatchBundleReporter, MirrorCoverageMatrixReporter, MirrorNextBatchReporter, MirrorOperatorChecklistReporter, MirrorOrchestrator, MirrorStatusReporter, RequestEstimateReporter, SchemaStatusReporter, StopPolicyReporter
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


class PreBackfillFakeClient:
    token = "fake-token-for-hash-only"

    def request(self, api_name, params, fields=None):
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(api_name, params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
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


class CalendarRangeFakeClient(PreBackfillFakeClient):
    def query_paginated(self, api_name, params, fields, page_size=None):
        if api_name != "trade_cal":
            return super().query_paginated(api_name, params, fields, page_size)
        from datetime import datetime, timedelta

        start = params.get("start_date", "20250101")
        end = params.get("end_date", "20250131")
        current = datetime.strptime(start, "%Y%m%d")
        stop = datetime.strptime(end, "%Y%m%d")
        previous_open = "20241231"
        items = []
        while current <= stop:
            date = current.strftime("%Y%m%d")
            is_open = 1 if current.weekday() < 5 else 0
            items.append(["SSE", date, is_open, previous_open])
            if is_open:
                previous_open = date
            current += timedelta(days=1)
        fields_list = ["exchange", "cal_date", "is_open", "pretrade_date"]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": fields_list, "items": items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=fields_list, items=items)


class PreBackfillOperationsTestCase(unittest.TestCase):
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

    def metadata_counts(self):
        with sqlite3.connect(self.catalog.db_path) as conn:
            return {
                "schemas": conn.execute("select count(*) from schemas").fetchone()[0],
                "schema_changes": conn.execute("select count(*) from schema_changes").fetchone()[0],
                "quarantine": conn.execute("select count(*) from quarantine_files").fetchone()[0],
            }

    def file_count_under(self, path: Path) -> int:
        if not path.exists():
            return 0
        return sum(1 for item in path.rglob("*") if item.is_file())

    def guardrail_counts(self):
        backup_catalog = self.backup / "_catalog" / "catalog.sqlite"
        return {
            "mirror_catalog": self.counts(self.root),
            "backup_catalog": self.counts(self.backup) if backup_catalog.exists() else None,
            "mirror_raw_files": self.file_count_under(self.root / "raw"),
            "mirror_lake_files": self.file_count_under(self.root / "lake"),
            "backup_raw_files": self.file_count_under(self.backup / "raw"),
            "backup_lake_files": self.file_count_under(self.backup / "lake"),
        }

    def run_cli(self, *args, check=True, token="fake-status-token"):
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

    def build_pilot(self):
        result = MirrorOrchestrator(self.root, self.catalog, PreBackfillFakeClient(), sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="pilot",
            start_date="20250101",
            end_date="20250110",
            max_jobs_per_api=20,
            backup_target=str(self.backup),
        )
        self.assertEqual(result.status, "succeeded")

    def rebuild_backup(self):
        plan = BackupPlanner(self.root, self.catalog).plan(self.backup)
        BackupExecutor(self.root, self.catalog).backup(plan)

    def make_failed_catalog_rows(self, suffix: str = "daily"):
        run_id = self.catalog.create_run("fetch")
        job_key = f"job_failed_{suffix}"
        self.catalog.upsert_job(job_key, run_id, "daily", {"trade_date": "20250102"}, ["ts_code", "trade_date"], "running")
        self.catalog.update_job_failed(job_key, "schema drift", "schema_incompatible")
        self.catalog.record_quarantine(run_id, job_key, "daily", "schema_incompatible", f"raw/daily/{suffix}.jsonl.zst", 12, "abc123")
        self.catalog.record_validation(
            None,
            "daily",
            "failed",
            {"files": 1, "failures": 1, "record_count": 0, "raw_event_count": 0},
            [(None, "schema_incompatible", "schema drift")],
        )
        self.catalog.finish_run(run_id, "failed", error_message="schema drift", error_type="schema_incompatible", summary={"api_name": "daily", "failed_jobs": 1})
        return run_id, job_key

    def add_schema_pair(self, *, incompatible: bool = False):
        self.catalog.insert_schema("schema_daily_1", "daily", ["ts_code", "trade_date"], {"ts_code": "string", "trade_date": "string"}, {"ts_code": False, "trade_date": False})
        self.catalog.insert_schema("schema_daily_2", "daily", ["ts_code", "trade_date", "close"], {"ts_code": "string", "trade_date": "string", "close": "string" if incompatible else "float"}, {"ts_code": False, "trade_date": False, "close": True})
        self.catalog.record_schema_change(
            "daily",
            "schema_daily_1",
            "schema_daily_2",
            "incompatible_type_change" if incompatible else "add_column",
            {"added": ["close"]},
            approved=not incompatible,
        )

    def fetch_trade_cal_range(self, start_date: str, end_date: str):
        result = FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": start_date, "end_date": end_date},
            CalendarRangeFakeClient(),
        )
        self.assertTrue(result.snapshot_id)

    def cover_daily_like_range(self, start_date: str, end_date: str, apis: list[str] | None = None):
        self.fetch_trade_cal_range(start_date, end_date)
        dates, calendar = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(
            start_date=start_date,
            end_date=end_date,
            trading_days_only=True,
            calendar_exchange="SSE",
        )
        self.assertTrue(dates)
        for api_name in apis or DAILY_LIKE_MIRROR_APIS:
            plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(
                api_name,
                dates,
                max_jobs=len(dates),
                calendar_metadata=calendar,
            )
            result = BackfillExecutor(self.root, self.catalog).execute(plan, PreBackfillFakeClient())
            self.assertEqual(result.status, "succeeded")

    def cover_date_api(self, api_name: str, dates: list[str]):
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(api_name, dates, max_jobs=len(dates))
        result = BackfillExecutor(self.root, self.catalog).execute(plan, PreBackfillFakeClient())
        self.assertEqual(result.status, "succeeded")

    def cover_january_matrix(self):
        self.cover_daily_like_range("20250101", "20250131")
        self.cover_date_api("weekly", ["20250103", "20250110", "20250117", "20250124", "20250131"])
        self.cover_date_api("monthly", ["20250131"])


class MirrorStatusDashboardTests(PreBackfillOperationsTestCase):
    def test_healthy_fake_mirror_status_is_read_only(self):
        self.build_pilot()
        before = self.counts()
        report = MirrorStatusReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "mirror-status/v1")
        self.assertEqual(report.catalog_status["schema_version"], 2)
        self.assertEqual(report.backup_status, "succeeded")
        self.assertEqual(report.restore_check_status, "succeeded")
        self.assertEqual(report.readiness_status, "warning")
        self.assertTrue(report.ready_for_controlled_full_backfill)
        self.assertGreater(report.latest_snapshot_count, 0)
        self.assertFalse(report.backup_possible_mutation)
        self.assertFalse(report.token_plaintext_found)
        self.assertFalse(report.blocking_errors)
        self.assertEqual({row["api_name"] for row in report.daily_like_coverage_summary}, {"daily", "adj_factor", "daily_basic", "suspend_d"})

    def test_missing_backup_blocks_status(self):
        self.build_pilot()
        missing = self.base / "missing-backup"
        report = MirrorStatusReporter().report(root=self.root, backup=missing, scope="low-risk-a-share")
        self.assertEqual(report.backup_status, "missing")
        self.assertEqual(report.restore_check_status, "not_checked")
        self.assertFalse(report.ready_for_controlled_full_backfill)
        self.assertTrue(any("backup not found" in error for error in report.blocking_errors))

    def test_mutated_backup_blocks_status(self):
        self.build_pilot()
        Validator(self.backup, CatalogStore(self.backup)).validate_latest_snapshots(record=True)
        report = MirrorStatusReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(report.backup_status, "succeeded")
        self.assertEqual(report.restore_check_status, "failed")
        self.assertTrue(report.backup_possible_mutation)
        self.assertFalse(report.ready_for_controlled_full_backfill)

    def test_missing_catalog_is_reported(self):
        self.build_pilot()
        missing_root = self.base / "missing-root"
        report = MirrorStatusReporter().report(root=missing_root, backup=self.backup, scope="low-risk-a-share")
        self.assertFalse(report.catalog_status["present"])
        self.assertFalse(report.ready_for_controlled_full_backfill)
        self.assertTrue(any("catalog not found" in error for error in report.blocking_errors))

    def test_cli_json_contract_and_no_side_effects(self):
        self.build_pilot()
        before = self.counts()
        result = self.run_cli(
            "mirror-status",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "low-risk-a-share",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "root",
            "backup",
            "catalog_status",
            "backup_status",
            "restore_check_status",
            "readiness_status",
            "ready_for_controlled_full_backfill",
            "latest_snapshot_count",
            "enabled_executable_endpoint_count",
            "disabled_inventory_endpoint_count",
            "daily_like_coverage_summary",
            "backup_possible_mutation",
            "token_plaintext_found",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-status/v1")
        self.assertNotIn("fake-status-token", result.stdout)


class MirrorAuditReportTests(PreBackfillOperationsTestCase):
    def test_fake_catalog_audit_with_backup_summary_is_read_only(self):
        self.build_pilot()
        before = self.counts()
        report = MirrorAuditReporter().report(root=self.root, backup=self.backup, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "mirror-audit/v1")
        self.assertEqual(report.run_count_by_type["mirror"], 1)
        self.assertGreater(report.succeeded_run_count, 0)
        self.assertEqual(report.failed_run_count, 0)
        self.assertGreater(report.job_count_by_status["done"], 0)
        self.assertGreater(report.snapshot_count_by_api["daily"], 0)
        self.assertEqual(report.backup_summary["restore_check_status"], "succeeded")
        self.assertFalse(report.blocking_errors)

    def test_failed_job_quarantine_and_validation_failures_appear(self):
        _, job_key = self.make_failed_catalog_rows()
        before = self.counts()
        report = MirrorAuditReporter().report(root=self.root, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.run_count_by_type["fetch"], 1)
        self.assertEqual(report.failed_run_count, 1)
        self.assertEqual(report.job_count_by_status["failed"], 1)
        self.assertEqual(report.validation_status_counts["failed"], 1)
        self.assertEqual(report.quarantined_count, 1)
        self.assertEqual(report.failed_jobs[0]["job_key"], job_key)
        self.assertEqual(report.failed_jobs[0]["last_error_type"], "schema_incompatible")

    def test_since_and_limit_are_stable(self):
        self.make_failed_catalog_rows("one")
        self.make_failed_catalog_rows("two")
        report = MirrorAuditReporter().report(root=self.root, scope="low-risk-a-share", limit=1)
        self.assertEqual(len(report.failed_jobs), 1)
        future = MirrorAuditReporter().report(root=self.root, scope="low-risk-a-share", since="29990101")
        self.assertEqual(future.run_count_by_type, {})
        self.assertEqual(future.failed_jobs, [])

    def test_cli_json_contract_and_no_side_effects_for_audit(self):
        self.make_failed_catalog_rows()
        before = self.counts()
        result = self.run_cli(
            "mirror-audit",
            "--root", str(self.root),
            "--scope", "low-risk-a-share",
            "--limit", "1",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "root",
            "backup",
            "scope",
            "since",
            "limit",
            "run_count_by_type",
            "succeeded_run_count",
            "failed_run_count",
            "job_count_by_status",
            "validation_status_counts",
            "snapshot_count_by_api",
            "failed_jobs",
            "quarantined_count",
            "latest_run_id",
            "backup_summary",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-audit/v1")
        self.assertEqual(len(payload["failed_jobs"]), 1)

    def test_missing_catalog_blocks_audit_without_creating_catalog(self):
        missing_root = self.base / "missing-root"
        report = MirrorAuditReporter().report(root=missing_root, scope="low-risk-a-share")
        self.assertFalse((missing_root / "_catalog" / "catalog.sqlite").exists())
        self.assertTrue(any("catalog not found" in error for error in report.blocking_errors))


class MirrorNextBatchRecommenderTests(PreBackfillOperationsTestCase):
    def test_no_coverage_recommends_january(self):
        before = self.counts()
        report = MirrorNextBatchReporter().report(root=self.root, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.current_completed_months, [])
        self.assertIsNone(report.last_complete_month)
        self.assertEqual(report.recommended_next_start_date, "20250101")
        self.assertEqual(report.recommended_next_end_date, "20250131")
        self.assertIn("no completed month", report.reason)
        self.assertEqual(report.required_trade_cal_range["status"], "missing_snapshot")
        self.assertFalse(report.blocking_errors)

    def test_january_covered_recommends_february(self):
        self.cover_daily_like_range("20250101", "20250131")
        before = self.counts()
        report = MirrorNextBatchReporter().report(root=self.root, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.current_completed_months, ["202501"])
        self.assertEqual(report.last_complete_month, "202501")
        self.assertEqual(report.recommended_next_start_date, "20250201")
        self.assertEqual(report.recommended_next_end_date, "20250228")
        self.assertIn("latest complete month is 202501", report.reason)
        self.assertIn("USER_CONFIRMATION_REQUIRED", json.dumps(report.execute_command_preview))

    def test_february_covered_recommends_march(self):
        self.cover_daily_like_range("20250101", "20250131")
        self.cover_daily_like_range("20250201", "20250228")
        before = self.counts()
        report = MirrorNextBatchReporter().report(root=self.root, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.current_completed_months, ["202501", "202502"])
        self.assertEqual(report.last_complete_month, "202502")
        self.assertEqual(report.recommended_next_start_date, "20250301")
        self.assertEqual(report.recommended_next_end_date, "20250331")

    def test_partial_coverage_recommends_missing_month(self):
        self.cover_daily_like_range("20250101", "20250131", apis=["daily"])
        before = self.counts()
        report = MirrorNextBatchReporter().report(root=self.root, scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.current_completed_months, [])
        self.assertEqual(report.recommended_next_start_date, "20250101")
        self.assertEqual(report.recommended_next_end_date, "20250131")
        self.assertIn("partial coverage", report.reason)
        self.assertEqual(report.required_trade_cal_range["status"], "covered")

    def test_cli_json_contract_and_no_side_effects_for_next_batch(self):
        self.cover_daily_like_range("20250101", "20250131")
        before = self.counts()
        result = self.run_cli(
            "mirror-next-batch",
            "--root", str(self.root),
            "--scope", "low-risk-a-share",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "root",
            "scope",
            "current_completed_months",
            "last_complete_month",
            "recommended_next_start_date",
            "recommended_next_end_date",
            "reason",
            "required_trade_cal_range",
            "estimated_request_count",
            "recommended_max_jobs_per_api",
            "plan_command_preview",
            "execute_command_preview",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-next-batch/v1")
        self.assertEqual(payload["recommended_next_start_date"], "20250201")
        self.assertIn("USER_CONFIRMATION_REQUIRED", json.dumps(payload["execute_command_preview"]))


class MirrorCoverageMatrixTests(PreBackfillOperationsTestCase):
    def test_complete_coverage_matrix_is_read_only(self):
        self.cover_january_matrix()
        before = self.counts()
        report = MirrorCoverageMatrixReporter().report(root=self.root, scope="low-risk-a-share", start_date="20250101", end_date="20250131")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "mirror-coverage-matrix/v1")
        self.assertEqual({item["api"] for item in report.items}, {"daily", "adj_factor", "daily_basic", "suspend_d", "weekly", "monthly"})
        self.assertTrue(all(item["status"] == "complete" for item in report.items))
        self.assertTrue(all(item["missing_dates"] == 0 for item in report.items))

    def test_partial_coverage_matrix_reports_missing_dates(self):
        self.cover_daily_like_range("20250101", "20250131", apis=["daily"])
        report = MirrorCoverageMatrixReporter().report(root=self.root, scope="low-risk-a-share", start_date="20250101", end_date="20250131")
        by_api = {item["api"]: item for item in report.items}
        self.assertEqual(by_api["daily"]["status"], "complete")
        self.assertEqual(by_api["adj_factor"]["status"], "partial")
        self.assertGreater(by_api["adj_factor"]["missing_dates"], 0)
        self.assertTrue(by_api["adj_factor"]["missing_date_sample"])
        self.assertEqual(by_api["weekly"]["status"], "partial")

    def test_missing_trade_cal_blocks_daily_like_rows(self):
        before = self.counts()
        report = MirrorCoverageMatrixReporter().report(root=self.root, scope="low-risk-a-share", start_date="20250101", end_date="20250131")
        self.assertEqual(self.counts(), before)
        by_api = {item["api"]: item for item in report.items}
        for api_name in DAILY_LIKE_MIRROR_APIS:
            self.assertEqual(by_api[api_name]["status"], "blocked_missing_trade_cal")
        self.assertEqual(by_api["weekly"]["status"], "partial")

    def test_cli_json_contract_and_no_side_effects_for_coverage_matrix(self):
        self.cover_january_matrix()
        before = self.counts()
        result = self.run_cli(
            "mirror-coverage-matrix",
            "--root", str(self.root),
            "--scope", "low-risk-a-share",
            "--start-date", "20250101",
            "--end-date", "20250131",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "root",
            "scope",
            "start_date",
            "end_date",
            "items",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-coverage-matrix/v1")
        self.assertEqual(len(payload["items"]), 6)


class RequestEstimateReportTests(PreBackfillOperationsTestCase):
    def test_january_estimate_uses_local_trade_cal(self):
        self.fetch_trade_cal_range("20250101", "20250131")
        before = self.counts()
        report = RequestEstimateReporter().report(root=self.root, scope="low-risk-a-share", start_date="20250101", end_date="20250131")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "request-estimate/v1")
        self.assertEqual(report.planned_trade_cal_requests, 0)
        self.assertGreater(report.daily_like_requests, 0)
        self.assertGreater(report.weekly_monthly_requests, 0)
        self.assertTrue(report.not_a_quota_guarantee)
        self.assertIn(report.risk_level, {"low", "moderate", "high"})

    def test_missing_trade_cal_range_warns_and_defers_daily_like(self):
        self.build_pilot()
        report = RequestEstimateReporter().report(root=self.root, scope="low-risk-a-share", start_date="20250201", end_date="20250228")
        self.assertEqual(report.planned_trade_cal_requests, 1)
        self.assertEqual(report.daily_like_requests, 0)
        self.assertTrue(any("trade_cal range" in warning for warning in report.warnings))
        self.assertTrue(report.not_a_quota_guarantee)

    def test_cli_json_contract_and_no_side_effects_for_request_estimate(self):
        self.fetch_trade_cal_range("20250101", "20250131")
        before = self.counts()
        result = self.run_cli(
            "request-estimate",
            "--scope", "low-risk-a-share",
            "--start-date", "20250101",
            "--end-date", "20250131",
            "--root", str(self.root),
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "estimated_requests_by_api",
            "estimated_total_requests",
            "planned_trade_cal_requests",
            "daily_like_requests",
            "weekly_monthly_requests",
            "reference_refresh_requests",
            "risk_level",
            "assumptions",
            "warnings",
            "not_a_quota_guarantee",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "request-estimate/v1")
        self.assertTrue(payload["not_a_quota_guarantee"])


class MirrorBatchBundleTests(PreBackfillOperationsTestCase):
    def create_bundle(self, output: Path, *, overwrite: bool = False):
        return MirrorBatchBundleReporter().create(
            root=self.root,
            backup=self.backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
            output=output,
            overwrite=overwrite,
        )

    def test_bundle_created_outside_roots_and_catalog_unchanged(self):
        self.build_pilot()
        output = self.base / "bundle-202502"
        before = self.counts()
        result = self.create_bundle(output)
        self.assertEqual(self.counts(), before)
        self.assertEqual(result.status, "created")
        self.assertFalse(result.blocking_errors)
        expected = {
            "README.md",
            "batch_plan.json",
            "readiness.json",
            "review.json",
            "status.json",
            "audit.json",
            "stop_policy.json",
            "commands.sh",
        }
        self.assertEqual(set(result.files), expected)
        self.assertEqual({path.name for path in output.iterdir()}, expected)
        batch_plan = json.loads((output / "batch_plan.json").read_text())
        self.assertEqual(batch_plan["start_date"], "20250201")
        commands = (output / "commands.sh").read_text()
        self.assertIn("USER_CONFIRMATION_REQUIRED", commands)
        self.assertIn("# python3 -m tushare_mirror mirror-run", commands)

    def test_existing_output_refused_without_overwrite(self):
        self.build_pilot()
        output = self.base / "existing-bundle"
        output.mkdir()
        before = self.counts()
        result = self.create_bundle(output)
        self.assertEqual(self.counts(), before)
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("already exists" in error for error in result.blocking_errors))
        self.assertEqual(list(output.iterdir()), [])

    def test_output_inside_mirror_root_blocked(self):
        output = self.root / "bundle"
        result = self.create_bundle(output)
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("inside mirror root" in error for error in result.blocking_errors))
        self.assertFalse(output.exists())

    def test_output_inside_backup_root_blocked(self):
        output = self.backup / "bundle"
        result = self.create_bundle(output)
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("inside backup root" in error for error in result.blocking_errors))
        self.assertFalse(output.exists())

    def test_cli_json_contract_and_no_side_effects_for_bundle(self):
        self.build_pilot()
        output = self.base / "cli-bundle"
        before = self.counts()
        result = self.run_cli(
            "mirror-batch-bundle",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "low-risk-a-share",
            "--start-date", "20250201",
            "--end-date", "20250228",
            "--max-jobs-per-api", "20",
            "--output", str(output),
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "root",
            "backup",
            "output",
            "scope",
            "start_date",
            "end_date",
            "max_jobs_per_api",
            "status",
            "overwritten",
            "files",
            "commands_execute_guard",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-batch-bundle/v1")
        self.assertEqual(payload["status"], "created")
        self.assertEqual(payload["commands_execute_guard"], "USER_CONFIRMATION_REQUIRED")
        self.assertTrue((output / "commands.sh").exists())


class MirrorOperatorChecklistTests(PreBackfillOperationsTestCase):
    def checklist(self, *, token_available: bool = True):
        return MirrorOperatorChecklistReporter(token_available=token_available).report(
            root=self.root,
            backup=self.backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
        )

    def test_healthy_checklist_ready(self):
        self.build_pilot()
        before = self.counts()
        report = self.checklist()
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "mirror-operator-checklist/v1")
        self.assertTrue(report.paths_valid)
        self.assertTrue(report.backup_not_nested)
        self.assertTrue(report.restore_check_passed)
        self.assertTrue(report.backup_not_mutated)
        self.assertTrue(report.readiness_not_blocked)
        self.assertTrue(report.no_schema_quarantine)
        self.assertTrue(report.no_failed_validation)
        self.assertTrue(report.token_available)
        self.assertTrue(report.max_jobs_guardrail["passed"])
        self.assertTrue(report.batch_plan_available)
        self.assertTrue(report.ready)
        self.assertIn("USER_CONFIRMATION_REQUIRED", json.dumps(report.exact_execute_command))

    def test_missing_token_blocks_without_plaintext(self):
        self.build_pilot()
        report = self.checklist(token_available=False)
        self.assertFalse(report.token_available)
        self.assertFalse(report.ready)
        self.assertTrue(any("TUSHARE_TOKEN" in error for error in report.blocking_errors))
        self.assertNotIn("fake-token-for-hash-only", json.dumps(report.to_dict()))

    def test_mutated_backup_blocks_checklist(self):
        self.build_pilot()
        Validator(self.backup, CatalogStore(self.backup)).validate_latest_snapshots(record=True)
        report = self.checklist()
        self.assertFalse(report.backup_not_mutated)
        self.assertFalse(report.restore_check_passed)
        self.assertFalse(report.ready)
        self.assertTrue(any("modified after backup creation" in error for error in report.blocking_errors))

    def test_failed_readiness_blocks_checklist(self):
        self.build_pilot()
        with sqlite3.connect(self.catalog.db_path) as conn:
            conn.execute("update snapshots set status='superseded' where api_name='trade_cal'")
        report = self.checklist()
        self.assertFalse(report.readiness_not_blocked)
        self.assertFalse(report.ready)
        self.assertTrue(any("mirror-readiness is blocked" in error for error in report.blocking_errors))

    def test_cli_json_contract_and_no_side_effects_for_operator_checklist(self):
        self.build_pilot()
        before = self.counts()
        result = self.run_cli(
            "mirror-operator-checklist",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "low-risk-a-share",
            "--start-date", "20250201",
            "--end-date", "20250228",
            "--json",
            token="fake-checklist-token",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "paths_valid",
            "backup_not_nested",
            "restore_check_passed",
            "backup_not_mutated",
            "readiness_not_blocked",
            "no_schema_quarantine",
            "no_failed_validation",
            "token_available",
            "max_jobs_guardrail",
            "batch_plan_available",
            "disk_space_warning",
            "stop_conditions",
            "exact_plan_command",
            "exact_execute_command",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-operator-checklist/v1")
        self.assertTrue(payload["token_available"])
        self.assertIn("USER_CONFIRMATION_REQUIRED", json.dumps(payload["exact_execute_command"]))
        self.assertNotIn("fake-checklist-token", result.stdout)


class StopPolicyReportTests(PreBackfillOperationsTestCase):
    def test_low_risk_policy_present(self):
        before = self.counts()
        report = StopPolicyReporter().report(scope="low-risk-a-share")
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "stop-policy/v1")
        self.assertEqual(report.category, "low-risk-a-share")
        self.assertFalse(report.execution_blocked)
        self.assertIn("restore-check fails", report.stop_immediately)
        self.assertIn("mirror-run --execute", report.user_confirmation_required_conditions)

    def test_financial_policy_blocks_execution(self):
        report = StopPolicyReporter().report(category="financial")
        self.assertTrue(report.execution_blocked)
        self.assertTrue(report.blocking_errors)
        self.assertTrue(any("financial execution remains blocked" in item for item in report.stop_immediately))

    def test_intraday_policy_blocks_execution(self):
        report = StopPolicyReporter().report(category="intraday")
        self.assertTrue(report.execution_blocked)
        self.assertTrue(report.blocking_errors)
        self.assertTrue(any("intraday execution remains blocked" in item for item in report.stop_immediately))

    def test_backup_policy_present(self):
        report = StopPolicyReporter().report(category="backup")
        self.assertFalse(report.execution_blocked)
        self.assertIn("backup manifest validation fails", report.stop_immediately)
        self.assertIn("after every completed controlled batch", report.backup_required_conditions)

    def test_cli_json_contract_and_no_side_effects_for_stop_policy(self):
        before = self.counts()
        result = self.run_cli("stop-policy", "--category", "financial", "--json")
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "category",
            "execution_blocked",
            "stop_immediately",
            "continue_with_warning",
            "retryable_failures",
            "non_retryable_failures",
            "backup_required_conditions",
            "user_confirmation_required_conditions",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "stop-policy/v1")
        self.assertEqual(payload["category"], "financial")
        self.assertTrue(payload["execution_blocked"])


class SchemaStatusReportTests(PreBackfillOperationsTestCase):
    def test_fake_schema_changes_are_reported_read_only(self):
        self.add_schema_pair()
        before = self.metadata_counts()
        report = SchemaStatusReporter().report(root=self.root)
        self.assertEqual(self.metadata_counts(), before)
        self.assertEqual(report.report_version, "schema-status/v1")
        self.assertEqual(report.schema_count_by_api["daily"], 2)
        self.assertEqual(report.latest_schema_by_api["daily"]["schema_id"], "schema_daily_2")
        self.assertEqual(report.schema_change_count, 1)
        self.assertEqual(report.incompatible_schema_count, 0)
        self.assertEqual(report.pending_schema_change_count, 0)
        self.assertFalse(report.blocking_errors)

    def test_incompatible_schema_is_reported(self):
        self.add_schema_pair(incompatible=True)
        report = SchemaStatusReporter().report(root=self.root)
        self.assertEqual(report.schema_change_count, 1)
        self.assertEqual(report.incompatible_schema_count, 1)
        self.assertEqual(report.pending_schema_change_count, 1)
        self.assertTrue(any("incompatible schema" in error for error in report.blocking_errors))

    def test_quarantine_is_reported(self):
        run_id, job_key = self.make_failed_catalog_rows()
        report = SchemaStatusReporter().report(root=self.root)
        self.assertEqual(report.quarantine_count, 1)
        self.assertEqual(report.quarantined_apis, ["daily"])
        self.assertTrue(any("schema quarantine" in error for error in report.blocking_errors))

    def test_cli_json_contract_and_no_side_effects_for_schema_status(self):
        self.add_schema_pair()
        before = self.metadata_counts()
        result = self.run_cli(
            "schema-status",
            "--root", str(self.root),
            "--json",
        )
        self.assertEqual(self.metadata_counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "root",
            "schema_count_by_api",
            "latest_schema_by_api",
            "schema_change_count",
            "incompatible_schema_count",
            "pending_schema_change_count",
            "quarantine_count",
            "quarantined_apis",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "schema-status/v1")
        self.assertEqual(payload["schema_count_by_api"]["daily"], 2)


class BackupStatusDiagnosticsTests(PreBackfillOperationsTestCase):
    def test_clean_backup_status_is_read_only(self):
        self.build_pilot()
        before = self.counts()
        report = BackupStatusReporter().report(backup=self.backup)
        self.assertEqual(self.counts(), before)
        self.assertEqual(report.report_version, "backup-status/v1")
        self.assertTrue(report.manifest_valid)
        self.assertIsNotNone(report.backup_id)
        self.assertEqual(report.restore_check_status, "succeeded")
        self.assertFalse(report.possible_mutation)
        self.assertGreater(report.file_count, 0)
        self.assertEqual(report.recommended_action, "backup is ready for operator review")
        self.assertFalse(report.blocking_errors)

    def test_mutated_backup_status_blocks(self):
        self.build_pilot()
        Validator(self.backup, CatalogStore(self.backup)).validate_latest_snapshots(record=True)
        report = BackupStatusReporter().report(backup=self.backup)
        self.assertTrue(report.manifest_valid)
        self.assertTrue(report.possible_mutation)
        self.assertEqual(report.restore_check_status, "failed")
        self.assertIn("replace backup", report.recommended_action)
        self.assertTrue(report.blocking_errors)

    def test_missing_manifest_status_blocks(self):
        backup = self.base / "empty-backup"
        backup.mkdir()
        report = BackupStatusReporter().report(backup=backup)
        self.assertFalse(report.manifest_valid)
        self.assertEqual(report.restore_check_status, "failed")
        self.assertEqual(report.file_count, 0)
        self.assertTrue(any("manifest" in error for error in report.blocking_errors))

    def test_bad_manifest_status_blocks(self):
        backup = self.base / "bad-backup"
        backup.mkdir()
        (backup / "manifest.json").write_text("{bad json", encoding="utf-8")
        report = BackupStatusReporter().report(backup=backup)
        self.assertFalse(report.manifest_valid)
        self.assertEqual(report.restore_check_status, "failed")
        self.assertIn("fresh backup", report.recommended_action)
        self.assertTrue(report.blocking_errors)

    def test_cli_json_contract_and_no_side_effects_for_backup_status(self):
        self.build_pilot()
        before = self.counts()
        result = self.run_cli(
            "backup-status",
            "--backup", str(self.backup),
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "backup",
            "manifest_valid",
            "backup_id",
            "created_at",
            "snapshot_scope",
            "file_count",
            "raw_file_count",
            "lake_file_count",
            "catalog_checksum_status",
            "possible_mutation",
            "restore_check_status",
            "recommended_action",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "backup-status/v1")
        self.assertTrue(payload["manifest_valid"])


class ReadOnlyOperationalGuardrailTests(PreBackfillOperationsTestCase):
    def test_read_only_operational_commands_do_not_mutate_catalog_or_data_files(self):
        self.build_pilot()
        bundle_output = self.base / "guardrail-bundle"
        commands = [
            (
                "mirror-review",
                [
                    "mirror-review",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250101",
                    "--end-date", "20250110",
                    "--json",
                ],
            ),
            (
                "mirror-readiness",
                [
                    "mirror-readiness",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
            ),
            (
                "mirror-batch-plan",
                [
                    "mirror-batch-plan",
                    "--root", str(self.root),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250201",
                    "--end-date", "20250228",
                    "--max-jobs-per-api", "20",
                    "--json",
                ],
            ),
            (
                "mirror-status",
                [
                    "mirror-status",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
            ),
            (
                "mirror-audit",
                [
                    "mirror-audit",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
            ),
            (
                "mirror-next-batch",
                [
                    "mirror-next-batch",
                    "--root", str(self.root),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
            ),
            (
                "mirror-batch-bundle",
                [
                    "mirror-batch-bundle",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250201",
                    "--end-date", "20250228",
                    "--max-jobs-per-api", "20",
                    "--output", str(bundle_output),
                    "--json",
                ],
            ),
            (
                "mirror-operator-checklist",
                [
                    "mirror-operator-checklist",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250201",
                    "--end-date", "20250228",
                    "--json",
                ],
            ),
            ("stop-policy", ["stop-policy", "--scope", "low-risk-a-share", "--json"]),
            ("schema-status", ["schema-status", "--root", str(self.root), "--json"]),
            ("backup-status", ["backup-status", "--backup", str(self.backup), "--json"]),
            (
                "mirror-coverage-matrix",
                [
                    "mirror-coverage-matrix",
                    "--root", str(self.root),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250101",
                    "--end-date", "20250110",
                    "--json",
                ],
            ),
            (
                "request-estimate",
                [
                    "request-estimate",
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250201",
                    "--end-date", "20250228",
                    "--root", str(self.root),
                    "--json",
                ],
            ),
            ("api-infra-readiness", ["--root", str(self.root), "api-infra-readiness", "--json"]),
            ("pit-readiness", ["--root", str(self.root), "pit-readiness", "--json"]),
            (
                "object-plan",
                [
                    "--root", str(self.root),
                    "object-plan",
                    "--api", "news",
                    "--start-date", "20250101",
                    "--end-date", "20250131",
                    "--json",
                ],
            ),
            (
                "intraday-plan",
                [
                    "--root", str(self.root),
                    "intraday-plan",
                    "--api", "stk_mins",
                    "--freq", "1min",
                    "--start-date", "20250102",
                    "--end-date", "20250103",
                    "--bucket-count", "64",
                    "--json",
                ],
            ),
            (
                "storage-estimate",
                [
                    "--root", str(self.root),
                    "storage-estimate",
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250101",
                    "--end-date", "20251231",
                    "--json",
                ],
            ),
            ("compaction-plan", ["compaction-plan", "--root", str(self.root), "--api", "daily_basic", "--json"]),
            ("rate-policy", ["--root", str(self.root), "rate-policy", "--scope", "low-risk-a-share", "--json"]),
            ("endpoint-enable-checklist", ["--root", str(self.root), "endpoint-enable-checklist", "--api", "daily_basic", "--json"]),
            ("code-universe", ["--root", str(self.root), "code-universe", "--universe", "a_share_listed", "--limit", "3", "--json"]),
            (
                "code-list-plan",
                [
                    "--root", str(self.root),
                    "code-list-plan",
                    "--api", "stk_managers",
                    "--universe", "a_share_listed",
                    "--limit-codes", "3",
                    "--start-date", "20250101",
                    "--end-date", "20250131",
                    "--json",
                ],
            ),
            (
                "code-date-matrix-plan",
                [
                    "--root", str(self.root),
                    "code-date-matrix-plan",
                    "--api", "stk_managers",
                    "--universe", "a_share_listed",
                    "--limit-codes", "2",
                    "--dates", "20250102,20250103",
                    "--json",
                ],
            ),
            (
                "period-plan",
                [
                    "--root", str(self.root),
                    "period-plan",
                    "--api", "income",
                    "--periods", "20240331,20240630",
                    "--json",
                ],
            ),
            (
                "code-period-plan",
                [
                    "--root", str(self.root),
                    "code-period-plan",
                    "--api", "income",
                    "--universe", "a_share_listed",
                    "--limit-codes", "2",
                    "--periods", "20240331",
                    "--json",
                ],
            ),
        ]

        for command_name, args in commands:
            with self.subTest(command_name=command_name):
                before = self.guardrail_counts()
                result = self.run_cli(*args, check=False, token="secret-token-should-not-appear")
                after = self.guardrail_counts()
                self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
                self.assertEqual(after, before)
                self.assertEqual(after["mirror_catalog"]["validations"], before["mirror_catalog"]["validations"])
                self.assertEqual(after["mirror_raw_files"], before["mirror_raw_files"])
                self.assertEqual(after["mirror_lake_files"], before["mirror_lake_files"])
                self.assertNotIn("secret-token-should-not-appear", result.stdout)
                self.assertNotIn("secret-token-should-not-appear", result.stderr)


class CliHelpAndJsonContractPolishTests(PreBackfillOperationsTestCase):
    COMMANDS_WITH_HELP = [
        "mirror-status",
        "mirror-audit",
        "mirror-next-batch",
        "mirror-batch-bundle",
        "mirror-operator-checklist",
        "stop-policy",
        "schema-status",
        "backup-status",
        "mirror-coverage-matrix",
        "request-estimate",
    ]

    def test_help_says_read_only_and_no_real_requests_where_relevant(self):
        for command_name in self.COMMANDS_WITH_HELP:
            with self.subTest(command_name=command_name):
                result = self.run_cli(command_name, "--help", check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                help_text = " ".join(result.stdout.split())
                self.assertIn("Read-only", help_text)
                self.assertTrue(
                    any(
                        phrase in help_text
                        for phrase in [
                            "does not call Tushare",
                            "does not make real requests",
                            "does not fetch",
                            "queries local catalog",
                            "inspects local",
                            "does not inspect local data",
                            "writes only --output",
                        ]
                    ),
                    help_text,
                )

    def test_json_report_version_present_for_operations_contract(self):
        self.build_pilot()
        bundle_output = self.base / "phase12-bundle"
        commands = [
            (
                "mirror-status",
                "mirror-status/v1",
                [
                    "mirror-status",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
            ),
            (
                "mirror-audit",
                "mirror-audit/v1",
                [
                    "mirror-audit",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
            ),
            (
                "mirror-next-batch",
                "mirror-next-batch/v1",
                [
                    "mirror-next-batch",
                    "--root", str(self.root),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
            ),
            (
                "mirror-batch-bundle",
                "mirror-batch-bundle/v1",
                [
                    "mirror-batch-bundle",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250201",
                    "--end-date", "20250228",
                    "--max-jobs-per-api", "20",
                    "--output", str(bundle_output),
                    "--json",
                ],
            ),
            (
                "mirror-operator-checklist",
                "mirror-operator-checklist/v1",
                [
                    "mirror-operator-checklist",
                    "--root", str(self.root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250201",
                    "--end-date", "20250228",
                    "--json",
                ],
            ),
            ("stop-policy", "stop-policy/v1", ["stop-policy", "--scope", "low-risk-a-share", "--json"]),
            ("schema-status", "schema-status/v1", ["schema-status", "--root", str(self.root), "--json"]),
            ("backup-status", "backup-status/v1", ["backup-status", "--backup", str(self.backup), "--json"]),
            (
                "mirror-coverage-matrix",
                "mirror-coverage-matrix/v1",
                [
                    "mirror-coverage-matrix",
                    "--root", str(self.root),
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250101",
                    "--end-date", "20250110",
                    "--json",
                ],
            ),
            (
                "request-estimate",
                "request-estimate/v1",
                [
                    "request-estimate",
                    "--scope", "low-risk-a-share",
                    "--start-date", "20250201",
                    "--end-date", "20250228",
                    "--root", str(self.root),
                    "--json",
                ],
            ),
        ]
        for command_name, expected_version, args in commands:
            with self.subTest(command_name=command_name):
                result = self.run_cli(*args, check=False)
                self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
                payload = json.loads(result.stdout)
                self.assertEqual(payload["report_version"], expected_version)

    def test_missing_root_and_backup_errors_are_clear_json(self):
        self.build_pilot()
        missing_root = self.base / "missing-root"
        missing_backup = self.base / "missing-backup"
        cases = [
            (
                "mirror-status missing root",
                [
                    "mirror-status",
                    "--root", str(missing_root),
                    "--backup", str(self.backup),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
                "catalog not found",
            ),
            (
                "schema-status missing root",
                ["schema-status", "--root", str(missing_root), "--json"],
                "catalog not found",
            ),
            (
                "mirror-status missing backup",
                [
                    "mirror-status",
                    "--root", str(self.root),
                    "--backup", str(missing_backup),
                    "--scope", "low-risk-a-share",
                    "--json",
                ],
                "backup not found",
            ),
            (
                "backup-status missing backup",
                ["backup-status", "--backup", str(missing_backup), "--json"],
                "backup not found",
            ),
        ]
        for case_name, args, expected_error in cases:
            with self.subTest(case_name=case_name):
                result = self.run_cli(*args, check=False)
                self.assertNotEqual(result.returncode, 0)
                payload = json.loads(result.stdout)
                self.assertIn(expected_error, " ".join(payload["blocking_errors"]))


if __name__ == "__main__":
    unittest.main()
