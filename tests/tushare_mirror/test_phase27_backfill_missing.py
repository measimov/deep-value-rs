from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror import cli
from tushare_mirror.backfill import BackfillExecutor, BackfillPlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.coverage import CoverageReporter
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.missing_backfill import MissingBackfillPlanner
from tushare_mirror.store import FileLakeStore


class EchoClient:
    def __init__(self):
        self.calls = 0

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        date = params.get("trade_date") or "20250102"
        response_fields = ["ts_code", "trade_date", "close"]
        response_items = [["000001.SZ", date, 10.0 + self.calls]]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": response_items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class CalendarClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        response_fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": self.rows, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=self.rows)


class Phase27BackfillMissingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args, check=True):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def counts(self):
        with sqlite3.connect(self.catalog.db_path) as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def add_calendar(self):
        rows = [
            ["SSE", "20250101", "0", "20241231"],
            ["SSE", "20250102", "1", "20241231"],
            ["SSE", "20250103", "1", "20250102"],
            ["SSE", "20250104", "0", "20250103"],
            ["SSE", "20250105", "0", "20250103"],
            ["SSE", "20250106", "1", "20250103"],
        ]
        FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20250101", "end_date": "20250106"},
            CalendarClient(rows),
        )

    def test_dry_run_all_missing_and_cli_no_side_effects(self):
        before = self.counts()
        plan = MissingBackfillPlanner(self.root, self.catalog).plan("daily", dates="20250102,20250103", max_jobs=2)
        self.assertTrue(plan.dry_run)
        self.assertFalse(plan.execute)
        self.assertEqual(plan.candidate_jobs, 2)
        self.assertEqual(plan.planned_jobs, 2)
        self.assertEqual([item.existing_status for item in plan.items], ["missing", "missing"])
        self.assertEqual([item.will_execute for item in plan.items], [True, True])
        self.assertEqual(self.counts(), before)

        payload = json.loads(self.run_cli("backfill-missing", "--api", "daily", "--dates", "20250102,20250103", "--max-jobs", "2", "--json").stdout)
        self.assertEqual(payload["candidate_jobs"], 2)
        self.assertEqual(payload["planned_jobs"], 2)
        self.assertTrue(payload["items"][0]["will_execute"])
        self.assertEqual(self.counts(), before)
        table = self.run_cli("backfill-missing", "--api", "daily", "--dates", "20250102,20250103", "--max-jobs", "2").stdout
        self.assertIn("will_execute", table)

    def test_active_dates_are_not_executed_and_job_keys_match_backfill_plan(self):
        FileLakeStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250102"}, EchoClient())
        before = self.counts()
        plan = MissingBackfillPlanner(self.root, self.catalog).plan("daily", dates="20250102,20250103", max_jobs=2)
        by_date = {item.date: item for item in plan.items}
        self.assertFalse(by_date["20250102"].will_execute)
        self.assertEqual(by_date["20250102"].existing_status, "active_exists")
        self.assertTrue(by_date["20250103"].will_execute)
        self.assertEqual(by_date["20250103"].existing_status, "missing")
        full_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250103"], max_jobs=1)
        self.assertEqual(plan.backfill_plan.planned_jobs[0].job_key, full_plan.planned_jobs[0].job_key)
        self.assertEqual(self.counts(), before)

    def test_calendar_aware_missing_plan_filters_trading_days(self):
        self.add_calendar()
        before = self.counts()
        plan = MissingBackfillPlanner(self.root, self.catalog).plan(
            "daily_basic",
            start_date="20250101",
            end_date="20250106",
            trading_days_only=True,
            calendar_exchange="SSE",
            max_jobs=2,
        )
        self.assertEqual(plan.coverage["natural_days"], 6)
        self.assertEqual(plan.coverage["trading_days"], 3)
        self.assertEqual(plan.coverage["filtered_non_trading_dates"], ["20250101", "20250104", "20250105"])
        self.assertEqual(plan.candidate_jobs, 3)
        self.assertEqual(plan.planned_jobs, 2)
        self.assertTrue(plan.truncated_by_max_jobs)
        self.assertEqual([job.date for job in plan.backfill_plan.planned_jobs], ["20250102", "20250103"])
        self.assertEqual(self.counts(), before)

    def test_calendar_missing_and_weekly_trading_days_only_errors(self):
        missing = self.run_cli(
            "backfill-missing",
            "--api",
            "daily",
            "--start-date",
            "20250101",
            "--end-date",
            "20250110",
            "--trading-days-only",
            "--calendar-exchange",
            "SSE",
            "--max-jobs",
            "3",
            check=False,
        )
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("trading-days-only requires local trade_cal latest snapshot", missing.stderr)
        weekly = self.run_cli(
            "backfill-missing",
            "--api",
            "weekly",
            "--start-date",
            "20250101",
            "--end-date",
            "20250110",
            "--trading-days-only",
            "--calendar-exchange",
            "SSE",
            "--max-jobs",
            "3",
            check=False,
        )
        self.assertNotEqual(weekly.returncode, 0)
        self.assertIn("trading-days-only is only supported for daily-like endpoints", weekly.stderr)

    def test_execute_missing_success_coverage_lift_and_noop_rerun(self):
        plan = MissingBackfillPlanner(self.root, self.catalog).plan("daily", dates="20250102,20250103", max_jobs=2, execute=True)
        result = BackfillExecutor(self.root, self.catalog).execute(plan.backfill_plan, EchoClient(), validate_latest=True)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.summary["executed_jobs"], 2)
        self.assertEqual(result.summary["succeeded_jobs"], 2)
        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from validation_runs where api_name='daily'").fetchone()[0], 1)
        coverage = CoverageReporter(self.root, self.catalog).report("daily", dates="20250102,20250103")
        self.assertEqual(coverage.coverage_ratio, 1.0)
        before_rerun = self.counts()
        rerun = MissingBackfillPlanner(self.root, self.catalog).plan("daily", dates="20250102,20250103", max_jobs=2, execute=True)
        self.assertEqual(rerun.candidate_jobs, 0)
        self.assertEqual(rerun.planned_jobs, 0)
        self.assertTrue(all(not item.will_execute for item in rerun.items))
        self.assertEqual(self.counts(), before_rerun)

        no_op = self.run_cli("backfill-missing", "--api", "daily", "--dates", "20250102,20250103", "--max-jobs", "2", "--execute")
        self.assertIn("No missing jobs to backfill", no_op.stdout)
        self.assertEqual(self.counts(), before_rerun)

    def test_failed_retry_and_quarantine_blocked(self):
        failed_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250102"], max_jobs=1)
        failed_job = failed_plan.planned_jobs[0]
        failed_run = self.catalog.create_run("fetch")
        self.catalog.upsert_job(failed_job.job_key, failed_run, "daily", {"trade_date": "20250102"}, ["ts_code"], "failed")
        self.catalog.update_job_failed(failed_job.job_key, "forced", "network_error")

        quarantine_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250103"], max_jobs=1)
        quarantine_job = quarantine_plan.planned_jobs[0]
        quarantine_run = self.catalog.create_run("fetch")
        self.catalog.record_quarantine(quarantine_run, quarantine_job.job_key, "daily", "schema_incompatible", "_quarantine/test", None, None)

        default = MissingBackfillPlanner(self.root, self.catalog).plan("daily", dates="20250102,20250103,20250104", max_jobs=3)
        by_date = {item.date: item for item in default.items}
        self.assertFalse(by_date["20250102"].will_execute)
        self.assertEqual(by_date["20250102"].existing_status, "failed_exists")
        self.assertFalse(by_date["20250103"].will_execute)
        self.assertEqual(by_date["20250103"].existing_status, "quarantined_exists")
        self.assertTrue(by_date["20250104"].will_execute)
        self.assertEqual(default.candidate_jobs, 1)
        self.assertEqual(default.blocked_jobs, 1)

        retry = MissingBackfillPlanner(self.root, self.catalog).plan("daily", dates="20250102,20250103,20250104", max_jobs=3, retry_failed=True, execute=True)
        retry_dates = [job.date for job in retry.backfill_plan.planned_jobs]
        self.assertEqual(retry_dates, ["20250102", "20250104"])
        result = BackfillExecutor(self.root, self.catalog).execute(retry.backfill_plan, EchoClient())
        self.assertEqual(result.summary["executed_jobs"], 2)
        self.assertEqual(result.summary["succeeded_jobs"], 2)
        blocked_after = MissingBackfillPlanner(self.root, self.catalog).plan("daily", dates="20250103", max_jobs=1, retry_failed=True)
        self.assertEqual(blocked_after.blocked_jobs, 1)
        self.assertFalse(blocked_after.items[0].will_execute)

    def test_cli_parser_and_execute_flag(self):
        args = cli.build_parser().parse_args([
            "backfill-missing",
            "--api",
            "daily",
            "--dates",
            "20250102,20250103",
            "--max-jobs",
            "2",
            "--execute",
        ])
        self.assertTrue(args.execute)
        self.assertEqual(args.func, cli.cmd_backfill_missing)
        missing_max = self.run_cli("backfill-missing", "--api", "daily", "--dates", "20250102", check=False)
        self.assertNotEqual(missing_max.returncode, 0)
        self.assertIn("backfill-missing requires --max-jobs", missing_max.stderr)


if __name__ == "__main__":
    unittest.main()
