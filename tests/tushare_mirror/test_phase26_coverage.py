from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backfill import BackfillPlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.coverage import CoverageReporter
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.store import FileLakeStore


class EchoClient:
    def query_paginated(self, api_name, params, fields, page_size=None):
        date = params.get("trade_date") or "20250102"
        response_fields = ["ts_code", "trade_date", "close"]
        response_items = [["000001.SZ", date, 10.0]]
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

    def query_paginated(self, api_name, params, fields, page_size=None):
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


class Phase26CoverageTests(unittest.TestCase):
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

    def test_cli_coverage_requires_existing_catalog_without_creating_it(self):
        missing_root = Path(self.tmp.name) / "missing-lake"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tushare_mirror",
                "--root",
                str(missing_root),
                "coverage",
                "--api",
                "daily",
                "--dates",
                "20250102",
            ],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("catalog not found", result.stderr)
        self.assertFalse((missing_root / "_catalog" / "catalog.sqlite").exists())

    def test_empty_catalog_coverage_is_missing_and_read_only(self):
        before = self.counts()
        report = CoverageReporter(self.root, self.catalog).report("daily", dates="20250102,20250103")
        self.assertEqual(report.total_dates, 2)
        self.assertEqual(report.covered_dates, 0)
        self.assertEqual(report.missing_dates, 2)
        self.assertEqual(report.coverage_ratio, 0.0)
        self.assertEqual([item.existing_status for item in report.items], ["missing", "missing"])
        self.assertEqual([item.planned_action for item in report.items], ["fetch", "fetch"])
        self.assertEqual(self.counts(), before)

    def test_active_failed_quarantined_and_plan_consistency(self):
        store = FileLakeStore(self.root, self.catalog)
        store.fetch("daily", {"trade_date": "20250102"}, EchoClient())

        failed_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250103"], max_jobs=1)
        failed_job = failed_plan.planned_jobs[0]
        failed_run = self.catalog.create_run("fetch")
        self.catalog.upsert_job(failed_job.job_key, failed_run, "daily", {"trade_date": "20250103"}, ["ts_code"], "failed")
        self.catalog.update_job_failed(failed_job.job_key, "forced failure", "network_error")

        quarantine_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250104"], max_jobs=1)
        quarantine_job = quarantine_plan.planned_jobs[0]
        quarantine_run = self.catalog.create_run("fetch")
        self.catalog.record_quarantine(quarantine_run, quarantine_job.job_key, "daily", "schema_incompatible", "_quarantine/test", None, None)

        report = CoverageReporter(self.root, self.catalog).report(
            "daily",
            dates="20250102,20250103,20250104,20250105",
        )
        by_date = {item.date: item for item in report.items}
        self.assertEqual(by_date["20250102"].existing_status, "active_exists")
        self.assertEqual(by_date["20250102"].planned_action, "skip_existing")
        self.assertIsNotNone(by_date["20250102"].snapshot_id)
        self.assertEqual(by_date["20250102"].record_count, 1)
        self.assertEqual(by_date["20250102"].raw_event_count, 1)
        self.assertEqual(by_date["20250102"].file_count, 2)
        self.assertEqual(by_date["20250102"].last_job_status, "succeeded")
        self.assertEqual(by_date["20250103"].existing_status, "failed_exists")
        self.assertEqual(by_date["20250103"].planned_action, "retry_failed")
        self.assertEqual(by_date["20250103"].last_error_type, "network_error")
        self.assertEqual(by_date["20250104"].existing_status, "quarantined_exists")
        self.assertEqual(by_date["20250104"].planned_action, "blocked_quarantined")
        self.assertEqual(by_date["20250105"].existing_status, "missing")
        self.assertEqual(report.covered_dates, 1)
        self.assertEqual(report.missing_dates, 1)
        self.assertEqual(report.failed_dates, 1)
        self.assertEqual(report.quarantined_dates, 1)
        self.assertEqual(report.coverage_ratio, 0.25)

        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(
            "daily",
            ["20250102", "20250103", "20250104", "20250105"],
            max_jobs=4,
        )
        for item, planned in zip(report.items, plan.planned_jobs):
            self.assertEqual(item.existing_status, planned.existing_status)
            self.assertEqual(item.planned_action, planned.planned_action)

    def test_calendar_aware_coverage_and_no_side_effects(self):
        missing = self.run_cli(
            "coverage",
            "--api",
            "daily",
            "--start-date",
            "20250101",
            "--end-date",
            "20250110",
            "--trading-days-only",
            "--calendar-exchange",
            "SSE",
            check=False,
        )
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("trading-days-only requires local trade_cal latest snapshot", missing.stderr)

        rows = [
            ["SSE", "20250101", "0", "20241231"],
            ["SSE", "20250102", "1", "20241231"],
            ["SSE", "20250103", "1", "20250102"],
            ["SSE", "20250104", "0", "20250103"],
            ["SSE", "20250105", "0", "20250103"],
            ["SSE", "20250106", "true", "20250103"],
            ["SSE", "20250107", "1.0", "20250106"],
            ["SSE", "20250108", "1", "20250107"],
            ["SSE", "20250109", "1", "20250108"],
            ["SSE", "20250110", "1", "20250109"],
        ]
        FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20250101", "end_date": "20250110"},
            CalendarClient(rows),
        )
        before = self.counts()
        output = self.run_cli(
            "coverage",
            "--api",
            "daily",
            "--start-date",
            "20250101",
            "--end-date",
            "20250110",
            "--trading-days-only",
            "--calendar-exchange",
            "SSE",
            "--json",
        ).stdout
        payload = json.loads(output)
        self.assertEqual(payload["calendar_source"], "local trade_cal latest snapshot")
        self.assertEqual(payload["calendar_exchange"], "SSE")
        self.assertEqual(payload["requested_start_date"], "20250101")
        self.assertEqual(payload["requested_end_date"], "20250110")
        self.assertEqual(payload["natural_days"], 10)
        self.assertEqual(payload["trading_days"], 7)
        self.assertEqual(payload["filtered_non_trading_days"], 3)
        self.assertEqual(payload["filtered_non_trading_dates"], ["20250101", "20250104", "20250105"])
        self.assertEqual(payload["total_dates"], 7)
        self.assertEqual(payload["covered_dates"], 0)
        self.assertEqual(payload["missing_dates"], 7)
        self.assertEqual(len(payload["items"]), 7)
        self.assertEqual(self.counts(), before)

        weekly = self.run_cli(
            "coverage",
            "--api",
            "weekly",
            "--start-date",
            "20250101",
            "--end-date",
            "20250110",
            "--trading-days-only",
            check=False,
        )
        self.assertNotEqual(weekly.returncode, 0)
        self.assertIn("trading-days-only is only supported for daily-like endpoints", weekly.stderr)

    def test_cli_table_and_json_output(self):
        table = self.run_cli("coverage", "--api", "daily", "--dates", "20250102,20250103").stdout
        self.assertIn("existing_status", table)
        self.assertIn("coverage_ratio", table)
        payload = json.loads(self.run_cli("coverage", "--api", "daily", "--dates", "20250102,20250103", "--json").stdout)
        self.assertEqual(payload["api_name"], "daily")
        self.assertEqual(payload["total_dates"], 2)
        self.assertEqual(payload["items"][0]["planned_action"], "fetch")
