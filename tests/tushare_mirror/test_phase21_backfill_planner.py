from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror import cli
from tushare_mirror.backfill import BackfillExecutor, BackfillPlanner, DatePlanner, SUPPORTED_DATE_BACKFILL_APIS
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult, TushareError
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.store import FileLakeStore


EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class EchoClient:
    def __init__(self, bad_dates=None):
        self.calls = 0
        self.bad_dates = set(bad_dates or [])

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        date = params.get("trade_date") or params.get("cal_date") or "20250102"
        if api_name == "trade_cal":
            response_fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
            response_items = [
                ["SSE", "20250102", 1, "20241231"],
                ["SSE", "20250103", 1, "20250102"],
                ["SSE", "20250104", 0, "20250103"],
            ]
        else:
            response_fields = ["ts_code", "trade_date", "close"]
            close = "bad-close" if date in self.bad_dates else 10.0 + self.calls
            response_items = [["000001.SZ", date, close]]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": response_items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class FlakyRateLimitClient(EchoClient):
    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        if self.calls == 1:
            raise TushareError(api_name, -2002, "每分钟请求限制", {"code": -2002, "msg": "每分钟请求限制"})
        date = params.get("trade_date") or "20250102"
        response_fields = ["ts_code", "trade_date", "close"]
        response_items = [["000001.SZ", date, 11.0]]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": response_items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class Phase21BackfillPlannerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

    def test_date_planner_explicit_range_invalid_and_trading_days(self):
        planner = DatePlanner(self.root, self.catalog)
        self.assertEqual(planner.plan_dates(dates="20250103,20250102,20250102"), ["20250102", "20250103"])
        self.assertEqual(planner.plan_dates(start_date="20250102", end_date="20250104"), ["20250102", "20250103", "20250104"])
        with self.assertRaises(ValueError):
            planner.plan_dates(dates="2025bad")
        with self.assertRaises(ValueError):
            planner.plan_dates(start_date="20250105", end_date="20250104")
        with self.assertRaises(ValueError):
            planner.plan_dates(start_date="20250102", end_date="20250104", trading_days_only=True)

        FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20250102", "end_date": "20250104"},
            EchoClient(),
        )
        self.assertEqual(
            planner.plan_dates(start_date="20250102", end_date="20250104", trading_days_only=True),
            ["20250102", "20250103"],
        )

    def test_backfill_planner_supported_endpoints_and_dry_run_side_effects(self):
        planner = BackfillPlanner(self.root, self.catalog)
        for api_name in sorted(SUPPORTED_DATE_BACKFILL_APIS):
            plan = planner.plan_date_backfill(api_name, ["20250102"], max_jobs=1)
            self.assertEqual(plan.date_field, "trade_date")
            self.assertEqual(plan.planned_jobs[0].params, {"trade_date": "20250102"})
            self.assertEqual(plan.planned_jobs[0].existing_status, "missing")
            self.assertEqual(plan.planned_jobs[0].planned_action, "fetch")
        truncated = planner.plan_date_backfill("daily", ["20250102", "20250103", "20250104"], max_jobs=2)
        self.assertEqual(truncated.total_candidate_jobs, 3)
        self.assertEqual(len(truncated.planned_jobs), 2)
        self.assertTrue(truncated.warnings)
        with self.assertRaises(ValueError):
            planner.plan_date_backfill("stock_basic", ["20250102"], max_jobs=1)
        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from jobs").fetchone()[0], 0)
            self.assertEqual(conn.execute("select count(*) from snapshots").fetchone()[0], 0)

    def test_existing_statuses_active_failed_staged_and_quarantine(self):
        store = FileLakeStore(self.root, self.catalog)
        store.fetch("daily", {"trade_date": "20250102"}, EchoClient())
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250102"], max_jobs=1)
        self.assertEqual(plan.planned_jobs[0].existing_status, "active_exists")
        self.assertEqual(plan.planned_jobs[0].planned_action, "skip_existing")

        failed_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250103"], max_jobs=1)
        self.catalog.upsert_job(failed_plan.planned_jobs[0].job_key, self.catalog.create_run("fetch"), "daily", {"trade_date": "20250103"}, ["ts_code"], "failed")
        self.catalog.update_job_failed(failed_plan.planned_jobs[0].job_key, "forced", "network_error")
        failed = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250103"], max_jobs=1)
        self.assertEqual(failed.planned_jobs[0].existing_status, "failed_exists")
        self.assertEqual(failed.planned_jobs[0].planned_action, "retry_failed")

        staged_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250104"], max_jobs=1)
        run_id = self.catalog.create_run("fetch")
        self.catalog.insert_file(
            table_id=self.catalog.get_endpoint_config("daily")["table_id"],
            api_name="daily",
            content_type="lake",
            file_format="parquet",
            relative_path="lake/staged.parquet",
            staged_path=None,
            partition_values={"trade_date": "20250104"},
            record_count=0,
            source_item_count=0,
            raw_event_count=None,
            error_event_count=0,
            size_bytes=0,
            sha256=EMPTY_SHA256,
            schema_id="schema_staged",
            status="staged",
            run_id=run_id,
            job_key=staged_plan.planned_jobs[0].job_key,
        )
        staged = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250104"], max_jobs=1)
        self.assertEqual(staged.planned_jobs[0].existing_status, "staged_exists")

        store.fetch("daily", {"trade_date": "20250105"}, EchoClient(bad_dates={"20250105"}))
        quarantined = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250105"], max_jobs=1)
        self.assertEqual(quarantined.planned_jobs[0].existing_status, "quarantined_exists")
        self.assertEqual(quarantined.planned_jobs[0].planned_action, "blocked_quarantined")

    def test_backfill_execute_success_rerun_skip_and_validate_latest(self):
        dates = ["20250102", "20250103"]
        planner = BackfillPlanner(self.root, self.catalog)
        plan = planner.plan_date_backfill("daily", dates, max_jobs=2, dry_run=False)
        result = BackfillExecutor(self.root, self.catalog).execute(plan, EchoClient(), validate_latest=True)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.summary["succeeded_jobs"], 2)
        self.assertEqual(result.summary["planned_jobs"], 2)
        self.assertEqual(result.validation["api_name"], "daily")
        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from jobs where run_id=?", (result.run_id,)).fetchone()[0], 2)
            self.assertEqual(conn.execute("select count(*) from validation_runs where api_name='daily'").fetchone()[0], 1)

        rerun_plan = planner.plan_date_backfill("daily", dates, max_jobs=2, dry_run=False)
        self.assertTrue(all(job.planned_action == "skip_existing" for job in rerun_plan.planned_jobs))
        rerun = BackfillExecutor(self.root, self.catalog).execute(rerun_plan, EchoClient())
        self.assertEqual(rerun.summary["skipped_jobs"], 2)
        self.assertEqual(len(list((self.root / "lake").rglob("*.parquet"))), 2)

    def test_backfill_execute_rate_limit_retry_and_schema_quarantine(self):
        rate_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250102"], max_jobs=1, dry_run=False)
        rate_result = BackfillExecutor(self.root, self.catalog, FileLakeStore(self.root, self.catalog, retry_sleep=lambda _: None)).execute(rate_plan, FlakyRateLimitClient())
        self.assertEqual(rate_result.status, "succeeded")

        bad_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill("daily", ["20250103"], max_jobs=1, dry_run=False)
        bad = BackfillExecutor(self.root, self.catalog).execute(bad_plan, EchoClient(bad_dates={"20250103"}))
        self.assertEqual(bad.status, "failed")
        self.assertEqual(bad.summary["failed_jobs"], 1)
        self.assertEqual(bad.summary["quarantined_jobs"], 1)
        self.assertTrue(any((self.root / "_quarantine").rglob("*")))

    def test_cli_backfill_plan_and_backfill_dry_run_and_execute_parser(self):
        planned = json.loads(self.run_cli("backfill-plan", "--api", "daily", "--dates", "20250102,20250103", "--max-jobs", "2", "--json").stdout)
        self.assertEqual(planned["total_candidate_jobs"], 2)
        self.assertTrue(planned["dry_run"])
        dry = json.loads(self.run_cli("backfill", "--api", "daily", "--dates", "20250102,20250103", "--max-jobs", "2", "--json").stdout)
        self.assertTrue(dry["dry_run"])
        args = cli.build_parser().parse_args(["backfill", "--api", "daily", "--dates", "20250102,20250103", "--max-jobs", "2", "--execute"])
        self.assertTrue(args.execute)
        self.assertEqual(args.func, cli.cmd_backfill)


if __name__ == "__main__":
    unittest.main()
