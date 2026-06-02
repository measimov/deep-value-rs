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
from tushare_mirror.code_date_matrix_planner import (
    CodeDateMatrixItem,
    CodeDateMatrixPlan,
    CodeDateMatrixPlanner,
    CodeDateMatrixSummary,
)
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.planner import JobPlanner
from tushare_mirror.store import FileLakeStore


class CodeDateMatrixPlanModelTests(unittest.TestCase):
    def test_model_serialization_is_stable(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="stk_managers",
            universe="a_share_listed",
            source_snapshot_id="snap_local",
            total_codes=2,
            total_dates=2,
            limit_codes=2,
            max_dates=2,
        )
        item = CodeDateMatrixItem(
            api_name="stk_managers",
            ts_code="000001.SZ",
            date="20250102",
            params={"ts_code": "000001.SZ", "trade_date": "20250102"},
            job_key="job_abc",
            existing_status="missing",
            planned_action="fetch",
        )
        payload = CodeDateMatrixPlan(summary=summary, items=[item]).to_dict()
        self.assertFalse(payload["blocked"])
        self.assertFalse(payload["execution_allowed"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["candidate_jobs"], 4)
        self.assertEqual(payload["planned_jobs"], 4)
        self.assertEqual(payload["items"][0]["execution_allowed"], False)
        self.assertEqual(payload["items"][0]["would_require_real_request"], True)
        rendered = json.dumps(payload, sort_keys=True)
        self.assertIn('"api_name": "stk_managers"', rendered)
        self.assertIn('"items"', rendered)

    def test_candidate_limit_calculation_caps_code_and_candidate_counts(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="stk_managers",
            universe="a_share_listed",
            source_snapshot_id="snap_local",
            total_codes=30,
            total_dates=10,
            limit_codes=20,
            max_dates=10,
            max_candidate_jobs=100,
        )
        self.assertEqual(summary.total_codes, 30)
        self.assertEqual(summary.planned_codes, 20)
        self.assertEqual(summary.total_dates, 10)
        self.assertEqual(summary.planned_dates, 5)
        self.assertEqual(summary.candidate_jobs, 300)
        self.assertEqual(summary.planned_jobs, 100)
        self.assertTrue(summary.truncated_by_code_limit)
        self.assertFalse(summary.truncated_by_date_limit)
        self.assertTrue(summary.truncated_by_candidate_limit)

    def test_date_limit_truncation_flag(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="namechange",
            universe="a_share_listed",
            source_snapshot_id=None,
            total_codes=2,
            total_dates=25,
            limit_codes=5,
            max_dates=20,
            max_candidate_jobs=100,
        )
        self.assertEqual(summary.planned_codes, 2)
        self.assertEqual(summary.planned_dates, 20)
        self.assertEqual(summary.candidate_jobs, 50)
        self.assertEqual(summary.planned_jobs, 40)
        self.assertFalse(summary.truncated_by_code_limit)
        self.assertTrue(summary.truncated_by_date_limit)
        self.assertFalse(summary.truncated_by_candidate_limit)

    def test_blocking_errors_mark_plan_blocked(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="stk_rewards",
            universe="a_share_listed",
            source_snapshot_id=None,
            total_codes=0,
            total_dates=0,
            limit_codes=0,
            max_dates=0,
            blocking_errors=["limit_codes_required"],
        )
        payload = CodeDateMatrixPlan(summary=summary, items=[]).to_dict()
        self.assertTrue(payload["blocked"])
        self.assertEqual(payload["blocking_errors"], ["limit_codes_required"])
        self.assertEqual(payload["items"], [])


class CodeDateMatrixFakeClient:
    def __init__(self, fields: list[str], items: list[list[object]]):
        self.fields = fields
        self.items = items
        self.calls = 0

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": self.fields, "items": self.items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=self.fields, items=self.items)


class CodeDateMatrixPlannerCliTests(unittest.TestCase):
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

    def run_cli(self, *args, check=True):
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

    def seed_stock_basic(self, count: int = 4):
        fields = ["ts_code", "symbol", "name", "area", "industry", "market", "list_date"]
        base_codes = ["000001.SZ", "000002.SZ", "000004.SZ", "000006.SZ"]
        codes = base_codes + [f"{300000 + idx:06d}.SZ" for idx in range(max(0, count - len(base_codes)))]
        items = [[code, code.split(".")[0], f"name{idx}", "area", "industry", "主板", "20200101"] for idx, code in enumerate(codes[:count])]
        return FileLakeStore(self.root, self.catalog).fetch(
            "stock_basic",
            {"list_status": "L"},
            CodeDateMatrixFakeClient(fields, items),
        )

    def seed_trade_cal(self):
        fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
        items = [
            ["SSE", "20250101", "0", "20241231"],
            ["SSE", "20250102", "1", "20241231"],
            ["SSE", "20250103", "1", "20250102"],
            ["SSE", "20250104", "0", "20250103"],
            ["SSE", "20250105", "0", "20250103"],
        ]
        return FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20250101", "end_date": "20250105"},
            CodeDateMatrixFakeClient(fields, items),
        )

    def test_stk_managers_explicit_dates_plan_is_read_only(self):
        self.seed_stock_basic()
        before = self.counts()
        result = self.run_cli(
            "code-date-matrix-plan",
            "--api", "stk_managers",
            "--universe", "a_share_listed",
            "--limit-codes", "3",
            "--dates", "20250102,20250103",
            "--json",
        )
        after = self.counts()
        payload = json.loads(result.stdout)
        self.assertEqual(before, after)
        self.assertFalse(payload["blocked"])
        self.assertFalse(payload["execution_allowed"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["planned_codes"], 3)
        self.assertEqual(payload["planned_dates"], 2)
        self.assertEqual(payload["planned_jobs"], 6)
        self.assertEqual(payload["items"][0]["params"]["ann_date"], "20250102")
        self.assertFalse(payload["items"][0]["execution_allowed"])
        self.assertNotIn("secret-token-should-not-appear", result.stdout)
        self.assertNotIn("secret-token-should-not-appear", result.stderr)

    def test_namechange_and_stk_rewards_explicit_date_params(self):
        self.seed_stock_basic()
        namechange = CodeDateMatrixPlanner(self.root, self.catalog).plan(
            api_name="namechange",
            universe="a_share_listed",
            limit_codes=1,
            dates="20250102,20250103",
        )
        rewards = CodeDateMatrixPlanner(self.root, self.catalog).plan(
            api_name="stk_rewards",
            universe="a_share_listed",
            limit_codes=1,
            dates="20250102",
        )
        self.assertFalse(namechange.blocked)
        self.assertEqual(namechange.items[0].params["start_date"], "20250102")
        self.assertEqual(namechange.items[0].params["end_date"], "20250102")
        self.assertFalse(rewards.blocked)
        self.assertEqual(rewards.items[0].params["end_date"], "20250102")

    def test_trading_days_only_uses_local_trade_cal(self):
        self.seed_stock_basic()
        self.seed_trade_cal()
        before = self.counts()
        result = self.run_cli(
            "code-date-matrix-plan",
            "--api", "stk_managers",
            "--universe", "a_share_listed",
            "--limit-codes", "3",
            "--start-date", "20250101",
            "--end-date", "20250105",
            "--trading-days-only",
            "--calendar-exchange", "SSE",
            "--max-dates", "5",
            "--json",
        )
        after = self.counts()
        payload = json.loads(result.stdout)
        self.assertEqual(before, after)
        self.assertFalse(payload["blocked"])
        self.assertEqual(payload["planned_dates"], 2)
        self.assertEqual(sorted({item["date"] for item in payload["items"]}), ["20250102", "20250103"])
        self.assertIn("calendar_source=local trade_cal latest snapshot", payload["warnings"])

    def test_missing_trade_cal_and_missing_stock_basic_block(self):
        self.seed_stock_basic()
        no_calendar = CodeDateMatrixPlanner(self.root, self.catalog).plan(
            api_name="stk_managers",
            universe="a_share_listed",
            limit_codes=1,
            start_date="20250101",
            end_date="20250105",
            trading_days_only=True,
        )
        self.assertTrue(no_calendar.blocked)
        self.assertIn("trading-days-only requires local trade_cal latest snapshot", " ".join(no_calendar.summary.blocking_errors))

        self.tmp.cleanup()
        self.setUp()
        missing_stock = CodeDateMatrixPlanner(self.root, self.catalog).plan(
            api_name="stk_managers",
            universe="a_share_listed",
            limit_codes=1,
            dates="20250102",
        )
        self.assertTrue(missing_stock.blocked)
        self.assertEqual(missing_stock.summary.blocking_errors, ["missing_stock_basic_latest_snapshot"])

    def test_limits_and_disabled_or_unsupported_endpoint_block(self):
        self.seed_stock_basic()
        planner = CodeDateMatrixPlanner(self.root, self.catalog)
        self.assertIn("limit_codes_required", planner.plan(api_name="stk_managers", universe="a_share_listed", limit_codes=None, dates="20250102").summary.blocking_errors)
        self.assertIn("limit_codes_exceeds_phase_limit", " ".join(planner.plan(api_name="stk_managers", universe="a_share_listed", limit_codes=21, dates="20250102").summary.blocking_errors))
        self.assertIn("max_dates_exceeds_phase_limit", " ".join(planner.plan(api_name="stk_managers", universe="a_share_listed", limit_codes=1, dates="20250102", max_dates=21).summary.blocking_errors))
        self.assertIn("endpoint_disabled_inventory", planner.plan(api_name="dividend", universe="a_share_listed", limit_codes=1, dates="20250102").summary.blocking_errors)
        self.assertIn(
            "code_date_matrix_plan_not_supported_for_api:weekly",
            planner.plan(api_name="weekly", universe="a_share_listed", limit_codes=1, dates="20250102").summary.blocking_errors,
        )

    def test_candidate_jobs_above_phase_limit_are_truncated(self):
        self.seed_stock_basic(count=25)
        dates = ",".join(f"202501{day:02d}" for day in range(1, 21))
        before = self.counts()
        plan = CodeDateMatrixPlanner(self.root, self.catalog).plan(
            api_name="stk_managers",
            universe="a_share_listed",
            limit_codes=20,
            dates=dates,
        )
        after = self.counts()
        self.assertEqual(before, after)
        self.assertFalse(plan.blocked)
        self.assertEqual(plan.summary.candidate_jobs, 500)
        self.assertEqual(plan.summary.planned_codes, 20)
        self.assertEqual(plan.summary.planned_dates, 5)
        self.assertEqual(plan.summary.planned_jobs, 100)
        self.assertEqual(len(plan.items), 100)
        self.assertTrue(plan.summary.truncated_by_candidate_limit)

    def test_existing_active_data_becomes_skip_existing(self):
        self.seed_stock_basic()
        fields = ["ts_code", "ann_date", "name", "gender", "lev", "title", "edu", "national", "birthday", "begin_date", "end_date"]
        items = [["000001.SZ", "20250102", "manager", "M", "1", "title", "edu", "CN", "19800101", "20200101", None]]
        FileLakeStore(self.root, self.catalog).fetch(
            "stk_managers",
            {"ts_code": "000001.SZ", "ann_date": "20250102"},
            CodeDateMatrixFakeClient(fields, items),
        )
        before = self.counts()
        plan = CodeDateMatrixPlanner(self.root, self.catalog).plan(
            api_name="stk_managers",
            universe="a_share_listed",
            limit_codes=1,
            dates="20250102",
        )
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(plan.items[0].existing_status, "active_exists")
        self.assertEqual(plan.items[0].planned_action, "skip_existing")
        self.assertFalse(plan.items[0].would_require_real_request)

    def test_failed_staged_and_quarantined_statuses_are_reported(self):
        self.seed_stock_basic(count=3)
        planner = JobPlanner(self.root, self.catalog)
        rows = [
            ("000001.SZ", "20250102", "failed_exists", "retry_failed"),
            ("000002.SZ", "20250102", "staged_exists", "retry_failed"),
            ("000004.SZ", "20250102", "quarantined_exists", "blocked_quarantined"),
        ]
        for ts_code, date, status, _action in rows:
            fetch_plan = planner.plan_single_fetch("stk_managers", {"ts_code": ts_code, "ann_date": date})
            run_id = self.catalog.create_run("test")
            self.catalog.upsert_job(fetch_plan.job_key, run_id, "stk_managers", fetch_plan.params, fetch_plan.fields, "running")
            if status == "failed_exists":
                self.catalog.update_job_failed(fetch_plan.job_key, "failed for test", "rate_limited")
            elif status == "staged_exists":
                self.catalog.insert_file(
                    table_id=fetch_plan.table_id,
                    api_name="stk_managers",
                    content_type="lake",
                    file_format="parquet",
                    relative_path=fetch_plan.lake_path,
                    staged_path=None,
                    partition_values=fetch_plan.partition_values,
                    record_count=1,
                    source_item_count=1,
                    raw_event_count=None,
                    error_event_count=0,
                    size_bytes=1,
                    sha256="abc",
                    schema_id=None,
                    status="staged",
                    run_id=run_id,
                    job_key=fetch_plan.job_key,
                )
            elif status == "quarantined_exists":
                self.catalog.record_quarantine(run_id, fetch_plan.job_key, "stk_managers", "schema incompatible", "_quarantine/test", None, None)
        before = self.counts()
        plan = CodeDateMatrixPlanner(self.root, self.catalog).plan(
            api_name="stk_managers",
            universe="a_share_listed",
            limit_codes=3,
            dates="20250102",
        )
        after = self.counts()
        self.assertEqual(before, after)
        by_code = {item.ts_code: item for item in plan.items}
        self.assertEqual(by_code["000001.SZ"].existing_status, "failed_exists")
        self.assertEqual(by_code["000001.SZ"].planned_action, "retry_failed")
        self.assertEqual(by_code["000002.SZ"].existing_status, "staged_exists")
        self.assertEqual(by_code["000002.SZ"].planned_action, "retry_failed")
        self.assertEqual(by_code["000004.SZ"].existing_status, "quarantined_exists")
        self.assertEqual(by_code["000004.SZ"].planned_action, "blocked_quarantined")
        self.assertFalse(by_code["000004.SZ"].execution_allowed)


if __name__ == "__main__":
    unittest.main()
