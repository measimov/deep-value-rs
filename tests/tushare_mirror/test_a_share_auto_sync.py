from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import MirrorAutoSyncReporter, MirrorOrchestrator


class AutoSyncFakeClient:
    token = "fake-token-for-hash-only"

    def __init__(self):
        self.request_calls: list[str] = []
        self.query_calls: list[tuple[str, dict]] = []

    def request(self, api_name, params, fields=None):
        self.request_calls.append(api_name)
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.query_calls.append((api_name, dict(params)))
        fields_list = list(fields or [])
        if api_name == "trade_cal":
            fields_list = ["exchange", "cal_date", "is_open", "pretrade_date"]
            items = self._calendar_rows(params)
        else:
            items = [self._row(params, fields_list)]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": fields_list, "items": items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=fields_list, items=items)

    def _calendar_rows(self, params):
        start = datetime.strptime(params.get("start_date") or "20250201", "%Y%m%d")
        end = datetime.strptime(params.get("end_date") or "20250228", "%Y%m%d")
        rows = []
        current = start
        previous_open = "20250131"
        while current <= end:
            cal_date = current.strftime("%Y%m%d")
            is_open = 1 if current.weekday() < 5 else 0
            rows.append(["SSE", cal_date, is_open, previous_open])
            if is_open:
                previous_open = cal_date
            current += timedelta(days=1)
        return rows

    def _row(self, params, fields):
        values = []
        for field in fields:
            if field in {"ts_code", "index_code", "code"}:
                values.append("000001.SH")
            elif field in {"trade_date", "ann_date", "end_date", "start_date", "cal_date", "in_date", "out_date", "list_date", "setup_date", "base_date", "exp_date"}:
                values.append(params.get(field) or params.get("trade_date") or params.get("end_date") or "20250203")
            elif field == "exchange":
                values.append(params.get("exchange") or "SSE")
            elif field in {"is_open", "is_new", "is_pub"}:
                values.append("1")
            elif field in {"name", "symbol", "area", "industry", "market", "title", "gender", "lev", "edu", "national", "birthday", "begin_date", "resume", "change_reason", "suspend_timing", "suspend_type", "chairman", "manager", "secretary", "province", "city", "introduction", "website", "email", "office", "main_business", "business_scope", "fullname", "publisher", "index_type", "category", "weight_rule", "desc", "src", "level", "industry_name", "industry_code", "parent_code", "type"}:
                values.append("x")
            else:
                values.append(1.0)
        return values


class AShareAutoSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.root.mkdir()
        self.backup.mkdir()
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

    def test_auto_sync_dry_run_is_read_only_and_windows_are_bounded(self):
        before = self.counts()
        state = self.base / "state.json"
        result = self.run_cli(
            "mirror-auto-sync",
            "--root",
            str(self.root),
            "--backup",
            str(self.backup),
            "--scope",
            "a-share-low-risk",
            "--from-date",
            "20250201",
            "--to-date",
            "20250305",
            "--window-days",
            "20",
            "--max-jobs-per-api",
            "20",
            "--state",
            str(state),
            "--json",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["report_version"], "mirror-auto-sync/v1")
        self.assertEqual(payload["status"], "planned")
        self.assertEqual(payload["planned_window_count"], 2)
        self.assertFalse(payload["execute"])
        self.assertFalse(state.exists())
        self.assertEqual(before, self.counts())
        self.assertTrue(all(window["status"] == "planned" for window in payload["windows"]))

    def test_execute_requires_confirmation_and_state(self):
        before = self.counts()
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="a-share-low-risk",
            from_date="20250201",
            to_date="20250220",
            window_days=20,
            max_jobs_per_api=20,
            execute=True,
            client=AutoSyncFakeClient(),
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("--execute requires --confirm-auto-sync", result.blocking_errors)
        self.assertIn("--execute requires --state for checkpoint/resume", result.blocking_errors)
        self.assertEqual(before, self.counts())

    def test_fake_execute_writes_checkpoint_and_excludes_plan_only_endpoints(self):
        state = self.base / "auto-sync-state.json"
        client = AutoSyncFakeClient()
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="a-share-low-risk",
            from_date="20250201",
            to_date="20250220",
            window_days=20,
            max_jobs_per_api=20,
            state=state,
            execute=True,
            confirm_auto_sync=True,
            max_attempts=1,
            retry_backoff_seconds=0,
            client=client,
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "succeeded", result.to_dict())
        self.assertEqual(result.executed_window_count, 1)
        self.assertEqual(result.next_start_date, "20250221")
        state_payload = json.loads(state.read_text())
        self.assertEqual(state_payload["next_start_date"], "20250221")
        called = set(client.request_calls) | {api for api, _ in client.query_calls}
        self.assertFalse({"top10_holders", "concept_detail", "index_weight", "ths_member"} & called)

    def test_february_pilot_executes_dynamic_weekly_dates(self):
        result = MirrorOrchestrator(self.root, self.catalog, AutoSyncFakeClient(), sleep=lambda _: None).run(
            scope="a-share-low-risk",
            mode="pilot",
            start_date="20250201",
            end_date="20250220",
            max_jobs_per_api=20,
        )
        self.assertEqual(result.status, "succeeded")
        by_endpoint = {item["endpoint"]: item for item in result.summary["items"]}
        self.assertGreater(by_endpoint["weekly"]["executed_jobs"], 0)
        self.assertGreater(by_endpoint["index_weekly"]["executed_jobs"], 0)
        self.assertEqual(by_endpoint["monthly"]["executed_jobs"], 0)
        self.assertEqual(by_endpoint["index_monthly"]["executed_jobs"], 0)


if __name__ == "__main__":
    unittest.main()
