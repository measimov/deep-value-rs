from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from http.client import IncompleteRead
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult, TushareError, TUSHARE_API_URL
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator
from tushare_mirror.reader import LakeReader


class FakeClient:
    def __init__(self, fields=None, items=None):
        self.fields = fields or ["ts_code", "trade_date", "close"]
        self.items = items or [["000001.SZ", "20250102", 11.1]]
        self.calls = 0

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        response_fields = self.fields
        response_items = self.items
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": response_items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class FlakyRateLimitClient(FakeClient):
    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        if self.calls == 1:
            raise TushareError(api_name, -2002, "每分钟请求限制", {"code": -2002, "msg": "每分钟请求限制"})
        response_fields = self.fields
        response_items = self.items
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": response_items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class PermissionDeniedClient(FakeClient):
    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        raise TushareError(api_name, -2001, "权限不足", {"code": -2001, "msg": "权限不足"})


class IncompleteReadOnceClient(FakeClient):
    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        if self.calls == 1:
            raise IncompleteRead(b"partial-response")
        response_fields = self.fields
        response_items = self.items
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": response_items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class BadHashCatalog(CatalogStore):
    def insert_file(self, **kwargs):
        file_id = super().insert_file(**kwargs)
        if kwargs.get("content_type") == "lake":
            with self.transaction() as conn:
                conn.execute("update files set sha256='bad' where file_id=?", (file_id,))
        return file_id


class Phase11HardeningTests(unittest.TestCase):
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

    def test_default_tushare_api_url_uses_https(self):
        self.assertEqual(TUSHARE_API_URL, "https://api.tushare.pro")

    def test_catalog_cli_dry_run_version_and_backup(self):
        self.catalog.record_probe(
            "daily",
            "hashed-token",
            "accessible",
            {"trade_date": "20250102"},
            ["ts_code"],
            "2099-01-01T00:00:00Z",
            row_count=1,
        )
        dry = self.run_cli("fetch", "--api", "daily", "--params", '{"trade_date":"20250102"}', "--dry-run", "--json")
        plan = json.loads(dry.stdout)
        self.assertEqual(plan["api_name"], "daily")
        self.assertEqual(plan["permission_status"], "accessible")
        self.assertFalse(plan["existing_active_data"])
        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from jobs").fetchone()[0], 0)
            self.assertEqual(conn.execute("select count(*) from snapshots").fetchone()[0], 0)

        inspect = json.loads(self.run_cli("catalog-inspect", "--json").stdout)
        self.assertEqual(inspect["schema_version"], 2)
        self.assertEqual(inspect["endpoint_count"], 30)
        self.assertEqual(self.run_cli("catalog-version").stdout.strip(), "2")
        backup_path = self.root / "catalog-copy.sqlite"
        self.run_cli("catalog-backup", "--output", str(backup_path))
        with sqlite3.connect(backup_path) as conn:
            self.assertEqual(conn.execute("select count(*) from endpoints").fetchone()[0], 30)
            self.assertEqual(conn.execute("select value from catalog_meta where key='catalog_schema_version'").fetchone()[0], "2")
        permissions = json.loads(self.run_cli("show-permissions", "--api", "daily", "--json").stdout)
        self.assertEqual(permissions[0]["row_count"], 1)
        self.assertNotIn("super-secret", str(permissions))

    def test_retry_rate_limit_then_success_and_permission_not_retried(self):
        flaky = FlakyRateLimitClient()
        result = FileLakeStore(self.root, self.catalog, retry_sleep=lambda _: None).fetch("daily", {"trade_date": "20250102"}, flaky, max_attempts=2)
        self.assertIsNotNone(result.snapshot_id)
        self.assertEqual(flaky.calls, 2)
        row = self.catalog.get_job(result.job_key)
        self.assertEqual(row["status"], "done")
        self.assertIsNone(row["last_error_type"])

        denied = PermissionDeniedClient()
        with self.assertRaises(TushareError):
            FileLakeStore(self.root, self.catalog, retry_sleep=lambda _: None).fetch("daily", {"trade_date": "20250103"}, denied, max_attempts=3)
        self.assertEqual(denied.calls, 1)
        jobs = self.catalog.list_jobs("daily", 10)
        failed = [j for j in jobs if j["status"] == "failed"]
        self.assertEqual(failed[0]["last_error_type"], "permission_denied")

    def test_retry_incomplete_read_as_network_error(self):
        flaky = IncompleteReadOnceClient()
        result = FileLakeStore(self.root, self.catalog, retry_sleep=lambda _: None).fetch("daily", {"trade_date": "20250106"}, flaky, max_attempts=2)
        self.assertIsNotNone(result.snapshot_id)
        self.assertEqual(flaky.calls, 2)
        row = self.catalog.get_job(result.job_key)
        self.assertEqual(row["status"], "done")
        self.assertIsNone(row["last_error_type"])

    def test_idempotent_rerun_and_snapshot_files_unique(self):
        store = FileLakeStore(self.root, self.catalog)
        first = store.fetch("daily", {"trade_date": "20250102"}, FakeClient())
        second = store.fetch("daily", {"trade_date": "20250102"}, FakeClient())
        self.assertFalse(first.skipped)
        self.assertTrue(second.skipped)
        with sqlite3.connect(self.catalog.db_path) as conn:
            rows = conn.execute("select snapshot_id, count(*), count(distinct file_id) from snapshot_files group by snapshot_id").fetchall()
        self.assertTrue(rows)
        for _, total, distinct_count in rows:
            self.assertEqual(total, distinct_count)
        files = self.catalog.active_files_for_job(first.job_key, "daily", "lake")
        self.assertEqual(len(files), 1)

    def test_validation_failure_no_checkpoint_and_rerun_succeeds(self):
        bad_catalog = BadHashCatalog(self.root)
        result = FileLakeStore(self.root, bad_catalog).fetch("daily", {"trade_date": "20250105"}, FakeClient())
        self.assertIsNone(result.snapshot_id)
        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from checkpoint_state").fetchone()[0], 0)
        rerun = FileLakeStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250105"}, FakeClient())
        self.assertIsNotNone(rerun.snapshot_id)
        with sqlite3.connect(self.catalog.db_path) as conn:
            self.assertEqual(conn.execute("select count(*) from checkpoint_state").fetchone()[0], 1)

    def test_schema_incompatible_then_compatible_rerun_succeeds(self):
        store = FileLakeStore(self.root, self.catalog)
        store.fetch("daily", {"trade_date": "20250102"}, FakeClient(fields=["ts_code", "trade_date", "close"], items=[["000001.SZ", "20250102", 11]]))
        bad = store.fetch("daily", {"trade_date": "20250104"}, FakeClient(fields=["ts_code", "trade_date", "close"], items=[["000001.SZ", "20250104", "bad-close"]]))
        self.assertIsNone(bad.snapshot_id)
        good = store.fetch("daily", {"trade_date": "20250104"}, FakeClient(fields=["ts_code", "trade_date", "close"], items=[["000001.SZ", "20250104", 13]]))
        self.assertIsNotNone(good.snapshot_id)
        table = LakeReader(self.root, self.catalog).scan_api("daily", filters={"trade_date": "20250104"})
        self.assertEqual(table.num_rows, 1)

    def test_historical_trade_cal_string_is_open_normalizes_to_existing_int_schema(self):
        store = FileLakeStore(self.root, self.catalog)
        fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
        first = store.fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20250101", "end_date": "20250102"},
            FakeClient(fields=fields, items=[["SSE", "20250102", 1, "20241231"]]),
        )
        self.assertIsNotNone(first.snapshot_id)
        second = store.fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "19900101", "end_date": "19900120"},
            FakeClient(fields=fields, items=[["SSE", "19900119", "1", "19900118"]]),
        )
        self.assertIsNotNone(second.snapshot_id)
        self.assertFalse(any((self.root / "_quarantine").rglob("*")))
        table = LakeReader(self.root, self.catalog).scan_api("trade_cal", filters={"cal_date": "19900119"})
        self.assertEqual(table.num_rows, 1)
        self.assertIn("int", str(table.schema.field("is_open").type))
        self.assertEqual(table.column("is_open").to_pylist(), [1])

    def test_historical_weekly_monthly_numeric_strings_normalize_to_existing_float_schema(self):
        fields = ["ts_code", "trade_date", "close", "open", "high", "low", "pre_close", "change", "pct_chg", "vol", "amount"]
        for api_name in ("weekly", "monthly"):
            with self.subTest(api_name=api_name):
                store = FileLakeStore(self.root, self.catalog)
                store.fetch(
                    api_name,
                    {"trade_date": "20250103"},
                    FakeClient(fields=fields, items=[["000001.SZ", "20250103", 11.1, 10.1, 12.1, 9.1, 10.0, 1.1, 11.0, 100.0, 1000.0]]),
                )
                result = store.fetch(
                    api_name,
                    {"trade_date": "19900105"},
                    FakeClient(fields=fields, items=[["000001.SZ", "19900105", "12.2", "11.2", "13.2", "10.2", "11.1", "1.1", "9.91", "200.0", "2000.0"]]),
                )
                self.assertIsNotNone(result.snapshot_id)
                table = LakeReader(self.root, self.catalog).scan_api(api_name, filters={"trade_date": "19900105"})
                self.assertEqual(table.num_rows, 1)
                self.assertIn(str(table.schema.field("close").type), {"double", "float"})
                self.assertEqual(table.column("close").to_pylist(), [12.2])

    def test_reader_columns_filters_snapshot_id_and_status_filter(self):
        store = FileLakeStore(self.root, self.catalog)
        first = store.fetch("daily", {"trade_date": "20250102"}, FakeClient(items=[["000001.SZ", "20250102", 11.1]]))
        store.fetch("daily", {"trade_date": "20250103"}, FakeClient(items=[["000002.SZ", "20250103", 12.2]]))
        latest_filtered = LakeReader(self.root, self.catalog).scan_api("daily", filters={"trade_date": "20250102"}, columns=["ts_code"])
        self.assertEqual(latest_filtered.num_rows, 1)
        self.assertEqual(latest_filtered.column_names, ["ts_code"])
        historical = LakeReader(self.root, self.catalog).scan_api("daily", snapshot_id=first.snapshot_id)
        self.assertEqual(historical.num_rows, 1)

        latest = self.catalog.latest_snapshot("daily")
        lake_files = self.catalog.files_for_snapshot(latest["snapshot_id"], content_type="lake")
        self.catalog.update_file_status(lake_files[0]["file_id"], "quarantined", None)
        visible = LakeReader(self.root, self.catalog).list_active_files("daily")
        self.assertEqual(len(visible), len(lake_files) - 1)

    def test_observability_lists_after_fetch_and_validation(self):
        result = FileLakeStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250102"}, FakeClient())
        ok, _ = Validator(self.root, self.catalog).validate_snapshot("latest", "daily")
        self.assertTrue(ok)
        self.assertEqual(self.catalog.list_runs("daily", 20)[0]["job_count"], 1)
        self.assertEqual(self.catalog.list_jobs("daily", 20)[0]["job_key"], result.job_key)
        self.assertEqual(self.catalog.list_snapshots("daily", 20)[0]["snapshot_id"], result.snapshot_id)
        self.assertEqual(self.catalog.list_validations("daily", 20)[0]["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
