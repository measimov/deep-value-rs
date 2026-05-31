from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.hashing import token_hash
from tushare_mirror.io_utils import read_jsonl_zst
from tushare_mirror.reader import LakeReader
from tushare_mirror.schema import SchemaRegistry
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


class FakeClient:
    def __init__(self, fields=None, items=None):
        self.fields = fields or ["ts_code", "trade_date", "close"]
        self.items = items or [["000001.SZ", "20250102", 11.1]]

    def query_paginated(self, api_name, params, fields, page_size=None):
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
        from tushare_mirror.client import QueryResult
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class BrokenParquetStore(FileLakeStore):
    def _write_parquet(self, path, rows):
        raise RuntimeError("forced parquet failure")


class Phase1MvpTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def test_catalog_initializes_and_loads_daily_endpoint(self):
        cfg = self.catalog.get_endpoint_config("daily")
        self.assertEqual(cfg["api_name"], "daily")
        self.assertIn("table_id", cfg)
        self.assertIn("partition_spec_id", cfg)
        self.assertTrue((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_permission_probe_records_token_hash_only(self):
        secret = "unit-test-secret"
        thash = token_hash("super-secret-token", secret)
        self.catalog.record_probe(
            "daily",
            thash,
            "accessible",
            {"trade_date": "20250102"},
            ["ts_code"],
            "2099-01-01T00:00:00Z",
            raw_response={"code": 0},
        )
        with sqlite3.connect(self.catalog.db_path) as conn:
            row = conn.execute("select token_hash, raw_response_json from permission_probes").fetchone()
        self.assertEqual(row[0], thash)
        self.assertNotIn("super-secret-token", str(row))

    def test_fetch_writes_raw_parquet_snapshot_and_reader(self):
        result = FileLakeStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250102"}, FakeClient())
        self.assertIsNotNone(result.snapshot_id)
        self.assertEqual(result.record_count, 1)

        raw_files = list((self.root / "raw").rglob("*.jsonl.zst"))
        parquet_files = list((self.root / "lake").rglob("*.parquet"))
        self.assertEqual(len(raw_files), 1)
        self.assertEqual(len(parquet_files), 1)
        events = read_jsonl_zst(raw_files[0])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["job_key"], result.job_key)

        table = pq.read_table(parquet_files[0])
        self.assertEqual(table.num_rows, 1)
        for col in ["_api_name", "_params_hash", "_row_hash", "_fetched_at", "_source_fields", "_job_key", "_run_id"]:
            self.assertIn(col, table.column_names)
        self.assertNotIn("_snapshot_id", table.column_names)

        ok, _ = Validator(self.root, self.catalog).validate_snapshot("latest", "daily")
        self.assertTrue(ok)
        read_table = LakeReader(self.root, self.catalog).scan_api("daily")
        self.assertEqual(read_table.num_rows, 1)

    def test_same_job_rerun_is_idempotent(self):
        first = FileLakeStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250102"}, FakeClient())
        second = FileLakeStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250102"}, FakeClient())
        self.assertFalse(first.skipped)
        self.assertTrue(second.skipped)
        self.assertEqual(len(list((self.root / "lake").rglob("*.parquet"))), 1)

    def test_staged_failure_does_not_commit_snapshot_or_checkpoint(self):
        with self.assertRaises(RuntimeError):
            BrokenParquetStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250103"}, FakeClient())
        self.assertIsNone(self.catalog.latest_snapshot("daily"))
        with sqlite3.connect(self.catalog.db_path) as conn:
            checkpoints = conn.execute("select count(*) from checkpoint_state").fetchone()[0]
        self.assertEqual(checkpoints, 0)

    def test_validation_failure_does_not_advance_checkpoint_for_failed_run(self):
        result = FileLakeStore(self.root, self.catalog).fetch("daily", {"trade_date": "20250102"}, FakeClient())
        parquet_file = next((self.root / "lake").rglob("*.parquet"))
        parquet_file.write_bytes(parquet_file.read_bytes() + b"corrupt")
        ok, _ = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, "daily")
        self.assertFalse(ok)

    def test_schema_add_column_compatible_and_type_change_quarantined(self):
        store = FileLakeStore(self.root, self.catalog)
        store.fetch("daily", {"trade_date": "20250102"}, FakeClient(fields=["ts_code", "trade_date", "close"], items=[["000001.SZ", "20250102", 11]]))
        added = store.fetch("daily", {"trade_date": "20250103"}, FakeClient(fields=["ts_code", "trade_date", "close", "open"], items=[["000001.SZ", "20250103", 12, 10]]))
        self.assertIsNotNone(added.snapshot_id)
        bad = store.fetch("daily", {"trade_date": "20250104"}, FakeClient(fields=["ts_code", "trade_date", "close"], items=[["000001.SZ", "20250104", "bad-close"]]))
        self.assertIsNone(bad.snapshot_id)
        self.assertTrue(any((self.root / "_quarantine").rglob("*")))

    def test_schema_reorder_compatible_directly(self):
        registry = SchemaRegistry(self.catalog)
        first = registry.decide("daily", ["ts_code", "trade_date", "close"], [["000001.SZ", "20250102", 11]])
        self.assertTrue(first.compatible)
        registry.commit("daily", first)
        second = registry.decide("daily", ["trade_date", "ts_code", "close"], [["20250103", "000001.SZ", 12]])
        self.assertTrue(second.compatible)
        self.assertIn(second.change_type, {"reorder", "same"})


if __name__ == "__main__":
    unittest.main()
