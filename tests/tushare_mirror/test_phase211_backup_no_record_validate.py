from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backup import BackupExecutor, BackupInspector, BackupPlanner, RestoreChecker
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.io_utils import sha256_file
from tushare_mirror.store import FileLakeStore


class NoRecordClient:
    def query_paginated(self, api_name, params, fields, page_size=None):
        if api_name == "trade_cal":
            response_fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
            response_items = [["SSE", "20250102", 1, "20241231"]]
        elif api_name == "daily_basic":
            date = params.get("trade_date") or "20250102"
            response_fields = ["ts_code", "trade_date", "close", "total_mv"]
            response_items = [["000001.SZ", date, 10.0, 100000.0]]
        else:
            raise AssertionError(f"unexpected API call: {api_name}")
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": response_fields, "items": response_items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=response_fields, items=response_items)


class Phase211BackupNoRecordValidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "source-lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, root: Path, *args, check=True):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def counts(self, root: Path) -> dict[str, int]:
        with sqlite3.connect(root / "_catalog" / "catalog.sqlite") as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
                "validation_failures": conn.execute("select count(*) from validation_failures").fetchone()[0],
            }

    def seed_source(self) -> None:
        store = FileLakeStore(self.root, self.catalog)
        client = NoRecordClient()
        store.fetch("trade_cal", {"exchange": "SSE", "start_date": "20250102", "end_date": "20250102"}, client)
        store.fetch("daily_basic", {"trade_date": "20250102"}, client)

    def backup_to(self, name: str = "backup") -> Path:
        target = self.base / name
        plan = BackupPlanner(self.root, self.catalog).plan(target)
        result = BackupExecutor(self.root, self.catalog).backup(plan)
        self.assertEqual(result.status, "succeeded")
        return target

    def test_backup_root_no_record_validate_preserves_manifest_checksum(self):
        self.seed_source()
        source_counts = self.counts(self.root)
        backup_root = self.backup_to()
        catalog_path = backup_root / "_catalog" / "catalog.sqlite"
        manifest_catalog_hash = json.loads((backup_root / "manifest.json").read_text())["catalog"]["sha256"]
        self.assertEqual(sha256_file(catalog_path), manifest_catalog_hash)
        backup_counts = self.counts(backup_root)

        no_record = json.loads(self.run_cli(backup_root, "validate", "--snapshot", "latest", "--no-record", "--json").stdout)
        self.assertEqual(no_record["status"], "succeeded")
        self.assertEqual(len(no_record["results"]), 2)
        self.assertTrue(all(row["validation_id"] is None for row in no_record["results"]))
        self.assertEqual(self.counts(backup_root), backup_counts)
        self.assertEqual(sha256_file(catalog_path), manifest_catalog_hash)
        self.assertEqual(RestoreChecker().check(backup_root).status, "succeeded")
        self.assertEqual(BackupInspector().inspect(backup_root).catalog_checksum_status, "matched")

        normal = json.loads(self.run_cli(backup_root, "validate", "--snapshot", "latest", "--json").stdout)
        self.assertEqual(normal["status"], "succeeded")
        self.assertTrue(all(row["validation_id"] for row in normal["results"]))
        mutated_counts = self.counts(backup_root)
        self.assertEqual(mutated_counts["validations"], backup_counts["validations"] + 2)
        self.assertNotEqual(sha256_file(catalog_path), manifest_catalog_hash)
        check = RestoreChecker().check(backup_root)
        self.assertEqual(check.status, "failed")
        self.assertEqual(check.catalog_checksum_status, "mismatch")
        self.assertTrue(check.possible_mutation)
        self.assertIn("modified after backup creation", json.dumps(check.to_dict()))
        inspect = BackupInspector().inspect(backup_root)
        self.assertEqual(inspect.catalog_checksum_status, "mismatch")
        self.assertTrue(inspect.possible_mutation)
        self.assertEqual(self.counts(self.root), source_counts)

    def test_no_record_failure_returns_nonzero_without_validation_rows(self):
        store = FileLakeStore(self.root, self.catalog)
        store.fetch("daily_basic", {"trade_date": "20250102"}, NoRecordClient())
        before = self.counts(self.root)
        lake_file = next(row for row in self.catalog.files_for_snapshot(self.catalog.latest_snapshot("daily_basic")["snapshot_id"]) if row["content_type"] == "lake")
        (self.root / lake_file["relative_path"]).unlink()

        failed = self.run_cli(self.root, "validate", "--api", "daily_basic", "--snapshot", "latest", "--no-record", "--json", check=False)
        self.assertNotEqual(failed.returncode, 0)
        payload = json.loads(failed.stdout)
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["results"][0]["validation_id"], None)
        self.assertEqual(payload["results"][0]["failure_count"], 1)
        self.assertEqual(self.counts(self.root), before)

        normal = self.run_cli(self.root, "validate", "--api", "daily_basic", "--snapshot", "latest", "--json", check=False)
        self.assertNotEqual(normal.returncode, 0)
        after = self.counts(self.root)
        self.assertEqual(after["validations"], before["validations"] + 1)
        self.assertEqual(after["validation_failures"], before["validation_failures"] + 1)


    def test_backup_root_read_only_observability_commands_preserve_manifest_checksum(self):
        self.seed_source()
        backup_root = self.backup_to()
        catalog_path = backup_root / "_catalog" / "catalog.sqlite"
        manifest_catalog_hash = json.loads((backup_root / "manifest.json").read_text())["catalog"]["sha256"]
        before = self.counts(backup_root)

        commands = [
            ("catalog-version",),
            ("catalog-inspect",),
            ("show-snapshots", "--latest"),
            ("show-runs", "--limit", "20"),
            ("show-jobs", "--limit", "20"),
            ("show-validations", "--limit", "20"),
            ("show-permissions", "--limit", "20"),
            ("list-files", "--api", "daily_basic", "--snapshot", "latest"),
            ("coverage", "--api", "daily_basic", "--dates", "20250102"),
        ]
        for command in commands:
            with self.subTest(command=command):
                self.run_cli(backup_root, *command)
                self.assertEqual(self.counts(backup_root), before)
                self.assertEqual(sha256_file(catalog_path), manifest_catalog_hash)
                self.assertEqual(RestoreChecker().check(backup_root).status, "succeeded")



if __name__ == "__main__":
    unittest.main()
