from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backup import BackupExecutor, BackupPlanner, RestoreChecker
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
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


class Phase28BackupRestoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args, root=None, check=True):
        command = [sys.executable, "-m", "tushare_mirror"]
        if root is not None:
            command.extend(["--root", str(root)])
        else:
            command.extend(["--root", str(self.root)])
        command.extend(args)
        return subprocess.run(
            command,
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

    def fetch_daily(self, dates=("20250102",)):
        store = FileLakeStore(self.root, self.catalog)
        client = EchoClient()
        for date in dates:
            store.fetch("daily", {"trade_date": date}, client)

    def backup(self, target_name="backup"):
        target = self.base / target_name
        plan = BackupPlanner(self.root, self.catalog).plan(target)
        result = BackupExecutor(self.root, self.catalog).backup(plan)
        return target, plan, result

    def manifest(self, target):
        return json.loads((target / "manifest.json").read_text())

    def write_manifest(self, target, manifest):
        (target / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2))

    def test_backup_plan_empty_catalog_no_active_snapshots_and_no_target(self):
        target = self.base / "backup-empty"
        before = self.counts()
        plan = BackupPlanner(self.root, self.catalog).plan(target)
        self.assertEqual(plan.rejected_reason, "no_active_snapshots")
        self.assertEqual(plan.file_count, 0)
        self.assertFalse(target.exists())
        self.assertEqual(self.counts(), before)
        payload = json.loads(self.run_cli("backup-plan", "--target", str(target), "--json").stdout)
        self.assertEqual(payload["rejected_reason"], "no_active_snapshots")
        self.assertFalse(target.exists())

    def test_backup_plan_with_active_snapshot_is_read_only(self):
        self.fetch_daily()
        target = self.base / "backup-plan"
        before = self.counts()
        plan = BackupPlanner(self.root, self.catalog).plan(target)
        self.assertIsNone(plan.rejected_reason)
        self.assertEqual(plan.file_count, 2)
        self.assertEqual(plan.raw_file_count, 1)
        self.assertEqual(plan.lake_file_count, 1)
        self.assertTrue(plan.catalog_included)
        self.assertGreater(plan.total_size_bytes, 0)
        self.assertFalse(target.exists())
        self.assertEqual(self.counts(), before)
        payload = json.loads(self.run_cli("backup-plan", "--target", str(target), "--json").stdout)
        self.assertEqual(payload["file_count"], 2)
        self.assertGreater(payload["total_size_bytes"], 0)

    def test_backup_success_manifest_restore_check_overwrite_and_source_counts(self):
        self.fetch_daily(("20250102", "20250103"))
        source_counts = self.counts()
        target, plan, result = self.backup()
        self.assertEqual(result.status, "succeeded")
        self.assertTrue((target / "manifest.json").exists())
        self.assertTrue((target / "_catalog" / "catalog.sqlite").exists())
        self.assertTrue((target / "_catalog" / "endpoints" / "stock.yaml").exists())
        manifest = self.manifest(target)
        self.assertEqual(manifest["manifest_version"], 1)
        self.assertEqual(manifest["file_count"], plan.file_count)
        self.assertEqual(len(manifest["files"]), 4)
        self.assertIn("catalog", manifest)
        self.assertIn("endpoint_configs", manifest)
        self.assertNotIn("TUSHARE_TOKEN", json.dumps(manifest))
        self.assertEqual(self.counts(), source_counts)
        check = RestoreChecker().check(target)
        self.assertEqual(check.status, "succeeded")
        self.assertEqual(check.checked_file_count, 4)
        self.assertEqual(check.checked_raw_file_count, 2)
        self.assertEqual(check.checked_lake_file_count, 2)

        empty_target = self.base / "empty-backup-target"
        empty_target.mkdir()
        empty_backup = self.run_cli("backup", "--target", str(empty_target))
        self.assertIn("status", empty_backup.stdout)
        self.assertEqual(RestoreChecker().check(empty_target).status, "succeeded")

        no_overwrite = self.run_cli("backup", "--target", str(target), check=False)
        self.assertNotEqual(no_overwrite.returncode, 0)
        self.assertIn("already exists", no_overwrite.stderr)
        inside_source = self.run_cli("backup", "--target", str(self.root / "backup-inside-source"), check=False)
        self.assertNotEqual(inside_source.returncode, 0)
        self.assertIn("inside the source root", inside_source.stderr)
        overwritten = self.run_cli("backup", "--target", str(target), "--overwrite")
        self.assertIn("status", overwritten.stdout)
        self.assertEqual(RestoreChecker().check(target).status, "succeeded")
        file_target = self.base / "backup-file-target"
        file_target.write_text("old backup placeholder")
        overwritten_file = self.run_cli("backup", "--target", str(file_target), "--overwrite")
        self.assertIn("status", overwritten_file.stdout)
        self.assertTrue(file_target.is_dir())
        self.assertEqual(RestoreChecker().check(file_target).status, "succeeded")
        self.assertEqual(self.counts(), source_counts)

    def test_restore_check_detects_missing_and_tampered_files(self):
        self.fetch_daily()
        missing_target, _, _ = self.backup("backup-missing")
        manifest = self.manifest(missing_target)
        first_file = missing_target / manifest["files"][0]["backup_relative_path"]
        first_file.unlink()
        missing = RestoreChecker().check(missing_target)
        self.assertEqual(missing.status, "failed")
        self.assertGreater(missing.missing_file_count, 0)

        tamper_target, _, _ = self.backup("backup-tamper")
        tamper_manifest = self.manifest(tamper_target)
        raw = next(item for item in tamper_manifest["files"] if item["storage_layer"] == "raw")
        raw_path = tamper_target / raw["backup_relative_path"]
        raw_path.write_bytes(b"not zstd jsonl")
        tampered = RestoreChecker().check(tamper_target)
        self.assertEqual(tampered.status, "failed")
        self.assertGreater(tampered.checksum_failure_count, 0)
        self.assertGreater(tampered.raw_failure_count, 0)

        parquet_target, _, _ = self.backup("backup-parquet")
        parquet_manifest = self.manifest(parquet_target)
        lake = next(item for item in parquet_manifest["files"] if item["storage_layer"] == "lake")
        lake_path = parquet_target / lake["backup_relative_path"]
        lake_path.write_bytes(b"not parquet")
        bad_parquet = RestoreChecker().check(parquet_target)
        self.assertEqual(bad_parquet.status, "failed")
        self.assertGreater(bad_parquet.parquet_failure_count, 0)

    def test_restore_check_detects_manifest_count_mismatches_without_source_root(self):
        self.fetch_daily()
        target, _, _ = self.backup("backup-counts")
        manifest = self.manifest(target)
        raw = next(item for item in manifest["files"] if item["storage_layer"] == "raw")
        raw["raw_event_count"] = int(raw["raw_event_count"] or 0) + 1
        lake = next(item for item in manifest["files"] if item["storage_layer"] == "lake")
        lake["record_count"] = int(lake["record_count"] or 0) + 1
        self.write_manifest(target, manifest)
        shutil.rmtree(self.root)
        check = RestoreChecker().check(target)
        self.assertEqual(check.status, "failed")
        self.assertEqual(check.raw_event_count_failure_count, 1)
        self.assertEqual(check.record_count_failure_count, 1)
        self.assertEqual(check.file_count_failure_count, 0)

    def test_cli_backup_and_restore_check(self):
        self.fetch_daily()
        target = self.base / "backup-cli"
        plan = json.loads(self.run_cli("backup-plan", "--target", str(target), "--json").stdout)
        self.assertEqual(plan["file_count"], 2)
        self.assertFalse(target.exists())
        backup = json.loads(self.run_cli("backup", "--target", str(target), "--json").stdout)
        self.assertEqual(backup["status"], "succeeded")
        check = json.loads(self.run_cli("restore-check", "--backup", str(target), "--json", root=self.base / "unused-root").stdout)
        self.assertEqual(check["status"], "succeeded")
        table = self.run_cli("restore-check", "--backup", str(target), root=self.base / "unused-root").stdout
        self.assertIn("catalog_status", table)


if __name__ == "__main__":
    unittest.main()
