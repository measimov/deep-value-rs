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
from tushare_mirror.coverage import CoverageReporter
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.hashing import token_hash
from tushare_mirror.reader import LakeReader
from tushare_mirror.store import FileLakeStore


class RestoreRootClient:
    def query_paginated(self, api_name, params, fields, page_size=None):
        if api_name == "trade_cal":
            response_fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
            response_items = [
                ["SSE", "20250101", 0, "20241231"],
                ["SSE", "20250102", 1, "20241231"],
                ["SSE", "20250103", 1, "20250102"],
            ]
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


class Phase29RestoreAsRootTests(unittest.TestCase):
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
            }

    def seed_source(self) -> None:
        store = FileLakeStore(self.root, self.catalog)
        client = RestoreRootClient()
        store.fetch("trade_cal", {"exchange": "SSE", "start_date": "20250101", "end_date": "20250103"}, client)
        store.fetch("daily_basic", {"trade_date": "20250102"}, client)
        self.catalog.record_probe(
            "daily_basic",
            token_hash("super-secret-token", "phase29-test-secret"),
            "accessible",
            {"trade_date": "20250102"},
            ["ts_code", "trade_date"],
            "2026-01-01T00:00:00Z",
            raw_response={"status": "ok"},
            row_count=1,
        )

    def backup_to(self, target_name: str) -> Path:
        target = self.base / target_name
        plan = BackupPlanner(self.root, self.catalog).plan(target)
        result = BackupExecutor(self.root, self.catalog).backup(plan)
        self.assertEqual(result.status, "succeeded")
        return target

    def coverage_summary(self, root: Path) -> dict[str, object]:
        report = CoverageReporter(root, CatalogStore(root)).report(
            "daily_basic",
            start_date="20250101",
            end_date="20250103",
            trading_days_only=True,
            calendar_exchange="SSE",
        )
        return {
            "total_dates": report.total_dates,
            "covered_dates": report.covered_dates,
            "missing_dates": report.missing_dates,
            "coverage_ratio": report.coverage_ratio,
            "statuses": [(item.date, item.existing_status, item.planned_action) for item in report.items],
        }

    def test_backup_root_can_be_read_validated_and_covered_without_source_root(self):
        self.seed_source()
        source_counts = self.counts(self.root)
        source_coverage = self.coverage_summary(self.root)

        backup_root = self.backup_to("backup-root")
        backup_counts = self.counts(backup_root)
        manifest_text = (backup_root / "manifest.json").read_text()
        manifest = json.loads(manifest_text)
        self.assertEqual(manifest["source_root"], str(self.root))
        self.assertNotIn("super-secret-token", manifest_text)
        self.assertNotIn(b"super-secret-token", (backup_root / "_catalog" / "catalog.sqlite").read_bytes())

        check = RestoreChecker().check(backup_root)
        self.assertEqual(check.status, "succeeded")
        self.assertEqual(self.counts(backup_root), backup_counts)
        cli_check = self.run_cli(backup_root, "restore-check", "--backup", str(backup_root)).stdout
        self.assertIn("restore_check_writes", cli_check)
        self.assertIn("validation_runs", cli_check)
        self.assertEqual(self.counts(backup_root), backup_counts)

        inspect = json.loads(self.run_cli(backup_root, "catalog-inspect", "--json").stdout)
        self.assertEqual(inspect["schema_version"], 2)
        self.assertEqual(inspect["file_count"], 4)
        self.assertEqual(inspect["snapshot_count"], 2)
        snapshots = json.loads(self.run_cli(backup_root, "show-snapshots", "--latest", "--json").stdout)
        self.assertEqual({row["api_name"] for row in snapshots}, {"daily_basic", "trade_cal"})
        for api_name in ("trade_cal", "daily_basic"):
            rows = json.loads(self.run_cli(backup_root, "list-files", "--api", api_name, "--snapshot", "latest", "--json").stdout)
            self.assertEqual(len(rows), 1)
            self.assertTrue((backup_root / rows[0]["relative_path"]).exists())

        table = LakeReader(backup_root, CatalogStore(backup_root)).scan_api(
            "daily_basic",
            filters={"trade_date": "20250102"},
            columns=["ts_code", "trade_date"],
        )
        self.assertEqual(table.num_rows, 1)
        self.assertEqual(table.column_names, ["ts_code", "trade_date"])
        self.assertEqual(self.coverage_summary(backup_root), source_coverage)

        validation_before = self.counts(backup_root)["validations"]
        validation = json.loads(self.run_cli(backup_root, "validate", "--snapshot", "latest", "--json").stdout)
        self.assertEqual(validation["status"], "succeeded")
        self.assertEqual(len(validation["results"]), 2)
        self.assertEqual(self.counts(backup_root)["validations"], validation_before + 2)
        self.assertEqual(self.counts(self.root), source_counts)

        independent_backup = self.backup_to("backup-source-gone")
        moved_source = self.base / "source-lake-moved"
        shutil.move(str(self.root), str(moved_source))
        self.assertFalse(Path(manifest["source_root"]).exists())
        self.assertEqual(RestoreChecker().check(independent_backup).status, "succeeded")
        independent_reader = LakeReader(independent_backup, CatalogStore(independent_backup))
        self.assertEqual(independent_reader.scan_api("daily_basic").num_rows, 1)
        self.assertEqual(self.coverage_summary(independent_backup), source_coverage)


if __name__ == "__main__":
    unittest.main()
