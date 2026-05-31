from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.backup import BackupExecutor, BackupInspector, BackupManifestValidator, BackupPlanner, RestoreChecker
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.hashing import token_hash
from tushare_mirror.store import FileLakeStore


class ManifestClient:
    def query_paginated(self, api_name, params, fields, page_size=None):
        if api_name == "trade_cal":
            response_fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
            response_items = [
                ["SSE", "20250101", 0, "20241231"],
                ["SSE", "20250102", 1, "20241231"],
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


class Phase210BackupManifestTests(unittest.TestCase):
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
        client = ManifestClient()
        store.fetch("trade_cal", {"exchange": "SSE", "start_date": "20250101", "end_date": "20250102"}, client)
        store.fetch("daily_basic", {"trade_date": "20250102"}, client)
        self.catalog.record_probe(
            "daily_basic",
            token_hash("super-secret-token", "phase210-secret"),
            "accessible",
            {"trade_date": "20250102"},
            ["ts_code", "trade_date"],
            "2026-01-01T00:00:00Z",
            raw_response={"status": "ok"},
            row_count=1,
        )

    def backup_to(self, name: str = "backup") -> Path:
        target = self.base / name
        plan = BackupPlanner(self.root, self.catalog).plan(target)
        result = BackupExecutor(self.root, self.catalog).backup(plan)
        self.assertEqual(result.status, "succeeded")
        return target

    def manifest(self, target: Path) -> dict:
        return json.loads((target / "manifest.json").read_text())

    def write_manifest(self, target: Path, manifest: dict) -> None:
        (target / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2))

    def test_manifest_validator_required_fields_unknown_fields_and_source_provenance(self):
        self.seed_source()
        target = self.backup_to()
        manifest = self.manifest(target)
        validator = BackupManifestValidator()

        valid = validator.validate_manifest_dict(manifest)
        self.assertEqual(valid.status, "succeeded")
        self.assertEqual(valid.warning_count, 0)

        with_unknown = dict(manifest)
        with_unknown["future_field"] = {"ignored": True}
        unknown = validator.validate_manifest_dict(with_unknown)
        self.assertEqual(unknown.status, "succeeded")
        self.assertEqual(unknown.warning_count, 1)
        self.assertEqual(unknown.warnings[0]["reason"], "unknown_field")

        source_missing = dict(manifest)
        source_missing["source_root"] = str(self.base / "does-not-exist")
        self.assertEqual(validator.validate_manifest_dict(source_missing).status, "succeeded")

        missing_version = dict(manifest)
        missing_version.pop("manifest_version")
        result = validator.validate_manifest_dict(missing_version)
        self.assertEqual(result.status, "failed")
        self.assertIn("manifest_version", {err.get("field") for err in result.errors})

        unsupported = dict(manifest)
        unsupported["manifest_version"] = 999
        result = validator.validate_manifest_dict(unsupported)
        self.assertEqual(result.status, "failed")
        self.assertTrue(result.unsupported_manifest_version)

        missing_top = dict(manifest)
        missing_top.pop("files")
        self.assertEqual(validator.validate_manifest_dict(missing_top).status, "failed")

        missing_catalog_field = json.loads(json.dumps(manifest))
        missing_catalog_field["catalog"].pop("sha256")
        result = validator.validate_manifest_dict(missing_catalog_field)
        self.assertEqual(result.status, "failed")
        self.assertIn("catalog.sha256", {err.get("field") for err in result.errors})

        missing_file_field = json.loads(json.dumps(manifest))
        missing_file_field["files"][0].pop("api_name")
        result = validator.validate_manifest_dict(missing_file_field)
        self.assertEqual(result.status, "failed")
        self.assertIn("files[0].api_name", {err.get("field") for err in result.errors})

        bad_count = json.loads(json.dumps(manifest))
        bad_count["file_count"] += 1
        result = validator.validate_manifest_dict(bad_count)
        self.assertEqual(result.status, "failed")
        self.assertIn("file_count_mismatch", {err.get("reason") for err in result.errors})

        bad_sha = json.loads(json.dumps(manifest))
        bad_sha["files"][0]["sha256"] = ""
        result = validator.validate_manifest_dict(bad_sha)
        self.assertEqual(result.status, "failed")

    def test_backup_inspect_is_read_only_and_does_not_depend_on_source_root(self):
        self.seed_source()
        target = self.backup_to()
        before = self.counts(target)

        result = BackupInspector().inspect(target)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.manifest_validation_status, "succeeded")
        self.assertEqual(result.file_count, 4)
        self.assertEqual(result.raw_file_count, 2)
        self.assertEqual(result.lake_file_count, 2)
        self.assertTrue(result.catalog_present)
        self.assertEqual(result.catalog_counts["file_count"], 4)
        self.assertEqual(self.counts(target), before)

        table = self.run_cli(target, "backup-inspect", "--backup", str(target)).stdout
        self.assertIn("manifest_validation_status", table)
        self.assertIn("catalog_present", table)
        payload = json.loads(self.run_cli(target, "backup-inspect", "--backup", str(target), "--json").stdout)
        self.assertEqual(payload["status"], "succeeded")
        self.assertEqual(payload["manifest_error_count"], 0)
        self.assertEqual(self.counts(target), before)

        manifest = self.manifest(target)
        self.assertNotIn("super-secret-token", json.dumps(manifest))
        self.assertNotIn(b"super-secret-token", (target / "_catalog" / "catalog.sqlite").read_bytes())
        shutil.rmtree(self.root)
        self.assertEqual(BackupInspector().inspect(target).status, "succeeded")
        self.assertEqual(self.run_cli(target, "catalog-version").stdout.strip(), "2")
        inspect = json.loads(self.run_cli(target, "catalog-inspect", "--json").stdout)
        self.assertEqual(inspect["file_count"], 4)

        invalid = json.loads(json.dumps(manifest))
        invalid.pop("backup_id")
        self.write_manifest(target, invalid)
        failed = self.run_cli(target, "backup-inspect", "--backup", str(target), check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("missing_required_field", failed.stdout)

    def test_restore_check_manifest_schema_fail_fast_and_unknown_field_compatibility(self):
        self.seed_source()
        target = self.backup_to()
        before = self.counts(target)
        valid = RestoreChecker().check(target)
        self.assertEqual(valid.status, "succeeded")
        self.assertEqual(valid.manifest_validation_status, "succeeded")
        self.assertEqual(valid.checked_file_count, 4)
        self.assertEqual(self.counts(target), before)

        unknown_target = self.backup_to("backup-unknown")
        unknown_manifest = self.manifest(unknown_target)
        unknown_manifest["future_field"] = "allowed"
        self.write_manifest(unknown_target, unknown_manifest)
        unknown = RestoreChecker().check(unknown_target)
        self.assertEqual(unknown.status, "succeeded")
        self.assertEqual(unknown.manifest_warning_count, 1)
        self.assertEqual(unknown.checked_file_count, 4)

        unsupported_target = self.backup_to("backup-unsupported")
        unsupported_manifest = self.manifest(unsupported_target)
        unsupported_manifest["manifest_version"] = 999
        self.write_manifest(unsupported_target, unsupported_manifest)
        unsupported = RestoreChecker().check(unsupported_target)
        self.assertEqual(unsupported.status, "failed")
        self.assertTrue(unsupported.unsupported_manifest_version)
        self.assertEqual(unsupported.checked_file_count, 0)

        missing_target = self.backup_to("backup-missing-field")
        missing_manifest = self.manifest(missing_target)
        missing_manifest["files"][0].pop("backup_relative_path")
        self.write_manifest(missing_target, missing_manifest)
        missing = RestoreChecker().check(missing_target)
        self.assertEqual(missing.status, "failed")
        self.assertEqual(missing.checked_file_count, 0)
        self.assertIn("files[0].backup_relative_path", {err.get("field") for err in missing.failures})

        count_target = self.backup_to("backup-count-mismatch")
        count_manifest = self.manifest(count_target)
        count_manifest["file_count"] += 1
        self.write_manifest(count_target, count_manifest)
        count = RestoreChecker().check(count_target)
        self.assertEqual(count.status, "failed")
        self.assertEqual(count.checked_file_count, 0)
        self.assertEqual(count.file_count_failure_count, 1)

        bad_json_target = self.backup_to("backup-bad-json")
        (bad_json_target / "manifest.json").write_text("{not-json")
        bad_json = RestoreChecker().check(bad_json_target)
        self.assertEqual(bad_json.status, "failed")
        self.assertEqual(bad_json.manifest_validation_status, "failed")
        self.assertEqual(bad_json.checked_file_count, 0)

        missing_manifest_target = self.backup_to("backup-no-manifest")
        (missing_manifest_target / "manifest.json").unlink()
        no_manifest = RestoreChecker().check(missing_manifest_target)
        self.assertEqual(no_manifest.status, "failed")
        self.assertEqual(no_manifest.checked_file_count, 0)

    def test_cli_backup_inspect_and_restore_check_json_for_invalid_manifest(self):
        self.seed_source()
        target = self.backup_to()
        manifest = self.manifest(target)
        manifest["manifest_version"] = 999
        self.write_manifest(target, manifest)

        inspect = self.run_cli(target, "backup-inspect", "--backup", str(target), "--json", check=False)
        self.assertNotEqual(inspect.returncode, 0)
        inspect_payload = json.loads(inspect.stdout)
        self.assertEqual(inspect_payload["manifest_validation_status"], "failed")
        self.assertEqual(inspect_payload["manifest_error_count"], 1)

        restore = self.run_cli(target, "restore-check", "--backup", str(target), "--json", check=False)
        self.assertNotEqual(restore.returncode, 0)
        restore_payload = json.loads(restore.stdout)
        self.assertEqual(restore_payload["manifest_validation_status"], "failed")
        self.assertTrue(restore_payload["unsupported_manifest_version"])
        self.assertEqual(restore_payload["checked_file_count"], 0)


if __name__ == "__main__":
    unittest.main()
