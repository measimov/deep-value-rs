from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import MirrorPreflightChecker


class Phase34MirrorPreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, mirror: Path, backup: Path, **kwargs):
        defaults = {
            "scope": "low-risk-a-share",
            "mode": "pilot",
            "start_date": "20250101",
            "end_date": "20250131",
            "max_jobs_per_api": 20,
        }
        defaults.update(kwargs)
        return MirrorPreflightChecker(token_available=True).check(mirror_root=mirror, backup_target=backup, **defaults)

    def run_cli(self, *args, token: str | None = "fake-token"):
        env = dict(os.environ)
        if token is None:
            env.pop("TUSHARE_TOKEN", None)
        else:
            env["TUSHARE_TOKEN"] = token
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "mirror-preflight", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=False,
        )

    def test_valid_missing_paths_warn_under_tmp_and_are_ready_without_side_effects(self):
        mirror = self.base / "mirror"
        backup = self.base / "backup"
        result = self.check(mirror, backup)
        self.assertEqual(result.mirror_root_status, "missing")
        self.assertEqual(result.backup_target_status, "missing")
        self.assertEqual(result.path_relationship_status, "ok")
        self.assertEqual(result.status, "warning")
        self.assertTrue(result.ready_to_execute)
        self.assertTrue(result.token_available)
        self.assertTrue(any("/tmp" in warning for warning in result.warnings))
        self.assertFalse(mirror.exists())
        self.assertFalse(backup.exists())

    def test_path_relationship_blocks_nested_or_same_paths(self):
        same = self.base / "same"
        self.assertEqual(self.check(same, same).path_relationship_status, "same_path")
        self.assertEqual(self.check(same, same).status, "blocked")
        inside_backup = self.check(self.base / "mirror", self.base / "mirror" / "backup")
        self.assertEqual(inside_backup.path_relationship_status, "backup_inside_mirror")
        self.assertEqual(inside_backup.status, "blocked")
        inside_mirror = self.check(self.base / "backup" / "mirror", self.base / "backup")
        self.assertEqual(inside_mirror.path_relationship_status, "mirror_inside_backup")
        self.assertEqual(inside_mirror.status, "blocked")

    def test_unknown_non_empty_paths_are_blocked(self):
        mirror = self.base / "unknown-mirror"
        backup = self.base / "unknown-backup"
        mirror.mkdir()
        backup.mkdir()
        (mirror / "random.txt").write_text("x")
        (backup / "random.txt").write_text("x")
        result = self.check(mirror, backup)
        self.assertEqual(result.mirror_root_status, "non_empty_unknown")
        self.assertEqual(result.backup_target_status, "non_empty_unknown")
        self.assertEqual(result.status, "blocked")

    def test_existing_mirror_catalog_detected_without_mutation(self):
        mirror = self.base / "existing-mirror"
        backup = self.base / "backup"
        catalog = CatalogStore(mirror)
        catalog.init()
        load_into_catalog(mirror, catalog)
        before = catalog.inspect_summary()
        result = self.check(mirror, backup)
        after = catalog.inspect_summary()
        self.assertEqual(before, after)
        self.assertEqual(result.mirror_root_status, "existing_catalog")
        self.assertTrue(result.existing_catalog["present"])
        self.assertEqual(result.existing_catalog["schema_version"], 2)
        self.assertGreaterEqual(result.existing_catalog["endpoint_count"], 12)

    def test_existing_backup_manifest_detected_and_blocks_overwrite(self):
        mirror = self.base / "mirror"
        backup = self.base / "backup"
        backup.mkdir()
        (backup / "manifest.json").write_text(json.dumps({"manifest_version": 1}))
        result = self.check(mirror, backup)
        self.assertEqual(result.backup_target_status, "existing_manifest")
        self.assertTrue(result.existing_backup["present"])
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("manifest.json" in error for error in result.blocking_errors))

    def test_scope_mode_dates_and_max_jobs_guardrails(self):
        mirror = self.base / "mirror"
        backup = self.base / "backup"
        self.assertEqual(self.check(mirror, backup, scope="all").status, "blocked")
        self.assertEqual(self.check(mirror, backup, mode="full").status, "blocked")
        self.assertEqual(self.check(mirror, backup, start_date=None).status, "blocked")
        self.assertEqual(self.check(mirror, backup, max_jobs_per_api=21).status, "blocked")
        smoke = self.check(mirror, backup, mode="smoke", start_date=None, end_date=None, max_jobs_per_api=4)
        self.assertEqual(smoke.status, "blocked")

    def test_token_absence_blocks_without_creating_files_or_leaking_token(self):
        mirror = self.base / "mirror"
        backup = self.base / "backup"
        result = MirrorPreflightChecker(token_available=False).check(
            mirror_root=mirror,
            backup_target=backup,
            scope="low-risk-a-share",
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            max_jobs_per_api=20,
        )
        payload = json.dumps(result.to_dict())
        self.assertEqual(result.status, "blocked")
        self.assertFalse(result.token_available)
        self.assertIn("TUSHARE_TOKEN", payload)
        self.assertNotIn("fake-token", payload)
        self.assertFalse(mirror.exists())
        self.assertFalse(backup.exists())

    def test_cli_json_contains_required_fields_and_has_no_side_effects(self):
        mirror = self.base / "cli-mirror"
        backup = self.base / "cli-backup"
        result = self.run_cli(
            "--mirror-root", str(mirror),
            "--backup-target", str(backup),
            "--scope", "low-risk-a-share",
            "--mode", "pilot",
            "--start-date", "20250101",
            "--end-date", "20250131",
            "--max-jobs-per-api", "20",
            "--json",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        for key in [
            "status",
            "ready_to_execute",
            "mirror_root",
            "backup_target",
            "scope",
            "mode",
            "token_available",
            "mirror_root_status",
            "backup_target_status",
            "path_relationship_status",
            "disk_space",
            "existing_catalog",
            "existing_backup",
            "warnings",
            "blocking_errors",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["mirror_root_status"], "missing")
        self.assertEqual(payload["backup_target_status"], "missing")
        self.assertEqual(payload["path_relationship_status"], "ok")
        self.assertTrue(payload["ready_to_execute"])
        self.assertNotIn("fake-token", result.stdout)
        self.assertFalse(mirror.exists())
        self.assertFalse(backup.exists())

    def test_cli_blocked_returns_nonzero(self):
        root = self.base / "root"
        result = self.run_cli(
            "--mirror-root", str(root),
            "--backup-target", str(root),
            "--scope", "low-risk-a-share",
            "--mode", "pilot",
            "--start-date", "20250101",
            "--end-date", "20250131",
            "--max-jobs-per-api", "20",
            "--json",
        )
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(payload["path_relationship_status"], "same_path")


if __name__ == "__main__":
    unittest.main()
