from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.mirror import MirrorAutoSyncStatusReporter


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "tushare_mirror_periodic_sync.py"
INSTALL_SCRIPT = REPO_ROOT / "scripts" / "install_tushare_mirror_periodic_sync.sh"
SERVICE = REPO_ROOT / "ops" / "systemd" / "tushare-mirror-periodic-sync.service"
TIMER = REPO_ROOT / "ops" / "systemd" / "tushare-mirror-periodic-sync.timer"
STATE_NAMES = {
    "a-share-low-risk": "TuShare-auto-sync-state.json",
    "hk-low-risk": "TuShare-hk-low-risk-auto-sync-state.json",
    "us-low-risk": "TuShare-us-low-risk-auto-sync-state.json",
}


class PeriodicSyncOperationsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.state_dir = self.base / "state"
        self.root.mkdir()
        self.backup.mkdir()
        self.state_dir.mkdir()
        for scope, filename in STATE_NAMES.items():
            self.write_state(scope, filename)

    def tearDown(self):
        self.tmp.cleanup()

    def write_state(self, scope: str, filename: str) -> Path:
        path = self.state_dir / filename
        path.write_text(
            json.dumps(
                {
                    "state_version": (
                        "mirror-auto-sync-state/v1"
                        if scope == "a-share-low-risk"
                        else "mirror-auto-sync-state/v2"
                    ),
                    "root": str(self.root.resolve()),
                    "backup": str(self.backup.resolve()),
                    "scope": scope,
                    "completed_windows": [],
                    "failed_windows": [],
                    "in_progress_window": None,
                    "next_start_date": "20260701",
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return path

    def run_script(self, *extra: str, env: dict[str, str] | None = None):
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--root",
                str(self.root),
                "--backup",
                str(self.backup),
                "--state-dir",
                str(self.state_dir),
                "--json",
                *extra,
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

    def test_dry_run_plans_three_markets_in_order_without_side_effects(self):
        before = {path: path.read_text() for path in self.state_dir.iterdir()}
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "planned")
        self.assertFalse(payload["execute"])
        self.assertEqual(
            payload["scope_order"],
            ["a-share-low-risk", "hk-low-risk", "us-low-risk"],
        )
        self.assertEqual(payload["runs"], [])
        self.assertEqual(before, {path: path.read_text() for path in self.state_dir.iterdir()})
        command_by_scope = {item["scope"]: item["command"] for item in payload["commands"]}
        self.assertNotIn("--confirm-hk-us-auto-sync", command_by_scope["a-share-low-risk"])
        self.assertIn("--confirm-hk-us-auto-sync", command_by_scope["hk-low-risk"])
        self.assertIn("--confirm-hk-us-auto-sync", command_by_scope["us-low-risk"])
        self.assertNotIn("TUSHARE_TOKEN", result.stdout)

    def test_scope_filter_preserves_explicit_selection(self):
        result = self.run_script("--scope", "hk-low-risk")
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["scope_order"], ["hk-low-risk"])
        self.assertEqual(len(payload["commands"]), 1)

    def test_missing_checkpoint_blocks_instead_of_bootstrapping_full_pull(self):
        (self.state_dir / STATE_NAMES["us-low-risk"]).unlink()
        result = self.run_script()
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "blocked")
        self.assertTrue(
            any("existing checkpoint state is required" in item for item in payload["blocking_errors"])
        )

    def test_checkpoint_scope_mismatch_blocks(self):
        self.write_state("hk-low-risk", STATE_NAMES["us-low-risk"])
        result = self.run_script("--scope", "us-low-risk")
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertTrue(any("checkpoint scope mismatch" in item for item in payload["blocking_errors"]))

    def test_execute_requires_confirmation_and_token(self):
        env = os.environ.copy()
        env.pop("TUSHARE_TOKEN", None)
        result = self.run_script("--execute", env=env)
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertIn("--execute requires --confirm-periodic-sync", payload["blocking_errors"])
        self.assertIn("TUSHARE_TOKEN is required for execute mode", payload["blocking_errors"])

    def test_max_jobs_guardrail(self):
        result = self.run_script("--max-jobs-per-api", "21")
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertIn("--max-jobs-per-api must be between 1 and 20", payload["blocking_errors"])

    def test_systemd_units_are_sequential_persistent_and_guarded(self):
        service = SERVICE.read_text(encoding="utf-8")
        timer = TIMER.read_text(encoding="utf-8")
        self.assertIn("tushare_mirror_periodic_sync.py", service)
        self.assertIn("--execute --confirm-periodic-sync", service)
        self.assertIn("EnvironmentFile=", service)
        self.assertIn("OnCalendar=*-*-* 09:15:00 Asia/Shanghai", timer)
        self.assertIn("Persistent=true", timer)

    def test_install_script_dry_run_does_not_install_units(self):
        env_file = REPO_ROOT / ".env"
        self.assertTrue(env_file.exists())
        result = subprocess.run(
            [str(INSTALL_SCRIPT), "--dry-run", "--start-now"],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Would enable tushare-mirror-periodic-sync.timer", result.stdout)
        self.assertIn("Would start tushare-mirror-periodic-sync.service", result.stdout)

    def test_status_treats_retried_failure_as_covered(self):
        state = self.state_dir / STATE_NAMES["hk-low-risk"]
        payload = json.loads(state.read_text())
        window = {"start_date": "20260101", "end_date": "20260120"}
        payload["completed_windows"] = [{**window, "status": "succeeded"}]
        payload["failed_windows"] = [{**window, "status": "failed", "error_type": "network_error"}]
        state.write_text(json.dumps(payload) + "\n")

        result = MirrorAutoSyncStatusReporter().report(state=state)

        self.assertEqual(result.status, "ready")
        self.assertEqual(result.failed_window_count, 1)
        self.assertEqual(result.covered_failed_window_count, 1)
        self.assertEqual(result.uncovered_failed_window_count, 0)

    def test_status_keeps_uncovered_failure_blocked(self):
        state = self.state_dir / STATE_NAMES["hk-low-risk"]
        payload = json.loads(state.read_text())
        payload["failed_windows"] = [
            {
                "start_date": "20260101",
                "end_date": "20260120",
                "status": "failed",
                "error_type": "network_error",
            }
        ]
        state.write_text(json.dumps(payload) + "\n")

        result = MirrorAutoSyncStatusReporter().report(state=state)

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.covered_failed_window_count, 0)
        self.assertEqual(result.uncovered_failed_window_count, 1)


if __name__ == "__main__":
    unittest.main()
