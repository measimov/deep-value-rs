from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import CommandSafetyAnalyzer


class HKUSLowRiskPullCommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "lake"
        self.backup = self.base / "backup"
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

    def run_cli(self, *args, check=False):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def test_hk_pull_command_json_is_guarded_and_read_only(self):
        before = self.counts()
        result = self.run_cli(
            "mirror-pull-command",
            "--scope", "hk-low-risk",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--start-date", "19900101",
            "--end-date", "latest-trade-date",
            "--max-jobs-per-api", "20",
            "--json",
            check=True,
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["report_version"], "mirror-pull-command/v1")
        self.assertEqual(payload["scope"], "hk-low-risk")
        self.assertTrue(payload["user_confirmation_required"])
        self.assertIn("hk_tradecal", payload["calendar_dependency_summary"]["calendar_apis"])
        self.assertEqual(payload["pagination_strategy_summary"]["hk_daily_adj"], "offset")
        execute = next(command for command in payload["commands"] if command["command_name"] == "mirror-run-execute")
        self.assertTrue(execute["would_execute_real_requests"])
        self.assertIn("--scope hk-low-risk", execute["command_text"])
        self.assertEqual(self.counts(), before)

    def test_us_pull_command_bundle_is_guarded_and_safe(self):
        output = self.base / "us-pull"
        result = self.run_cli(
            "mirror-pull-command",
            "--scope", "us-low-risk",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--start-date", "19900101",
            "--end-date", "latest-trade-date",
            "--max-jobs-per-api", "20",
            "--output", str(output),
            "--json",
            check=True,
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "created")
        self.assertEqual(set(payload["files"]), {"README.md", "commands.sh", "plan.json", "request_estimate.json", "stop_policy.json"})
        plan = json.loads((output / "plan.json").read_text())
        self.assertEqual(plan["scope"], "us-low-risk")
        self.assertIn("us_tradecal", plan["calendar_dependency_summary"]["calendar_apis"])
        commands = (output / "commands.sh").read_text()
        self.assertIn("USER_CONFIRMATION_REQUIRED", commands)
        self.assertNotIn("\npython3 -m tushare_mirror mirror-run", commands)
        safety = CommandSafetyAnalyzer().analyze(file=output / "commands.sh")
        self.assertIn(safety.status, {"passed", "warning"})
        self.assertFalse(safety.blocking_errors)

    def test_global_pull_command_generation_is_explicit_composition(self):
        result = self.run_cli(
            "mirror-pull-command",
            "--scope", "global-equity-low-risk",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--start-date", "19900101",
            "--end-date", "latest-trade-date",
            "--max-jobs-per-api", "20",
            "--json",
            check=True,
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["scope"], "global-equity-low-risk")
        self.assertEqual(payload["calendar_dependency_summary"]["calendar_apis"], ["trade_cal", "hk_tradecal", "us_tradecal"])
        self.assertIn("hk_daily_adj", payload["pagination_strategy_summary"])
        self.assertIn("us_daily_adj", payload["pagination_strategy_summary"])

    def test_pull_command_blocks_output_inside_roots(self):
        inside_root = self.root / "pull"
        result = self.run_cli(
            "mirror-pull-command",
            "--scope", "hk-low-risk",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--start-date", "19900101",
            "--end-date", "latest-trade-date",
            "--max-jobs-per-api", "20",
            "--output", str(inside_root),
            "--json",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("output path must not be inside mirror root", result.stdout)

        inside_backup = self.backup / "pull"
        result = self.run_cli(
            "mirror-pull-command",
            "--scope", "us-low-risk",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--start-date", "19900101",
            "--end-date", "latest-trade-date",
            "--max-jobs-per-api", "20",
            "--output", str(inside_backup),
            "--json",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("output path must not be inside backup root", result.stdout)


if __name__ == "__main__":
    unittest.main()
