from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.financial_command import FinancialPullCommandReporter
from tushare_mirror.mirror import CommandSafetyAnalyzer


class HKUSFinancialPullCommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.root.mkdir()
        self.backup.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def create_bundle(self, output: Path, *, overwrite: bool = False):
        return FinancialPullCommandReporter().create(
            scope="hk-financial-raw",
            root=self.root,
            backup=self.backup,
            from_period="1990Q1",
            to_period="latest",
            limit_codes=20,
            max_periods=20,
            output=output,
            overwrite=overwrite,
        )

    def test_financial_pull_command_bundle_is_guarded_and_safe(self):
        output = self.base / "hk-financial-command"
        result = self.create_bundle(output)
        self.assertFalse(result.blocked)
        self.assertEqual(result.report_version, "financial-pull-command/v1")
        self.assertEqual(set(result.files), {"README.md", "commands.sh", "plan.json", "readiness.json", "probe_contract.json"})
        commands = (output / "commands.sh").read_text()
        self.assertIn("USER_CONFIRMATION_REQUIRED", commands)
        self.assertIn("financial-readiness", commands)
        self.assertIn("code-period-plan", commands)
        self.assertNotIn("--end-period latest", commands)
        self.assertFalse((output / "commands.sh").stat().st_mode & 0o111)
        plan = json.loads((output / "plan.json").read_text())
        self.assertTrue(plan["user_confirmation_required"])
        self.assertTrue(plan["not_a_full_pull"])
        safety = CommandSafetyAnalyzer().analyze(file=output / "commands.sh")
        self.assertIn(safety.status, {"passed", "warning"})
        self.assertEqual(safety.blocking_errors, [])

    def test_financial_pull_command_refuses_unsafe_or_existing_outputs(self):
        inside_root = self.root / "bundle"
        inside_backup = self.backup / "bundle"
        existing = self.base / "existing"
        existing.mkdir()
        root_result = self.create_bundle(inside_root)
        backup_result = self.create_bundle(inside_backup)
        existing_result = self.create_bundle(existing)
        self.assertIn("output path is inside mirror root", root_result.blocking_errors)
        self.assertIn("output path is inside backup root", backup_result.blocking_errors)
        self.assertIn("output path already exists; pass --overwrite to replace it", existing_result.blocking_errors)

        overwritten = self.create_bundle(existing, overwrite=True)
        self.assertFalse(overwritten.blocked)
        self.assertTrue((existing / "commands.sh").exists())

    def test_financial_pull_command_cli_json_and_no_output_mode(self):
        no_output = self.run_cli(
            "financial-pull-command",
            "--scope", "us-financial-raw",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--from-period", "2024Q4",
            "--to-period", "2024Q4",
            "--limit-codes", "2",
            "--max-periods", "1",
            "--json",
        )
        payload = json.loads(no_output.stdout)
        self.assertEqual(no_output.returncode, 0, no_output.stderr)
        self.assertEqual(payload["report_version"], "financial-pull-command/v1")
        self.assertEqual(payload["files"], [])
        self.assertTrue(payload["user_confirmation_required"])

        output = self.base / "cli-output"
        created = self.run_cli(
            "financial-pull-command",
            "--scope", "us-financial-raw",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--from-period", "2024Q4",
            "--to-period", "2024Q4",
            "--limit-codes", "2",
            "--max-periods", "1",
            "--output", str(output),
            "--json",
        )
        payload = json.loads(created.stdout)
        self.assertEqual(created.returncode, 0, created.stderr)
        self.assertEqual(set(payload["files"]), {"README.md", "commands.sh", "plan.json", "readiness.json", "probe_contract.json"})
        self.assertTrue((output / "commands.sh").exists())


if __name__ == "__main__":
    unittest.main()
