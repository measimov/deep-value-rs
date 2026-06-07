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


class HKUSFinancialSafetyRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.root.mkdir()
        self.backup.mkdir()
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)
        self.probe = self.base / "financial-probe.json"
        self.probe.write_text(
            json.dumps(
                {
                    "report_version": "hk-us-financial-pit-probe/v1",
                    "overall_status": "passed",
                    "token_plaintext_found": False,
                    "endpoints": {
                        "hk_income": {
                            "api_name": "hk_income",
                            "probe_status": "passed",
                            "observed_fields": ["ts_code", "end_date", "name", "ind_name", "ind_value"],
                            "observed_disclosure_fields": [],
                            "observed_row_count": 1,
                            "request_count": 1,
                        }
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        with sqlite3.connect(self.root / "_catalog" / "catalog.sqlite") as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def test_new_financial_read_only_commands_do_not_mutate_catalog(self):
        commands = [
            ("mirror-scope", "--scope", "hk-financial-raw", "--json"),
            ("hk-us-financial-probe-report", "--input", str(self.probe), "--json"),
            ("financial-readiness", "--scope", "hk-financial-raw", "--root", str(self.root), "--json"),
            ("financial-request-estimate", "--scope", "hk-financial-raw", "--from-period", "2024Q4", "--to-period", "2024Q4", "--limit-codes", "2", "--max-periods", "1", "--json"),
            ("financial-coverage-matrix", "--root", str(self.root), "--scope", "hk-financial-raw", "--periods", "20241231", "--limit-codes", "2", "--json"),
            ("financial-pull-command", "--scope", "hk-financial-raw", "--root", str(self.root), "--backup", str(self.backup), "--from-period", "2024Q4", "--to-period", "2024Q4", "--limit-codes", "2", "--max-periods", "1", "--json"),
        ]
        before = self.counts()
        for command in commands:
            with self.subTest(command=command[0]):
                result = self.run_cli(*command)
                payload = json.loads(result.stdout)
                self.assertIn("report_version", payload)
        self.assertEqual(before, self.counts())
        self.assertFalse(any((self.root / "raw").glob("**/*")))
        self.assertFalse(any((self.root / "lake").glob("**/*")))

    def test_financial_pull_command_writes_only_explicit_output_directory(self):
        output = self.base / "bundle"
        before = self.counts()
        result = self.run_cli(
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
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(payload["report_version"], "financial-pull-command/v1")
        self.assertEqual(before, self.counts())
        self.assertEqual(sorted(path.name for path in output.iterdir()), ["README.md", "commands.sh", "plan.json", "probe_contract.json", "readiness.json"])
        commands = (output / "commands.sh").read_text()
        self.assertIn("USER_CONFIRMATION_REQUIRED", commands)
        self.assertNotIn("TUSHARE_TOKEN", commands)
        self.assertFalse(any((self.root / "raw").glob("**/*")))
        self.assertFalse(any((self.root / "lake").glob("**/*")))


if __name__ == "__main__":
    unittest.main()
