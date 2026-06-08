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


class FinancialDisclosureCalendarRegressionTests(unittest.TestCase):
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
        self.sec_probe = self.base / "sec-probe.json"
        self.cross_check = self.base / "cross-check.json"
        self.sec_probe.write_text(
            json.dumps(
                {
                    "report_version": "sec-disclosure-probe/v1",
                    "overall_status": "passed",
                    "token_plaintext_found": False,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.cross_check.write_text(
            json.dumps(
                {
                    "report_version": "sec-tushare-disclosure-cross-check/v1",
                    "overall_status": "passed",
                    "sec_status": "passed",
                    "tushare_status": "passed",
                    "match_status": "exact",
                    "pit_strength_candidate": "availability_only",
                    "sec_disclosure_date": "20250226",
                    "tushare_notice_date": "20250226",
                    "date_delta_days": 0,
                    "match_confidence": 1.0,
                    "limitations": ["values are not reconciled"],
                    "token_plaintext_found": False,
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

    def test_disclosure_commands_do_not_mutate_catalog_or_data_files(self):
        output = self.base / "bundle"
        commands = [
            ("disclosure-source-report", "--json"),
            ("disclosure-plan", "--scope", "us-financial-raw", "--from-period", "2024Q4", "--to-period", "2024Q4", "--limit-codes", "1", "--json"),
            ("disclosure-availability", "--scope", "us-financial-raw", "--root", str(self.root), "--json"),
            ("disclosure-gate", "--scope", "us-financial-raw", "--api-name", "us_fina_indicator", "--ts-code", "NVDA.US", "--period", "20241231", "--json"),
            ("disclosure-contract-report", "--sec-probe", str(self.sec_probe), "--cross-check", str(self.cross_check), "--json"),
            (
                "disclosure-bundle",
                "--scope", "us-financial-raw",
                "--root", str(self.root),
                "--backup", str(self.backup),
                "--from-period", "2024Q4",
                "--to-period", "2024Q4",
                "--output", str(output),
                "--json",
            ),
        ]
        before = self.counts()
        for command in commands:
            with self.subTest(command=command[0]):
                result = self.run_cli(*command)
                payload = json.loads(result.stdout)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("report_version", payload)
        self.assertEqual(before, self.counts())
        self.assertFalse(any((self.root / "raw").glob("**/*")))
        self.assertFalse(any((self.root / "lake").glob("**/*")))
        self.assertTrue((output / "commands.sh").exists())


if __name__ == "__main__":
    unittest.main()
