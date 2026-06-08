from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.disclosure_reports import (
    DisclosureAvailabilityReporter,
    DisclosureBundleReporter,
    DisclosureGateReporter,
    DisclosurePlanReporter,
    DisclosureSourceReporter,
)
from tushare_mirror.financial_reports import FinancialReadinessReporter
from tushare_mirror.mirror import CommandSafetyAnalyzer
from tushare_mirror.pit import PITReadinessReporter


class DisclosureAvailabilityReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "mirror"
        self.root.mkdir()

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

    def test_disclosure_source_report_json_is_stable(self):
        report = DisclosureSourceReporter().report().to_dict()
        self.assertEqual(report["report_version"], "disclosure-source-report/v1")
        self.assertEqual(report["scope"], "all")
        self.assertEqual(report["availability_only_count"], 0)
        self.assertEqual(report["as_filed_verified_count"], 0)
        self.assertGreaterEqual(report["source_count"], 4)
        self.assertGreaterEqual(report["candidate_count"], 1)
        self.assertEqual(report["feature_eligible_count"], 0)

    def test_disclosure_plan_counts_candidate_without_feature_eligibility(self):
        report = DisclosurePlanReporter().report(
            scope="us-financial-raw",
            from_period="2024Q4",
            to_period="2024Q4",
            limit_codes=1,
        ).to_dict()
        self.assertEqual(report["report_version"], "disclosure-plan/v1")
        self.assertEqual(report["scope"], "us-financial-raw")
        self.assertEqual(report["candidate_count"], 1)
        self.assertEqual(report["availability_only_count"], 0)
        self.assertEqual(report["as_filed_verified_count"], 0)
        self.assertEqual(report["feature_eligible_count"], 0)
        self.assertEqual(report["planned_periods"], 1)
        self.assertEqual(report["planned_disclosure_checks"], 1)

    def test_disclosure_availability_and_gate_are_read_only(self):
        before = sorted(path.name for path in self.root.iterdir())
        availability = DisclosureAvailabilityReporter().report(scope="hk-financial-raw", root=self.root).to_dict()
        gate = DisclosureGateReporter().report(
            scope="us-financial-raw",
            api_name="us_fina_indicator",
            ts_code="NVDA.US",
            period="20241231",
        ).to_dict()
        after = sorted(path.name for path in self.root.iterdir())
        self.assertEqual(before, after)
        self.assertEqual(availability["report_version"], "disclosure-availability/v1")
        self.assertEqual(availability["raw_only_count"], 4)
        self.assertEqual(availability["feature_eligible_count"], 0)
        self.assertEqual(gate["report_version"], "disclosure-gate/v1")
        self.assertEqual(gate["candidate_count"], 1)
        self.assertEqual(gate["feature_eligible_count"], 0)
        self.assertEqual(gate["items"][0]["state"], "candidate")
        self.assertEqual(gate["items"][0]["feature_gate_status"], "blocked")
        self.assertIn("raw_only_not_feature_eligible", gate["items"][0]["feature_gate_blocking_errors"])

    def test_disclosure_report_clis_are_json_stable_and_read_only(self):
        before = sorted(path.name for path in self.root.iterdir())
        commands = [
            ("disclosure-source-report", "--json"),
            (
                "disclosure-plan",
                "--scope", "us-financial-raw",
                "--from-period", "2024Q4",
                "--to-period", "2024Q4",
                "--limit-codes", "1",
                "--json",
            ),
            (
                "disclosure-availability",
                "--scope", "us-financial-raw",
                "--root", str(self.root),
                "--json",
            ),
            (
                "disclosure-gate",
                "--scope", "us-financial-raw",
                "--api-name", "us_fina_indicator",
                "--ts-code", "NVDA.US",
                "--period", "20241231",
                "--json",
            ),
        ]
        versions = [
            "disclosure-source-report/v1",
            "disclosure-plan/v1",
            "disclosure-availability/v1",
            "disclosure-gate/v1",
        ]
        for command, version in zip(commands, versions):
            with self.subTest(command=command[0]):
                result = self.run_cli(*command)
                payload = json.loads(result.stdout)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(payload["report_version"], version)
                self.assertIn("availability_only_count", payload)
                self.assertIn("as_filed_verified_count", payload)
                self.assertEqual(payload["feature_eligible_count"], 0)
        after = sorted(path.name for path in self.root.iterdir())
        self.assertEqual(before, after)

    def test_existing_pit_and_financial_readiness_expose_disclosure_counters(self):
        pit = PITReadinessReporter().report().to_dict()
        financial = FinancialReadinessReporter().report(scope="us-financial-raw", root=self.root).to_dict()
        self.assertIn("availability_only_count", pit)
        self.assertIn("as_filed_verified_count", pit)
        self.assertEqual(pit["availability_only_count"], 0)
        self.assertEqual(pit["as_filed_verified_count"], 0)
        self.assertEqual(financial["availability_only_count"], 0)
        self.assertEqual(financial["as_filed_verified_count"], 0)
        self.assertEqual(financial["candidate_count"], 1)
        self.assertEqual(financial["feature_eligible_count"], 0)
        by_api = {item["api_name"]: item for item in financial["items"]}
        self.assertEqual(by_api["us_fina_indicator"]["disclosure_state"], "candidate")
        self.assertEqual(by_api["us_fina_indicator"]["pit_strength"], "raw_only")
        self.assertFalse(by_api["us_fina_indicator"]["feature_eligible"])

    def test_disclosure_bundle_created_outside_roots_and_commands_are_guarded(self):
        backup = Path(self.tmp.name) / "backup"
        backup.mkdir()
        output = Path(self.tmp.name) / "bundle"
        result = DisclosureBundleReporter().report(
            scope="us-financial-raw",
            root=self.root,
            backup=backup,
            from_period="2024Q4",
            to_period="2024Q4",
            output=output,
        ).to_dict()
        self.assertEqual(result["report_version"], "disclosure-bundle/v1")
        self.assertEqual(result["status"], "passed")
        self.assertEqual(
            sorted(path.name for path in output.iterdir()),
            ["README.md", "availability.json", "commands.sh", "disclosure_plan.json", "gate.json", "limitations.md", "source_report.json"],
        )
        commands = (output / "commands.sh").read_text()
        self.assertIn("USER_CONFIRMATION_REQUIRED", commands)
        self.assertNotIn("TUSHARE_TOKEN", commands)
        safety = CommandSafetyAnalyzer().analyze(file=output / "commands.sh")
        self.assertEqual(safety.status, "passed", safety.to_dict())
        self.assertFalse(any((self.root / name).exists() for name in ["raw", "lake", "_catalog"]))

    def test_disclosure_bundle_refuses_unsafe_or_existing_outputs(self):
        backup = Path(self.tmp.name) / "backup"
        backup.mkdir()
        existing = Path(self.tmp.name) / "existing"
        existing.mkdir()
        for output, expected in [
            (self.root / "bundle", "output path is inside mirror root"),
            (backup / "bundle", "output path is inside backup root"),
            (existing, "output path already exists; rerun with --overwrite"),
        ]:
            with self.subTest(output=output):
                result = DisclosureBundleReporter().report(
                    scope="us-financial-raw",
                    root=self.root,
                    backup=backup,
                    from_period="2024Q4",
                    to_period="2024Q4",
                    output=output,
                ).to_dict()
                self.assertEqual(result["status"], "blocked")
                self.assertIn(expected, result["blocking_errors"])

    def test_disclosure_bundle_cli_json_and_overwrite(self):
        backup = Path(self.tmp.name) / "backup"
        backup.mkdir()
        output = Path(self.tmp.name) / "cli-bundle"
        output.mkdir()
        result = self.run_cli(
            "disclosure-bundle",
            "--scope", "us-financial-raw",
            "--root", str(self.root),
            "--backup", str(backup),
            "--from-period", "2024Q4",
            "--to-period", "2024Q4",
            "--output", str(output),
            "--overwrite",
            "--json",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(payload["report_version"], "disclosure-bundle/v1")
        self.assertIn("commands.sh", payload["files"])
        self.assertTrue(payload["user_confirmation_required"])
        self.assertTrue(payload["commands_guarded"])


if __name__ == "__main__":
    unittest.main()
