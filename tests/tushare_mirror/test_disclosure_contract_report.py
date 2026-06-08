from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.disclosure_contract import DisclosureContractReporter


class DisclosureContractReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir="/tmp")
        self.base = Path(self.tmp.name)
        self.sec_probe = self.base / "sec-probe.json"
        self.cross_check = self.base / "cross-check.json"
        self.write_sec_probe()

    def tearDown(self):
        self.tmp.cleanup()

    def write_sec_probe(self):
        self.sec_probe.write_text(
            json.dumps(
                {
                    "report_version": "sec-disclosure-probe/v1",
                    "overall_status": "passed",
                    "token_plaintext_found": False,
                    "matched_filings": [{"form": "10-K", "filing_date": "2025-02-26"}],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def write_cross_check(
        self,
        *,
        sec_status: str = "passed",
        tushare_status: str = "passed",
        match_status: str = "exact",
        pit_strength_candidate: str = "availability_only",
        sec_disclosure_date: str | None = "20250226",
        tushare_notice_date: str | None = "20250226",
        date_delta_days: int | None = 0,
    ):
        self.cross_check.write_text(
            json.dumps(
                {
                    "report_version": "sec-tushare-disclosure-cross-check/v1",
                    "overall_status": "passed",
                    "sec_status": sec_status,
                    "tushare_status": tushare_status,
                    "match_status": match_status,
                    "pit_strength_candidate": pit_strength_candidate,
                    "sec_disclosure_date": sec_disclosure_date,
                    "tushare_notice_date": tushare_notice_date,
                    "date_delta_days": date_delta_days,
                    "match_confidence": 1.0 if match_status == "exact" else 0.5,
                    "limitations": ["values are not reconciled"],
                    "token_plaintext_found": False,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def test_exact_sec_tushare_fixture_can_mark_availability_only(self):
        self.write_cross_check()
        report = DisclosureContractReporter().report(sec_probe=self.sec_probe, cross_check=self.cross_check).to_dict()
        self.assertEqual(report["report_version"], "disclosure-contract-report/v1")
        self.assertEqual(report["source_status"], "passed")
        self.assertEqual(report["schema_status"], "passed")
        self.assertEqual(report["sec_status"], "passed")
        self.assertEqual(report["tushare_status"], "passed")
        self.assertEqual(report["match_status"], "exact")
        self.assertEqual(report["pit_strength_candidate"], "availability_only")
        self.assertTrue(report["can_mark_availability_only"])
        self.assertFalse(report["can_mark_as_filed_verified"])
        self.assertEqual(report["blocking_errors"], [])

    def test_missing_tushare_notice_date_blocks_availability(self):
        self.write_cross_check(tushare_status="notice_date_missing", tushare_notice_date=None, match_status="unmatched", pit_strength_candidate="raw_only")
        report = DisclosureContractReporter().report(sec_probe=self.sec_probe, cross_check=self.cross_check).to_dict()
        self.assertFalse(report["can_mark_availability_only"])
        self.assertEqual(report["match_status"], "unmatched")
        self.assertIn("missing Tushare notice_date prevents availability_only", report["warnings"])

    def test_candidate_match_does_not_enter_feature_layer(self):
        self.write_cross_check(match_status="candidate", pit_strength_candidate="availability_only", date_delta_days=2)
        report = DisclosureContractReporter().report(sec_probe=self.sec_probe, cross_check=self.cross_check).to_dict()
        self.assertFalse(report["can_mark_availability_only"])
        self.assertFalse(report["can_mark_as_filed_verified"])
        self.assertIn("candidate match cannot enter the feature layer", report["warnings"])

    def test_cli_json_is_stable_and_read_only(self):
        self.write_cross_check(match_status="near", date_delta_days=2)
        before = {path.name: path.stat().st_mtime_ns for path in self.base.iterdir()}
        result = self.run_cli(
            "disclosure-contract-report",
            "--sec-probe",
            str(self.sec_probe),
            "--cross-check",
            str(self.cross_check),
            "--json",
        )
        after = {path.name: path.stat().st_mtime_ns for path in self.base.iterdir()}
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(payload["report_version"], "disclosure-contract-report/v1")
        self.assertEqual(payload["match_status"], "near")
        self.assertTrue(payload["can_mark_availability_only"])
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
