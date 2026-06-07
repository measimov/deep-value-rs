from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.financial_probe import HKUSFinancialProbeReporter


def write_probe(path: Path, *, version: str = "hk-us-financial-pit-probe/v1") -> None:
    endpoints = [
        ("hk_income", "passed", ["ts_code", "end_date", "name", "ind_name", "ind_value"], []),
        ("hk_balancesheet", "passed", ["ts_code", "name", "end_date", "ind_name", "ind_value"], []),
        ("hk_cashflow", "passed", ["ts_code", "end_date", "name", "ind_name", "ind_value"], []),
        ("hk_fina_indicator", "passed", ["ts_code", "end_date", "notice_date", "currency"], ["notice_date"]),
        ("us_income", "empty_but_authorized", ["ts_code", "end_date", "ind_type", "name", "ind_name", "ind_value", "report_type"], []),
        ("us_balancesheet", "permission_denied", [], []),
        ("us_cashflow", "contract_changed", [], []),
        ("us_fina_indicator", "passed", ["ts_code", "end_date", "notice_date", "currency"], ["notice_date"]),
    ]
    payload = {
        "report_version": version,
        "token_plaintext_found": False,
        "endpoints": [
            {
                "api_name": api,
                "probe_status": status,
                "observed_fields": fields,
                "observed_disclosure_fields": disclosure,
                "observed_row_count": 1 if status == "passed" else 0,
            }
            for api, status, fields, disclosure in endpoints
        ],
    }
    path.write_text(json.dumps(payload, sort_keys=True))


class HKUSFinancialProbeReportTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_probe_report_compares_observed_fields_to_inventory_pit_assumptions(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            path = Path(tmp) / "financial-probe.json"
            write_probe(path)
            report = HKUSFinancialProbeReporter().report(input_path=path).to_dict()

        endpoints = {item["api_name"]: item for item in report["endpoints"]}
        self.assertEqual(report["report_version"], "hk-us-financial-probe-report/v1")
        self.assertIn("hk_income", report["raw_executable_candidates"])
        self.assertIn("hk_income", report["blocked_without_disclosure_date"])
        self.assertFalse(endpoints["hk_income"]["pit_safe_candidate"])
        self.assertIn("ann_date", endpoints["hk_income"]["missing_assumed_pit_fields"])
        self.assertIn("us_income", report["blocked_without_disclosure_date"])
        self.assertFalse(endpoints["us_income"]["raw_executable_candidate"])
        self.assertEqual(endpoints["us_income"]["recommended_execution_status"], "plan_only_empty_probe")
        self.assertIn("hk_fina_indicator", report["pit_safe_candidates"])
        self.assertIn("us_fina_indicator", report["pit_safe_candidates"])
        self.assertIn("us_balancesheet", report["permission_blocked"])
        self.assertIn("us_cashflow", report["contract_blocked"])

    def test_cli_json_contract_is_stable_and_read_only(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            path = Path(tmp) / "financial-probe.json"
            write_probe(path)
            before = set(Path(tmp).iterdir())
            result = self.run_cli("hk-us-financial-probe-report", "--input", str(path), "--json")
            after = set(Path(tmp).iterdir())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(before, after)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["report_version"], "hk-us-financial-probe-report/v1")
        self.assertEqual(payload["endpoint_count"], 8)
        self.assertIn("raw_executable_candidates", payload)
        self.assertIn("pit_safe_candidates", payload)

    def test_missing_or_invalid_probe_input_blocks(self):
        result = HKUSFinancialProbeReporter().report(input_path="/tmp/does-not-exist-financial-probe.json")
        self.assertTrue(result.blocking_errors)
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            path = Path(tmp) / "bad.json"
            path.write_text("{bad")
            result = HKUSFinancialProbeReporter().report(input_path=path)
        self.assertTrue(result.blocking_errors)

    def test_unsupported_or_token_plaintext_probe_report_blocks(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            path = Path(tmp) / "financial-probe.json"
            write_probe(path, version="old")
            payload = json.loads(path.read_text())
            payload["token_plaintext_found"] = True
            path.write_text(json.dumps(payload))
            result = HKUSFinancialProbeReporter().report(input_path=path)
        self.assertIn("unsupported_probe_report_version", result.blocking_errors)
        self.assertIn("probe_report_token_plaintext_found", result.blocking_errors)
