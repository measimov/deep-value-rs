from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from scripts import tushare_real_smoke


class FakeProbeClient:
    def __init__(self):
        self.calls: list[tuple[str, dict, list[str]]] = []

    def request(self, api_name, params, fields=None):
        fields_list = list(fields or [])
        self.calls.append((api_name, dict(params), fields_list))
        row = []
        for field in fields_list:
            if field == "ts_code":
                row.append("AAPL" if api_name.startswith("us_") else "00001.HK")
            elif field in {"trade_date", "cal_date", "list_date", "delist_date"}:
                row.append("20250102")
            elif field == "is_open":
                row.append(1)
            elif field in {"exchange", "classify", "list_status"}:
                row.append("NAS" if api_name.startswith("us_") else "L")
            elif field in {"name", "enname", "pretrade_date"}:
                row.append("x")
            else:
                row.append(1.0)
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [row]}}


class FakeFinancialProbeClient:
    def __init__(self, permission_denied: set[str] | None = None):
        self.calls: list[tuple[str, dict, list[str]]] = []
        self.permission_denied = permission_denied or set()

    def request(self, api_name, params, fields=None):
        fields_list = list(fields or [])
        self.calls.append((api_name, dict(params), fields_list))
        if api_name in self.permission_denied:
            return {"code": -2001, "msg": "permission denied", "data": {"fields": fields_list, "items": []}}
        row = []
        for field in fields_list:
            if field == "ts_code":
                row.append("NVDA" if api_name.startswith("us_") else "00700.HK")
            elif field in {"end_date", "start_date", "std_report_date", "financial_date"}:
                row.append("20241231")
            elif field == "notice_date":
                row.append("20250315")
            elif field in {"name", "ind_name", "security_name_abbr", "currency", "report_type", "ind_type", "accounting_standards"}:
                row.append("x")
            else:
                row.append(1.0)
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [row]}}


class HKUSLowRiskProbeHarnessTests(unittest.TestCase):
    def run_script(self, *args, env=None):
        return subprocess.run(
            [sys.executable, "scripts/tushare_real_smoke.py", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

    def test_help_lists_hk_us_probe_flag(self):
        result = self.run_script("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("--hk-us-low-risk-probe", result.stdout)
        self.assertIn("--hk-us-financial-pit-probe", result.stdout)
        self.assertIn("--max-requests-per-endpoint", result.stdout)

    def test_probe_without_token_writes_blocked_report_and_sends_no_requests(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "probe.json"
            with redirect_stdout(StringIO()):
                code = tushare_real_smoke.run_hk_us_low_risk_probe(output, 2, token="")
            payload = json.loads(output.read_text())
        self.assertEqual(code, 2)
        self.assertEqual(payload["overall_status"], "blocked")
        self.assertFalse(payload["real_requests_sent"])
        self.assertEqual(payload["endpoints"], [])
        self.assertIn("TUSHARE_TOKEN is required", payload["blocking_errors"][0])

    def test_probe_with_fake_client_is_bounded_and_redacted(self):
        client = FakeProbeClient()
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "probe.json"
            with redirect_stdout(StringIO()):
                code = tushare_real_smoke.run_hk_us_low_risk_probe(output, 2, token="secret-token-1234567890", client=client)
            raw = output.read_text()
            payload = json.loads(raw)
        self.assertEqual(code, 0)
        self.assertTrue(payload["real_requests_sent"])
        self.assertNotIn("secret-token-1234567890", raw)
        self.assertFalse(payload["token_plaintext_found"])
        endpoints = {item["endpoint"]: item for item in payload["endpoints"]}
        self.assertIn("hk_basic", endpoints)
        self.assertIn("us_daily_adj", endpoints)
        self.assertLessEqual(max(item["request_count"] for item in payload["endpoints"]), 2)
        self.assertLessEqual(max(item["page_count_tested"] for item in payload["endpoints"]), 2)
        self.assertTrue(endpoints["us_basic"]["pagination_probe_attempted"])
        self.assertTrue(endpoints["us_basic"]["pagination_supported"])
        self.assertTrue(all("token" not in json.dumps(call).lower() for call in client.calls))

    def test_probe_output_must_be_under_tmp(self):
        output = Path(__file__).resolve().parents[2] / "hk-us-probe.json"
        with self.assertRaisesRegex(ValueError, "under /tmp"):
            tushare_real_smoke.run_hk_us_low_risk_probe(output, 2, token="")

    def test_probe_request_cap_is_enforced_before_requests(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "probe.json"
            with self.assertRaisesRegex(ValueError, "<= 2"):
                tushare_real_smoke.run_hk_us_low_risk_probe(output, 3, token="secret-token", client=FakeProbeClient())

    def test_financial_probe_with_fake_client_is_bounded_redacted_and_records_disclosure_fields(self):
        client = FakeFinancialProbeClient()
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "financial-probe.json"
            with redirect_stdout(StringIO()):
                code = tushare_real_smoke.run_hk_us_financial_pit_probe(output, 1, token="secret-token-1234567890", client=client)
            raw = output.read_text()
            payload = json.loads(raw)
        self.assertEqual(code, 0)
        self.assertEqual(payload["report_version"], "hk-us-financial-pit-probe/v1")
        self.assertTrue(payload["real_requests_sent"])
        self.assertNotIn("secret-token-1234567890", raw)
        self.assertFalse(payload["token_plaintext_found"])
        self.assertEqual(len(payload["endpoints"]), 8)
        self.assertLessEqual(max(item["request_count"] for item in payload["endpoints"]), 1)
        endpoints = {item["api_name"]: item for item in payload["endpoints"]}
        self.assertEqual(endpoints["hk_income"]["probe_status"], "passed")
        self.assertEqual(endpoints["hk_income"]["observed_disclosure_fields"], [])
        self.assertEqual(endpoints["hk_fina_indicator"]["observed_disclosure_fields"], ["notice_date"])
        self.assertTrue(all("token" not in json.dumps(call).lower() for call in client.calls))

    def test_financial_probe_without_token_writes_blocked_report_and_sends_no_requests(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "financial-probe.json"
            with redirect_stdout(StringIO()):
                code = tushare_real_smoke.run_hk_us_financial_pit_probe(output, 1, token="")
            payload = json.loads(output.read_text())
        self.assertEqual(code, 2)
        self.assertEqual(payload["overall_status"], "blocked")
        self.assertFalse(payload["real_requests_sent"])
        self.assertEqual(payload["endpoints"], [])

    def test_financial_probe_permission_denied_is_warning_not_code_failure(self):
        client = FakeFinancialProbeClient(permission_denied={"us_income"})
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "financial-probe.json"
            with redirect_stdout(StringIO()):
                code = tushare_real_smoke.run_hk_us_financial_pit_probe(output, 1, token="secret-token", client=client)
            payload = json.loads(output.read_text())
        self.assertEqual(code, 0)
        self.assertEqual(payload["overall_status"], "warning")
        endpoints = {item["api_name"]: item for item in payload["endpoints"]}
        self.assertEqual(endpoints["us_income"]["probe_status"], "permission_denied")

    def test_financial_probe_request_cap_and_output_path_are_enforced(self):
        output = Path(__file__).resolve().parents[2] / "financial-probe.json"
        with self.assertRaisesRegex(ValueError, "under /tmp"):
            tushare_real_smoke.run_hk_us_financial_pit_probe(output, 1, token="")
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            with self.assertRaisesRegex(ValueError, "<= 2"):
                tushare_real_smoke.run_hk_us_financial_pit_probe(Path(tmp) / "probe.json", 3, token="secret-token", client=FakeFinancialProbeClient())

    def test_no_default_real_request_is_selected(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 2)
        self.assertIn("No endpoints selected", result.stderr)
        self.assertNotIn("TUSHARE_TOKEN is required", result.stderr)
