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

    def test_no_default_real_request_is_selected(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 2)
        self.assertIn("No endpoints selected", result.stderr)
        self.assertNotIn("TUSHARE_TOKEN is required", result.stderr)
