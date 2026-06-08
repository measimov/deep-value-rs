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


class FakeSecHttp:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, headers: dict[str, str]):
        self.calls.append((url, dict(headers)))
        return {
            "filings": {
                "recent": {
                    "form": ["10-K", "8-K", "10-Q"],
                    "filingDate": ["2025-02-26", "2025-01-13", "2024-11-20"],
                    "reportDate": ["2024-12-31", "2025-01-13", "2024-10-27"],
                    "accessionNumber": ["0001045810-25-000023", "0001045810-25-000005", "0001045810-24-000316"],
                    "acceptanceDateTime": ["2025-02-26T21:36:00.000Z", "2025-01-13T21:10:00.000Z", "2024-11-20T21:15:00.000Z"],
                    "primaryDocument": ["nvda-20250126.htm", "nvda-8k.htm", "nvda-20241027.htm"],
                }
            }
        }


class FakeCrossCheckTushareClient:
    def __init__(self, notice_date: str | None, msg: str | None = None):
        self.notice_date = notice_date
        self.msg = msg
        self.calls: list[tuple[str, dict, list[str]]] = []

    def request(self, api_name, params, fields=None):
        fields_list = list(fields or [])
        self.calls.append((api_name, dict(params), fields_list))
        if self.notice_date is None:
            return {"code": 0, "msg": self.msg, "data": {"fields": fields_list, "items": []}}
        row = []
        for field in fields_list:
            if field == "ts_code":
                row.append(params.get("ts_code"))
            elif field in {"end_date", "start_date", "std_report_date", "financial_date"}:
                row.append(params.get("period"))
            elif field == "notice_date":
                row.append(self.notice_date)
            else:
                row.append("x")
        return {"code": 0, "msg": self.msg, "data": {"fields": fields_list, "items": [row]}}


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
        self.assertIn("--sec-disclosure-probe", result.stdout)
        self.assertIn("--sec-tushare-disclosure-cross-check", result.stdout)
        self.assertIn("--max-requests-per-endpoint", result.stdout)
        self.assertIn("--max-requests", result.stdout)

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

    def test_sec_disclosure_probe_with_fake_http_is_bounded_and_writes_event(self):
        fake_http = FakeSecHttp()
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "sec-probe.json"
            with redirect_stdout(StringIO()):
                code = tushare_real_smoke.run_sec_disclosure_probe(
                    output,
                    ticker="NVDA",
                    cik="0001045810",
                    period="20241231",
                    max_requests=3,
                    http_get=fake_http,
                )
            payload = json.loads(output.read_text())
        self.assertEqual(code, 0)
        self.assertEqual(payload["report_version"], "sec-disclosure-probe/v1")
        self.assertEqual(payload["overall_status"], "passed")
        self.assertEqual(payload["request_count"], 1)
        self.assertTrue(payload["real_requests_sent"])
        self.assertEqual(len(fake_http.calls), 1)
        url, headers = fake_http.calls[0]
        self.assertIn("CIK0001045810.json", url)
        self.assertTrue(headers["User-Agent"])
        self.assertEqual(payload["matched_filings"][0]["form"], "10-K")
        event = payload["disclosure_events"][0]
        self.assertEqual(event["source"], "sec_edgar_submissions")
        self.assertEqual(event["pit_strength"], "availability_only")
        self.assertEqual(event["disclosure_date"], "20250226")
        self.assertFalse(event["as_filed_value_verified"])

    def test_sec_disclosure_probe_output_path_and_request_cap_are_enforced(self):
        output = Path(__file__).resolve().parents[2] / "sec-probe.json"
        with self.assertRaisesRegex(ValueError, "under /tmp"):
            tushare_real_smoke.run_sec_disclosure_probe(
                output,
                ticker="NVDA",
                cik="0001045810",
                period="20241231",
                max_requests=3,
                http_get=FakeSecHttp(),
            )
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            with self.assertRaisesRegex(ValueError, "<= 3"):
                tushare_real_smoke.run_sec_disclosure_probe(
                    Path(tmp) / "sec-probe.json",
                    ticker="NVDA",
                    cik="0001045810",
                    period="20241231",
                    max_requests=4,
                    http_get=FakeSecHttp(),
                )

    def test_sec_tushare_cross_check_token_missing_is_structured(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            output = Path(tmp) / "cross-check.json"
            with redirect_stdout(StringIO()):
                code = tushare_real_smoke.run_sec_tushare_disclosure_cross_check(
                    output,
                    api_name="us_fina_indicator",
                    ts_code="NVDA.US",
                    ticker="NVDA",
                    cik="0001045810",
                    period="20241231",
                    max_sec_requests=3,
                    max_tushare_requests=1,
                    token="",
                    sec_http_get=FakeSecHttp(),
                )
            payload = json.loads(output.read_text())
        self.assertEqual(code, 0)
        self.assertEqual(payload["report_version"], "sec-tushare-disclosure-cross-check/v1")
        self.assertEqual(payload["sec_status"], "passed")
        self.assertEqual(payload["tushare_status"], "blocked_token_missing")
        self.assertEqual(payload["tushare_request_count"], 0)
        self.assertEqual(payload["overall_status"], "warning")
        self.assertEqual(payload["sec_disclosure_date"], "20250226")

    def test_sec_tushare_cross_check_classifies_exact_near_period_and_unmatched(self):
        cases = [
            ("20250226", "exact", 0, "availability_only"),
            ("20250228", "near", 2, "availability_only"),
            ("20250320", "period_only", 22, "raw_only"),
            (None, "unmatched", None, "raw_only"),
        ]
        for notice_date, match_status, delta, strength in cases:
            with self.subTest(notice_date=notice_date):
                client = FakeCrossCheckTushareClient(notice_date)
                with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
                    output = Path(tmp) / "cross-check.json"
                    with redirect_stdout(StringIO()):
                        code = tushare_real_smoke.run_sec_tushare_disclosure_cross_check(
                            output,
                            api_name="us_fina_indicator",
                            ts_code="NVDA.US",
                            ticker="NVDA",
                            cik="0001045810",
                            period="20241231",
                            max_sec_requests=3,
                            max_tushare_requests=1,
                            token="secret-token-1234567890",
                            sec_http_get=FakeSecHttp(),
                            tushare_client=client,
                        )
                    payload = json.loads(output.read_text())
                    raw = output.read_text()
                self.assertEqual(code, 0)
                self.assertEqual(payload["match_status"], match_status)
                self.assertEqual(payload["date_delta_days"], delta)
                self.assertEqual(payload["pit_strength_candidate"], strength)
                self.assertEqual(client.calls[0][1]["ts_code"], "NVDA")
                self.assertNotIn("secret-token-1234567890", raw)

    def test_sec_tushare_cross_check_limits_and_output_path_are_enforced(self):
        output = Path(__file__).resolve().parents[2] / "cross-check.json"
        kwargs = {
            "api_name": "us_fina_indicator",
            "ts_code": "NVDA.US",
            "ticker": "NVDA",
            "cik": "0001045810",
            "period": "20241231",
            "max_sec_requests": 3,
            "max_tushare_requests": 1,
            "token": "",
            "sec_http_get": FakeSecHttp(),
        }
        with self.assertRaisesRegex(ValueError, "under /tmp"):
            tushare_real_smoke.run_sec_tushare_disclosure_cross_check(output, **kwargs)
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            with self.assertRaisesRegex(ValueError, "<= 3"):
                tushare_real_smoke.run_sec_tushare_disclosure_cross_check(
                    Path(tmp) / "cross-check.json",
                    **{**kwargs, "max_sec_requests": 4},
                )
            with self.assertRaisesRegex(ValueError, "<= 1"):
                tushare_real_smoke.run_sec_tushare_disclosure_cross_check(
                    Path(tmp) / "cross-check.json",
                    **{**kwargs, "max_tushare_requests": 2},
                )

    def test_no_default_real_request_is_selected(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 2)
        self.assertIn("No endpoints selected", result.stderr)
        self.assertNotIn("TUSHARE_TOKEN is required", result.stderr)
