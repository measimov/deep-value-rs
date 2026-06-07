from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.source_metadata import (
    HIGH_RISK_HK_US_APIS,
    hk_us_low_risk_source_endpoints,
    hk_us_low_risk_source_map_json,
    load_hk_us_low_risk_source_map,
    validate_hk_us_low_risk_source_map,
)


class HKUSLowRiskSourceMapTests(unittest.TestCase):
    def test_source_metadata_loads_without_side_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            before = set(Path(tmp).iterdir())
            payload = load_hk_us_low_risk_source_map()
            after = set(Path(tmp).iterdir())
        self.assertEqual(before, after)
        self.assertEqual(payload["source_map_version"], "hk-us-low-risk-source-map/v1")
        endpoints = {item["api_name"]: item for item in payload["endpoints"]}
        for api_name in [
            "hk_basic",
            "hk_tradecal",
            "hk_daily",
            "hk_daily_adj",
            "hk_adjfactor",
            "us_basic",
            "us_tradecal",
            "us_daily",
            "us_daily_adj",
            "us_adjfactor",
        ]:
            self.assertEqual(endpoints[api_name]["recommendation"], "executable_candidate")
            self.assertEqual(endpoints[api_name]["real_probe_status"], "passed")
            self.assertEqual(endpoints[api_name]["real_probe_observed"]["status"], "accessible")
            self.assertGreater(endpoints[api_name]["real_probe_observed"]["row_count"], 0)

    def test_all_candidate_endpoints_have_doc_urls_and_fields(self):
        endpoints = hk_us_low_risk_source_endpoints()
        self.assertGreaterEqual(len(endpoints), 20)
        for endpoint in endpoints:
            with self.subTest(api_name=endpoint["api_name"]):
                self.assertTrue(endpoint["doc_url"].startswith("https://tushare.pro/document/2?doc_id="))
                self.assertTrue(endpoint["documented_params"])
                self.assertTrue(endpoint["documented_fields"])
                self.assertIn(endpoint["recommendation"], {"executable_candidate", "plan_only", "disabled"})

    def test_minute_realtime_and_financial_endpoints_are_not_executable(self):
        endpoints = {item["api_name"]: item for item in hk_us_low_risk_source_endpoints()}
        for api_name in HIGH_RISK_HK_US_APIS | {"hk_mins", "rt_hk_k"}:
            with self.subTest(api_name=api_name):
                self.assertIn(api_name, endpoints)
                self.assertNotEqual(endpoints[api_name]["recommendation"], "executable_candidate")
                joined_notes = " ".join(endpoints[api_name]["safety_notes"])
                self.assertTrue("outside this goal" in joined_notes or "no execution in this goal" in joined_notes)

    def test_financial_source_map_distinguishes_documented_fields_from_pit_assumptions(self):
        endpoints = {item["api_name"]: item for item in hk_us_low_risk_source_endpoints()}
        statement_apis = [
            "hk_income",
            "hk_balancesheet",
            "hk_cashflow",
            "us_income",
            "us_balancesheet",
            "us_cashflow",
        ]
        for api_name in statement_apis:
            with self.subTest(api_name=api_name):
                item = endpoints[api_name]
                self.assertEqual(item["documented_output_fields"], item["documented_fields"])
                self.assertEqual(item["pit_disclosure_fields_in_documented_output"], [])
                self.assertEqual(item["pit_disclosure_availability"], "uncertain")
                self.assertIn("ann_date", item["assumed_pit_fields"])
                self.assertFalse(item["raw_mirror_candidate"])
                self.assertFalse(item["pit_safe_candidate"])
                self.assertIn("do not include ann_date", item["pit_disclosure_concern"])

        for api_name in ["hk_fina_indicator", "us_fina_indicator"]:
            with self.subTest(api_name=api_name):
                item = endpoints[api_name]
                self.assertIn("notice_date", item["documented_output_fields"])
                self.assertEqual(item["pit_disclosure_fields_in_documented_output"], ["notice_date"])
                self.assertEqual(item["pit_disclosure_availability"], "notice_date_possible")
                self.assertEqual(item["pagination_verification_status"], "pending_financial_probe")

    def test_pagination_findings_are_recorded_for_doc_ambiguous_endpoints(self):
        endpoints = {item["api_name"]: item for item in hk_us_low_risk_source_endpoints()}
        for api_name in ["hk_daily_adj", "us_daily"]:
            with self.subTest(api_name=api_name):
                self.assertEqual(endpoints[api_name]["recommended_pagination_strategy"], "offset_limit")
                self.assertTrue(endpoints[api_name]["missing_metadata"])
                observed = endpoints[api_name]["real_probe_observed"]
                self.assertTrue(observed["pagination_probe_attempted"])
                self.assertTrue(observed["pagination_supported"])

    def test_source_map_json_is_stable_and_contains_no_token_plaintext(self):
        payload = json.loads(hk_us_low_risk_source_map_json())
        encoded = json.dumps(payload, sort_keys=True)
        self.assertEqual(payload["source_map_version"], "hk-us-low-risk-source-map/v1")
        self.assertNotIn("TUSHARE_TOKEN", encoded)
        self.assertNotIn("fake-token", encoded)

    def test_source_map_validation_passes(self):
        self.assertEqual(validate_hk_us_low_risk_source_map(), [])
