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
            "us_basic",
            "us_tradecal",
            "us_daily",
            "us_daily_adj",
            "us_adjfactor",
        ]:
            self.assertEqual(endpoints[api_name]["recommendation"], "executable_candidate")
            self.assertEqual(endpoints[api_name]["real_probe_status"], "pending")

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

    def test_pagination_risks_are_explicit_for_ambiguous_endpoints(self):
        endpoints = {item["api_name"]: item for item in hk_us_low_risk_source_endpoints()}
        for api_name in ["hk_daily_adj", "us_daily"]:
            with self.subTest(api_name=api_name):
                self.assertEqual(endpoints[api_name]["recommended_pagination_strategy"], "unknown_until_probe")
                self.assertTrue(endpoints[api_name]["missing_metadata"])

    def test_source_map_json_is_stable_and_contains_no_token_plaintext(self):
        payload = json.loads(hk_us_low_risk_source_map_json())
        encoded = json.dumps(payload, sort_keys=True)
        self.assertEqual(payload["source_map_version"], "hk-us-low-risk-source-map/v1")
        self.assertNotIn("TUSHARE_TOKEN", encoded)
        self.assertNotIn("fake-token", encoded)

    def test_source_map_validation_passes(self):
        self.assertEqual(validate_hk_us_low_risk_source_map(), [])
