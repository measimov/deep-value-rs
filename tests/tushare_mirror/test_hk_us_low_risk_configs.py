from __future__ import annotations

import unittest

from tushare_mirror.capabilities import capability_from_config
from tushare_mirror.endpoints import bundled_endpoint_config, load_bundled_endpoint_configs, load_inventory_configs
from tushare_mirror.policy import EndpointExecutionPolicy, ExecutionPolicyRequest


HK_US_EXECUTABLE_APIS = {
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
}

HK_US_DISABLED_APIS = {
    "hk_mins",
    "rt_hk_k",
    "hk_income",
    "hk_balancesheet",
    "hk_cashflow",
    "hk_fina_indicator",
    "us_income",
    "us_balancesheet",
    "us_cashflow",
    "us_fina_indicator",
}


class HKUSLowRiskEndpointConfigTests(unittest.TestCase):
    def test_executable_configs_load_with_required_metadata(self):
        configs = {cfg["api_name"]: cfg for cfg in load_bundled_endpoint_configs()}
        self.assertTrue(HK_US_EXECUTABLE_APIS.issubset(configs))
        for api_name in HK_US_EXECUTABLE_APIS:
            with self.subTest(api_name=api_name):
                cfg = bundled_endpoint_config(api_name)
                capability = capability_from_config(cfg)
                self.assertEqual(capability.execution_status, "enabled")
                self.assertIn(capability.planner_kind, {"single_snapshot", "date_backfill", "calendar_backfill"})
                self.assertIn(capability.pagination_mode, {"none", "offset"})
                self.assertEqual(cfg["real_probe_status"], "passed")
                self.assertTrue(cfg["doc_url"].startswith("https://tushare.pro/document/2?doc_id="))
                self.assertTrue(cfg["default_fields"])
                self.assertTrue(cfg["supported_params"])
                self.assertTrue(cfg["required_infra"])

    def test_pagination_metadata_matches_probe_contract(self):
        configs = {cfg["api_name"]: bundled_endpoint_config(cfg["api_name"]) for cfg in load_bundled_endpoint_configs() if cfg["api_name"] in HK_US_EXECUTABLE_APIS}
        for api_name in ["hk_daily_adj", "us_basic", "us_daily", "us_daily_adj"]:
            with self.subTest(api_name=api_name):
                self.assertEqual(configs[api_name]["pagination_mode"], "offset")
                self.assertGreater(configs[api_name]["page_size"], 0)
                self.assertIn("offset", configs[api_name]["supported_params"])
                self.assertIn("limit", configs[api_name]["supported_params"])
        for api_name in ["hk_basic", "hk_tradecal", "hk_daily", "hk_adjfactor", "us_tradecal", "us_adjfactor"]:
            with self.subTest(api_name=api_name):
                self.assertEqual(configs[api_name]["pagination_mode"], "none")
                self.assertNotIn("offset", configs[api_name]["supported_params"])
                self.assertNotIn("limit", configs[api_name]["supported_params"])

    def test_disabled_inventory_entries_load_and_remain_non_executable(self):
        inventory = {cfg["api_name"]: cfg for cfg in load_inventory_configs()}
        executable = {cfg["api_name"] for cfg in load_bundled_endpoint_configs()}
        self.assertTrue(HK_US_DISABLED_APIS.issubset(inventory))
        self.assertTrue(HK_US_DISABLED_APIS.isdisjoint(executable))
        for api_name in HK_US_DISABLED_APIS:
            with self.subTest(api_name=api_name):
                cfg = inventory[api_name]
                self.assertEqual(cfg["execution_status"], "disabled")
                self.assertTrue(cfg["reason_disabled"])
                decision = EndpointExecutionPolicy().decide(
                    ExecutionPolicyRequest(endpoint_config=cfg, user_command="fetch", max_jobs=1)
                )
                self.assertFalse(decision.execution_allowed)
                self.assertIn(decision.decision, {"blocked", "unsupported"})

    def test_executable_policy_allows_only_supported_low_risk_planners(self):
        for api_name in HK_US_EXECUTABLE_APIS:
            with self.subTest(api_name=api_name):
                cfg = bundled_endpoint_config(api_name)
                decision = EndpointExecutionPolicy().decide(
                    ExecutionPolicyRequest(endpoint_config=cfg, user_command="fetch", max_jobs=1)
                )
                self.assertTrue(decision.execution_allowed, decision.to_dict())
                self.assertFalse(decision.requires_code_loop)
                self.assertFalse(decision.requires_period_loop)

