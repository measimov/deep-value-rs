from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_bundled_endpoint_configs, load_into_catalog, load_inventory_configs
from tushare_mirror.mirror import MirrorScopeReporter


class AShareLowRiskScopeTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

    def test_a_share_low_risk_scope_exists_and_is_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            before = set(Path(tmp).iterdir())
            report = MirrorScopeReporter().report(scope="a-share-low-risk")
            after = set(Path(tmp).iterdir())
        payload = report.to_dict()
        self.assertEqual(before, after)
        self.assertEqual(payload["report_version"], "mirror-scope/v1")
        self.assertEqual(payload["scope"], "a-share-low-risk")
        self.assertIn("stock_basic", payload["endpoints_in_scope"])
        self.assertIn("stock_company", payload["endpoints_in_scope"])
        self.assertIn("daily", payload["executable_now"])
        self.assertIn("stock_company", payload["executable_now"])
        self.assertIn("concept", payload["executable_now"])
        self.assertIn("index_daily", payload["executable_now"])
        self.assertIn("top10_holders", payload["disabled"])
        self.assertEqual(payload["missing_metadata"], [])
        self.assertIn("next_enablement_step", payload)

    def test_high_risk_families_are_excluded(self):
        payload = MirrorScopeReporter().report(scope="a-share-low-risk").to_dict()
        scoped = set(payload["endpoints_in_scope"])
        excluded = {
            "stk_mins",
            "moneyflow_hsgt",
            "income",
            "balancesheet",
            "cashflow",
            "fina_indicator",
            "anns",
            "news",
            "realtime_quote",
        }
        self.assertTrue(scoped.isdisjoint(excluded))
        for pattern in ["minute", "tick", "order", "realtime", "income", "anns", "news"]:
            self.assertIn(pattern, payload["excluded_high_risk_patterns"])

    def test_cli_json_contract_is_stable(self):
        result = self.run_cli("mirror-scope", "--scope", "a-share-low-risk", "--json")
        payload = json.loads(result.stdout)
        self.assertEqual(payload["report_version"], "mirror-scope/v1")
        for key in [
            "endpoints_in_scope",
            "executable_now",
            "plan_only",
            "disabled",
            "blocked_reason",
            "missing_metadata",
            "next_enablement_step",
        ]:
            self.assertIn(key, payload)
        self.assertIn("daily", payload["executable_now"])
        self.assertIn("stock_company", payload["executable_now"])
        self.assertIn("top10_holders", payload["disabled"])

    def test_unknown_scope_is_rejected(self):
        result = subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "mirror-scope", "--scope", "all", "--json"],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unknown mirror scope", result.stderr)


class AShareLowRiskEndpointMetadataTests(unittest.TestCase):
    EXECUTABLE_ADDED = {
        "stock_company",
        "concept",
        "index_basic",
        "index_daily",
        "index_weekly",
        "index_monthly",
        "ths_index",
        "index_classify",
    }

    DISABLED_LOW_RISK = {
        "top10_holders",
        "top10_floatholders",
        "stk_holdernumber",
        "stk_holdertrade",
        "pledge_stat",
        "pledge_detail",
        "repurchase",
        "concept_detail",
        "index_weight",
        "index_member",
        "ths_member",
    }

    def test_new_executable_configs_load_with_required_metadata(self):
        configs = {cfg["api_name"]: cfg for cfg in load_bundled_endpoint_configs()}
        self.assertTrue(self.EXECUTABLE_ADDED <= set(configs))
        for api_name in self.EXECUTABLE_ADDED:
            cfg = configs[api_name]
            for key in [
                "family",
                "domain",
                "endpoint_kind",
                "planner_kind",
                "execution_status",
                "volume_class",
                "partition_template",
                "supported_params",
                "default_fields",
                "probe",
                "risk_level",
                "required_infra",
                "notes",
            ]:
                self.assertIn(key, cfg, f"{api_name}:{key}")
            self.assertEqual(cfg["execution_status"], "enabled")
            self.assertIn(cfg["planner_kind"], {"single_snapshot", "calendar_backfill", "explicit_dates"})

    def test_unsafe_candidates_are_disabled_inventory_only(self):
        inventory = {cfg["api_name"]: cfg for cfg in load_inventory_configs()}
        self.assertTrue(self.DISABLED_LOW_RISK <= set(inventory))
        for api_name in self.DISABLED_LOW_RISK:
            cfg = inventory[api_name]
            self.assertEqual(cfg["execution_status"], "disabled")
            self.assertIn(cfg["planner_kind"], {"code_list", "code_date_matrix", "code_period_matrix", "date_backfill"})
            self.assertTrue(cfg["required_infra"])
            self.assertIn("domain", cfg)
            self.assertIn("family", cfg)

    def test_executable_catalog_excludes_disabled_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "lake"
            catalog = CatalogStore(root)
            catalog.init()
            load_into_catalog(root, catalog)
            executable = {row["api_name"] for row in catalog.list_endpoints()}
        inventory = {cfg["api_name"] for cfg in load_inventory_configs()}
        self.assertTrue(self.EXECUTABLE_ADDED <= executable)
        self.assertTrue(self.DISABLED_LOW_RISK.isdisjoint(executable))
        self.assertTrue(inventory.isdisjoint(executable))


if __name__ == "__main__":
    unittest.main()
