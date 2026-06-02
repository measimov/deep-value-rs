from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tushare_mirror.capabilities import (
    ENDPOINT_KIND_VALUES,
    PLANNER_KIND_VALUES,
    CapabilityValidationError,
    capability_from_config,
    normalize_endpoint_capability,
)
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog


class EndpointCapabilityTaxonomyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.catalog = CatalogStore(self.root)
        self.catalog.init()

    def tearDown(self):
        self.tmp.cleanup()

    def test_enabled_endpoint_configs_are_normalized_with_allowed_capabilities(self):
        load_into_catalog(self.root, self.catalog)
        endpoints = self.catalog.list_endpoints()
        self.assertEqual(len(endpoints), 12)
        for row in endpoints:
            cfg = self.catalog.get_endpoint_config(row["api_name"])
            capability = capability_from_config(cfg)
            self.assertIn(capability.endpoint_kind, ENDPOINT_KIND_VALUES)
            self.assertIn(capability.planner_kind, PLANNER_KIND_VALUES)
            self.assertEqual(capability.execution_status, "enabled")
            self.assertIsInstance(capability.supported_params, list)
            self.assertIsInstance(capability.default_fields, list)
            self.assertIsInstance(capability.probe_params, dict)
            self.assertIsInstance(capability.probe_fields, list)
            self.assertIn("requires_disclosure_date", capability.pit_safety)
            self.assertIn("pagination_mode", capability.to_dict())

    def test_known_low_risk_endpoint_kinds_and_planners_are_inferred(self):
        load_into_catalog(self.root, self.catalog)
        expected = {
            "stock_basic": ("reference_snapshot", "single_snapshot"),
            "trade_cal": ("calendar", "single_snapshot"),
            "daily": ("daily_bar", "calendar_backfill"),
            "adj_factor": ("daily_metric", "calendar_backfill"),
            "daily_basic": ("daily_metric", "calendar_backfill"),
            "weekly": ("daily_bar", "explicit_dates"),
            "monthly": ("daily_bar", "explicit_dates"),
            "suspend_d": ("event", "calendar_backfill"),
            "namechange": ("event", "single_snapshot"),
            "hs_const": ("constituent", "single_snapshot"),
            "stk_managers": ("company_governance", "single_snapshot"),
            "stk_rewards": ("company_governance", "single_snapshot"),
        }
        for api_name, (endpoint_kind, planner_kind) in expected.items():
            cfg = self.catalog.get_endpoint_config(api_name)
            self.assertEqual(cfg["endpoint_kind"], endpoint_kind)
            self.assertEqual(cfg["planner_kind"], planner_kind)

    def test_invalid_endpoint_kind_and_planner_kind_fail_clearly(self):
        base = {
            "api_name": "bad_api",
            "family": "stock",
            "market": "a",
            "domain": "stock",
            "volume_class": "S0_STATIC",
            "endpoint_kind": "reference_snapshot",
            "planner_kind": "single_snapshot",
            "pagination_mode": "paged",
            "execution_status": "enabled",
            "probe": {"params": {}, "fields": []},
        }
        bad_endpoint = dict(base, endpoint_kind="not_a_kind")
        with self.assertRaisesRegex(CapabilityValidationError, "unsupported endpoint_kind"):
            capability_from_config(bad_endpoint)
        bad_planner = dict(base, planner_kind="not_a_planner")
        with self.assertRaisesRegex(CapabilityValidationError, "unsupported planner_kind"):
            capability_from_config(bad_planner)

    def test_normalization_supplies_required_capability_defaults(self):
        cfg = normalize_endpoint_capability(
            {
                "api_name": "daily",
                "family": "stock",
                "market": "a",
                "domain": "stock",
                "volume_class": "D1_DAILY_NARROW",
                "partition": {"name": "daily_month_v1", "date_field": "trade_date"},
                "probe": {"params": {"trade_date": "20250102"}, "fields": ["ts_code", "trade_date"]},
            }
        )
        self.assertEqual(cfg["endpoint_kind"], "daily_bar")
        self.assertEqual(cfg["planner_kind"], "calendar_backfill")
        self.assertEqual(cfg["partition_template"], "year_month")
        self.assertEqual(cfg["primary_date_field"], "trade_date")
        self.assertEqual(cfg["execution_status"], "enabled")

