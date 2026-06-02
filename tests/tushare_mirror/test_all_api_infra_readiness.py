from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sqlite3

from tushare_mirror.capabilities import (
    ENDPOINT_KIND_VALUES,
    PLANNER_KIND_VALUES,
    CapabilityValidationError,
    capability_from_config,
    normalize_endpoint_capability,
)
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.planner_registry import BLOCKED_PLANNER_INFRA, PlannerRegistry, PlannerRegistryRequest, planner_registry_summary


class EndpointCapabilityTaxonomyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.catalog = CatalogStore(self.root)
        self.catalog.init()

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        with sqlite3.connect(self.root / "_catalog" / "catalog.sqlite") as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

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


class PlannerRegistryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        with sqlite3.connect(self.root / "_catalog" / "catalog.sqlite") as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def test_supported_planner_kinds_resolve_without_catalog_mutation(self):
        registry = PlannerRegistry(self.root, self.catalog)
        before = self.counts()
        cases = [
            PlannerRegistryRequest("stock_basic", "single_snapshot", params={"list_status": "L"}),
            PlannerRegistryRequest("daily", "calendar_backfill", dates=["20250102"], max_jobs=1),
            PlannerRegistryRequest("adj_factor", "date_backfill", dates=["20250102"], max_jobs=1),
            PlannerRegistryRequest("weekly", "explicit_dates", dates=["20250103"], max_jobs=1),
        ]
        results = [registry.plan(case) for case in cases]
        after = self.counts()
        self.assertEqual(before, after)
        self.assertTrue(all(result.status == "supported" for result in results))
        self.assertEqual([result.planner_kind for result in results], ["single_snapshot", "calendar_backfill", "date_backfill", "explicit_dates"])
        for result in results:
            payload = result.to_dict()
            self.assertIn("requires_real_requests", payload)
            self.assertIn("requires_user_confirmation", payload)
            self.assertIsNotNone(payload["plan"])

    def test_unsupported_future_planner_kinds_block_safely(self):
        registry = PlannerRegistry(self.root, self.catalog)
        before = self.counts()
        for planner_kind, missing in BLOCKED_PLANNER_INFRA.items():
            result = registry.blocked_plan(f"future_{planner_kind}", planner_kind)
            self.assertEqual(result.status, "blocked")
            self.assertEqual(result.blocked_reason, "planner_infrastructure_missing")
            self.assertEqual(result.missing_infrastructure, missing)
            self.assertEqual(result.planned_jobs, 0)
            self.assertTrue(result.requires_user_confirmation)
            payload = result.to_dict()
            self.assertEqual(payload["plan_type"], "blocked")
            self.assertIn("missing_infrastructure", payload)
        after = self.counts()
        self.assertEqual(before, after)

    def test_planner_registry_summary_is_stable(self):
        summary = planner_registry_summary()
        self.assertEqual(summary["supported_planner_kinds"], ["calendar_backfill", "date_backfill", "explicit_dates", "single_snapshot"])
        for planner_kind in ["code_list", "code_date_matrix", "period", "object_download", "bucketed_intraday", "realtime_poll"]:
            self.assertIn(planner_kind, summary["blocked_planner_kinds"])
            self.assertIn(planner_kind, summary["blocked_missing_infrastructure"])
