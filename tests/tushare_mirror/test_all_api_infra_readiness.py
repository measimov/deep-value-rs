from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
import sqlite3
import subprocess
import sys

from tushare_mirror.api_infra import ApiInfrastructureReadinessReporter
from tushare_mirror.capabilities import (
    ENDPOINT_KIND_VALUES,
    PLANNER_KIND_VALUES,
    CapabilityValidationError,
    capability_from_config,
    normalize_endpoint_capability,
)
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import enrich_endpoint_config, load_into_catalog, load_inventory_configs, validate_inventory_config
from tushare_mirror.errors import MirrorError
from tushare_mirror.mirror import MirrorPlanner
from tushare_mirror.policy import EndpointExecutionPolicy, ExecutionPolicyRequest
from tushare_mirror.planner import JobPlanner
from tushare_mirror.planner_registry import BLOCKED_PLANNER_INFRA, PlannerRegistry, PlannerRegistryRequest, planner_registry_summary
from tushare_mirror.store import FileLakeStore


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


class DisabledEndpointInventoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.catalog = CatalogStore(self.root)
        self.catalog.init()

    def tearDown(self):
        self.tmp.cleanup()

    def test_inventory_stubs_parse_and_remain_disabled(self):
        inventory = load_inventory_configs()
        self.assertGreaterEqual(len(inventory), 10)
        api_names = {item["api_name"] for item in inventory}
        self.assertIn("income", api_names)
        self.assertIn("anns", api_names)
        self.assertIn("stk_mins", api_names)
        self.assertTrue(all(item["execution_status"] == "disabled" for item in inventory))
        self.assertTrue(all(item["required_infra"] for item in inventory))

    def test_disabled_inventory_endpoints_do_not_enter_executable_catalog_or_scope(self):
        load_into_catalog(self.root, self.catalog)
        executable = {row["api_name"] for row in self.catalog.list_endpoints()}
        inventory = {item["api_name"] for item in load_inventory_configs()}
        self.assertTrue(inventory.isdisjoint(executable))
        self.assertEqual(len(executable), 12)
        plan = MirrorPlanner(self.root, self.catalog).plan(scope="low-risk-a-share", mode="smoke", max_jobs_per_api=3)
        planned = {item.endpoint for item in plan.items}
        self.assertTrue(inventory.isdisjoint(planned))

    def test_disabled_endpoint_cannot_be_planned_for_fetch_without_enablement(self):
        load_into_catalog(self.root, self.catalog)
        with self.assertRaisesRegex(KeyError, "endpoint not found"):
            JobPlanner(self.root, self.catalog).plan_single_fetch("income", {})

    def test_malformed_inventory_fails_clearly(self):
        with self.assertRaisesRegex(ValueError, "malformed inventory endpoint"):
            validate_inventory_config({"api_name": "bad_inventory"}, "bad.yaml")
        with self.assertRaisesRegex(ValueError, "must be disabled"):
            validate_inventory_config(
                {
                    "api_name": "bad_inventory",
                    "endpoint_kind": "macro",
                    "planner_kind": "period",
                    "execution_status": "enabled",
                    "reason_disabled": "test",
                    "required_infra": ["period planner"],
                    "risk_level": "medium",
                    "notes": "test",
                },
                "bad.yaml",
            )


class ExecutionPolicyGuardrailTests(unittest.TestCase):
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

    def test_low_risk_existing_endpoints_are_allowed_by_policy(self):
        policy = EndpointExecutionPolicy()
        for row in self.catalog.list_endpoints():
            cfg = self.catalog.get_endpoint_config(row["api_name"])
            decision = policy.decide(ExecutionPolicyRequest(endpoint_config=cfg, scope="low-risk-a-share", mode="pilot", user_command="fetch", max_jobs=1))
            self.assertEqual(decision.decision, "allow", cfg["api_name"])
            self.assertTrue(decision.allowed)
            self.assertFalse(decision.missing_infrastructure)

    def test_disabled_and_unsupported_inventory_endpoints_block_with_clear_json(self):
        policy = EndpointExecutionPolicy()
        inventory = {item["api_name"]: item for item in load_inventory_configs()}
        for api_name in ["income", "fina_indicator", "stk_mins", "tick", "anns", "news", "dividend"]:
            decision = policy.decide(ExecutionPolicyRequest(endpoint_config=inventory[api_name], scope="low-risk-a-share", mode="pilot", user_command="fetch", max_jobs=1))
            self.assertEqual(decision.decision, "blocked")
            payload = decision.to_dict()
            self.assertEqual(payload["api_name"], api_name)
            self.assertIn(payload["reason"], {"endpoint_disabled", "missing_required_infrastructure"})
            self.assertTrue(payload["missing_infrastructure"])
            self.assertTrue(payload["requires_user_confirmation"])

    def test_policy_blocks_high_risk_classes_even_if_misconfigured_as_enabled(self):
        policy = EndpointExecutionPolicy()
        risky = {
            "api_name": "income",
            "endpoint_kind": "financial_statement",
            "planner_kind": "code_period_matrix",
            "execution_status": "enabled",
        }
        decision = policy.decide(ExecutionPolicyRequest(endpoint_config=risky, user_command="fetch", max_jobs=1))
        self.assertEqual(decision.decision, "blocked")
        self.assertIn("PIT", " ".join(decision.missing_infrastructure))

    def test_disabled_catalog_endpoint_cannot_fetch_or_mutate_catalog(self):
        cfg = {
            "api_name": "disabled_stock_basic",
            "family": "stock_reference",
            "market": "a",
            "domain": "stock",
            "permission_class": "regular",
            "volume_class": "S0_STATIC",
            "partition_template": "snapshot_date",
            "supported_params": ["list_status"],
            "default_fields": ["ts_code", "name"],
            "probe": {"params": {"list_status": "L"}, "fields": ["ts_code", "name"]},
            "page_size": 5000,
            "endpoint_kind": "reference_snapshot",
            "planner_kind": "single_snapshot",
            "execution_status": "disabled",
        }
        enriched, table_id, partition_spec_id = enrich_endpoint_config(cfg)
        self.catalog.upsert_endpoint(enriched, table_id, partition_spec_id)
        before = self.counts()

        class NoCallClient:
            def query_paginated(self, *args, **kwargs):
                raise AssertionError("disabled endpoint should not call Tushare client")

        with self.assertRaisesRegex(MirrorError, "endpoint execution blocked"):
            FileLakeStore(self.root, self.catalog).fetch("disabled_stock_basic", {"list_status": "L"}, NoCallClient(), max_attempts=1)
        after = self.counts()
        self.assertEqual(before, after)

    def test_code_list_plan_is_policy_dry_run_only(self):
        cfg = dict(self.catalog.get_endpoint_config("namechange"))
        cfg["planner_kind"] = "code_list"
        policy = EndpointExecutionPolicy()
        decision = policy.decide(
            ExecutionPolicyRequest(
                endpoint_config=cfg,
                scope="low-risk-a-share",
                mode="pilot",
                user_command="code-list-plan",
                requires_real_requests=False,
                requires_code_loop=True,
                max_codes_required=5,
            )
        )
        self.assertEqual(decision.decision, "dry_run_only")
        self.assertFalse(decision.allowed)
        self.assertFalse(decision.execution_allowed)
        self.assertFalse(decision.user_confirmation_required)
        self.assertTrue(decision.requires_code_loop)
        self.assertEqual(decision.max_codes_required, 5)
        self.assertIsNone(decision.blocked_reason)

    def test_direct_fetch_for_code_list_endpoint_is_blocked_without_client_call(self):
        cfg = dict(self.catalog.get_endpoint_config("namechange"))
        cfg["api_name"] = "namechange_code_loop"
        cfg["planner_kind"] = "code_list"
        cfg["execution_status"] = "enabled"
        enriched, table_id, partition_spec_id = enrich_endpoint_config(cfg)
        self.catalog.upsert_endpoint(enriched, table_id, partition_spec_id)
        before = self.counts()

        class NoCallClient:
            def query_paginated(self, *args, **kwargs):
                raise AssertionError("code-list endpoint should not call Tushare client")

        with self.assertRaisesRegex(MirrorError, "endpoint execution blocked"):
            FileLakeStore(self.root, self.catalog).fetch("namechange_code_loop", {"ts_code": "000001.SZ"}, NoCallClient(), max_attempts=1)
        after = self.counts()
        self.assertEqual(before, after)

    def test_direct_fetch_for_disabled_code_list_endpoint_is_blocked_without_client_call(self):
        cfg = dict(self.catalog.get_endpoint_config("namechange"))
        cfg["api_name"] = "disabled_namechange_code_loop"
        cfg["planner_kind"] = "code_list"
        cfg["execution_status"] = "disabled"
        enriched, table_id, partition_spec_id = enrich_endpoint_config(cfg)
        self.catalog.upsert_endpoint(enriched, table_id, partition_spec_id)
        before = self.counts()

        class NoCallClient:
            def query_paginated(self, *args, **kwargs):
                raise AssertionError("disabled code-list endpoint should not call Tushare client")

        with self.assertRaisesRegex(MirrorError, "endpoint execution blocked"):
            FileLakeStore(self.root, self.catalog).fetch("disabled_namechange_code_loop", {"ts_code": "000001.SZ"}, NoCallClient(), max_attempts=1)
        after = self.counts()
        self.assertEqual(before, after)

    def test_mirror_run_scope_excludes_code_loop_inventory(self):
        plan = MirrorPlanner(self.root, self.catalog).plan(
            scope="low-risk-a-share",
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            max_jobs_per_api=20,
        )
        planned = {item.endpoint for item in plan.items}
        self.assertNotIn("dividend", planned)
        self.assertNotIn("pledge_stat", planned)


class ApiInfraReadinessReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "unused-root"

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args, check=True):
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = "secret-token-should-not-appear"
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=check,
        )

    def test_readiness_report_is_stable_and_includes_inventory(self):
        report = ApiInfrastructureReadinessReporter().report()
        payload = report.to_dict()
        self.assertEqual(payload["enabled_executable_endpoint_count"], 12)
        self.assertGreaterEqual(payload["disabled_inventory_endpoint_count"], 10)
        self.assertIn("calendar_backfill", payload["supported_planner_kinds"])
        self.assertIn("code_date_matrix", payload["blocked_planner_kinds"])
        self.assertIn("income", payload["missing_infrastructure_by_category"]["needs_pit"])
        self.assertIn("anns", payload["missing_infrastructure_by_category"]["needs_object_store"])
        self.assertIn("stk_mins", payload["missing_infrastructure_by_category"]["needs_intraday_bucket"])
        self.assertIn("realtime_quote", payload["missing_infrastructure_by_category"]["needs_realtime_policy"])
        self.assertIn("daily_basic", payload["missing_infrastructure_by_category"]["low_risk_ready"])
        self.assertEqual(payload["code_universe_provider"], "implemented")
        self.assertEqual(payload["code_list_planner"], "plan_only")
        self.assertEqual(payload["code_date_matrix_planner"], "plan_only")
        self.assertFalse(payload["executable_code_loop"])
        self.assertEqual(payload["max_safe_code_plan_limit"], 20)
        self.assertIn("small real smoke", payload["missing_for_execution"])

    def test_cli_json_fields_and_no_side_effects(self):
        result = self.run_cli("--root", str(self.root), "api-infra-readiness", "--json")
        payload = json.loads(result.stdout)
        self.assertIn("supported_endpoint_kinds", payload)
        self.assertIn("missing_infrastructure_by_category", payload)
        self.assertIn("next_recommended_infra_phases", payload)
        self.assertEqual(payload["code_universe_provider"], "implemented")
        self.assertEqual(payload["code_list_planner"], "plan_only")
        self.assertFalse(payload["executable_code_loop"])
        self.assertNotIn("secret-token-should-not-appear", result.stdout)
        self.assertNotIn("secret-token-should-not-appear", result.stderr)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_cli_table_output_is_read_only(self):
        result = self.run_cli("--root", str(self.root), "api-infra-readiness")
        self.assertIn("enabled_executable_endpoint_count", result.stdout)
        self.assertIn("code_list_planner", result.stdout)
        self.assertIn("needs_pit", result.stdout)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())
