from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_bundled_endpoint_configs, load_into_catalog, load_inventory_configs
from tushare_mirror.mirror import A_SHARE_LOW_RISK_ENDPOINTS, MirrorOrchestrator, MirrorPlanner, MirrorScopeReporter
from tushare_mirror.planner import JobPlanner
from tushare_mirror.reader import LakeReader
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


NEW_EXECUTABLE_FIXTURES = {
    "stock_company": (
        ["ts_code", "exchange", "chairman", "manager", "secretary", "reg_capital", "setup_date", "province", "city", "introduction", "website", "email", "office", "employees", "main_business", "business_scope"],
        [["600000.SH", "SSE", "Alice", "Bob", "Carol", 1000.0, "19990101", "Shanghai", "Shanghai", "intro", "https://example.invalid", "ir@example.invalid", "office", 100, "banking", "scope"]],
        {"exchange": "SSE"},
    ),
    "concept": (
        ["code", "name", "src"],
        [["TS1", "concept one", "ts"]],
        {"src": "ts"},
    ),
    "index_basic": (
        ["ts_code", "name", "fullname", "market", "publisher", "index_type", "category", "base_date", "base_point", "list_date", "weight_rule", "desc", "exp_date"],
        [["000001.SH", "index", "index full", "SSE", "SSE", "composite", "scale", "19901219", 100.0, "19910715", "rule", "desc", None]],
        {"market": "SSE"},
    ),
    "index_daily": (
        ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
        [["000001.SH", "20250102", 3000.0, 3010.0, 2990.0, 3005.0, 3001.0, 4.0, 0.13, 100000.0, 1000000.0]],
        {"trade_date": "20250102"},
    ),
    "index_weekly": (
        ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
        [["000001.SH", "20250103", 3000.0, 3020.0, 2980.0, 3015.0, 2990.0, 25.0, 0.84, 500000.0, 5000000.0]],
        {"trade_date": "20250103"},
    ),
    "index_monthly": (
        ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
        [["000001.SH", "20250127", 3000.0, 3050.0, 2950.0, 3030.0, 2990.0, 40.0, 1.34, 800000.0, 8000000.0]],
        {"trade_date": "20250127"},
    ),
    "ths_index": (
        ["ts_code", "name", "count", "exchange", "list_date", "type"],
        [["885001.TI", "ths industry", 100, "A", "20200101", "N"]],
        {"exchange": "A", "type": "N"},
    ),
    "index_classify": (
        ["index_code", "industry_name", "level", "industry_code", "is_pub", "parent_code", "src"],
        [["801010.SI", "agriculture", "L1", "801010", "1", None, "SW2021"]],
        {"src": "SW2021", "level": "L1"},
    ),
}


class ApiFakeClient:
    def __init__(self, fields, items):
        self.fields = fields
        self.items = items

    def query_paginated(self, api_name, params, fields, page_size=None):
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": self.fields, "items": self.items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=self.fields, items=self.items)


class MirrorRunFakeClient:
    token = "fake-token-for-hash-only"

    def __init__(self):
        self.request_calls: list[str] = []
        self.query_calls: list[tuple[str, dict]] = []

    def request(self, api_name, params, fields=None):
        self.request_calls.append(api_name)
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.query_calls.append((api_name, dict(params)))
        fields_list = list(fields or [])
        if api_name == "trade_cal":
            fields_list = ["exchange", "cal_date", "is_open", "pretrade_date"]
            items = [
                ["SSE", "20250101", 0, "20241231"],
                ["SSE", "20250102", 1, "20241231"],
                ["SSE", "20250103", 1, "20250102"],
                ["SSE", "20250106", 1, "20250103"],
                ["SSE", "20250107", 1, "20250106"],
                ["SSE", "20250108", 1, "20250107"],
                ["SSE", "20250109", 1, "20250108"],
                ["SSE", "20250110", 1, "20250109"],
            ]
        else:
            items = [self._row(params, fields_list)]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": fields_list, "items": items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=fields_list, items=items)

    def _row(self, params, fields):
        values = []
        for field in fields:
            if field in {"ts_code", "index_code", "code"}:
                values.append("000001.SH")
            elif field in {"trade_date", "ann_date", "end_date", "start_date", "cal_date", "in_date", "out_date", "list_date", "setup_date", "base_date", "exp_date"}:
                values.append(params.get(field) or params.get("trade_date") or params.get("end_date") or "20250102")
            elif field == "exchange":
                values.append(params.get("exchange") or "SSE")
            elif field in {"is_open", "is_new", "is_pub"}:
                values.append("1")
            elif field in {"name", "symbol", "area", "industry", "market", "title", "gender", "lev", "edu", "national", "birthday", "begin_date", "resume", "change_reason", "suspend_timing", "suspend_type", "chairman", "manager", "secretary", "province", "city", "introduction", "website", "email", "office", "main_business", "business_scope", "fullname", "publisher", "index_type", "category", "weight_rule", "desc", "src", "level", "industry_name", "industry_code", "parent_code", "type"}:
                values.append("x")
            else:
                values.append(1.0)
        return values


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


class AShareLowRiskPlannerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        import sqlite3

        with sqlite3.connect(self.catalog.db_path) as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def test_planner_resolves_all_scope_endpoints_without_side_effects(self):
        before = self.counts()
        plan = MirrorPlanner(self.root, self.catalog).plan(
            scope="a-share-low-risk",
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            max_jobs_per_api=20,
        )
        self.assertEqual(self.counts(), before)
        by_endpoint = {item.endpoint: item for item in plan.items}
        self.assertEqual(set(A_SHARE_LOW_RISK_ENDPOINTS), set(by_endpoint))
        self.assertEqual(by_endpoint["index_daily"].plan_status, "blocked_until_trade_cal")
        self.assertEqual(by_endpoint["concept"].planned_action, "fetch")
        self.assertEqual(by_endpoint["index_weekly"].planned_action, "date_backfill")
        self.assertEqual(by_endpoint["top10_holders"].plan_status, "plan_only_no_execution")
        self.assertFalse(by_endpoint["top10_holders"].will_execute)

    def test_partition_resolver_works_for_new_executable_endpoints(self):
        planner = JobPlanner(self.root, self.catalog)
        cases = {
            "stock_company": ({"exchange": "SSE"}, "api=stock_company/snapshot_date="),
            "concept": ({"src": "ts"}, "api=concept/snapshot_date="),
            "index_basic": ({"market": "SSE"}, "domain=index/api=index_basic/snapshot_date="),
            "index_daily": ({"trade_date": "20250102"}, "domain=index/api=index_daily/year=2025/month=01"),
            "index_weekly": ({"trade_date": "20250103"}, "domain=index/api=index_weekly/year=2025/month=01"),
            "index_monthly": ({"trade_date": "20250127"}, "domain=index/api=index_monthly/year=2025/month=01"),
            "ths_index": ({"exchange": "A", "type": "N"}, "domain=index/api=ths_index/snapshot_date="),
            "index_classify": ({"src": "SW2021", "level": "L1"}, "domain=index/api=index_classify/snapshot_date="),
        }
        for api_name, (params, path_part) in cases.items():
            plan = planner.plan_single_fetch(api_name, params)
            self.assertIn(path_part, plan.lake_path, api_name)
            self.assertEqual(plan.planned_actions[0], "request_tushare", api_name)
        self.assertEqual(self.counts()["jobs"], 0)


class AShareLowRiskExecutableFixtureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

    def test_new_executable_endpoints_fetch_validate_reader_and_list_files(self):
        store = FileLakeStore(self.root, self.catalog)
        planner = JobPlanner(self.root, self.catalog)
        for api_name, (fields, items, params) in NEW_EXECUTABLE_FIXTURES.items():
            probe = planner.plan_probe(api_name)
            self.assertTrue(probe.fields, api_name)
            result = store.fetch(api_name, params, ApiFakeClient(fields, items))
            self.assertIsNotNone(result.snapshot_id, api_name)
            self.assertEqual(result.record_count, len(items), api_name)
            ok, _ = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, api_name)
            self.assertTrue(ok, api_name)
            table = LakeReader(self.root, self.catalog).scan_api(api_name)
            self.assertEqual(table.num_rows, len(items), api_name)
            files = self.catalog.files_for_snapshot(result.snapshot_id)
            self.assertEqual(len([row for row in files if row["content_type"] == "raw"]), 1, api_name)
            self.assertEqual(len([row for row in files if row["content_type"] == "lake"]), 1, api_name)
            lake_file = next(row for row in files if row["content_type"] == "lake")
            self.assertTrue(lake_file["schema_id"], api_name)
            listed = json.loads(self.run_cli("list-files", "--api", api_name, "--snapshot", "latest", "--json").stdout)
            self.assertEqual(len(listed), 1, api_name)
            self.assertEqual(listed[0]["api_name"], api_name)

    def test_plan_only_inventory_endpoints_do_not_fetch_or_enter_mirror_run_plan(self):
        planner = MirrorPlanner(self.root, self.catalog)
        plan = planner.plan(
            scope="a-share-low-risk",
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            max_jobs_per_api=20,
        )
        by_endpoint = {item.endpoint: item for item in plan.items}
        for api_name in ["top10_holders", "concept_detail", "index_weight", "ths_member"]:
            self.assertEqual(by_endpoint[api_name].plan_status, "plan_only_no_execution")
            self.assertFalse(by_endpoint[api_name].will_execute)
            with self.assertRaisesRegex(KeyError, "endpoint not found"):
                JobPlanner(self.root, self.catalog).plan_single_fetch(api_name, {})


class AShareLowRiskMirrorOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        import sqlite3

        with sqlite3.connect(self.catalog.db_path) as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
            }

    def run_cli(self, *args, check=True):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def test_mirror_plan_a_share_low_risk_json_is_stable(self):
        result = self.run_cli(
            "mirror-plan",
            "--scope",
            "a-share-low-risk",
            "--mode",
            "pilot",
            "--start-date",
            "20250101",
            "--end-date",
            "20250131",
            "--max-jobs-per-api",
            "20",
            "--json",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["scope"], "a-share-low-risk")
        self.assertEqual(payload["endpoint_count"], len(A_SHARE_LOW_RISK_ENDPOINTS))
        by_endpoint = {item["endpoint"]: item for item in payload["items"]}
        self.assertEqual(by_endpoint["index_daily"]["plan_status"], "blocked_until_trade_cal")
        self.assertEqual(by_endpoint["top10_holders"]["plan_status"], "plan_only_no_execution")
        self.assertFalse(by_endpoint["top10_holders"]["will_execute"])

    def test_mirror_run_without_execute_is_dry_run_only_for_new_scope(self):
        before = self.counts()
        result = self.run_cli("mirror-run", "--scope", "a-share-low-risk", "--mode", "smoke", "--max-jobs-per-api", "3")
        self.assertIn("dry-run only", result.stdout)
        after = self.counts()
        self.assertEqual(before["runs"], after["runs"])
        self.assertEqual(before["snapshots"], after["snapshots"])

    def test_fake_smoke_executes_safe_subset_and_excludes_code_loops(self):
        client = MirrorRunFakeClient()
        result = MirrorOrchestrator(self.root, self.catalog, client, sleep=lambda _: None).run(
            scope="a-share-low-risk",
            mode="smoke",
            max_jobs_per_api=3,
        )
        self.assertEqual(result.status, "succeeded")
        endpoints = {item["endpoint"]: item for item in result.summary["items"]}
        self.assertEqual(len(endpoints), len(A_SHARE_LOW_RISK_ENDPOINTS))
        self.assertEqual(endpoints["index_daily"]["status"], "succeeded")
        self.assertEqual(endpoints["top10_holders"]["status"], "excluded")
        self.assertEqual(endpoints["concept_detail"]["status"], "excluded")
        called = set(client.request_calls) | {api for api, _ in client.query_calls}
        self.assertFalse({"top10_holders", "concept_detail", "index_weight", "ths_member"} & called)
        self.assertNotIn("fake-token-for-hash-only", json.dumps(result.to_dict()))

    def test_fake_pilot_excludes_stock_code_smoke_endpoints_and_code_loops(self):
        client = MirrorRunFakeClient()
        result = MirrorOrchestrator(self.root, self.catalog, client, sleep=lambda _: None).run(
            scope="a-share-low-risk",
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            max_jobs_per_api=20,
        )
        self.assertEqual(result.status, "succeeded")
        endpoints = {item["endpoint"]: item for item in result.summary["items"]}
        self.assertEqual(endpoints["namechange"]["status"], "excluded")
        self.assertEqual(endpoints["stk_managers"]["status"], "excluded")
        self.assertEqual(endpoints["stk_rewards"]["status"], "excluded")
        self.assertEqual(endpoints["index_daily"]["executed_jobs"], 7)
        called = set(client.request_calls) | {api for api, _ in client.query_calls}
        self.assertFalse({"namechange", "stk_managers", "stk_rewards", "top10_holders"} & called)


class AShareLowRiskRealSmokeCommandTests(unittest.TestCase):
    def run_script(self, *args):
        return subprocess.run(
            [sys.executable, "scripts/tushare_real_smoke.py", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_help_lists_a_share_low_risk_smoke(self):
        result = self.run_script("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("--a-share-low-risk-smoke", result.stdout)
        self.assertIn("--print-commands", result.stdout)

    def test_a_share_low_risk_command_list_generated_without_requests(self):
        result = self.run_script(
            "--a-share-low-risk-smoke",
            "--root",
            "/tmp/tushare-a-share-low-risk-smoke",
            "--print-commands",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertFalse(payload["real_requests_sent"])
        self.assertIn("stock_company", payload["endpoints"])
        self.assertIn("index_daily", payload["endpoints"])
        self.assertGreater(payload["endpoint_count"], 12)
        self.assertTrue(any("fetch --api index_daily" in command for command in payload["commands"]))
        self.assertFalse(payload["safety_limits"]["stock_loop"])
        self.assertFalse(payload["safety_limits"]["full_backfill"])

    def test_no_default_real_request_is_selected(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 2)
        self.assertIn("No endpoints selected", result.stderr)
        self.assertNotIn("TUSHARE_TOKEN is required", result.stderr)


if __name__ == "__main__":
    unittest.main()
