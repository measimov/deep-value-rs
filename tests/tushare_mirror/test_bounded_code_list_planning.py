from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.code_list_planner import CodeListPlanner, MAX_CODE_LIST_PLAN_CODES
from tushare_mirror.code_universe import CodeUniverseProvider
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.store import FileLakeStore


class CodeUniverseFakeClient:
    def __init__(self, fields: list[str], items: list[list[object]]):
        self.fields = fields
        self.items = items
        self.calls = 0

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.calls += 1
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": self.fields, "items": self.items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=self.fields, items=self.items)


class CodeUniverseProviderTests(unittest.TestCase):
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

    def run_cli(self, *args, check=True):
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = "secret-token-should-not-appear"
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=check,
        )

    def seed_stock_basic(self):
        fields = ["ts_code", "symbol", "name", "area", "industry", "market", "list_date"]
        items = [
            ["000001.SZ", "000001", "平安银行", "深圳", "银行", "主板", "19910403"],
            ["002001.SZ", "002001", "新和成", "浙江", "医药", "中小板", "20040625"],
            ["300001.SZ", "300001", "特锐德", "山东", "电气", "创业板", "20091030"],
            ["688001.SH", "688001", "华兴源创", "江苏", "电子", "科创板", "20190722"],
        ]
        return FileLakeStore(self.root, self.catalog).fetch(
            "stock_basic",
            {"list_status": "L"},
            CodeUniverseFakeClient(fields, items),
        )

    def seed_hs_const(self):
        fields = ["ts_code", "hs_type", "in_date", "out_date", "is_new"]
        items = [
            ["600000.SH", "SH", "20200101", None, "1"],
            ["000001.SZ", "SZ", "20200101", None, "1"],
            ["000002.SZ", "SZ", "20200101", None, "1"],
        ]
        return FileLakeStore(self.root, self.catalog).fetch(
            "hs_const",
            {"hs_type": "SH", "is_new": "1"},
            CodeUniverseFakeClient(fields, items),
        )

    def test_reads_fake_stock_basic_latest_and_limit_is_read_only(self):
        snapshot = self.seed_stock_basic()
        before = self.counts()
        result = CodeUniverseProvider(self.root, self.catalog).get("a_share_listed", limit=2)
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(result.source_api, "stock_basic")
        self.assertEqual(result.source_snapshot_id, snapshot.snapshot_id)
        self.assertEqual(result.source_record_count, 4)
        self.assertEqual(result.code_count, 4)
        self.assertEqual(result.codes_sample, ["000001.SZ", "002001.SZ"])
        self.assertIsNone(result.blocked_reason)

    def test_missing_stock_basic_blocks(self):
        before = self.counts()
        result = CodeUniverseProvider(self.root, self.catalog).get("a_share_listed", limit=20)
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(result.blocked_reason, "missing_stock_basic_latest_snapshot")
        self.assertEqual(result.code_count, 0)

    def test_market_filters_where_supported(self):
        self.seed_stock_basic()
        provider = CodeUniverseProvider(self.root, self.catalog)
        self.assertEqual(provider.get("a_share_mainboard").codes, ["000001.SZ"])
        self.assertEqual(provider.get("a_share_sme").codes, ["002001.SZ"])
        self.assertEqual(provider.get("a_share_chinext").codes, ["300001.SZ"])
        self.assertEqual(provider.get("a_share_star").codes, ["688001.SH"])

    def test_hs_const_universes_use_local_hs_const(self):
        snapshot = self.seed_hs_const()
        provider = CodeUniverseProvider(self.root, self.catalog)
        sh = provider.get("hs_const_sh", limit=20)
        sz = provider.get("hs_const_sz", limit=1)
        self.assertEqual(sh.source_api, "hs_const")
        self.assertEqual(sh.source_snapshot_id, snapshot.snapshot_id)
        self.assertEqual(sh.codes, ["600000.SH"])
        self.assertEqual(sz.code_count, 2)
        self.assertEqual(sz.codes_sample, ["000001.SZ"])

    def test_cli_json_stable_and_no_side_effects_or_token_plaintext(self):
        self.seed_stock_basic()
        before = self.counts()
        result = self.run_cli("code-universe", "--universe", "a_share_listed", "--limit", "3", "--json")
        after = self.counts()
        payload = json.loads(result.stdout)
        self.assertEqual(before, after)
        self.assertEqual(payload["universe_name"], "a_share_listed")
        self.assertEqual(payload["source_api"], "stock_basic")
        self.assertEqual(payload["code_count"], 4)
        self.assertEqual(len(payload["codes_sample"]), 3)
        self.assertNotIn("codes", payload)
        self.assertNotIn("secret-token-should-not-appear", result.stdout)
        self.assertNotIn("secret-token-should-not-appear", result.stderr)

    def test_cli_missing_source_returns_blocked_result(self):
        before = self.counts()
        result = self.run_cli("code-universe", "--universe", "a_share_listed", "--json", check=False)
        after = self.counts()
        payload = json.loads(result.stdout)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(before, after)
        self.assertEqual(payload["blocked_reason"], "missing_stock_basic_latest_snapshot")


class BoundedCodeListPlannerTests(unittest.TestCase):
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

    def run_cli(self, *args, check=True):
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = "secret-token-should-not-appear"
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "--root", str(self.root), *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=check,
        )

    def seed_stock_basic(self):
        fields = ["ts_code", "symbol", "name", "area", "industry", "market", "list_date"]
        items = [
            ["000001.SZ", "000001", "平安银行", "深圳", "银行", "主板", "19910403"],
            ["002001.SZ", "002001", "新和成", "浙江", "医药", "中小板", "20040625"],
            ["300001.SZ", "300001", "特锐德", "山东", "电气", "创业板", "20091030"],
            ["688001.SH", "688001", "华兴源创", "江苏", "电子", "科创板", "20190722"],
        ]
        return FileLakeStore(self.root, self.catalog).fetch(
            "stock_basic",
            {"list_status": "L"},
            CodeUniverseFakeClient(fields, items),
        )

    def test_namechange_plan_with_fake_stock_basic_is_plan_only(self):
        source = self.seed_stock_basic()
        before = self.counts()
        plan = CodeListPlanner(self.root, self.catalog).plan("namechange", "a_share_listed", limit_codes=2)
        after = self.counts()
        self.assertEqual(before, after)
        self.assertFalse(plan.blocked)
        self.assertFalse(plan.execution_allowed)
        self.assertEqual(plan.source_snapshot_id, source.snapshot_id)
        self.assertEqual(plan.total_codes, 4)
        self.assertEqual(plan.planned_codes, 2)
        self.assertEqual(plan.candidate_jobs, 2)
        self.assertEqual([item.ts_code for item in plan.items], ["000001.SZ", "002001.SZ"])
        self.assertTrue(all(item.planned_action == "fetch" for item in plan.items))
        self.assertTrue(all(item.would_require_real_request for item in plan.items))
        self.assertTrue(all(item.job_key for item in plan.items))

    def test_legacy_enabled_endpoint_without_capability_fields_can_be_planned(self):
        cfg = dict(self.catalog.get_endpoint_config("namechange"))
        table_id = cfg["table_id"]
        partition_spec_id = cfg["partition_spec_id"]
        cfg.pop("execution_status", None)
        cfg.pop("planner_kind", None)
        cfg.pop("endpoint_kind", None)
        self.catalog.upsert_endpoint(cfg, table_id, partition_spec_id)
        self.seed_stock_basic()
        before = self.counts()
        plan = CodeListPlanner(self.root, self.catalog).plan("namechange", "a_share_listed", limit_codes=1)
        after = self.counts()
        self.assertEqual(before, after)
        self.assertFalse(plan.blocked)
        self.assertEqual(plan.candidate_jobs, 1)
        self.assertEqual(plan.items[0].ts_code, "000001.SZ")

    def test_stk_managers_plan_with_date_range_and_json_cli(self):
        self.seed_stock_basic()
        before = self.counts()
        result = self.run_cli(
            "code-list-plan",
            "--api", "stk_managers",
            "--universe", "a_share_listed",
            "--limit-codes", "3",
            "--start-date", "20250101",
            "--end-date", "20250131",
            "--json",
        )
        after = self.counts()
        payload = json.loads(result.stdout)
        self.assertEqual(before, after)
        self.assertFalse(payload["blocked"])
        self.assertFalse(payload["execution_allowed"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["candidate_jobs"], 3)
        self.assertEqual(payload["items"][0]["params"]["start_date"], "20250101")
        self.assertEqual(payload["items"][0]["params"]["end_date"], "20250131")
        self.assertNotIn("secret-token-should-not-appear", result.stdout)
        self.assertNotIn("secret-token-should-not-appear", result.stderr)

    def test_limit_codes_required_and_phase_limit_blocks(self):
        self.seed_stock_basic()
        missing = self.run_cli(
            "code-list-plan",
            "--api", "namechange",
            "--universe", "a_share_listed",
            check=False,
        )
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("limit-codes", missing.stderr)
        too_many = self.run_cli(
            "code-list-plan",
            "--api", "namechange",
            "--universe", "a_share_listed",
            "--limit-codes", str(MAX_CODE_LIST_PLAN_CODES + 1),
            "--json",
            check=False,
        )
        payload = json.loads(too_many.stdout)
        self.assertNotEqual(too_many.returncode, 0)
        self.assertTrue(payload["blocked"])
        self.assertIn("limit_codes_exceeds_phase_limit", payload["blocked_reason"])

    def test_missing_universe_source_blocks_without_side_effects(self):
        before = self.counts()
        plan = CodeListPlanner(self.root, self.catalog).plan("namechange", "a_share_listed", limit_codes=5)
        after = self.counts()
        self.assertEqual(before, after)
        self.assertTrue(plan.blocked)
        self.assertEqual(plan.blocked_reason, "missing_stock_basic_latest_snapshot")
        self.assertEqual(plan.candidate_jobs, 0)

    def test_disabled_inventory_endpoint_blocks_even_with_local_universe(self):
        self.seed_stock_basic()
        before = self.counts()
        plan = CodeListPlanner(self.root, self.catalog).plan("dividend", "a_share_listed", limit_codes=5)
        after = self.counts()
        self.assertEqual(before, after)
        self.assertTrue(plan.blocked)
        self.assertEqual(plan.blocked_reason, "endpoint_disabled_inventory")

    def test_endpoint_without_ts_code_or_incompatible_planner_blocks(self):
        self.seed_stock_basic()
        no_code = CodeListPlanner(self.root, self.catalog).plan("trade_cal", "a_share_listed", limit_codes=1)
        self.assertTrue(no_code.blocked)
        self.assertEqual(no_code.blocked_reason, "endpoint_does_not_support_ts_code")
        incompatible = CodeListPlanner(self.root, self.catalog).plan("weekly", "a_share_listed", limit_codes=1)
        self.assertTrue(incompatible.blocked)
        self.assertIn("planner_kind_not_code_list_compatible", incompatible.blocked_reason or "")


if __name__ == "__main__":
    unittest.main()
