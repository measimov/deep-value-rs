from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import enrich_endpoint_config, load_into_catalog
from tushare_mirror.errors import MirrorError
from tushare_mirror.pit import validate_pit_safety
from tushare_mirror.reader import LakeReader
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


HK_US_FINANCIAL_RAW_FIXTURES: dict[str, dict[str, object]] = {
    "hk_income": {
        "scope": "hk-financial-raw",
        "market": "hk",
        "endpoint_kind": "financial_statement",
        "fields": ["ts_code", "end_date", "name", "ind_name", "ind_value"],
        "items": [["00001.HK", "20241231", "HK Co", "Revenue", "100.0"]],
        "params": {"ts_code": "00001.HK", "period": "20241231"},
        "pit_status": "blocked_without_disclosure_date",
    },
    "hk_balancesheet": {
        "scope": "hk-financial-raw",
        "market": "hk",
        "endpoint_kind": "financial_statement",
        "fields": ["ts_code", "name", "end_date", "ind_name", "ind_value"],
        "items": [["00001.HK", "HK Co", "20241231", "Total Assets", "200.0"]],
        "params": {"ts_code": "00001.HK", "period": "20241231"},
        "pit_status": "blocked_without_disclosure_date",
    },
    "hk_cashflow": {
        "scope": "hk-financial-raw",
        "market": "hk",
        "endpoint_kind": "financial_statement",
        "fields": ["ts_code", "end_date", "name", "ind_name", "ind_value"],
        "items": [["00001.HK", "20241231", "HK Co", "Cash From Ops", "50.0"]],
        "params": {"ts_code": "00001.HK", "period": "20241231"},
        "pit_status": "blocked_without_disclosure_date",
    },
    "hk_fina_indicator": {
        "scope": "hk-financial-raw",
        "market": "hk",
        "endpoint_kind": "financial_indicator",
        "fields": ["ts_code", "end_date", "start_date", "std_report_date", "currency", "report_type"],
        "items": [["00001.HK", "20241231", "20240101", "20241231", "HKD", "1"]],
        "params": {"ts_code": "00001.HK", "period": "20241231"},
        "pit_status": "blocked_without_disclosure_date",
    },
    "us_fina_indicator": {
        "scope": "us-financial-raw",
        "market": "us",
        "endpoint_kind": "financial_indicator",
        "fields": [
            "ts_code",
            "end_date",
            "ind_type",
            "security_name_abbr",
            "accounting_standards",
            "notice_date",
            "start_date",
            "std_report_date",
            "financial_date",
            "currency",
            "report_type",
        ],
        "items": [["NVDA", "20241231", "Q", "Nvidia", "US GAAP", "20250226", "20240101", "20241231", "20241231", "USD", "Q4"]],
        "params": {"ts_code": "NVDA", "period": "20241231"},
        "pit_status": "complete",
    },
}


class FinancialRawFixtureClient:
    def __init__(self, fields: list[str], items: list[list[object]]):
        self.fields = fields
        self.items = items
        self.calls: list[tuple[str, dict[str, object], list[str], int | None]] = []

    def query_paginated(self, api_name, params, fields, page_size=None):
        fields_list = list(fields or [])
        self.calls.append((api_name, dict(params), fields_list, page_size))
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": self.fields, "items": self.items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=self.fields, items=self.items)


class HKUSFinancialRawFakeFixtureTests(unittest.TestCase):
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

    def upsert_financial_raw_endpoint(self, api_name: str, fixture: dict[str, object]) -> dict[str, object]:
        fields = list(fixture["fields"])
        pit_fields = ["notice_date"] if "notice_date" in fields else ["ann_date", "f_ann_date"]
        usable_after = "notice_date" if "notice_date" in fields else "ann_date"
        cfg = {
            "api_name": api_name,
            "family": f"{fixture['market']}_financial",
            "market": fixture["market"],
            "domain": "financial",
            "namespace": f"tushare.{fixture['market']}_financial",
            "volume_class": "F1_FINANCIAL_BOUNDED_RAW",
            "endpoint_kind": fixture["endpoint_kind"],
            "planner_kind": "code_period_matrix",
            "execution_status": "enabled",
            "partition_template": "period_year",
            "primary_date_field": "period",
            "period_field": "period",
            "supported_params": ["ts_code", "period"],
            "default_fields": fields,
            "probe": {"params": dict(fixture["params"]), "fields": fields},
            "pit_safety": {
                "pit_required": True,
                "period_field": "period",
                "announcement_date_fields": pit_fields,
                "usable_after_field": usable_after,
                "fallback_usable_after_policy": "block_without_disclosure_date",
                "allow_without_disclosure_date": False,
                "lookahead_risk": True,
                "strategy_safe_default": False,
            },
        }
        enriched, table_id, partition_spec_id = enrich_endpoint_config(cfg)
        self.catalog.upsert_endpoint(enriched, table_id, partition_spec_id)
        return enriched

    def test_plain_fetch_remains_blocked_for_financial_raw_endpoints(self):
        fixture = HK_US_FINANCIAL_RAW_FIXTURES["hk_income"]
        self.upsert_financial_raw_endpoint("hk_income", fixture)
        with self.assertRaises(MirrorError) as ctx:
            FileLakeStore(self.root, self.catalog).fetch(
                "hk_income",
                fixture["params"],
                FinancialRawFixtureClient(fixture["fields"], fixture["items"]),
                max_attempts=1,
            )
        self.assertIn("endpoint execution blocked", str(ctx.exception))

    def test_guarded_financial_raw_fixtures_fetch_validate_and_read(self):
        store = FileLakeStore(self.root, self.catalog)
        for api_name, fixture in HK_US_FINANCIAL_RAW_FIXTURES.items():
            with self.subTest(api_name=api_name):
                cfg = self.upsert_financial_raw_endpoint(api_name, fixture)
                pit = validate_pit_safety(cfg, observed_fields=list(fixture["fields"]))
                self.assertEqual(pit.status, fixture["pit_status"])

                client = FinancialRawFixtureClient(list(fixture["fields"]), list(fixture["items"]))
                result = store.fetch(
                    api_name,
                    fixture["params"],
                    client,
                    max_attempts=1,
                    run_type="financial-raw-fetch",
                    scope=str(fixture["scope"]),
                    max_codes_required=1,
                    requires_pit_handling=False,
                )
                self.assertIsNotNone(result.snapshot_id, api_name)
                self.assertEqual(result.record_count, len(fixture["items"]), api_name)

                ok, failures = Validator(self.root, self.catalog).validate_snapshot(str(result.snapshot_id), api_name)
                self.assertTrue(ok, failures)
                table = LakeReader(self.root, self.catalog).scan_api(api_name, snapshot_id=str(result.snapshot_id))
                self.assertEqual(table.num_rows, len(fixture["items"]), api_name)

                files = self.catalog.files_for_snapshot(str(result.snapshot_id))
                raw_files = [row for row in files if row["content_type"] == "raw"]
                lake_files = [row for row in files if row["content_type"] == "lake"]
                self.assertEqual(len(raw_files), 1, api_name)
                self.assertEqual(len(lake_files), 1, api_name)
                partition = json.loads(lake_files[0]["partition_values_json"])
                self.assertEqual(partition["period_year"], "2024")
                self.assertEqual(partition["period_date"], "20241231")
                self.assertTrue(lake_files[0]["schema_id"], api_name)
                self.assertEqual(client.calls[0][3], 5000, api_name)

                listed = json.loads(self.run_cli("list-files", "--api", api_name, "--snapshot", "latest", "--json").stdout)
                self.assertEqual(len(listed), 1, api_name)
                self.assertEqual(listed[0]["api_name"], api_name)

    def test_guarded_financial_raw_fetch_requires_scope_and_bounded_codes(self):
        fixture = HK_US_FINANCIAL_RAW_FIXTURES["us_fina_indicator"]
        self.upsert_financial_raw_endpoint("us_fina_indicator", fixture)
        store = FileLakeStore(self.root, self.catalog)
        with self.assertRaises(MirrorError) as missing_scope:
            store.fetch(
                "us_fina_indicator",
                fixture["params"],
                FinancialRawFixtureClient(fixture["fields"], fixture["items"]),
                max_attempts=1,
                run_type="financial-raw-fetch",
                max_codes_required=1,
                requires_pit_handling=False,
            )
        with self.assertRaises(MirrorError) as too_many_codes:
            store.fetch(
                "us_fina_indicator",
                fixture["params"],
                FinancialRawFixtureClient(fixture["fields"], fixture["items"]),
                max_attempts=1,
                run_type="financial-raw-fetch",
                scope="us-financial-raw",
                max_codes_required=21,
                requires_pit_handling=False,
            )
        self.assertIn("financial_raw_guardrails_not_satisfied", str(missing_scope.exception))
        self.assertIn("financial_raw_guardrails_not_satisfied", str(too_many_codes.exception))


if __name__ == "__main__":
    unittest.main()
