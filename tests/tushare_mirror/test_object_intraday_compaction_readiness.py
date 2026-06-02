from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.capabilities import ENDPOINT_KIND_VALUES
from tushare_mirror.object_text import object_text_metadata_from_config, validate_object_text_metadata


class ObjectTextMetadataTests(unittest.TestCase):
    def test_object_text_endpoint_kinds_are_allowed(self):
        for endpoint_kind in [
            "object_document",
            "text_news",
            "research_report",
            "announcement",
            "html_text",
            "unknown_object_text",
        ]:
            self.assertIn(endpoint_kind, ENDPOINT_KIND_VALUES)

    def test_valid_object_index_metadata_is_complete_and_json_stable(self):
        result = validate_object_text_metadata(
            {
                "api_name": "news",
                "endpoint_kind": "text_news",
                "object_strategy": {
                    "object_index_required": True,
                    "object_download_required": False,
                    "content_addressed_storage": True,
                    "sha256_dedup_required": True,
                    "source_url_field": "url",
                    "publish_time_fields": ["datetime", "publish_time"],
                    "title_fields": ["title"],
                    "object_id_fields": ["news_id"],
                    "metadata_lake_required": True,
                    "execution_blocked_until_object_store_enabled": True,
                },
            }
        )
        self.assertEqual(result.status, "complete")
        self.assertFalse(result.metadata.object_download_required)
        self.assertTrue(result.metadata.execution_blocked_until_object_store_enabled)
        rendered = json.dumps(result.to_dict(), sort_keys=True)
        self.assertIn('"object_id_fields": ["news_id"]', rendered)
        self.assertIn('"source_url_field": "url"', rendered)

    def test_missing_object_id_or_source_fields_blocks_when_required(self):
        result = validate_object_text_metadata(
            {
                "api_name": "anns",
                "endpoint_kind": "object_document",
                "object_strategy": {
                    "object_index_required": True,
                    "object_download_required": True,
                    "binary_storage_layer": "objects",
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("missing_object_id_fields", result.errors)
        self.assertIn("missing_source_url_field", result.errors)
        self.assertIn("object_download_execution_blocked", result.errors)

    def test_download_execution_is_blocked_even_with_complete_download_metadata(self):
        result = validate_object_text_metadata(
            {
                "api_name": "anns",
                "endpoint_kind": "announcement",
                "object_strategy": {
                    "object_index_required": True,
                    "object_download_required": True,
                    "content_addressed_storage": True,
                    "sha256_dedup_required": True,
                    "content_type_field": "content_type",
                    "source_url_field": "url",
                    "publish_time_fields": ["ann_date"],
                    "title_fields": ["title"],
                    "object_id_fields": ["ann_id"],
                    "metadata_lake_required": True,
                    "binary_storage_layer": "objects",
                    "execution_blocked_until_object_store_enabled": True,
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.errors, ["object_download_execution_blocked"])

    def test_metadata_defaults_for_inventory_object_document_are_blocked(self):
        metadata = object_text_metadata_from_config({"api_name": "anns", "endpoint_kind": "object_document"})
        self.assertTrue(metadata.object_index_required)
        self.assertTrue(metadata.object_download_required)
        self.assertTrue(metadata.execution_blocked_until_object_store_enabled)
        result = validate_object_text_metadata({"api_name": "anns", "endpoint_kind": "object_document"})
        self.assertIn("missing_object_id_fields", result.errors)
        self.assertIn("missing_source_url_field", result.errors)
        self.assertIn("missing_binary_storage_layer", result.errors)


class ObjectPlanCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "unused-root"

    def tearDown(self):
        self.tmp.cleanup()

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

    def test_object_plan_blocks_object_and_text_inventory_without_side_effects(self):
        for api_name in ["anns", "news", "report_rc"]:
            with self.subTest(api_name=api_name):
                result = self.run_cli(
                    "object-plan",
                    "--api",
                    api_name,
                    "--start-date",
                    "20250101",
                    "--end-date",
                    "20250131",
                    "--json",
                )
                payload = json.loads(result.stdout)
                self.assertTrue(payload["blocked"])
                self.assertFalse(payload["execution_allowed"])
                self.assertTrue(payload["would_require_real_request"])
                self.assertFalse(payload["would_download_objects"])
                self.assertEqual(payload["blocked_reason"], "object_index_store_policy_missing")
                self.assertIn("required_infra", payload)
                self.assertNotIn("secret-token-should-not-appear", result.stdout)
                self.assertNotIn("secret-token-should-not-appear", result.stderr)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_invalid_or_missing_date_range_returns_clear_error_without_catalog(self):
        result = self.run_cli(
            "object-plan",
            "--api",
            "anns",
            "--start-date",
            "20250132",
            "--end-date",
            "20250131",
            "--json",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertIn("invalid start-date", payload["blocking_errors"][0])
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_table_output_is_stable_and_read_only(self):
        result = self.run_cli(
            "object-plan",
            "--api",
            "news",
            "--start-date",
            "20250101",
            "--end-date",
            "20250131",
        )
        self.assertIn("api_name", result.stdout)
        self.assertIn("execution_allowed", result.stdout)
        self.assertIn("object_index_store_policy_missing", result.stdout)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())


if __name__ == "__main__":
    unittest.main()
