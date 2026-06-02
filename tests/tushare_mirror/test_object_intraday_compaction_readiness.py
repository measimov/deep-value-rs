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
from tushare_mirror.capabilities import ENDPOINT_KIND_VALUES
from tushare_mirror.compaction import CompactionPlanner
from tushare_mirror.intraday import intraday_metadata_from_config, validate_intraday_metadata
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


class IntradayBucketMetadataTests(unittest.TestCase):
    def test_intraday_endpoint_kinds_are_allowed(self):
        for endpoint_kind in ["minute_bar", "tick", "order", "realtime"]:
            self.assertIn(endpoint_kind, ENDPOINT_KIND_VALUES)

    def test_minute_metadata_defaults_are_valid_and_block_execution(self):
        result = validate_intraday_metadata({"api_name": "stk_mins", "endpoint_kind": "minute_bar"})
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.metadata.freq, "1min")
        self.assertEqual(result.metadata.bucket_count, 64)
        self.assertTrue(result.metadata.compaction_required)
        self.assertTrue(result.metadata.execution_blocked_until_bucket_policy_enabled)
        rendered = json.dumps(result.to_dict(), sort_keys=True)
        self.assertIn('"bucket_count": 64', rendered)
        self.assertIn('"compaction_required": true', rendered)

    def test_tick_metadata_defaults_to_larger_bucket_count(self):
        metadata = intraday_metadata_from_config({"api_name": "tick", "endpoint_kind": "tick"})
        self.assertEqual(metadata.bucket_count, 128)
        self.assertEqual(metadata.target_file_size_mb, 256)
        self.assertEqual(metadata.max_file_size_mb, 1024)

    def test_invalid_bucket_count_is_rejected(self):
        result = validate_intraday_metadata(
            {
                "api_name": "stk_mins",
                "endpoint_kind": "minute_bar",
                "intraday_strategy": {
                    "freq": "1min",
                    "bucket_count": 63,
                    "compaction_required": True,
                    "execution_blocked_until_bucket_policy_enabled": True,
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("invalid_bucket_count", result.errors)

    def test_compaction_required_and_execution_block_are_enforced(self):
        result = validate_intraday_metadata(
            {
                "api_name": "tick",
                "endpoint_kind": "tick",
                "intraday_strategy": {
                    "bucket_count": 128,
                    "compaction_required": False,
                    "execution_blocked_until_bucket_policy_enabled": False,
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("compaction_required_false", result.errors)
        self.assertIn("execution_not_blocked", result.errors)


class IntradayPlanCliTests(unittest.TestCase):
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

    def test_minute_and_tick_plans_are_blocked_and_read_only(self):
        cases = [
            ("stk_mins", ["--freq", "1min"], 64),
            ("tick", [], 128),
        ]
        for api_name, freq_args, bucket_count in cases:
            with self.subTest(api_name=api_name):
                result = self.run_cli(
                    "intraday-plan",
                    "--api",
                    api_name,
                    *freq_args,
                    "--start-date",
                    "20250102",
                    "--end-date",
                    "20250103",
                    "--bucket-count",
                    str(bucket_count),
                    "--json",
                )
                payload = json.loads(result.stdout)
                self.assertTrue(payload["blocked"])
                self.assertFalse(payload["execution_allowed"])
                self.assertEqual(payload["bucket_count"], bucket_count)
                self.assertEqual(payload["blocked_reason"], "bucket_policy_missing")
                self.assertIn("compaction policy", payload["required_infra"])
                self.assertNotIn("secret-token-should-not-appear", result.stdout)
                self.assertNotIn("secret-token-should-not-appear", result.stderr)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_invalid_bucket_and_date_range_return_errors_without_catalog(self):
        result = self.run_cli(
            "intraday-plan",
            "--api",
            "stk_mins",
            "--freq",
            "1min",
            "--start-date",
            "20250102",
            "--end-date",
            "20250103",
            "--bucket-count",
            "63",
            "--json",
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertIn("invalid_bucket_count", payload["required_infra"])
        self.assertFalse(payload["execution_allowed"])

        result = self.run_cli(
            "intraday-plan",
            "--api",
            "tick",
            "--start-date",
            "20250199",
            "--end-date",
            "20250103",
            "--bucket-count",
            "128",
            "--json",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertIn("invalid start-date", payload["blocking_errors"][0])
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_intraday_plan_table_output_is_stable(self):
        result = self.run_cli(
            "intraday-plan",
            "--api",
            "stk_mins",
            "--freq",
            "1min",
            "--start-date",
            "20250102",
            "--end-date",
            "20250103",
            "--bucket-count",
            "64",
        )
        self.assertIn("estimated_partition_strategy", result.stdout)
        self.assertIn("bucket_policy_missing", result.stdout)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())


class StorageEstimateCliTests(unittest.TestCase):
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

    def test_low_risk_estimate_uses_pilot_reference_without_side_effects(self):
        result = self.run_cli(
            "storage-estimate",
            "--scope",
            "low-risk-a-share",
            "--start-date",
            "20250101",
            "--end-date",
            "20251231",
            "--json",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["scope"], "low-risk-a-share")
        self.assertEqual(payload["confidence"], "medium")
        self.assertGreater(payload["estimated_raw_files"], 0)
        self.assertGreater(payload["estimated_lake_files"], 0)
        self.assertIn("January 2025 low-risk pilot", " ".join(payload["assumptions"]))
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_intraday_estimate_warns_low_confidence(self):
        result = self.run_cli(
            "storage-estimate",
            "--category",
            "intraday",
            "--api",
            "stk_mins",
            "--freq",
            "1min",
            "--start-date",
            "20250102",
            "--end-date",
            "20250131",
            "--bucket-count",
            "64",
            "--json",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["category"], "intraday")
        self.assertEqual(payload["confidence"], "low")
        self.assertEqual(payload["estimated_size_class"], "potentially_large")
        self.assertIn("bucketed intraday execution", " ".join(payload["warnings"]))
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_storage_estimate_invalid_input_returns_error_without_catalog(self):
        result = self.run_cli(
            "storage-estimate",
            "--scope",
            "low-risk-a-share",
            "--category",
            "intraday",
            "--start-date",
            "20250101",
            "--end-date",
            "20250131",
            "--json",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertIn("choose either scope or category", payload["blocking_errors"][0])
        self.assertNotIn("secret-token-should-not-appear", result.stdout)
        self.assertNotIn("secret-token-should-not-appear", result.stderr)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())


class CompactionPlanTests(unittest.TestCase):
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

    def add_lake_snapshot(self, api_name: str, sizes: list[int], partition: dict[str, str]):
        file_ids = []
        for idx, size in enumerate(sizes):
            file_ids.append(
                self.catalog.insert_file(
                    table_id="tbl_test",
                    api_name=api_name,
                    content_type="lake",
                    file_format="parquet",
                    relative_path=f"lake/api={api_name}/part-{idx}.parquet",
                    staged_path=None,
                    partition_values=partition,
                    record_count=1,
                    source_item_count=None,
                    raw_event_count=None,
                    error_event_count=None,
                    size_bytes=size,
                    sha256=f"{idx:064x}",
                    schema_id=None,
                    status="staged",
                    run_id="run_test",
                    job_key=f"job_{idx}",
                )
            )
        return self.catalog.commit_snapshot(
            api_name=api_name,
            table_id="tbl_test",
            file_ids=file_ids,
            run_id="run_test",
            checkpoint_key=f"ckpt_{api_name}",
            cursor="test",
        )

    def test_no_candidates_is_read_only(self):
        self.add_lake_snapshot("daily_basic", [2 * 1024 * 1024, 3 * 1024 * 1024], {"year": "2025", "month": "01"})
        before = self.counts()
        plan = CompactionPlanner(self.root, self.catalog).plan("daily_basic")
        after = self.counts()
        self.assertEqual(before, after)
        self.assertEqual(plan.partitions_checked, 1)
        self.assertEqual(plan.candidate_partitions, [])
        self.assertFalse(plan.execution_allowed)

    def test_fake_small_file_candidates(self):
        self.add_lake_snapshot("daily_basic", [128] * 5, {"year": "2025", "month": "01"})
        plan = CompactionPlanner(self.root, self.catalog).plan("daily_basic")
        self.assertEqual(len(plan.candidate_partitions), 1)
        self.assertEqual(plan.small_file_count, 5)
        self.assertEqual(plan.candidate_partitions[0].estimated_action, "compact_small_files")

    def test_oversized_file_candidate(self):
        self.add_lake_snapshot("daily_basic", [2 * 1024 * 1024 * 1024], {"year": "2025", "month": "01"})
        plan = CompactionPlanner(self.root, self.catalog).plan("daily_basic")
        self.assertEqual(len(plan.candidate_partitions), 1)
        self.assertEqual(plan.oversized_file_count, 1)
        self.assertEqual(plan.candidate_partitions[0].estimated_action, "split_or_rewrite_oversized_files")

    def test_cli_json_no_snapshot_and_no_side_effects(self):
        before = self.counts()
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tushare_mirror",
                "compaction-plan",
                "--root",
                str(self.root),
                "--api",
                "daily_basic",
                "--json",
            ],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        after = self.counts()
        self.assertEqual(before, after)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["api_name"], "daily_basic")
        self.assertFalse(payload["execution_allowed"])
        self.assertIn("no latest snapshot", " ".join(payload["warnings"]))


class RatePolicyCliTests(unittest.TestCase):
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

    def test_low_risk_policy_is_present_and_read_only(self):
        result = self.run_cli("rate-policy", "--scope", "low-risk-a-share", "--json")
        payload = json.loads(result.stdout)
        self.assertEqual(payload["scope"], "low-risk-a-share")
        self.assertEqual(payload["max_requests_per_batch"], 20)
        self.assertTrue(payload["execution_allowed"])
        self.assertIn("rate_limited", payload["retryable_errors"])
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_financial_and_intraday_policies_block_execution(self):
        for category in ["financial", "intraday"]:
            with self.subTest(category=category):
                result = self.run_cli("rate-policy", "--category", category, "--json", check=False)
                self.assertNotEqual(result.returncode, 0)
                payload = json.loads(result.stdout)
                self.assertEqual(payload["category"], category)
                self.assertFalse(payload["execution_allowed"])
                self.assertIn(f"{category}_execution_blocked", payload["blocking_errors"])
                self.assertNotIn("secret-token-should-not-appear", result.stdout)
                self.assertNotIn("secret-token-should-not-appear", result.stderr)
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())

    def test_rate_policy_invalid_input_is_clear(self):
        result = self.run_cli("rate-policy", "--category", "unknown", "--json", check=False)
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertIn("unsupported category", payload["blocking_errors"][0])
        self.assertFalse((self.root / "_catalog" / "catalog.sqlite").exists())


if __name__ == "__main__":
    unittest.main()
