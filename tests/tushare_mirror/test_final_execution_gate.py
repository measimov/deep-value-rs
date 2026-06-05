from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from tushare_mirror.backup import BackupExecutor, BackupPlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.mirror import FinalGateReporter, MirrorBatchBundleReporter, confirmation_phrase
from tushare_mirror.validation import Validator

try:
    from .test_pre_backfill_operations import PreBackfillOperationsTestCase
except ImportError:
    from test_pre_backfill_operations import PreBackfillOperationsTestCase


class FinalGateModelTests(PreBackfillOperationsTestCase):
    def promotion_backup(self, name: str = "final-gate-backup") -> Path:
        backup = self.base / name
        plan = BackupPlanner(self.root, self.catalog).plan(backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        return backup

    def create_bundle(self, backup: Path, name: str = "bundle-202502") -> Path:
        bundle = self.base / name
        result = MirrorBatchBundleReporter().create(
            root=self.root,
            backup=backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
            output=bundle,
        )
        self.assertEqual(result.status, "created")
        return bundle

    def prepare_staged_gate(self) -> tuple[Path, Path]:
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.promotion_backup()
        bundle = self.create_bundle(backup)
        return backup, bundle

    def report(self, backup: Path, bundle: Path, *, token_available: bool = True):
        return FinalGateReporter(token_available=token_available).report(
            root=self.root,
            backup=backup,
            bundle=bundle,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
        )

    def test_healthy_staged_february_gate_is_read_only(self):
        backup, bundle = self.prepare_staged_gate()
        before = self.counts()
        result = self.report(backup, bundle)
        self.assertEqual(self.counts(), before)
        self.assertEqual(result.report_version, "mirror-final-gate/v1")
        self.assertEqual(result.gate_status, "warning")
        self.assertTrue(result.ready_for_user_confirmed_execute)
        self.assertTrue(result.ready_for_dependency_stage)
        self.assertEqual(result.ready_for_full_batch_after_dependency, "pending")
        self.assertEqual(result.dependency_stage["dependency_status"], "missing")
        self.assertEqual(result.dependency_stage["dependency_action"], "fetch_trade_cal_first")
        self.assertFalse(result.dependency_stage["natural_day_fallback"])
        self.assertEqual(result.confirmation_phrase, "CONFIRM LOW-RISK-A-SHARE 20250201-20250228 MAXJOBS20")
        self.assertTrue(result.do_not_run_automatically)
        self.assertFalse(result.blocking_errors)
        self.assertIn("mirror-run", result.command_preview["command"])
        self.assertEqual(result.command_preview["confirmation"], "USER_CONFIRMATION_REQUIRED")
        self.assertTrue(any(check["name"] == "command_safety_warning_only" and check["status"] == "warning" for check in result.checks))
        self.assertNotIn("secret-token-should-not-appear", json.dumps(result.to_dict()))

    def test_missing_bundle_blocks_gate(self):
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.promotion_backup()
        result = self.report(backup, self.base / "missing-bundle")
        self.assertEqual(result.gate_status, "blocked")
        self.assertFalse(result.ready_for_user_confirmed_execute)
        self.assertTrue(any("bundle" in error for error in result.blocking_errors))

    def test_invalid_bundle_blocks_gate(self):
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.promotion_backup()
        bundle = self.base / "invalid-bundle"
        bundle.mkdir()
        (bundle / "README.md").write_text("old bundle\n", encoding="utf-8")
        result = self.report(backup, bundle)
        self.assertEqual(result.gate_status, "blocked")
        self.assertFalse(result.ready_for_user_confirmed_execute)
        self.assertTrue(any("bundle verification" in error for error in result.blocking_errors))

    def test_unguarded_command_blocks_gate(self):
        backup, bundle = self.prepare_staged_gate()
        (bundle / "commands.sh").write_text(
            "python3 -m tushare_mirror mirror-run --root /tmp/mirror --scope low-risk-a-share --execute\n",
            encoding="utf-8",
        )
        result = self.report(backup, bundle)
        self.assertEqual(result.gate_status, "blocked")
        self.assertTrue(any("command safety" in error for error in result.blocking_errors))
        self.assertTrue(any("unguarded execute" in error for error in result.blocking_errors))

    def test_backup_mutation_blocks_gate(self):
        backup, bundle = self.prepare_staged_gate()
        Validator(backup, CatalogStore(backup)).validate_latest_snapshots(record=True)
        result = self.report(backup, bundle)
        self.assertEqual(result.gate_status, "blocked")
        self.assertFalse(result.ready_for_user_confirmed_execute)
        self.assertTrue(any("possible_mutation" in error or "modified after backup creation" in error for error in result.blocking_errors))

    def test_token_missing_blocks_gate_without_plaintext(self):
        backup, bundle = self.prepare_staged_gate()
        result = self.report(backup, bundle, token_available=False)
        payload = json.dumps(result.to_dict())
        self.assertEqual(result.gate_status, "blocked")
        self.assertFalse(result.ready_for_user_confirmed_execute)
        self.assertTrue(any("TUSHARE_TOKEN is not available" in error for error in result.blocking_errors))
        self.assertNotIn("secret-token-should-not-appear", payload)

    def test_json_contract_and_confirmation_phrase_stability(self):
        backup, bundle = self.prepare_staged_gate()
        before = self.counts()
        result = self.report(backup, bundle)
        self.assertEqual(self.counts(), before)
        payload = result.to_dict()
        for key in [
            "report_version",
            "gate_status",
            "ready_for_user_confirmed_execute",
            "ready_for_dependency_stage",
            "ready_for_full_batch_after_dependency",
            "requested_range",
            "max_jobs_per_api",
            "estimated_request_count",
            "dependency_stage",
            "command_preview",
            "final_command_preview",
            "confirmation_phrase",
            "blocking_errors",
            "warnings",
            "safety_boundaries",
            "do_not_run_automatically",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(confirmation_phrase("low-risk-a-share", "20250201", "20250228", 20), payload["confirmation_phrase"])
        self.assertNotEqual(
            confirmation_phrase("low-risk-a-share", "20250301", "20250331", 20),
            payload["confirmation_phrase"],
        )
        self.assertNotIn("token", payload["confirmation_phrase"].lower())

