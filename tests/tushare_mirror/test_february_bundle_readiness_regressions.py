from __future__ import annotations

import json

from tushare_mirror.backup import BackupExecutor, BackupPlanner
from tushare_mirror.mirror import (
    MirrorBatchBundleReporter,
    MirrorBatchBundleVerifier,
    MirrorBatchCertificateReporter,
    MirrorBatchLedgerReporter,
    MirrorCoverageMatrixReporter,
    MirrorOpsReportReporter,
    MonthlyPromotionChecklistReporter,
    RequestEstimateReporter,
)

from .test_pre_backfill_operations import PreBackfillOperationsTestCase


class FebruaryBundleReadinessRegressionTests(PreBackfillOperationsTestCase):
    def backup_after_january_coverage(self):
        backup = self.base / "february-readiness-backup"
        plan = BackupPlanner(self.root, self.catalog).plan(backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        return backup

    def test_pre_manifest_bundle_regeneration_and_verify_diagnostics_are_read_only(self):
        self.build_pilot()
        output = self.base / "pre-manifest-february-bundle"
        output.mkdir()
        (output / "README.md").write_text("old bundle\n", encoding="utf-8")
        before = self.counts()

        blocked = MirrorBatchBundleReporter().create(
            root=self.root,
            backup=self.backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
            output=output,
        )
        verify_blocked = MirrorBatchBundleVerifier().verify(bundle=output)
        regenerated = MirrorBatchBundleReporter().create(
            root=self.root,
            backup=self.backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
            output=output,
            overwrite=True,
        )
        verify_ready = MirrorBatchBundleVerifier().verify(bundle=output)

        self.assertEqual(self.counts(), before)
        self.assertEqual(blocked.status, "blocked")
        self.assertTrue(any("not a valid manifest-bearing bundle" in error for error in blocked.blocking_errors))
        self.assertTrue(verify_blocked.pre_manifest_bundle_detected)
        self.assertEqual(verify_blocked.recommended_action, "Regenerate bundle with mirror-batch-bundle --overwrite")
        self.assertEqual(regenerated.status, "created")
        self.assertEqual(verify_ready.status, "passed")

    def test_staged_february_readiness_reports_dependency_without_hard_blockers(self):
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.backup_after_january_coverage()
        bundle = self.base / "verified-february-bundle"
        created = MirrorBatchBundleReporter().create(
            root=self.root,
            backup=backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
            output=bundle,
        )
        before = self.counts()

        coverage = MirrorCoverageMatrixReporter().report(root=self.root, scope="low-risk-a-share", start_date="20250101", end_date="20250131")
        estimate = RequestEstimateReporter().report(root=self.root, scope="low-risk-a-share", start_date="20250201", end_date="20250228")
        promotion = MonthlyPromotionChecklistReporter(token_available=True).report(
            root=self.root,
            backup=backup,
            scope="low-risk-a-share",
            from_month="202501",
            to_month="202502",
            bundle=bundle,
        )
        ops = MirrorOpsReportReporter(token_available=True).report(
            root=self.root,
            backup=backup,
            scope="low-risk-a-share",
            start_date="20250101",
            end_date="20250131",
            next_start_date="20250201",
            next_end_date="20250228",
        )
        ledger = MirrorBatchLedgerReporter().report(root=self.root, scope="low-risk-a-share", bundle=bundle)

        self.assertEqual(self.counts(), before)
        self.assertEqual(created.status, "created")
        by_api = {item["api"]: item for item in coverage.items}
        self.assertEqual(by_api["monthly"]["status"], "complete")
        self.assertEqual(by_api["monthly"]["coverage_class"], "weekly_monthly")
        self.assertEqual(estimate.dependency_status, "missing")
        self.assertEqual(estimate.dependency_action, "fetch_trade_cal_first")
        self.assertFalse(estimate.natural_day_fallback)
        self.assertEqual(promotion.status, "staged")
        self.assertFalse(promotion.hard_blockers)
        self.assertTrue(promotion.ready_for_dependency_stage)
        self.assertEqual(ops.overall_status, "staged")
        self.assertTrue(ops.ready_for_next_user_confirmed_batch)
        self.assertEqual(ledger.planned_batches[0]["execution_state"], "not_executed")
        self.assertEqual(ledger.planned_batches[0]["batch_state"], "dependency_stage_planned")
        self.assertNotIn("fake-status-token", json.dumps(promotion.to_dict()))

    def test_february_completion_certificate_blocks_before_execution_without_catalog_side_effects(self):
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.backup_after_january_coverage()
        output = self.base / "future-february-certificate"
        before = self.counts()

        result = MirrorBatchCertificateReporter().create(
            root=self.root,
            backup=backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            output=output,
        )

        self.assertEqual(self.counts(), before)
        self.assertEqual(result.status, "blocked")
        self.assertFalse(output.exists())
        self.assertTrue(any("not completed" in error for error in result.blocking_errors))
