from __future__ import annotations

import json
import hashlib
import sqlite3
from pathlib import Path

from tushare_mirror.backup import BackupExecutor, BackupPlanner
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.mirror import CommandSafetyAnalyzer, ExecuteReadinessReporter, ExecuteScriptReporter, FinalGateReporter, MirrorBatchBundleReporter, confirmation_phrase
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
        self.assertEqual(result.command_preview["confirmation_phrase"], result.confirmation_phrase)
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


class FinalGateCliTests(PreBackfillOperationsTestCase):
    def promotion_backup(self, name: str = "final-gate-cli-backup") -> Path:
        backup = self.base / name
        plan = BackupPlanner(self.root, self.catalog).plan(backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        return backup

    def prepare_staged_gate(self) -> tuple[Path, Path]:
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.promotion_backup()
        bundle = self.base / "cli-bundle-202502"
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
        return backup, bundle

    def final_gate_args(self, backup: Path, bundle: Path) -> list[str]:
        return [
            "mirror-final-gate",
            "--root", str(self.root),
            "--backup", str(backup),
            "--bundle", str(bundle),
            "--scope", "low-risk-a-share",
            "--start-date", "20250201",
            "--end-date", "20250228",
            "--max-jobs-per-api", "20",
        ]

    def test_cli_table_output_is_read_only(self):
        backup, bundle = self.prepare_staged_gate()
        before = self.counts()
        result = self.run_cli(*self.final_gate_args(backup, bundle))
        self.assertEqual(self.counts(), before)
        self.assertEqual(result.returncode, 0)
        self.assertIn("gate_status", result.stdout)
        self.assertIn("ready_for_user_confirmed_execute", result.stdout)
        self.assertIn("command_safety_warning_only", result.stdout)

    def test_cli_json_output_is_stable_and_read_only(self):
        backup, bundle = self.prepare_staged_gate()
        before = self.counts()
        result = self.run_cli(*self.final_gate_args(backup, bundle), "--json")
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
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
            "final_command_preview",
            "blocking_errors",
            "warnings",
            "safety_boundaries",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-final-gate/v1")
        self.assertEqual(payload["gate_status"], "warning")
        self.assertTrue(payload["ready_for_user_confirmed_execute"])

    def test_cli_missing_root_error_is_clear(self):
        backup, bundle = self.prepare_staged_gate()
        result = self.run_cli(
            "mirror-final-gate",
            "--root", str(self.base / "missing-root"),
            "--backup", str(backup),
            "--bundle", str(bundle),
            "--scope", "low-risk-a-share",
            "--start-date", "20250201",
            "--end-date", "20250228",
            "--max-jobs-per-api", "20",
            "--json",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["gate_status"], "blocked")
        self.assertTrue(any("catalog not found" in error or "review" in error for error in payload["blocking_errors"]))

    def test_cli_missing_backup_error_is_clear(self):
        backup, bundle = self.prepare_staged_gate()
        result = self.run_cli(
            *self.final_gate_args(self.base / "missing-backup", bundle),
            "--json",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["gate_status"], "blocked")
        self.assertTrue(any("backup" in error for error in payload["blocking_errors"]))

    def test_cli_missing_bundle_error_is_clear(self):
        backup, _ = self.prepare_staged_gate()
        result = self.run_cli(
            *self.final_gate_args(backup, self.base / "missing-bundle"),
            "--json",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["gate_status"], "blocked")
        self.assertTrue(any("bundle" in error for error in payload["blocking_errors"]))


class ExecuteScriptGeneratorTests(PreBackfillOperationsTestCase):
    def prepare_bundle(self) -> tuple[Path, Path]:
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.base / "execute-script-backup"
        plan = BackupPlanner(self.root, self.catalog).plan(backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        bundle = self.base / "execute-script-bundle"
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
        return backup, bundle

    def create_script(self, backup: Path, bundle: Path, output: Path, *, overwrite: bool = False):
        return ExecuteScriptReporter(token_available=True).create(
            root=self.root,
            backup=backup,
            bundle=bundle,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
            output=output,
            overwrite=overwrite,
        )

    def test_script_generated_outside_roots_and_not_executed(self):
        backup, bundle = self.prepare_bundle()
        output = self.base / "execute-202502.sh"
        before = self.counts()
        result = self.create_script(backup, bundle, output)
        self.assertEqual(self.counts(), before)
        self.assertEqual(result.report_version, "mirror-execute-script/v1")
        self.assertEqual(result.status, "created")
        self.assertEqual(result.command_safety_status, "warning")
        self.assertTrue(output.exists())
        content = output.read_text(encoding="utf-8")
        self.assertIn("CONFIRM LOW-RISK-A-SHARE 20250201-20250228 MAXJOBS20", content)
        self.assertIn("USER_CONFIRMATION_REQUIRED", content)
        self.assertIn("mirror-final-gate", content)
        self.assertIn("mirror-run", content)
        self.assertIn("--execute", content)
        self.assertIn("validate --latest-all --no-record", content)
        self.assertIn("backup-inspect", content)
        self.assertIn("restore-check", content)
        self.assertIn("mirror-review", content)
        self.assertIn("mirror-next-batch", content)
        self.assertNotIn("\npython3 -m tushare_mirror mirror-run", content)
        safety = CommandSafetyAnalyzer().analyze(file=output)
        self.assertEqual(safety.status, "warning")
        self.assertFalse(safety.blocking_errors)

    def test_output_inside_root_blocked(self):
        backup, bundle = self.prepare_bundle()
        output = self.root / "execute.sh"
        result = self.create_script(backup, bundle, output)
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("inside mirror root" in error for error in result.blocking_errors))
        self.assertFalse(output.exists())

    def test_output_inside_backup_blocked(self):
        backup, bundle = self.prepare_bundle()
        output = backup / "execute.sh"
        result = self.create_script(backup, bundle, output)
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("inside backup root" in error for error in result.blocking_errors))
        self.assertFalse(output.exists())

    def test_overwrite_behavior(self):
        backup, bundle = self.prepare_bundle()
        output = self.base / "overwrite-execute.sh"
        output.write_text("old\n", encoding="utf-8")
        refused = self.create_script(backup, bundle, output)
        self.assertEqual(refused.status, "blocked")
        self.assertEqual(output.read_text(encoding="utf-8"), "old\n")
        replaced = self.create_script(backup, bundle, output, overwrite=True)
        self.assertEqual(replaced.status, "created")
        self.assertTrue(replaced.overwritten)
        self.assertIn("mirror-final-gate", output.read_text(encoding="utf-8"))

    def test_cli_json_contract_and_no_side_effects(self):
        backup, bundle = self.prepare_bundle()
        output = self.base / "cli-execute.sh"
        before = self.counts()
        result = self.run_cli(
            "mirror-execute-script",
            "--root", str(self.root),
            "--backup", str(backup),
            "--bundle", str(bundle),
            "--scope", "low-risk-a-share",
            "--start-date", "20250201",
            "--end-date", "20250228",
            "--max-jobs-per-api", "20",
            "--output", str(output),
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["report_version"], "mirror-execute-script/v1")
        self.assertEqual(payload["status"], "created")
        self.assertEqual(payload["command_safety_status"], "warning")
        self.assertTrue(output.exists())


class ExecuteReadinessReporterTests(PreBackfillOperationsTestCase):
    def prepare_bundle(self) -> tuple[Path, Path]:
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.base / "execute-readiness-backup"
        plan = BackupPlanner(self.root, self.catalog).plan(backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        bundle = self.base / "execute-readiness-bundle"
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
        return backup, bundle

    def report(self, backup: Path, bundle: Path):
        return ExecuteReadinessReporter(token_available=True).report(
            root=self.root,
            backup=backup,
            bundle=bundle,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
        )

    def test_healthy_staged_readiness_is_read_only(self):
        backup, bundle = self.prepare_bundle()
        before = self.counts()
        result = self.report(backup, bundle)
        self.assertEqual(self.counts(), before)
        self.assertEqual(result.report_version, "mirror-execute-readiness/v1")
        self.assertEqual(result.execute_readiness_status, "warning")
        self.assertTrue(result.may_execute_after_user_confirmation)
        self.assertTrue(result.must_not_execute_automatically)
        self.assertEqual(result.final_gate_status, "warning")
        self.assertEqual(result.bundle_status, "passed")
        self.assertEqual(result.command_safety_status, "warning")
        self.assertEqual(result.rehearsal_status, "passed")
        self.assertEqual(result.promotion_status, "staged")
        self.assertEqual(result.backup_status, "succeeded")
        self.assertEqual(result.token_hygiene_status, "passed")
        self.assertEqual(result.estimated_request_count, 6)
        self.assertEqual(result.confirmation_phrase, "CONFIRM LOW-RISK-A-SHARE 20250201-20250228 MAXJOBS20")
        self.assertIn("mirror-run", result.exact_user_confirmed_command)

    def test_bundle_blocker_propagates(self):
        backup, _ = self.prepare_bundle()
        bundle = self.base / "blocked-readiness-bundle"
        bundle.mkdir()
        (bundle / "README.md").write_text("old bundle\n", encoding="utf-8")
        result = self.report(backup, bundle)
        self.assertEqual(result.execute_readiness_status, "blocked")
        self.assertFalse(result.may_execute_after_user_confirmation)
        self.assertEqual(result.bundle_status, "blocked")
        self.assertTrue(any("bundle" in error for error in result.blocking_errors))

    def test_backup_mutation_propagates(self):
        backup, bundle = self.prepare_bundle()
        Validator(backup, CatalogStore(backup)).validate_latest_snapshots(record=True)
        result = self.report(backup, bundle)
        self.assertEqual(result.execute_readiness_status, "blocked")
        self.assertFalse(result.may_execute_after_user_confirmation)
        self.assertTrue(any("possible_mutation" in error or "modified after backup creation" in error for error in result.blocking_errors))

    def test_cli_json_contract_and_no_side_effects(self):
        backup, bundle = self.prepare_bundle()
        before = self.counts()
        result = self.run_cli(
            "mirror-execute-readiness",
            "--root", str(self.root),
            "--backup", str(backup),
            "--bundle", str(bundle),
            "--scope", "low-risk-a-share",
            "--start-date", "20250201",
            "--end-date", "20250228",
            "--max-jobs-per-api", "20",
            "--json",
        )
        self.assertEqual(self.counts(), before)
        payload = json.loads(result.stdout)
        for key in [
            "report_version",
            "execute_readiness_status",
            "may_execute_after_user_confirmation",
            "must_not_execute_automatically",
            "final_gate_status",
            "bundle_status",
            "command_safety_status",
            "rehearsal_status",
            "promotion_status",
            "backup_status",
            "token_hygiene_status",
            "estimated_request_count",
            "confirmation_phrase",
            "exact_user_confirmed_command",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["report_version"], "mirror-execute-readiness/v1")
        self.assertEqual(payload["execute_readiness_status"], "warning")
        self.assertTrue(payload["may_execute_after_user_confirmation"])


class FinalGateReadOnlyRegressionTests(PreBackfillOperationsTestCase):
    def prepare_bundle(self) -> tuple[Path, Path]:
        self.build_pilot()
        self.cover_january_matrix()
        backup = self.base / "readonly-backup"
        plan = BackupPlanner(self.root, self.catalog).plan(backup)
        BackupExecutor(self.root, self.catalog).backup(plan)
        bundle = self.base / "readonly-bundle"
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
        return backup, bundle

    def backup_catalog_checksum(self, backup: Path) -> str:
        path = backup / "_catalog" / "catalog.sqlite"
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def test_final_gate_commands_do_not_mutate_catalogs_or_data_files(self):
        backup, bundle = self.prepare_bundle()
        bundle_output = self.base / "readonly-generated-bundle"
        script_output = self.base / "readonly-execute.sh"
        commands = [
            [
                "mirror-final-gate",
                "--root", str(self.root),
                "--backup", str(backup),
                "--bundle", str(bundle),
                "--scope", "low-risk-a-share",
                "--start-date", "20250201",
                "--end-date", "20250228",
                "--max-jobs-per-api", "20",
                "--json",
            ],
            [
                "mirror-execute-script",
                "--root", str(self.root),
                "--backup", str(backup),
                "--bundle", str(bundle),
                "--scope", "low-risk-a-share",
                "--start-date", "20250201",
                "--end-date", "20250228",
                "--max-jobs-per-api", "20",
                "--output", str(script_output),
                "--json",
            ],
            [
                "mirror-batch-bundle",
                "--root", str(self.root),
                "--backup", str(backup),
                "--scope", "low-risk-a-share",
                "--start-date", "20250201",
                "--end-date", "20250228",
                "--max-jobs-per-api", "20",
                "--output", str(bundle_output),
                "--json",
            ],
            ["mirror-execute-readiness", "--root", str(self.root), "--backup", str(backup), "--bundle", str(bundle), "--scope", "low-risk-a-share", "--start-date", "20250201", "--end-date", "20250228", "--max-jobs-per-api", "20", "--json"],
            ["mirror-batch-bundle-verify", "--bundle", str(bundle), "--json"],
            ["command-safety-check", "--file", str(bundle / "commands.sh"), "--json"],
            ["mirror-batch-rehearse", "--root", str(self.root), "--backup", str(backup), "--bundle", str(bundle), "--json"],
        ]
        for args in commands:
            with self.subTest(command=args[0]):
                before = self.guardrail_counts()
                before_checksum = self.backup_catalog_checksum(backup)
                result = self.run_cli(*args)
                after = self.guardrail_counts()
                after_checksum = self.backup_catalog_checksum(backup)
                self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
                self.assertEqual(after, before)
                self.assertEqual(after["mirror_catalog"]["validations"], before["mirror_catalog"]["validations"])
                self.assertEqual(after["mirror_raw_files"], before["mirror_raw_files"])
                self.assertEqual(after["mirror_lake_files"], before["mirror_lake_files"])
                self.assertEqual(before_checksum, after_checksum)
