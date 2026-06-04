from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.mirror import DAILY_LIKE_MIRROR_APIS, MirrorBatchBundleReporter, MirrorOrchestrator
from tushare_mirror.store import FileLakeStore

try:
    from .test_pre_backfill_operations import CalendarRangeFakeClient, PreBackfillFakeClient
except ImportError:  # pragma: no cover - unittest discover loads modules by filename
    from test_pre_backfill_operations import CalendarRangeFakeClient, PreBackfillFakeClient


class BatchExecutionSafetyReadOnlyContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)
        self._prepare_ready_fake_mirror()
        self.bundle = self._create_bundle()

    def tearDown(self):
        self.tmp.cleanup()

    def _prepare_ready_fake_mirror(self) -> None:
        result = MirrorOrchestrator(self.root, self.catalog, PreBackfillFakeClient(), sleep=lambda _: None).run(
            scope="low-risk-a-share",
            mode="pilot",
            start_date="20250101",
            end_date="20250110",
            max_jobs_per_api=20,
            backup_target=str(self.backup),
        )
        self.assertEqual(result.status, "succeeded")
        self._cover_source_month()
        self._fetch_trade_cal("20250201", "20250228")

    def _cover_source_month(self) -> None:
        from tushare_mirror.backfill import BackfillExecutor, BackfillPlanner, DatePlanner

        self._fetch_trade_cal("20250101", "20250131")
        dates, calendar = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(
            start_date="20250101",
            end_date="20250131",
            trading_days_only=True,
            calendar_exchange="SSE",
        )
        self.assertTrue(dates)
        for api_name in DAILY_LIKE_MIRROR_APIS:
            plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(
                api_name,
                dates,
                max_jobs=len(dates),
                calendar_metadata=calendar,
            )
            result = BackfillExecutor(self.root, self.catalog).execute(plan, PreBackfillFakeClient())
            self.assertEqual(result.status, "succeeded")
        for api_name, explicit_dates in {
            "weekly": ["20250103", "20250110", "20250117", "20250124", "20250131"],
            "monthly": ["20250131"],
        }.items():
            plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(api_name, explicit_dates, max_jobs=len(explicit_dates))
            result = BackfillExecutor(self.root, self.catalog).execute(plan, PreBackfillFakeClient())
            self.assertEqual(result.status, "succeeded")

    def _fetch_trade_cal(self, start_date: str, end_date: str) -> None:
        result = FileLakeStore(self.root, self.catalog).fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": start_date, "end_date": end_date},
            CalendarRangeFakeClient(),
        )
        self.assertTrue(result.snapshot_id)

    def _create_bundle(self) -> Path:
        output = self.base / "bundle-202502"
        result = MirrorBatchBundleReporter().create(
            root=self.root,
            backup=self.backup,
            scope="low-risk-a-share",
            start_date="20250201",
            end_date="20250228",
            max_jobs_per_api=20,
            output=output,
        )
        self.assertEqual(result.status, "created")
        return output

    def _state(self) -> dict[str, object]:
        return {
            "catalog": self._catalog_counts(self.root),
            "backup_catalog": self._catalog_counts(self.backup),
            "root_files": self._file_count(self.root),
            "backup_files": self._file_count(self.backup),
            "root_raw_files": self._file_count(self.root / "raw"),
            "root_lake_files": self._file_count(self.root / "lake"),
            "backup_raw_files": self._file_count(self.backup / "raw"),
            "backup_lake_files": self._file_count(self.backup / "lake"),
        }

    def _catalog_counts(self, root: Path) -> dict[str, int]:
        import sqlite3

        with sqlite3.connect(root / "_catalog" / "catalog.sqlite") as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
                "schemas": conn.execute("select count(*) from schemas").fetchone()[0],
                "schema_changes": conn.execute("select count(*) from schema_changes").fetchone()[0],
                "quarantine": conn.execute("select count(*) from quarantine_files").fetchone()[0],
            }

    def _file_count(self, path: Path) -> int:
        if not path.exists():
            return 0
        return sum(1 for item in path.rglob("*") if item.is_file())

    def _run_cli(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env["TUSHARE_TOKEN"] = "fake-readonly-contract-token"
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=check,
        )

    def _assert_read_only(self, *args: str, check: bool = True) -> dict[str, object]:
        before = self._state()
        result = self._run_cli(*args, check=check)
        self.assertEqual(self._state(), before)
        if result.stdout:
            return json.loads(result.stdout)
        return {}

    def test_new_batch_safety_commands_are_read_only(self):
        commands = [
            ["mirror-batch-bundle-verify", "--bundle", str(self.bundle), "--json"],
            ["command-safety-check", "--file", str(self.bundle / "commands.sh"), "--json"],
            [
                "mirror-batch-rehearse",
                "--root", str(self.root),
                "--backup", str(self.backup),
                "--bundle", str(self.bundle),
                "--json",
            ],
            ["mirror-batch-ledger", "--root", str(self.root), "--scope", "low-risk-a-share", "--json"],
            ["mirror-failure-drill", "--scenario", "rate_limited", "--scope", "low-risk-a-share", "--json"],
            ["path-diagnostics", "--root", str(self.root), "--backup", str(self.backup), "--json"],
            ["token-hygiene", "--path", str(self.root), "--json"],
            [
                "monthly-promotion-checklist",
                "--root", str(self.root),
                "--backup", str(self.backup),
                "--scope", "low-risk-a-share",
                "--from-month", "202501",
                "--to-month", "202502",
                "--json",
            ],
            [
                "mirror-ops-report",
                "--root", str(self.root),
                "--backup", str(self.backup),
                "--scope", "low-risk-a-share",
                "--start-date", "20250101",
                "--end-date", "20250131",
                "--next-start-date", "20250201",
                "--next-end-date", "20250228",
                "--json",
            ],
        ]
        for command in commands:
            with self.subTest(command=command[0]):
                payload = self._assert_read_only(*command)
                self.assertIn("report_version", payload)

    def test_certificate_writes_only_explicit_output_path(self):
        output = self.base / "certificate-output"
        before = self._state()
        payload = self._assert_read_only(
            "mirror-batch-certificate",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "low-risk-a-share",
            "--start-date", "20250101",
            "--end-date", "20250131",
            "--output", str(output),
            "--json",
        )
        self.assertEqual(self._state(), before)
        self.assertEqual(payload["report_version"], "mirror-batch-certificate/v1")
        self.assertEqual(payload["status"], "created")
        self.assertTrue((output / "certificate.json").exists())
        self.assertTrue((output / "certificate.md").exists())
