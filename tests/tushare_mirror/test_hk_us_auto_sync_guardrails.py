from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.errors import ErrorType, MirrorError
from tushare_mirror.mirror import CommandSafetyAnalyzer, GLOBAL_EQUITY_LOW_RISK_SCOPE, MirrorAutoSyncCommandReporter, MirrorAutoSyncReporter, MirrorRunResult
from tushare_mirror.mirror import MirrorActiveWriterDetector, MirrorAutoSyncLock


class HKUSAutoSyncFakeClient:
    token = "fake-token-for-hash-only"

    def __init__(self):
        self.request_calls: list[str] = []
        self.query_calls: list[tuple[str, dict]] = []

    def request(self, api_name, params, fields=None):
        self.request_calls.append(api_name)
        fields_list = list(fields or [])
        return {"code": 0, "msg": None, "data": {"fields": fields_list, "items": [self._row(api_name, params, fields_list)]}}

    def query_paginated(self, api_name, params, fields, page_size=None):
        self.query_calls.append((api_name, dict(params)))
        fields_list = list(fields or [])
        if api_name in {"hk_tradecal", "us_tradecal"}:
            fields_list = ["cal_date", "is_open", "pretrade_date"]
            items = self._calendar_rows(params)
        else:
            items = [self._row(api_name, params, fields_list)]
        event = {
            "code": 0,
            "msg": None,
            "data": {"fields": fields_list, "items": items, "has_more": False},
            "_http_status": 200,
            "_page_index": 0,
            "_request_params": dict(params),
        }
        return QueryResult(events=[event], fields=fields_list, items=items)

    def _calendar_rows(self, params):
        start = datetime.strptime(params.get("start_date") or "20250101", "%Y%m%d")
        end = datetime.strptime(params.get("end_date") or "20250110", "%Y%m%d")
        rows = []
        previous_open = "20241231"
        current = start
        while current <= end:
            cal_date = current.strftime("%Y%m%d")
            is_open = "1" if current.weekday() < 5 else "0"
            rows.append([cal_date, is_open, previous_open])
            if is_open == "1":
                previous_open = cal_date
            current += timedelta(days=1)
        return rows

    def _row(self, api_name, params, fields):
        values = []
        for field in fields:
            if field == "ts_code":
                values.append("AAPL" if api_name.startswith("us_") else "00001.HK")
            elif field in {"trade_date", "cal_date", "start_date", "end_date", "list_date", "delist_date"}:
                values.append(params.get(field) or params.get("trade_date") or "20250102")
            elif field == "is_open":
                values.append("1")
            elif field == "pretrade_date":
                values.append("20241231")
            elif field in {"name", "fullname", "enname", "cn_spell", "market", "list_status", "isin", "curr_type", "classify"}:
                values.append("x")
            else:
                values.append(1.0)
        return values


class HKUSWindowRetryFakeClient(HKUSAutoSyncFakeClient):
    def __init__(self, *, api_name: str, error_type: ErrorType, failures: int):
        super().__init__()
        self.failing_api_name = api_name
        self.error_type = error_type
        self.failures = failures

    def query_paginated(self, api_name, params, fields, page_size=None):
        if api_name == self.failing_api_name and self.failures > 0:
            self.failures -= 1
            raise MirrorError(self.error_type, self.error_type.value)
        return super().query_paginated(api_name, params, fields, page_size=page_size)


class HKUSAutoSyncGuardrailTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "mirror"
        self.backup = self.base / "backup"
        self.root.mkdir()
        self.backup.mkdir()
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def counts(self):
        with sqlite3.connect(self.catalog.db_path) as conn:
            return {
                "runs": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "jobs": conn.execute("select count(*) from jobs").fetchone()[0],
                "files": conn.execute("select count(*) from files").fetchone()[0],
                "snapshots": conn.execute("select count(*) from snapshots").fetchone()[0],
                "validations": conn.execute("select count(*) from validation_runs").fetchone()[0],
            }

    def run_cli(self, *args, check=False):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
        )

    def test_hk_auto_sync_readiness_fields_are_stable_and_read_only(self):
        before = self.counts()
        state = self.base / "hk-state.json"
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=False,
        )
        payload = result.to_dict()
        self.assertEqual(result.status, "planned")
        self.assertEqual(payload["report_version"], "mirror-auto-sync/v1")
        self.assertTrue(payload["execute_supported"])
        self.assertEqual(payload["calendar_api"], "hk_tradecal")
        self.assertEqual(payload["calendar_dependency_status"], "required")
        self.assertEqual(payload["state_path_status"], "safe")
        self.assertEqual(payload["lock_status"]["status"], "not_required_dry_run")
        self.assertEqual(payload["schema_status"], "clear")
        self.assertFalse(payload["token_available"])
        self.assertIn("hk_daily_adj", payload["pagination_summary"])
        self.assertIn("hk_income", payload["excluded_endpoints"])
        self.assertIn("hk_daily", payload["executable_endpoints"])
        self.assertEqual(
            payload["confirmation_phrase"],
            "CONFIRM HK-LOW-RISK AUTO-SYNC 20250101-20250110 MAXJOBS20",
        )
        self.assertFalse(payload["confirmation_reviewed"])
        self.assertTrue(payload["do_not_run_automatically"])
        self.assertFalse(state.exists())
        self.assertEqual(before, self.counts())
        self.assertNotIn("fake-token", json.dumps(payload))

    def test_us_auto_sync_readiness_reports_calendar_and_pagination(self):
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="us-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "us-state.json",
            token_available=True,
        )
        payload = result.to_dict()
        self.assertEqual(payload["calendar_api"], "us_tradecal")
        self.assertEqual(payload["pagination_summary"]["us_daily"], "offset_limit")
        self.assertTrue(payload["token_available"])
        self.assertIn("us_income", payload["excluded_endpoints"])
        self.assertEqual(payload["backup_status"], "exists")

    def test_global_auto_sync_readiness_is_not_executable(self):
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope=GLOBAL_EQUITY_LOW_RISK_SCOPE,
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "global-state.json",
            token_available=True,
        )
        payload = result.to_dict()
        self.assertFalse(payload["execute_supported"])
        self.assertIn("not an auto-sync execution scope", payload["execute_blocked_reason"])
        self.assertEqual(payload["calendar_dependency_status"], "composed_scope_read_only")

    def test_hk_execute_requires_extra_confirmation(self):
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "hk-state.json",
            execute=True,
            confirm_auto_sync=True,
            client=HKUSAutoSyncFakeClient(),
            token_available=True,
            active_writer_detector=MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 999999),
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("--execute for HK/US requires --confirm-hk-us-auto-sync", result.blocking_errors)
        self.assertFalse(result.confirmation_reviewed)

    def test_confirmation_phrase_is_generated_from_parsed_arguments(self):
        first = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="us-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "us-state.json",
            token_available=True,
        )
        second = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="us-low-risk",
            from_date="20250102",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "us-state.json",
            token_available=True,
        )
        self.assertEqual(first.confirmation_phrase, "CONFIRM US-LOW-RISK AUTO-SYNC 20250101-20250110 MAXJOBS20")
        self.assertNotEqual(first.confirmation_phrase, second.confirmation_phrase)

    def test_active_legacy_writer_blocks_hk_execute(self):
        detector = MirrorActiveWriterDetector(
            process_entries=[
                {
                    "pid": 111,
                    "cmdline": f"python3 -m tushare_mirror mirror-auto-sync --root {self.root} --execute",
                }
            ],
            now=lambda: self.catalog.db_path.stat().st_mtime + 999999,
        )
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "hk-state.json",
            execute=True,
            confirm_auto_sync=True,
            confirm_hk_us_auto_sync=True,
            client=HKUSAutoSyncFakeClient(),
            token_available=True,
            active_writer_detector=detector,
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "blocked")
        self.assertTrue(result.lock_status["active_writer_detected"])
        self.assertIn("possible active mirror writer detected", result.blocking_errors)

    def test_fake_hk_execute_gate_passes_with_explicit_confirmation(self):
        state = self.base / "hk-execute-state.json"
        client = HKUSAutoSyncFakeClient()
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            execute=True,
            confirm_auto_sync=True,
            confirm_hk_us_auto_sync=True,
            client=client,
            token_available=True,
            active_writer_detector=MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 999999),
            retry_backoff_seconds=0,
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "succeeded", result.to_dict())
        self.assertTrue(result.confirmation_reviewed)
        self.assertEqual(json.loads(state.read_text())["state_version"], "mirror-auto-sync-state/v2")
        self.assertEqual(result.next_start_date, "20250111")
        self.assertFalse((self.base / "hk-execute-state.json.lock").exists())
        called = [api for api, _ in client.query_calls] + client.request_calls
        self.assertIn("hk_tradecal", called)
        self.assertIn("hk_daily", called)
        self.assertLess(called.index("hk_tradecal"), called.index("hk_daily"))
        self.assertFalse({"hk_income", "hk_mins", "rt_hk_k"} & set(called))

    def test_existing_lock_blocks_hk_execute_without_client_calls(self):
        state = self.base / "hk-locked-state.json"
        lock_path = self.base / "hk-locked-state.json.lock"
        lock_path.write_text(
            json.dumps(
                {
                    "lock_version": "mirror-auto-sync-lock/v1",
                    "scope": "a-share-low-risk",
                    "root": str(self.root),
                    "backup": str(self.backup),
                    "pid": os.getpid(),
                    "hostname": os.uname().nodename if hasattr(os, "uname") else "unknown",
                    "started_at": "2026-06-07T00:00:00Z",
                    "command_kind": "mirror-auto-sync",
                    "state_path": str(self.base / "a-share-state.json"),
                }
            )
        )
        client = HKUSAutoSyncFakeClient()
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            execute=True,
            confirm_auto_sync=True,
            confirm_hk_us_auto_sync=True,
            client=client,
            token_available=True,
            active_writer_detector=MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 999999),
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("active auto-sync lock exists", result.blocking_errors)
        self.assertEqual(client.query_calls, [])
        self.assertEqual(client.request_calls, [])

    def test_global_execute_is_blocked(self):
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope=GLOBAL_EQUITY_LOW_RISK_SCOPE,
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=self.base / "global-state.json",
            execute=True,
            confirm_auto_sync=True,
            client=HKUSAutoSyncFakeClient(),
            token_available=True,
            active_writer_detector=MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 999999),
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("global-equity-low-risk auto-sync execute is not supported; run HK and US separately", result.blocking_errors)

    def test_rate_limit_failure_retries_current_window_and_succeeds(self):
        state = self.base / "hk-retry-state.json"
        client = HKUSWindowRetryFakeClient(api_name="hk_daily", error_type=ErrorType.RATE_LIMITED, failures=3)
        sleeps: list[float] = []
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            execute=True,
            confirm_auto_sync=True,
            confirm_hk_us_auto_sync=True,
            max_attempts=2,
            retry_backoff_seconds=0,
            client=client,
            token_available=True,
            active_writer_detector=MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 999999),
            sleep=sleeps.append,
        )
        self.assertEqual(result.status, "succeeded", result.to_dict())
        self.assertEqual(result.windows[0]["attempts"], 2)
        self.assertEqual(result.succeeded_window_count, 1)
        payload = json.loads(state.read_text())
        self.assertEqual(payload["next_start_date"], "20250111")
        self.assertEqual(payload["attempt_history"][-1]["attempts"], 2)

    def test_permission_denied_stops_without_window_retry(self):
        state = self.base / "hk-permission-state.json"
        client = HKUSWindowRetryFakeClient(api_name="hk_tradecal", error_type=ErrorType.PERMISSION_DENIED, failures=1)
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250110",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            execute=True,
            confirm_auto_sync=True,
            confirm_hk_us_auto_sync=True,
            max_attempts=3,
            retry_backoff_seconds=0,
            client=client,
            token_available=True,
            active_writer_detector=MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 999999),
            sleep=lambda _: None,
        )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.failed_window_count, 1)
        self.assertEqual(result.windows[0]["attempts"], 1)
        payload = json.loads(state.read_text())
        self.assertEqual(payload["next_start_date"], "20250101")
        self.assertEqual(payload["last_error_type"], "permission_denied")

    def test_backup_and_validation_failures_are_not_retryable(self):
        reporter = MirrorAutoSyncReporter()
        backup_failed = MirrorRunResult(
            run_id="run-backup",
            status="failed",
            summary={
                "backup_status": "failed",
                "restore_check_status": "not_requested",
                "validation_status": "succeeded",
                "items": [{"endpoint": "hk_daily", "status": "failed", "error_type": "rate_limited"}],
            },
        )
        validation_failed = MirrorRunResult(
            run_id="run-validation",
            status="failed",
            summary={
                "backup_status": "not_requested",
                "restore_check_status": "not_requested",
                "validation_status": "failed",
                "items": [{"endpoint": "hk_daily", "status": "failed", "error_type": "rate_limited"}],
            },
        )
        self.assertFalse(reporter._retryable_result(backup_failed))
        self.assertFalse(reporter._retryable_result(validation_failed))

    def test_auto_sync_command_bundle_is_guarded_and_safe(self):
        before = self.counts()
        output = self.base / "hk-command"
        result = MirrorAutoSyncCommandReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="19900101",
            to_date="20250110",
            window_days=20,
            max_jobs_per_api=20,
            state=self.base / "hk-state.json",
            output=output,
        )
        self.assertEqual(result.status, "created", result.to_dict())
        self.assertEqual(set(result.files), {"README.md", "commands.sh", "readiness.json", "request_estimate.json", "stop_policy.json"})
        commands = (output / "commands.sh").read_text()
        self.assertIn("USER_CONFIRMATION_REQUIRED", commands)
        self.assertIn("--confirm-hk-us-auto-sync", commands)
        self.assertNotIn("\npython3 -m tushare_mirror mirror-auto-sync", commands)
        safety = CommandSafetyAnalyzer().analyze(file=output / "commands.sh")
        self.assertIn(safety.status, {"passed", "warning"})
        self.assertFalse(safety.blocking_errors)
        self.assertEqual(before, self.counts())

    def test_auto_sync_command_blocks_unsafe_output_paths(self):
        inside_root = self.root / "bundle"
        result = MirrorAutoSyncCommandReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="19900101",
            to_date="20250110",
            window_days=20,
            max_jobs_per_api=20,
            state=self.base / "hk-state.json",
            output=inside_root,
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("output path must not be inside mirror root", result.blocking_errors)

        existing = self.base / "existing-command"
        existing.mkdir()
        result = MirrorAutoSyncCommandReporter().create(
            root=self.root,
            backup=self.backup,
            scope="us-low-risk",
            from_date="19900101",
            to_date="20250110",
            window_days=20,
            max_jobs_per_api=20,
            state=self.base / "us-state.json",
            output=existing,
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("output path already exists", "; ".join(result.blocking_errors))

    def test_auto_sync_command_cli_json_contract(self):
        result = self.run_cli(
            "mirror-auto-sync-command",
            "--root", str(self.root),
            "--backup", str(self.backup),
            "--scope", "us-low-risk",
            "--from-date", "19900101",
            "--to-date", "20250110",
            "--window-days", "20",
            "--max-jobs-per-api", "20",
            "--state", str(self.base / "us-state.json"),
            "--json",
            check=True,
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["report_version"], "mirror-auto-sync-command/v1")
        self.assertEqual(payload["scope"], "us-low-risk")
        self.assertTrue(payload["user_confirmation_required"])
        self.assertIn("--confirm-hk-us-auto-sync", payload["commands"][0]["command_text"])

    def test_auto_sync_lock_acquire_release_and_second_writer_block(self):
        lock_path = self.base / "auto-sync.lock"
        first = MirrorAutoSyncLock(
            lock_path=lock_path,
            scope="hk-low-risk",
            root=self.root,
            backup=self.backup,
            state_path=self.base / "hk-state.json",
            pid=123,
            hostname="host-a",
            pid_alive=lambda pid: pid == 123,
        )
        acquired = first.acquire()
        self.assertEqual(acquired["status"], "acquired")
        self.assertTrue(lock_path.exists())

        second = MirrorAutoSyncLock(
            lock_path=lock_path,
            scope="us-low-risk",
            root=self.root,
            backup=self.backup,
            state_path=self.base / "us-state.json",
            pid=456,
            hostname="host-a",
            pid_alive=lambda pid: pid == 123,
        )
        blocked = second.acquire()
        self.assertEqual(blocked["status"], "locked")
        self.assertIn("active auto-sync lock exists", blocked["blocking_errors"])

        released = first.release()
        self.assertEqual(released["status"], "released")
        self.assertFalse(lock_path.exists())

    def test_stale_or_unknown_lock_blocks_by_default(self):
        lock_path = self.base / "stale.lock"
        lock_path.write_text(
            json.dumps(
                {
                    "lock_version": "mirror-auto-sync-lock/v1",
                    "scope": "hk-low-risk",
                    "root": str(self.root),
                    "backup": str(self.backup),
                    "pid": 999999,
                    "hostname": "host-a",
                    "started_at": "2026-06-07T00:00:00Z",
                    "command_kind": "mirror-auto-sync",
                    "state_path": str(self.base / "hk-state.json"),
                }
            )
        )
        lock = MirrorAutoSyncLock(
            lock_path=lock_path,
            scope="us-low-risk",
            root=self.root,
            backup=self.backup,
            state_path=self.base / "us-state.json",
            hostname="host-a",
            pid_alive=lambda pid: False,
        )
        status = lock.status()
        self.assertEqual(status["status"], "stale_or_unknown")
        self.assertIn("stale or unknown", status["blocking_errors"][0])

    def test_legacy_active_writer_detection_blocks_same_root_execute_process(self):
        detector = MirrorActiveWriterDetector(
            process_entries=[
                {
                    "pid": 778009,
                    "cmdline": (
                        f"python3 -m tushare_mirror mirror-auto-sync --root {self.root} "
                        "--backup /tmp/backup --scope a-share-low-risk --execute"
                    ),
                },
                {"pid": 1, "cmdline": "python3 -m tushare_mirror mirror-auto-sync --root /other --execute"},
            ],
            now=lambda: 1000.0,
        )
        status = detector.report(root=self.root)
        self.assertEqual(status["status"], "active_writer_possible")
        self.assertTrue(status["active_writer_detected"])
        self.assertEqual(status["process_matches"][0]["pid"], 778009)

    def test_recent_catalog_write_is_reported_as_active_writer_signal(self):
        detector = MirrorActiveWriterDetector(process_entries=[], now=lambda: self.catalog.db_path.stat().st_mtime + 1)
        status = detector.report(root=self.root)
        self.assertEqual(status["status"], "active_writer_possible")
        self.assertTrue(any(signal["path"].endswith("catalog.sqlite") for signal in status["recent_file_signals"]))

    def test_v1_a_share_state_still_resumes_from_next_start_date(self):
        state = self.base / "a-share-state.json"
        state.write_text(
            json.dumps(
                {
                    "state_version": "mirror-auto-sync-state/v1",
                    "root": str(self.root),
                    "backup": str(self.backup),
                    "scope": "a-share-low-risk",
                    "from_date": "20250101",
                    "to_date": "20250120",
                    "resolved_to_date": "20250120",
                    "window_days": 10,
                    "max_jobs_per_api": 20,
                    "completed_windows": [],
                    "next_start_date": "20250111",
                }
            )
        )
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="a-share-low-risk",
            from_date="20250101",
            to_date="20250120",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=True,
        )
        self.assertTrue(result.resume_from_state)
        self.assertEqual(result.windows[0]["start_date"], "20250111")

    def test_v2_hk_interrupted_window_is_retried_conservatively(self):
        state = self.base / "hk-state.json"
        state.write_text(
            json.dumps(
                {
                    "state_version": "mirror-auto-sync-state/v2",
                    "root": str(self.root),
                    "backup": str(self.backup),
                    "scope": "hk-low-risk",
                    "from_date": "20250101",
                    "to_date": "20250120",
                    "resolved_to_date": "20250120",
                    "window_days": 10,
                    "max_jobs_per_api": 20,
                    "completed_windows": [],
                    "failed_windows": [],
                    "in_progress_window": {
                        "start_date": "20250105",
                        "end_date": "20250114",
                        "started_at": "2026-06-07T00:00:00Z",
                    },
                    "next_start_date": "20250115",
                    "attempt_history": [],
                }
            )
        )
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250120",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=True,
        )
        self.assertTrue(result.resume_from_state)
        self.assertEqual(result.windows[0]["start_date"], "20250105")
        self.assertEqual(result.next_start_date, "20250105")

    def test_malformed_state_blocks_with_clear_error(self):
        state = self.base / "bad-state.json"
        state.write_text(json.dumps({"state_version": "mirror-auto-sync-state/v99"}))
        result = MirrorAutoSyncReporter().create(
            root=self.root,
            backup=self.backup,
            scope="hk-low-risk",
            from_date="20250101",
            to_date="20250120",
            window_days=10,
            max_jobs_per_api=20,
            state=state,
            token_available=True,
        )
        self.assertEqual(result.status, "blocked")
        self.assertTrue(any("unsupported auto-sync state file version" in error for error in result.blocking_errors))
        self.assertEqual(result.windows, [])

    def test_atomic_state_write_replaces_file_without_tmp_residue(self):
        state = self.base / "atomic-state.json"
        payload = {
            "state_version": "mirror-auto-sync-state/v2",
            "scope": "hk-low-risk",
            "completed_windows": [],
            "failed_windows": [],
        }
        MirrorAutoSyncReporter()._atomic_write_state(state, payload)
        self.assertEqual(json.loads(state.read_text())["scope"], "hk-low-risk")
        self.assertFalse((self.base / ".atomic-state.json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
