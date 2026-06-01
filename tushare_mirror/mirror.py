from __future__ import annotations

import os
import shutil
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .backup import BackupExecutor, BackupInspector, BackupPlanner, RestoreChecker
from .backfill import BackfillExecutor, BackfillPlanner, DatePlanner
from .catalog import CatalogStore
from .coverage import CoverageReporter
from .client import classify_probe_response
from .endpoints import load_into_catalog
from .errors import classify_exception, retry_delay_seconds, should_retry
from .hashing import token_hash
from .io_utils import now_utc
from .planner import JobPlanner
from .reader import LakeReader
from .store import FileLakeStore
from .validation import Validator

LOW_RISK_A_SHARE_ENDPOINTS = [
    "stock_basic",
    "trade_cal",
    "hs_const",
    "daily",
    "adj_factor",
    "daily_basic",
    "weekly",
    "monthly",
    "suspend_d",
    "namechange",
    "stk_managers",
    "stk_rewards",
]

SMOKE_REFERENCE_FETCHES: dict[str, dict[str, Any]] = {
    "stock_basic": {"list_status": "L"},
    "trade_cal": {"exchange": "SSE", "start_date": "20250101", "end_date": "20250110"},
    "hs_const": {"hs_type": "SH", "is_new": "1"},
    "namechange": {"ts_code": "000001.SZ"},
    "stk_managers": {"ts_code": "000001.SZ"},
    "stk_rewards": {"ts_code": "000001.SZ"},
}

SMOKE_CALENDAR_BACKFILL_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d"]
SMOKE_EXPLICIT_DATE_APIS: dict[str, list[str]] = {
    "weekly": ["20250103", "20250110"],
    "monthly": ["20250127", "20250228"],
}

PILOT_JAN_2025_WEEKLY_DATES = ["20250103", "20250110", "20250117", "20250124", "20250127"]
PILOT_JAN_2025_MONTHLY_DATES = ["20250127"]

PILOT_BACKFILL_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d", "weekly", "monthly"]
DAILY_LIKE_MIRROR_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d"]
MODE_MAX_JOBS = {"smoke": 3, "pilot": 20}


def _valid_until_for(status: str) -> str:
    now = datetime.now(timezone.utc)
    if status in {"accessible", "empty_but_accessible"}:
        delta = timedelta(days=7)
    elif status in {"rate_limited", "network_error", "server_error", "unknown_error"}:
        delta = timedelta(days=1)
    else:
        delta = timedelta(days=30)
    return (now + delta).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class MirrorPlanItem:
    endpoint: str
    category: str
    requires_trade_cal: bool
    plan_status: str
    planned_jobs: int
    max_jobs: int
    existing_coverage: str | None
    missing_jobs: int
    blocked_reason: str | None
    will_execute: bool
    params: dict[str, Any] | None = None
    dates: list[str] | None = None
    permission_status: str | None = None
    planned_action: str | None = None
    required_by: list[str] | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MirrorPlan:
    scope: str
    mode: str
    root: str
    start_date: str | None
    end_date: str | None
    max_jobs_per_api: int
    endpoint_count: int
    planned_endpoint_count: int
    blocked_endpoint_count: int
    total_planned_jobs: int
    requires_real_requests: bool
    dry_run: bool
    items: list[MirrorPlanItem]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["items"] = [item.to_dict() for item in self.items]
        return data

    def summary(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "mode": self.mode,
            "root": self.root,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "max_jobs_per_api": self.max_jobs_per_api,
            "endpoint_count": self.endpoint_count,
            "planned_endpoint_count": self.planned_endpoint_count,
            "blocked_endpoint_count": self.blocked_endpoint_count,
            "total_planned_jobs": self.total_planned_jobs,
            "requires_real_requests": self.requires_real_requests,
            "dry_run": self.dry_run,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class MirrorRunResult:
    run_id: str | None
    status: str
    summary: dict[str, Any]
    validation: dict[str, Any] | None = None
    backup: dict[str, Any] | None = None
    restore_check: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "summary": self.summary,
            "validation": self.validation,
            "backup": self.backup,
            "restore_check": self.restore_check,
        }


@dataclass(frozen=True)
class MirrorReviewResult:
    root: str
    backup: str
    scope: str
    mode: str
    start_date: str
    end_date: str
    calendar_exchange: str
    root_status: str
    backup_status: str
    catalog_status: dict[str, Any]
    latest_snapshots: list[dict[str, Any]]
    endpoint_summary: list[dict[str, Any]]
    coverage_summary: list[dict[str, Any]]
    validation_status: str
    validation_results: list[dict[str, Any]]
    backup_inspect: dict[str, Any] | None
    backup_restore_check: dict[str, Any] | None
    backup_catalog_checksum_status: str | None
    backup_possible_mutation: bool
    artifact_size: dict[str, Any]
    token_plaintext_found: bool
    ready_for_next_batch: bool
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "root_status": self.root_status,
            "backup_status": self.backup_status,
            "validation_status": self.validation_status,
            "backup_catalog_checksum_status": self.backup_catalog_checksum_status,
            "backup_possible_mutation": self.backup_possible_mutation,
            "token_plaintext_found": self.token_plaintext_found,
            "ready_for_next_batch": self.ready_for_next_batch,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class MirrorReadinessResult:
    root: str
    backup: str
    scope: str
    readiness_status: str
    ready_for_controlled_full_backfill: bool
    checks: dict[str, Any]
    warnings: list[str]
    blocking_errors: list[str]
    review: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "readiness_status": self.readiness_status,
            "ready_for_controlled_full_backfill": self.ready_for_controlled_full_backfill,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class MirrorBatchEndpointPlan:
    endpoint: str
    category: str
    requires_trade_cal: bool
    plan_status: str
    planned_action: str
    total_candidate_jobs: int
    planned_jobs: int
    missing_jobs: int
    skipped_jobs: int
    blocked_jobs: int
    max_jobs: int
    truncated: bool
    dates: list[str]
    refresh_strategy: str | None = None
    blocked_reason: str | None = None
    warnings: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MirrorBatchPlan:
    batch_id: str
    scope: str
    root: str
    start_date: str
    end_date: str
    calendar_exchange: str
    max_jobs_per_api: int
    endpoint_plans: list[MirrorBatchEndpointPlan]
    total_candidate_jobs: int
    total_planned_jobs: int
    blocked_endpoints: int
    warnings: list[str]
    estimated_request_count: int
    requires_execute_confirmation: bool
    trade_cal_dependency_status: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["endpoint_plans"] = [item.to_dict() for item in self.endpoint_plans]
        return data

    def summary(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "scope": self.scope,
            "root": self.root,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "calendar_exchange": self.calendar_exchange,
            "max_jobs_per_api": self.max_jobs_per_api,
            "total_candidate_jobs": self.total_candidate_jobs,
            "total_planned_jobs": self.total_planned_jobs,
            "blocked_endpoints": self.blocked_endpoints,
            "estimated_request_count": self.estimated_request_count,
            "requires_execute_confirmation": self.requires_execute_confirmation,
            "trade_cal_dependency_status": self.trade_cal_dependency_status,
            "warnings": self.warnings,
        }


def ensure_mirror_scope(scope: str) -> None:
    if scope != "low-risk-a-share":
        raise ValueError("unknown mirror scope: %s; supported: low-risk-a-share" % scope)


def ensure_mirror_mode(mode: str) -> None:
    if mode not in MODE_MAX_JOBS:
        raise ValueError("unknown mirror mode: %s; supported: smoke, pilot" % mode)


@dataclass(frozen=True)
class MirrorPreflightResult:
    status: str
    ready_to_execute: bool
    mirror_root: str
    backup_target: str
    scope: str
    mode: str
    start_date: str | None
    end_date: str | None
    max_jobs_per_api: int
    token_available: bool
    mirror_root_status: str
    backup_target_status: str
    path_relationship_status: str
    disk_space: dict[str, Any]
    existing_catalog: dict[str, Any]
    existing_backup: dict[str, Any]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


def _resolve_path(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_under_tmp(path: Path) -> bool:
    resolved = _resolve_path(path)
    tmp = Path('/tmp').resolve(strict=False)
    return resolved == tmp or _is_relative_to(resolved, tmp)


def _nearest_existing_parent(path: Path) -> Path | None:
    current = _resolve_path(path)
    if current.exists():
        return current if current.is_dir() else current.parent
    for parent in [current.parent, *current.parents]:
        if parent.exists():
            return parent
    return None


def _disk_free(path: Path) -> tuple[int | None, str | None]:
    parent = _nearest_existing_parent(path.parent)
    if parent is None:
        return None, f'no existing parent for {path}'
    try:
        return int(shutil.disk_usage(parent).free), None
    except Exception as exc:  # pragma: no cover - platform specific
        return None, str(exc)


def _token_available_from_env() -> bool:
    if os.environ.get('TUSHARE_TOKEN'):
        return True
    env_path = Path('.env')
    if not env_path.exists():
        return False
    try:
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            if key.strip() == 'TUSHARE_TOKEN' and value.strip():
                return True
    except OSError:
        return False
    return False


def _token_value_from_env() -> str | None:
    if os.environ.get("TUSHARE_TOKEN"):
        return os.environ.get("TUSHARE_TOKEN")
    env_path = Path(".env")
    if not env_path.exists():
        return None
    try:
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "TUSHARE_TOKEN" and value.strip():
                return value.strip()
    except OSError:
        return None
    return None


def _contains_token_plaintext(paths: list[Path], token: str | None = None) -> bool:
    secret = token if token is not None else _token_value_from_env()
    if not secret or len(secret) < 8:
        return False
    needle = secret.encode()
    for root in paths:
        if not root.exists():
            continue
        files = [root] if root.is_file() else (path for path in root.rglob("*") if path.is_file())
        for path in files:
            try:
                with path.open("rb") as handle:
                    carry = b""
                    while True:
                        chunk = handle.read(1024 * 1024)
                        if not chunk:
                            break
                        data = carry + chunk
                        if needle in data:
                            return True
                        carry = data[-max(len(needle) - 1, 0):]
            except OSError:
                continue
    return False


def _artifact_size(paths: list[Path]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for label, root in paths:
        file_count = 0
        total_size = 0
        if root.exists():
            files = [root] if root.is_file() else (path for path in root.rglob("*") if path.is_file())
            for path in files:
                try:
                    stat = path.stat()
                except OSError:
                    continue
                file_count += 1
                total_size += stat.st_size
        summary[f"{label}_file_count"] = file_count
        summary[f"{label}_total_size_bytes"] = total_size
    return summary


def _catalog_counts_readonly(catalog_path: Path) -> dict[str, Any]:
    uri = f"file:{catalog_path.resolve().as_posix()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.row_factory = sqlite3.Row
        version = conn.execute("select value from catalog_meta where key='catalog_schema_version'").fetchone()
        latest_count = conn.execute("select count(*) from snapshots where status='current'").fetchone()[0]
        active_file_count = conn.execute("select count(*) from files where status='current'").fetchone()[0]
        return {
            'present': True,
            'schema_version': int(version[0]) if version else None,
            'endpoint_count': conn.execute('select count(*) from endpoints').fetchone()[0],
            'snapshot_count': conn.execute('select count(*) from snapshots').fetchone()[0],
            'latest_snapshots_count': latest_count,
            'active_file_count': active_file_count,
            'has_active_data': active_file_count > 0,
        }
    finally:
        conn.close()


class MirrorReviewer:
    def review(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str = "low-risk-a-share",
        mode: str = "pilot",
        start_date: str = "20250101",
        end_date: str = "20250131",
        calendar_exchange: str = "SSE",
    ) -> MirrorReviewResult:
        ensure_mirror_scope(scope)
        ensure_mirror_mode(mode)
        mirror_root = Path(root)
        backup_root = Path(backup)
        warnings: list[str] = []
        blocking_errors: list[str] = []
        catalog_status: dict[str, Any] = {"present": False}
        latest_snapshots: list[dict[str, Any]] = []
        endpoint_summary: list[dict[str, Any]] = []
        coverage_summary: list[dict[str, Any]] = []
        validation_status = "not_checked"
        validation_results: list[dict[str, Any]] = []
        backup_inspect_dict: dict[str, Any] | None = None
        backup_restore_dict: dict[str, Any] | None = None
        backup_checksum_status: str | None = None
        backup_possible_mutation = False

        root_status = "missing"
        catalog = CatalogStore(mirror_root, read_only=True)
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}")
        else:
            root_status = "existing_catalog"
            try:
                catalog_status = catalog.inspect_summary()
                latest_snapshots = catalog.list_snapshots(latest=True, limit=100)
                endpoint_summary = self._endpoint_summary(catalog)
                ok, validation_results = Validator(mirror_root, catalog).validate_latest_snapshots(record=False)
                validation_status = "succeeded" if ok else "failed"
                if not ok:
                    blocking_errors.append("validate --snapshot latest --no-record failed")
                coverage_summary = self._coverage_summary(mirror_root, catalog, start_date, end_date, calendar_exchange, warnings, blocking_errors)
            except Exception as exc:
                blocking_errors.append(f"catalog review failed: {exc}")

        backup_status = "missing"
        if not backup_root.exists():
            blocking_errors.append(f"backup not found: {backup_root}")
        else:
            backup_status = "present"
            try:
                inspect = BackupInspector().inspect(backup_root)
                restore = RestoreChecker().check(backup_root)
                backup_inspect_dict = inspect.to_dict()
                backup_restore_dict = restore.to_dict()
                backup_checksum_status = restore.catalog_checksum_status or inspect.catalog_checksum_status
                backup_possible_mutation = bool(restore.possible_mutation or inspect.possible_mutation)
                if inspect.status != "succeeded":
                    blocking_errors.append("backup-inspect failed")
                if restore.status != "succeeded":
                    blocking_errors.append("restore-check failed")
                if backup_possible_mutation:
                    blocking_errors.append("backup catalog may have been modified after backup creation")
            except Exception as exc:
                blocking_errors.append(f"backup review failed: {exc}")

        token_plaintext_found = _contains_token_plaintext([mirror_root, backup_root])
        if token_plaintext_found:
            blocking_errors.append("token plaintext found in mirror or backup artifact")

        artifact_size = _artifact_size([("root", mirror_root), ("backup", backup_root)])
        ready = not blocking_errors and not any("missing trading dates" in warning for warning in warnings)
        return MirrorReviewResult(
            root=str(mirror_root),
            backup=str(backup_root),
            scope=scope,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            calendar_exchange=calendar_exchange,
            root_status=root_status,
            backup_status=backup_status,
            catalog_status=catalog_status,
            latest_snapshots=latest_snapshots,
            endpoint_summary=endpoint_summary,
            coverage_summary=coverage_summary,
            validation_status=validation_status,
            validation_results=validation_results,
            backup_inspect=backup_inspect_dict,
            backup_restore_check=backup_restore_dict,
            backup_catalog_checksum_status=backup_checksum_status,
            backup_possible_mutation=backup_possible_mutation,
            artifact_size=artifact_size,
            token_plaintext_found=token_plaintext_found,
            ready_for_next_batch=ready,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    def _endpoint_summary(self, catalog: CatalogStore) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for endpoint in LOW_RISK_A_SHARE_ENDPOINTS:
            snapshot = catalog.latest_snapshot(endpoint)
            if not snapshot:
                rows.append({
                    "endpoint": endpoint,
                    "status": "missing_snapshot",
                    "snapshot_id": None,
                    "record_count": 0,
                    "raw_event_count": 0,
                    "raw_files": 0,
                    "lake_files": 0,
                })
                continue
            files = catalog.files_for_snapshot(str(snapshot["snapshot_id"]))
            rows.append({
                "endpoint": endpoint,
                "status": "current",
                "snapshot_id": snapshot.get("snapshot_id"),
                "record_count": sum(int(row.get("record_count") or 0) for row in files if row.get("content_type") == "lake"),
                "raw_event_count": sum(int(row.get("raw_event_count") or 0) for row in files if row.get("content_type") == "raw"),
                "raw_files": sum(1 for row in files if row.get("content_type") == "raw"),
                "lake_files": sum(1 for row in files if row.get("content_type") == "lake"),
            })
        return rows

    def _coverage_summary(
        self,
        root: Path,
        catalog: CatalogStore,
        start_date: str,
        end_date: str,
        calendar_exchange: str,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for api in DAILY_LIKE_MIRROR_APIS:
            try:
                report = CoverageReporter(root, catalog).report(
                    api,
                    start_date=start_date,
                    end_date=end_date,
                    trading_days_only=True,
                    calendar_exchange=calendar_exchange,
                )
                data = report.to_dict()
                data.pop("items", None)
                data["active_exists_dates"] = [item.date for item in report.items if item.existing_status == "active_exists"]
                data["missing_trading_dates"] = [item.date for item in report.items if item.existing_status == "missing"]
                rows.append(data)
                if report.missing_dates:
                    warnings.append(f"{api} has {report.missing_dates} missing trading dates in review range")
            except Exception as exc:
                rows.append({"api_name": api, "status": "failed", "error": str(exc)})
                blocking_errors.append(f"{api} coverage failed: {exc}")
        return rows


class MirrorReadinessReporter:
    CONTROLLED_FULL_BACKFILL_WARNINGS = [
        "only 2025-01 pilot coverage has been proven; this is not a full mirror",
        "event/company endpoints are not stock-looped",
        "weekly/monthly do not use trading-days-only",
        "financial/PIT/minute/tick/object/postgres datasets are not covered",
        "remote disaster recovery is not implemented",
        "compaction is not implemented",
    ]

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str = "low-risk-a-share",
    ) -> MirrorReadinessResult:
        ensure_mirror_scope(scope)
        review = MirrorReviewer().review(
            root=root,
            backup=backup,
            scope=scope,
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            calendar_exchange="SSE",
        )
        checks = self._checks(review, Path(root), Path(backup))
        blocking_errors = list(review.blocking_errors)
        for name, check in checks.items():
            if check.get("required") and not check.get("passed"):
                message = check.get("message") or f"{name} failed"
                if message not in blocking_errors:
                    blocking_errors.append(message)
        warnings = list(dict.fromkeys([*review.warnings, *self.CONTROLLED_FULL_BACKFILL_WARNINGS]))
        readiness_status = "blocked" if blocking_errors else ("warning" if warnings else "ready")
        return MirrorReadinessResult(
            root=str(root),
            backup=str(backup),
            scope=scope,
            readiness_status=readiness_status,
            ready_for_controlled_full_backfill=not blocking_errors,
            checks=checks,
            warnings=warnings,
            blocking_errors=blocking_errors,
            review=review.to_dict(),
        )

    def _checks(self, review: MirrorReviewResult, root: Path, backup: Path) -> dict[str, Any]:
        endpoint_status = {row["endpoint"]: row for row in review.endpoint_summary}
        coverage_complete = bool(review.coverage_summary) and all(
            int(row.get("total_dates") or 0) > 0
            and int(row.get("missing_dates") or 0) == 0
            and int(row.get("failed_dates") or 0) == 0
            and int(row.get("quarantined_dates") or 0) == 0
            for row in review.coverage_summary
        )
        relationship = MirrorPreflightChecker(token_available=True)._path_relationship(_resolve_path(root), _resolve_path(backup))
        schema_version = review.catalog_status.get("schema_version")
        checks = {
            "catalog_opens": {
                "required": True,
                "passed": review.root_status == "existing_catalog" and bool(review.catalog_status.get("schema_version")),
                "message": "catalog cannot be opened",
            },
            "supported_schema": {
                "required": True,
                "passed": isinstance(schema_version, int) and schema_version >= 2,
                "message": "catalog schema version is unsupported",
            },
            "latest_snapshots_exist": {
                "required": True,
                "passed": bool(review.latest_snapshots),
                "message": "latest snapshots are missing",
            },
            "trade_cal_latest_exists": {
                "required": True,
                "passed": (endpoint_status.get("trade_cal") or {}).get("status") == "current",
                "message": "trade_cal latest snapshot is missing",
            },
            "backup_exists": {
                "required": True,
                "passed": review.backup_status == "present",
                "message": "backup artifact is missing",
            },
            "restore_check_succeeds": {
                "required": True,
                "passed": (review.backup_restore_check or {}).get("status") == "succeeded",
                "message": "restore-check failed",
            },
            "backup_possible_mutation_false": {
                "required": True,
                "passed": not review.backup_possible_mutation,
                "message": "backup possible_mutation is true",
            },
            "validate_no_record_succeeds": {
                "required": True,
                "passed": review.validation_status == "succeeded",
                "message": "validate --snapshot latest --no-record failed",
            },
            "pilot_coverage_complete": {
                "required": True,
                "passed": coverage_complete,
                "message": "pilot coverage is incomplete for one or more daily-like endpoints",
            },
            "token_plaintext_not_found": {
                "required": True,
                "passed": not review.token_plaintext_found,
                "message": "token plaintext found in mirror or backup artifact",
            },
            "backup_not_nested_inside_mirror": {
                "required": True,
                "passed": relationship == "ok",
                "message": f"unsafe backup path relationship: {relationship}",
            },
            "cli_guardrails_exist": {
                "required": True,
                "passed": MODE_MAX_JOBS.get("smoke") == 3 and MODE_MAX_JOBS.get("pilot") == 20,
                "message": "mirror CLI max-jobs guardrails are missing",
            },
        }
        return checks


class MirrorBatchPlanner:
    REFERENCE_APIS = ["stock_basic", "hs_const"]
    EVENT_APIS = ["namechange", "stk_managers", "stk_rewards"]

    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        *,
        scope: str,
        start_date: str,
        end_date: str,
        calendar_exchange: str = "SSE",
        max_jobs_per_api: int = 20,
    ) -> MirrorBatchPlan:
        ensure_mirror_scope(scope)
        if max_jobs_per_api <= 0:
            raise ValueError("--max-jobs-per-api must be positive")
        if max_jobs_per_api > MODE_MAX_JOBS["pilot"]:
            raise ValueError("mirror-batch-plan max-jobs-per-api cannot exceed 20")
        start = DatePlanner(self.root, self.catalog)._normalize_date(start_date)
        end = DatePlanner(self.root, self.catalog)._normalize_date(end_date)
        if start > end:
            raise ValueError("start_date must be <= end_date")
        warnings = ["mirror-batch-plan is read-only; execute requires separate user confirmation"]
        calendar = self._calendar_range(start, end, calendar_exchange)
        endpoint_plans: list[MirrorBatchEndpointPlan] = []
        endpoint_plans.append(self._trade_cal_plan(start, end, calendar_exchange, calendar))
        for endpoint in self.REFERENCE_APIS:
            endpoint_plans.append(self._reference_plan(endpoint))
        for endpoint in DAILY_LIKE_MIRROR_APIS:
            endpoint_plans.append(self._daily_like_plan(endpoint, start, end, calendar_exchange, max_jobs_per_api, calendar))
        endpoint_plans.append(self._explicit_date_plan("weekly", self._weekly_dates(start, end), max_jobs_per_api, "weekly uses bounded explicit date planning only"))
        endpoint_plans.append(self._explicit_date_plan("monthly", self._monthly_dates(start, end), max_jobs_per_api, "monthly uses bounded explicit date planning only"))
        for endpoint in self.EVENT_APIS:
            endpoint_plans.append(self._excluded_plan(endpoint))
        total_candidate = sum(item.total_candidate_jobs for item in endpoint_plans)
        total_planned = sum(item.planned_jobs for item in endpoint_plans)
        blocked = sum(1 for item in endpoint_plans if item.plan_status.startswith("blocked"))
        estimated = sum(item.missing_jobs for item in endpoint_plans if item.plan_status not in {"excluded_no_stock_loop"})
        return MirrorBatchPlan(
            batch_id=f"batch_{start}_{end}",
            scope=scope,
            root=str(self.root),
            start_date=start,
            end_date=end,
            calendar_exchange=calendar_exchange.upper(),
            max_jobs_per_api=max_jobs_per_api,
            endpoint_plans=endpoint_plans,
            total_candidate_jobs=total_candidate,
            total_planned_jobs=total_planned,
            blocked_endpoints=blocked,
            warnings=warnings,
            estimated_request_count=estimated,
            requires_execute_confirmation=True,
            trade_cal_dependency_status=calendar["status"],
        )

    def _calendar_range(self, start: str, end: str, exchange: str) -> dict[str, Any]:
        natural = self._natural_dates(start, end)
        snapshot = self.catalog.latest_snapshot("trade_cal")
        if not snapshot:
            return {
                "status": "missing_snapshot",
                "natural_dates": natural,
                "open_dates": [],
                "missing_calendar_dates": natural,
                "filtered_non_trading_dates": [],
            }
        try:
            table = LakeReader(self.root, self.catalog).scan_api("trade_cal", columns=["exchange", "cal_date", "is_open"])
        except Exception as exc:
            return {
                "status": "unreadable",
                "error": str(exc),
                "natural_dates": natural,
                "open_dates": [],
                "missing_calendar_dates": natural,
                "filtered_non_trading_dates": [],
            }
        exchange_upper = exchange.upper()
        present: set[str] = set()
        open_dates: set[str] = set()
        if table.num_rows:
            exchanges = table["exchange"].to_pylist()
            cal_dates = table["cal_date"].to_pylist()
            flags = table["is_open"].to_pylist()
            planner = DatePlanner(self.root, self.catalog)
            for source_exchange, cal_date, flag in zip(exchanges, cal_dates, flags):
                if str(source_exchange).upper() != exchange_upper:
                    continue
                try:
                    date = planner._normalize_date(str(cal_date))
                except ValueError:
                    continue
                if start <= date <= end:
                    present.add(date)
                    if planner._is_open_flag(flag):
                        open_dates.add(date)
        missing = sorted(set(natural) - present)
        open_sorted = sorted(open_dates)
        return {
            "status": "covered" if not missing else "missing_range",
            "natural_dates": natural,
            "open_dates": open_sorted,
            "missing_calendar_dates": missing,
            "filtered_non_trading_dates": sorted(set(natural) - set(open_sorted) - set(missing)),
        }

    def _trade_cal_plan(self, start: str, end: str, exchange: str, calendar: dict[str, Any]) -> MirrorBatchEndpointPlan:
        missing = calendar["status"] != "covered"
        return MirrorBatchEndpointPlan(
            endpoint="trade_cal",
            category="calendar_dependency",
            requires_trade_cal=False,
            plan_status="planned" if missing else "current",
            planned_action="fetch_calendar_range" if missing else "skip_existing_range",
            total_candidate_jobs=1,
            planned_jobs=1 if missing else 0,
            missing_jobs=1 if missing else 0,
            skipped_jobs=0 if missing else 1,
            blocked_jobs=0,
            max_jobs=1,
            truncated=False,
            dates=[],
            refresh_strategy=f"ensure SSE trade_cal coverage for {start}-{end}",
            blocked_reason=None,
            warnings=[f"missing calendar dates: {calendar['missing_calendar_dates']}"] if missing else [],
        )

    def _reference_plan(self, endpoint: str) -> MirrorBatchEndpointPlan:
        snapshot = self.catalog.latest_snapshot(endpoint)
        missing = snapshot is None
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="reference",
            requires_trade_cal=False,
            plan_status="planned" if missing else "current",
            planned_action="fetch_reference_once" if missing else "skip_existing_reference",
            total_candidate_jobs=1,
            planned_jobs=1 if missing else 0,
            missing_jobs=1 if missing else 0,
            skipped_jobs=0 if missing else 1,
            blocked_jobs=0,
            max_jobs=1,
            truncated=False,
            dates=[],
            refresh_strategy="fetch once if missing; do not refetch blindly",
        )

    def _daily_like_plan(self, endpoint: str, start: str, end: str, exchange: str, max_jobs: int, calendar: dict[str, Any]) -> MirrorBatchEndpointPlan:
        if calendar["status"] != "covered":
            return MirrorBatchEndpointPlan(
                endpoint=endpoint,
                category="daily_like",
                requires_trade_cal=True,
                plan_status="blocked_until_trade_cal",
                planned_action="blocked",
                total_candidate_jobs=0,
                planned_jobs=0,
                missing_jobs=0,
                skipped_jobs=0,
                blocked_jobs=1,
                max_jobs=max_jobs,
                truncated=False,
                dates=[],
                blocked_reason="missing_trade_cal_range",
                warnings=["daily-like endpoints do not fall back to natural days"],
            )
        metadata = {
            "calendar_source": "local trade_cal latest snapshot",
            "exchange": exchange.upper(),
            "requested_start_date": start,
            "requested_end_date": end,
            "natural_days": len(calendar["natural_dates"]),
            "trading_days": len(calendar["open_dates"]),
            "filtered_non_trading_days": len(calendar["filtered_non_trading_dates"]),
            "filtered_non_trading_dates": calendar["filtered_non_trading_dates"],
        }
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, calendar["open_dates"], max_jobs=max_jobs, calendar_metadata=metadata)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        blocked = sum(1 for job in plan.planned_jobs if job.planned_action == "blocked_quarantined")
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="daily_like",
            requires_trade_cal=True,
            plan_status="planned" if missing else "current",
            planned_action="calendar_backfill_missing",
            total_candidate_jobs=plan.total_candidate_jobs,
            planned_jobs=len(plan.planned_jobs),
            missing_jobs=missing,
            skipped_jobs=skipped,
            blocked_jobs=blocked,
            max_jobs=max_jobs,
            truncated=plan.truncated_by_max_jobs,
            dates=[job.date for job in plan.planned_jobs],
            warnings=plan.warnings,
        )

    def _explicit_date_plan(self, endpoint: str, dates: list[str], max_jobs: int, notes: str) -> MirrorBatchEndpointPlan:
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        blocked = sum(1 for job in plan.planned_jobs if job.planned_action == "blocked_quarantined")
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="date_based",
            requires_trade_cal=False,
            plan_status="planned" if missing else "current",
            planned_action="bounded_explicit_date_plan",
            total_candidate_jobs=plan.total_candidate_jobs,
            planned_jobs=len(plan.planned_jobs),
            missing_jobs=missing,
            skipped_jobs=skipped,
            blocked_jobs=blocked,
            max_jobs=max_jobs,
            truncated=plan.truncated_by_max_jobs,
            dates=[job.date for job in plan.planned_jobs],
            refresh_strategy=notes,
            warnings=plan.warnings,
        )

    def _excluded_plan(self, endpoint: str) -> MirrorBatchEndpointPlan:
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="event_company",
            requires_trade_cal=False,
            plan_status="excluded_no_stock_loop",
            planned_action="excluded",
            total_candidate_jobs=0,
            planned_jobs=0,
            missing_jobs=0,
            skipped_jobs=0,
            blocked_jobs=0,
            max_jobs=0,
            truncated=False,
            dates=[],
            refresh_strategy="excluded from controlled batch planning; no stock loop",
        )

    def _natural_dates(self, start: str, end: str) -> list[str]:
        current = datetime.strptime(start, "%Y%m%d")
        stop = datetime.strptime(end, "%Y%m%d")
        out = []
        while current <= stop:
            out.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return out

    def _weekly_dates(self, start: str, end: str) -> list[str]:
        dates = []
        for date in self._natural_dates(start, end):
            if datetime.strptime(date, "%Y%m%d").weekday() == 4:
                dates.append(date)
        return dates

    def _monthly_dates(self, start: str, end: str) -> list[str]:
        dates = self._natural_dates(start, end)
        if not dates:
            return []
        by_month: dict[str, str] = {}
        for date in dates:
            by_month[date[:6]] = date
        return sorted(by_month.values())


class MirrorPreflightChecker:
    PILOT_SIZE_ESTIMATE_BYTES = 100 * 1024 * 1024

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def check(
        self,
        *,
        mirror_root: Path | str,
        backup_target: Path | str,
        scope: str,
        mode: str,
        start_date: str | None = None,
        end_date: str | None = None,
        max_jobs_per_api: int | None = None,
    ) -> MirrorPreflightResult:
        mirror = _resolve_path(Path(mirror_root))
        backup = _resolve_path(Path(backup_target))
        warnings: list[str] = []
        blocking_errors: list[str] = []

        self._check_scope_mode(scope, mode, start_date, end_date, max_jobs_per_api, blocking_errors)
        token_available = self._token_available_override if self._token_available_override is not None else _token_available_from_env()
        if not token_available:
            blocking_errors.append('TUSHARE_TOKEN is not available; mirror-run --execute would fail')

        mirror_status, catalog_info = self._inspect_mirror_root(mirror, warnings, blocking_errors)
        backup_status, backup_info = self._inspect_backup_target(backup, warnings, blocking_errors)
        relationship = self._path_relationship(mirror, backup)
        if relationship != 'ok':
            blocking_errors.append(f'unsafe path relationship: {relationship}')
        if _is_under_tmp(mirror):
            warnings.append('mirror_root is under /tmp; use a durable path for long-lived mirror data')
        if _is_under_tmp(backup):
            warnings.append('backup_target is under /tmp; use a durable path for long-lived backup artifacts')

        disk_space = self._disk_space_summary(mirror, backup, warnings)
        status = 'blocked' if blocking_errors else ('warning' if warnings else 'passed')
        return MirrorPreflightResult(
            status=status,
            ready_to_execute=not blocking_errors,
            mirror_root=str(mirror),
            backup_target=str(backup),
            scope=scope,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=int(max_jobs_per_api or MODE_MAX_JOBS.get(mode, 0)),
            token_available=bool(token_available),
            mirror_root_status=mirror_status,
            backup_target_status=backup_status,
            path_relationship_status=relationship,
            disk_space=disk_space,
            existing_catalog=catalog_info,
            existing_backup=backup_info,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    def _check_scope_mode(self, scope: str, mode: str, start_date: str | None, end_date: str | None, max_jobs_per_api: int | None, blocking_errors: list[str]) -> None:
        try:
            ensure_mirror_scope(scope)
        except ValueError as exc:
            blocking_errors.append(str(exc))
        try:
            ensure_mirror_mode(mode)
        except ValueError as exc:
            blocking_errors.append(str(exc))
            return
        if mode == 'pilot' and (not start_date or not end_date):
            blocking_errors.append('pilot mode requires --start-date and --end-date')
        max_jobs = max_jobs_per_api if max_jobs_per_api is not None else MODE_MAX_JOBS[mode]
        if max_jobs <= 0:
            blocking_errors.append('--max-jobs-per-api must be positive')
        elif max_jobs > MODE_MAX_JOBS[mode]:
            blocking_errors.append(f'{mode} mode max-jobs-per-api cannot exceed {MODE_MAX_JOBS[mode]}')

    def _inspect_mirror_root(self, mirror: Path, warnings: list[str], blocking_errors: list[str]) -> tuple[str, dict[str, Any]]:
        default_catalog = {'present': False, 'schema_version': None, 'endpoint_count': 0, 'snapshot_count': 0, 'latest_snapshots_count': 0, 'active_file_count': 0, 'has_active_data': False}
        if not mirror.exists():
            self._parent_warning(mirror, 'mirror_root', warnings)
            return 'missing', default_catalog
        if not mirror.is_dir():
            blocking_errors.append('mirror_root exists but is not a directory')
            return 'non_empty_unknown', default_catalog
        catalog_path = mirror / '_catalog' / 'catalog.sqlite'
        if catalog_path.exists():
            try:
                return 'existing_catalog', _catalog_counts_readonly(catalog_path)
            except Exception as exc:
                blocking_errors.append(f'existing mirror catalog is unreadable: {exc}')
                return 'existing_catalog', default_catalog | {'present': True}
        if not any(mirror.iterdir()):
            return 'empty', default_catalog
        blocking_errors.append('mirror_root is non-empty but has no _catalog/catalog.sqlite')
        return 'non_empty_unknown', default_catalog

    def _inspect_backup_target(self, backup: Path, warnings: list[str], blocking_errors: list[str]) -> tuple[str, dict[str, Any]]:
        default_backup = {'present': False, 'backup_id': None, 'manifest_version': None, 'snapshot_scope': None, 'file_count': None, 'catalog_checksum_status': None, 'possible_mutation': False}
        if not backup.exists():
            self._parent_warning(backup, 'backup_target', warnings)
            return 'missing', default_backup
        if not backup.is_dir():
            blocking_errors.append('backup_target exists but is not a directory')
            return 'non_empty_unknown', default_backup
        manifest = backup / 'manifest.json'
        if manifest.exists():
            inspect = BackupInspector().inspect(backup)
            info = {
                'present': True,
                'backup_id': inspect.backup_id,
                'manifest_version': inspect.manifest_version,
                'snapshot_scope': inspect.snapshot_scope,
                'file_count': inspect.file_count,
                'catalog_checksum_status': inspect.catalog_checksum_status,
                'possible_mutation': inspect.possible_mutation,
                'manifest_validation_status': inspect.manifest_validation_status,
            }
            blocking_errors.append('backup_target already contains manifest.json; choose a new target or clear it explicitly')
            return 'existing_manifest', info
        if not any(backup.iterdir()):
            return 'empty', default_backup
        blocking_errors.append('backup_target is non-empty and is not a recognized backup artifact')
        return 'non_empty_unknown', default_backup

    def _path_relationship(self, mirror: Path, backup: Path) -> str:
        if mirror == backup:
            return 'same_path'
        if _is_relative_to(backup, mirror):
            return 'backup_inside_mirror'
        if _is_relative_to(mirror, backup):
            return 'mirror_inside_backup'
        return 'ok'

    def _disk_space_summary(self, mirror: Path, backup: Path, warnings: list[str]) -> dict[str, Any]:
        mirror_free, mirror_warning = _disk_free(mirror)
        backup_free, backup_warning = _disk_free(backup)
        disk_warning = None
        if mirror_warning or backup_warning:
            disk_warning = '; '.join(filter(None, [mirror_warning, backup_warning]))
            warnings.append(f'disk space could not be fully checked: {disk_warning}')
        enough = None
        if mirror_free is not None and backup_free is not None:
            enough = mirror_free >= self.PILOT_SIZE_ESTIMATE_BYTES and backup_free >= self.PILOT_SIZE_ESTIMATE_BYTES
            if not enough:
                warnings.append('available disk space is below the rough one-month pilot estimate')
        return {
            'mirror_parent_free_bytes': mirror_free,
            'backup_parent_free_bytes': backup_free,
            'pilot_estimate_bytes': self.PILOT_SIZE_ESTIMATE_BYTES,
            'expected_size_class': 'tens_of_mb',
            'enough_for_pilot': enough,
            'warning': disk_warning,
        }

    def _parent_warning(self, path: Path, label: str, warnings: list[str]) -> None:
        if path.parent.exists():
            return
        warnings.append(f'{label} parent directory does not exist and must be created before execution: {path.parent}')


class MirrorPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        *,
        scope: str = "low-risk-a-share",
        mode: str = "smoke",
        start_date: str | None = None,
        end_date: str | None = None,
        max_jobs_per_api: int | None = None,
    ) -> MirrorPlan:
        ensure_mirror_scope(scope)
        ensure_mirror_mode(mode)
        max_jobs = max_jobs_per_api or MODE_MAX_JOBS[mode]
        if max_jobs <= 0:
            raise ValueError("--max-jobs-per-api must be positive")
        if max_jobs > MODE_MAX_JOBS[mode]:
            raise ValueError(f"{mode} mode max-jobs-per-api cannot exceed {MODE_MAX_JOBS[mode]}")
        items = self._smoke_items(max_jobs) if mode == "smoke" else self._pilot_items(start_date, end_date, max_jobs)
        planned_endpoint_count = sum(1 for item in items if item.plan_status in {"planned", "skip_existing"})
        blocked_endpoint_count = sum(1 for item in items if item.plan_status.startswith("blocked"))
        return MirrorPlan(
            scope=scope,
            mode=mode,
            root=str(self.root),
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs,
            endpoint_count=len(items),
            planned_endpoint_count=planned_endpoint_count,
            blocked_endpoint_count=blocked_endpoint_count,
            total_planned_jobs=sum(item.planned_jobs for item in items if item.plan_status not in {"blocked", "blocked_until_trade_cal", "excluded_from_pilot_execution"}),
            requires_real_requests=True,
            dry_run=True,
            items=items,
            warnings=[],
        )

    def _smoke_items(self, max_jobs: int) -> list[MirrorPlanItem]:
        items: list[MirrorPlanItem] = []
        for endpoint in ["stock_basic", "trade_cal", "hs_const"]:
            items.append(self._fetch_item(endpoint, "snapshot_reference", SMOKE_REFERENCE_FETCHES[endpoint], max_jobs=1))
        trade_cal_ready = bool(self.catalog.latest_snapshot("trade_cal"))
        for endpoint in SMOKE_CALENDAR_BACKFILL_APIS:
            if not trade_cal_ready:
                items.append(self._blocked_item(endpoint, "daily_like", True, max_jobs, "missing_trade_cal_snapshot"))
            else:
                items.append(self._calendar_backfill_item(endpoint, "20250101", "20250110", max_jobs))
        for endpoint, dates in SMOKE_EXPLICIT_DATE_APIS.items():
            items.append(self._date_backfill_item(endpoint, dates, min(max_jobs, len(dates)), requires_trade_cal=False))
        for endpoint in ["namechange", "stk_managers", "stk_rewards"]:
            items.append(self._fetch_item(endpoint, "event_snapshot", SMOKE_REFERENCE_FETCHES[endpoint], max_jobs=1))
        return items

    def _pilot_items(self, start_date: str | None, end_date: str | None, max_jobs: int) -> list[MirrorPlanItem]:
        if not start_date or not end_date:
            raise ValueError("pilot mode requires --start-date and --end-date")
        items = [
            self._fetch_item("stock_basic", "snapshot_reference", {"list_status": "L"}, max_jobs=1),
            self._fetch_item("trade_cal", "calendar_dependency", {"exchange": "SSE", "start_date": start_date, "end_date": end_date}, max_jobs=1, planned_action="fetch_calendar", required_by=["daily", "adj_factor", "daily_basic", "suspend_d"]),
            self._fetch_item("hs_const", "snapshot_reference", {"hs_type": "SH", "is_new": "1"}, max_jobs=1),
        ]
        trade_cal_ready = bool(self.catalog.latest_snapshot("trade_cal"))
        for endpoint in ["daily", "adj_factor", "daily_basic", "suspend_d"]:
            if not trade_cal_ready:
                items.append(self._blocked_item(endpoint, "daily_like", True, max_jobs, "missing_trade_cal_snapshot", plan_status="blocked_until_trade_cal", notes="calendar-aware backfill waits for local trade_cal latest snapshot"))
            else:
                items.append(self._calendar_backfill_item(endpoint, start_date, end_date, max_jobs))
        items.append(self._date_backfill_item("weekly", self._pilot_weekly_dates(start_date, end_date), max_jobs, requires_trade_cal=False, notes="weekly does not use trading-days-only in Phase 3.1"))
        items.append(self._date_backfill_item("monthly", self._pilot_monthly_dates(start_date, end_date), max_jobs, requires_trade_cal=False, notes="monthly does not use trading-days-only in Phase 3.1"))
        for endpoint in ["namechange", "stk_managers", "stk_rewards"]:
            items.append(self._excluded_item(endpoint, "event_snapshot", max_jobs, "excluded_from_pilot_execution", "pilot does not run stock loops; smoke mode only fetches 000001.SZ once"))
        return items

    def _fetch_item(self, endpoint: str, category: str, params: dict[str, Any], max_jobs: int, *, planned_action: str = "fetch", required_by: list[str] | None = None) -> MirrorPlanItem:
        plan = JobPlanner(self.root, self.catalog).plan_single_fetch(endpoint, params)
        status = "skip_existing" if plan.existing_active_data else "planned"
        return MirrorPlanItem(
            endpoint=endpoint,
            category=category,
            requires_trade_cal=False,
            plan_status=status,
            planned_jobs=0 if plan.existing_active_data else 1,
            max_jobs=max_jobs,
            existing_coverage="active_exists" if plan.existing_active_data else "missing",
            missing_jobs=0 if plan.existing_active_data else 1,
            blocked_reason=None,
            will_execute=not plan.existing_active_data,
            params=plan.params,
            permission_status=plan.permission_status,
            planned_action="skip_existing" if plan.existing_active_data else planned_action,
            required_by=required_by,
        )

    def _calendar_backfill_item(self, endpoint: str, start_date: str, end_date: str, max_jobs: int) -> MirrorPlanItem:
        dates, calendar = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(
            start_date=start_date,
            end_date=end_date,
            trading_days_only=True,
            calendar_exchange="SSE",
        )
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs, calendar_metadata=calendar)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        status = "skip_existing" if skipped == len(plan.planned_jobs) and plan.planned_jobs else "planned"
        return MirrorPlanItem(
            endpoint=endpoint,
            category="daily_like",
            requires_trade_cal=True,
            plan_status=status,
            planned_jobs=len(plan.planned_jobs),
            max_jobs=max_jobs,
            existing_coverage=f"covered={skipped},missing_or_retry={missing}",
            missing_jobs=missing,
            blocked_reason=None,
            will_execute=missing > 0,
            dates=[job.date for job in plan.planned_jobs],
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="calendar_backfill",
        )

    def _date_backfill_item(self, endpoint: str, dates: list[str], max_jobs: int, *, requires_trade_cal: bool, notes: str | None = None) -> MirrorPlanItem:
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        status = "skip_existing" if skipped == len(plan.planned_jobs) and plan.planned_jobs else "planned"
        return MirrorPlanItem(
            endpoint=endpoint,
            category="date_based",
            requires_trade_cal=requires_trade_cal,
            plan_status=status,
            planned_jobs=len(plan.planned_jobs),
            max_jobs=max_jobs,
            existing_coverage=f"covered={skipped},missing_or_retry={missing}",
            missing_jobs=missing,
            blocked_reason=None,
            will_execute=missing > 0,
            dates=[job.date for job in plan.planned_jobs],
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="date_backfill",
            notes=notes,
        )

    def _blocked_item(self, endpoint: str, category: str, requires_trade_cal: bool, max_jobs: int, reason: str, *, plan_status: str = "blocked", notes: str | None = None) -> MirrorPlanItem:
        return MirrorPlanItem(
            endpoint=endpoint,
            category=category,
            requires_trade_cal=requires_trade_cal,
            plan_status=plan_status,
            planned_jobs=0,
            max_jobs=max_jobs,
            existing_coverage=None,
            missing_jobs=0,
            blocked_reason=reason,
            will_execute=False,
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="blocked",
            notes=notes,
        )

    def _excluded_item(self, endpoint: str, category: str, max_jobs: int, reason: str, notes: str) -> MirrorPlanItem:
        return MirrorPlanItem(
            endpoint=endpoint,
            category=category,
            requires_trade_cal=False,
            plan_status="excluded_from_pilot_execution",
            planned_jobs=0,
            max_jobs=max_jobs,
            existing_coverage=None,
            missing_jobs=0,
            blocked_reason=reason,
            will_execute=False,
            params={"ts_code": "000001.SZ"},
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="excluded",
            notes=notes,
        )

    def _pilot_weekly_dates(self, start_date: str, end_date: str) -> list[str]:
        return [date for date in PILOT_JAN_2025_WEEKLY_DATES if start_date <= date <= end_date]

    def _pilot_monthly_dates(self, start_date: str, end_date: str) -> list[str]:
        return [date for date in PILOT_JAN_2025_MONTHLY_DATES if start_date <= date <= end_date]


class MirrorOrchestrator:
    def __init__(self, root: Path | str, catalog: CatalogStore, client, *, sleep=time.sleep):
        self.root = Path(root)
        self.catalog = catalog
        self.client = client
        self.sleep = sleep

    def run(
        self,
        *,
        scope: str,
        mode: str,
        max_jobs_per_api: int,
        start_date: str | None = None,
        end_date: str | None = None,
        backup_target: str | None = None,
    ) -> MirrorRunResult:
        ensure_mirror_scope(scope)
        ensure_mirror_mode(mode)
        if max_jobs_per_api <= 0:
            raise ValueError("--max-jobs-per-api must be positive")
        if max_jobs_per_api > MODE_MAX_JOBS[mode]:
            raise ValueError(f"{mode} mode max-jobs-per-api cannot exceed {MODE_MAX_JOBS[mode]}")
        run_id = self.catalog.create_run("mirror")
        items: list[dict[str, Any]] = []
        probe_statuses = self._probe_all()
        trade_cal_status = self._execute_fetch("trade_cal", self._trade_cal_params(mode, start_date, end_date), run_id, probe_statuses)
        items.append(trade_cal_status)
        for endpoint in ["stock_basic", "hs_const"]:
            items.append(self._execute_fetch(endpoint, SMOKE_REFERENCE_FETCHES[endpoint], run_id, probe_statuses))
        if self.catalog.latest_snapshot("trade_cal") and trade_cal_status["status"] not in {"failed", "blocked"}:
            for endpoint in SMOKE_CALENDAR_BACKFILL_APIS if mode == "smoke" else ["daily", "adj_factor", "daily_basic", "suspend_d"]:
                items.append(self._execute_calendar_backfill(endpoint, mode, max_jobs_per_api, start_date, end_date, probe_statuses))
        else:
            for endpoint in SMOKE_CALENDAR_BACKFILL_APIS if mode == "smoke" else ["daily", "adj_factor", "daily_basic", "suspend_d"]:
                items.append(self._blocked(endpoint, "daily_like", "missing_or_failed_trade_cal"))
        if mode == "smoke":
            date_groups = SMOKE_EXPLICIT_DATE_APIS.items()
        else:
            if not start_date or not end_date:
                raise ValueError("pilot mode requires --start-date and --end-date")
            date_groups = [("weekly", self._pilot_weekly_dates(start_date, end_date)), ("monthly", self._pilot_monthly_dates(start_date, end_date))]
        for endpoint, dates in date_groups:
            items.append(self._execute_date_backfill(endpoint, list(dates), max_jobs_per_api, probe_statuses))
        if mode == "smoke":
            for endpoint in ["namechange", "stk_managers", "stk_rewards"]:
                items.append(self._execute_fetch(endpoint, SMOKE_REFERENCE_FETCHES[endpoint], run_id, probe_statuses))
        validation_ok, validation_results = Validator(self.root, self.catalog).validate_latest_snapshots(record=True)
        validation = {
            "status": "succeeded" if validation_ok else "failed",
            "results": validation_results,
        }
        backup = None
        restore_check = None
        if backup_target:
            plan = BackupPlanner(self.root, self.catalog).plan(backup_target)
            backup_result = BackupExecutor(self.root, self.catalog).backup(plan)
            backup = backup_result.to_dict()
            restore = RestoreChecker().check(Path(backup_target))
            restore_check = restore.to_dict()
        summary = self._summary(scope, mode, max_jobs_per_api, backup_target, items, validation, backup, restore_check)
        status = "failed" if summary["failed_endpoints"] or summary.get("critical_dependency_failed") else "succeeded"
        self.catalog.finish_run(run_id, status, None if status == "succeeded" else "mirror had failed endpoints", None, summary)
        return MirrorRunResult(run_id, status, summary, validation, backup, restore_check)

    def _trade_cal_params(self, mode: str, start_date: str | None, end_date: str | None) -> dict[str, Any]:
        if mode == "pilot":
            if not start_date or not end_date:
                raise ValueError("pilot mode requires --start-date and --end-date")
            return {"exchange": "SSE", "start_date": start_date, "end_date": end_date}
        return SMOKE_REFERENCE_FETCHES["trade_cal"]

    def _pilot_weekly_dates(self, start_date: str, end_date: str) -> list[str]:
        return [date for date in PILOT_JAN_2025_WEEKLY_DATES if start_date <= date <= end_date]

    def _pilot_monthly_dates(self, start_date: str, end_date: str) -> list[str]:
        return [date for date in PILOT_JAN_2025_MONTHLY_DATES if start_date <= date <= end_date]

    def _probe_all(self) -> dict[str, str]:
        statuses: dict[str, str] = {}
        thash = token_hash(getattr(self.client, "token", "mirror-client"))
        planner = JobPlanner(self.root, self.catalog)
        for endpoint in LOW_RISK_A_SHARE_ENDPOINTS:
            probe = planner.plan_probe(endpoint)
            response, status, error = self._probe(endpoint, probe.params, probe.fields)
            row_count = len(((response.get("data") or {}).get("items")) or [])
            error_type = None if status in {"accessible", "empty_but_accessible"} else status
            self.catalog.record_probe(endpoint, thash, status, probe.params, probe.fields, _valid_until_for(status), error, response, row_count=row_count, error_type=error_type)
            statuses[endpoint] = status
        return statuses

    def _probe(self, api_name: str, params: dict[str, Any], fields: list[str], max_attempts: int = 3) -> tuple[dict[str, Any], str, str | None]:
        attempt = 1
        while True:
            try:
                response = self.client.request(api_name, params, fields)
                status, error = classify_probe_response(response)
            except Exception as exc:
                response = {"error": str(exc)}
                err = classify_exception(exc)
                status, error = err.value, str(exc)
            try:
                retryable = should_retry(status, attempt, max_attempts)
            except ValueError:
                retryable = False
            if status in {"accessible", "empty_but_accessible"} or not retryable:
                return response, status, error
            self.sleep(retry_delay_seconds(status, attempt))
            attempt += 1

    def _permission_blocks(self, endpoint: str, probe_statuses: Mapping[str, str]) -> str | None:
        status = probe_statuses.get(endpoint) or "unknown"
        if status in {"permission_denied", "invalid_params", "invalid_endpoint", "rate_limited", "network_error", "server_error", "unknown_error"}:
            return status
        return None

    def _execute_fetch(self, endpoint: str, params: dict[str, Any], run_id: str, probe_statuses: Mapping[str, str]) -> dict[str, Any]:
        blocked = self._permission_blocks(endpoint, probe_statuses)
        if blocked:
            return self._blocked(endpoint, "fetch", blocked)
        plan = JobPlanner(self.root, self.catalog).plan_single_fetch(endpoint, params)
        if plan.existing_active_data:
            job = self.catalog.get_job(plan.job_key) or {}
            snap = self.catalog.latest_snapshot(endpoint)
            return {
                "endpoint": endpoint,
                "category": "fetch",
                "status": "skipped",
                "planned_jobs": 1,
                "executed_jobs": 0,
                "skipped_jobs": 1,
                "record_count": job.get("record_count"),
                "raw_event_count": job.get("raw_event_count"),
                "snapshot_id": snap.get("snapshot_id") if snap else None,
                "notes": "active data already exists",
            }
        try:
            result = FileLakeStore(self.root, self.catalog).fetch(endpoint, params, self.client, run_id=run_id, finish_run=False)
            job = self.catalog.get_job(result.job_key) or {}
            return {
                "endpoint": endpoint,
                "category": "fetch",
                "status": "succeeded" if result.snapshot_id or result.skipped else "failed",
                "planned_jobs": 1,
                "executed_jobs": 0 if result.skipped else 1,
                "skipped_jobs": 1 if result.skipped else 0,
                "record_count": job.get("record_count") or result.record_count,
                "raw_event_count": job.get("raw_event_count"),
                "snapshot_id": result.snapshot_id,
                "job_key": result.job_key,
            }
        except Exception as exc:
            return {
                "endpoint": endpoint,
                "category": "fetch",
                "status": "failed",
                "planned_jobs": 1,
                "executed_jobs": 1,
                "skipped_jobs": 0,
                "record_count": None,
                "raw_event_count": None,
                "snapshot_id": None,
                "error_type": classify_exception(exc).value,
                "error": str(exc),
            }

    def _execute_calendar_backfill(self, endpoint: str, mode: str, max_jobs: int, start_date: str | None, end_date: str | None, probe_statuses: Mapping[str, str]) -> dict[str, Any]:
        if self._permission_blocks(endpoint, probe_statuses):
            return self._blocked(endpoint, "daily_like", self._permission_blocks(endpoint, probe_statuses) or "blocked")
        start = start_date if mode == "pilot" else "20250101"
        end = end_date if mode == "pilot" else "20250110"
        dates, calendar = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(start_date=start, end_date=end, trading_days_only=True, calendar_exchange="SSE")
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs, calendar_metadata=calendar)
        result = BackfillExecutor(self.root, self.catalog).execute(plan, self.client, validate_latest=True)
        return self._backfill_item(endpoint, "daily_like", result)

    def _execute_date_backfill(self, endpoint: str, dates: list[str], max_jobs: int, probe_statuses: Mapping[str, str]) -> dict[str, Any]:
        if self._permission_blocks(endpoint, probe_statuses):
            return self._blocked(endpoint, "date_based", self._permission_blocks(endpoint, probe_statuses) or "blocked")
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs)
        result = BackfillExecutor(self.root, self.catalog).execute(plan, self.client, validate_latest=True)
        return self._backfill_item(endpoint, "date_based", result)

    def _backfill_item(self, endpoint: str, category: str, result) -> dict[str, Any]:
        summary = result.summary
        return {
            "endpoint": endpoint,
            "category": category,
            "status": result.status,
            "planned_jobs": summary.get("planned_jobs"),
            "executed_jobs": summary.get("executed_jobs"),
            "skipped_jobs": summary.get("skipped_jobs"),
            "succeeded_jobs": summary.get("succeeded_jobs"),
            "failed_jobs": summary.get("failed_jobs"),
            "blocked_jobs": summary.get("blocked_jobs"),
            "record_count": sum(int(row.record_count or 0) for row in result.results),
            "raw_event_count": sum(int(row.raw_event_count or 0) for row in result.results),
            "snapshot_id": (self.catalog.latest_snapshot(endpoint) or {}).get("snapshot_id"),
            "backfill_run_id": result.run_id,
        }

    def _blocked(self, endpoint: str, category: str, reason: str) -> dict[str, Any]:
        return {
            "endpoint": endpoint,
            "category": category,
            "status": "blocked",
            "planned_jobs": 0,
            "executed_jobs": 0,
            "skipped_jobs": 0,
            "blocked_reason": reason,
        }

    def _summary(self, scope: str, mode: str, max_jobs: int, backup_target: str | None, items: list[dict[str, Any]], validation: dict[str, Any], backup: dict[str, Any] | None, restore_check: dict[str, Any] | None) -> dict[str, Any]:
        failed = [item for item in items if item.get("status") == "failed"]
        blocked = [item for item in items if item.get("status") == "blocked"]
        critical_dependency_failed = any(item.get("endpoint") == "trade_cal" and item.get("status") in {"failed", "blocked"} for item in items)
        succeeded = [item for item in items if item.get("status") == "succeeded"]
        skipped = [item for item in items if item.get("status") == "skipped"]
        return {
            "scope": scope,
            "mode": mode,
            "execute": True,
            "endpoint_count": len(items),
            "succeeded_endpoints": len(succeeded),
            "skipped_endpoints": len(skipped),
            "blocked_endpoints": len(blocked),
            "failed_endpoints": len(failed),
            "critical_dependency_failed": critical_dependency_failed,
            "total_jobs_executed": sum(int(item.get("executed_jobs") or 0) for item in items),
            "max_jobs_per_api": max_jobs,
            "backup_target": backup_target,
            "backup_status": (backup or {}).get("status") if backup else "not_requested",
            "restore_check_status": (restore_check or {}).get("status") if restore_check else "not_requested",
            "validation_status": validation.get("status"),
            "created_at": now_utc(),
            "items": items,
        }


def init_catalog_if_requested(root: Path, init_if_missing: bool) -> CatalogStore:
    catalog = CatalogStore(root)
    if catalog.db_path.exists():
        return catalog
    if not init_if_missing:
        raise FileNotFoundError(f"catalog not found: {catalog.db_path}; run init-catalog first or pass --init-if-missing")
    catalog.init()
    load_into_catalog(root, catalog)
    return catalog
