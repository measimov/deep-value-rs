from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .backup import BackupExecutor, BackupPlanner, RestoreChecker
from .backfill import BackfillExecutor, BackfillPlanner, DatePlanner
from .catalog import CatalogStore
from .client import classify_probe_response
from .endpoints import load_into_catalog
from .errors import classify_exception, retry_delay_seconds, should_retry
from .hashing import token_hash
from .io_utils import now_utc
from .planner import JobPlanner
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


def ensure_mirror_scope(scope: str) -> None:
    if scope != "low-risk-a-share":
        raise ValueError("unknown mirror scope: %s; supported: low-risk-a-share" % scope)


def ensure_mirror_mode(mode: str) -> None:
    if mode not in MODE_MAX_JOBS:
        raise ValueError("unknown mirror mode: %s; supported: smoke, pilot" % mode)


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
