from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .errors import ErrorType, classify_exception
from .io_utils import now_utc
from .planner import JobPlanner
from .reader import LakeReader
from .store import FileLakeStore
from .validation import Validator

SUPPORTED_DATE_BACKFILL_APIS = {
    "daily": "trade_date",
    "adj_factor": "trade_date",
    "daily_basic": "trade_date",
    "weekly": "trade_date",
    "monthly": "trade_date",
    "suspend_d": "trade_date",
}

PHASE21_EXECUTE_MAX_JOBS = 20


@dataclass(frozen=True)
class BackfillJobPlan:
    api_name: str
    date: str
    params: dict[str, Any]
    params_hash: str
    job_key: str
    partition_values: dict[str, Any]
    raw_path: str
    lake_path_prefix: str
    existing_status: str
    planned_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BackfillPlan:
    plan_id: str
    api_name: str
    date_field: str
    dates: list[str]
    total_candidate_jobs: int
    max_jobs: int
    planned_jobs: list[BackfillJobPlan]
    skipped_jobs: int
    blocked_jobs: int
    rejected_reason: str | None
    dry_run: bool
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["planned_jobs"] = [job.to_dict() for job in self.planned_jobs]
        return data


@dataclass(frozen=True)
class BackfillJobResult:
    date: str
    job_key: str
    action: str
    status: str
    record_count: int | None
    raw_event_count: int | None
    snapshot_id: str | None
    error_type: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BackfillExecutionResult:
    run_id: str
    status: str
    results: list[BackfillJobResult]
    summary: dict[str, Any]
    validation: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "results": [row.to_dict() for row in self.results],
            "summary": self.summary,
            "validation": self.validation,
        }


class DatePlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan_dates(
        self,
        *,
        dates: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        trading_days_only: bool = False,
    ) -> list[str]:
        if dates:
            if isinstance(dates, str):
                values = [item.strip() for item in dates.split(",") if item.strip()]
            else:
                values = list(dates)
            planned = sorted({self._normalize_date(value) for value in values})
        else:
            if not start_date or not end_date:
                raise ValueError("backfill requires --dates or both --start-date and --end-date")
            start = self._parse_date(start_date)
            end = self._parse_date(end_date)
            if start > end:
                raise ValueError("start_date must be <= end_date")
            planned = []
            current = start
            while current <= end:
                planned.append(current.strftime("%Y%m%d"))
                current += timedelta(days=1)
        if trading_days_only:
            return self._filter_trading_days(planned)
        return planned

    def _filter_trading_days(self, dates: list[str]) -> list[str]:
        if not self.catalog.latest_snapshot("trade_cal"):
            raise ValueError("trading-days-only requires a local trade_cal latest snapshot; fetch trade_cal first")
        wanted = set(dates)
        table = LakeReader(self.root, self.catalog).scan_api("trade_cal", columns=["cal_date", "is_open"])
        if table.num_rows == 0:
            return []
        cal_dates = table["cal_date"].to_pylist() if "cal_date" in table.column_names else []
        is_open = table["is_open"].to_pylist() if "is_open" in table.column_names else []
        open_dates: list[str] = []
        for cal_date, open_flag in zip(cal_dates, is_open):
            value = str(cal_date)
            if value in wanted and str(open_flag) in {"1", "1.0", "True", "true"}:
                open_dates.append(value)
        return sorted(set(open_dates))

    def _normalize_date(self, value: str) -> str:
        parsed = self._parse_date(value)
        return parsed.strftime("%Y%m%d")

    def _parse_date(self, value: str) -> datetime:
        text = str(value).strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                pass
        raise ValueError(f"invalid date: {value}")


class BackfillPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog
        self.job_planner = JobPlanner(root, catalog)

    def plan_date_backfill(
        self,
        api_name: str,
        dates: list[str],
        max_jobs: int,
        fields: list[str] | None = None,
        dry_run: bool = True,
    ) -> BackfillPlan:
        if api_name not in SUPPORTED_DATE_BACKFILL_APIS:
            supported = ", ".join(sorted(SUPPORTED_DATE_BACKFILL_APIS))
            raise ValueError(f"Phase 2.1 scoped backfill does not support {api_name}; supported: {supported}")
        if max_jobs <= 0:
            raise ValueError("max_jobs must be positive")
        normalized_dates = sorted({DatePlanner(self.root, self.catalog)._normalize_date(date) for date in dates})
        date_field = SUPPORTED_DATE_BACKFILL_APIS[api_name]
        warnings: list[str] = []
        if len(normalized_dates) > max_jobs:
            warnings.append(f"truncated candidate jobs from {len(normalized_dates)} to max_jobs={max_jobs}")
        planned_jobs: list[BackfillJobPlan] = []
        for date in normalized_dates[:max_jobs]:
            params = {date_field: date}
            fetch_plan = self.job_planner.plan_single_fetch(api_name, params, fields)
            existing_status, action = self._existing_status_and_action(api_name, fetch_plan.job_key, fetch_plan.existing_active_data)
            planned_jobs.append(
                BackfillJobPlan(
                    api_name=api_name,
                    date=date,
                    params=fetch_plan.params,
                    params_hash=fetch_plan.params_hash,
                    job_key=fetch_plan.job_key,
                    partition_values=fetch_plan.partition_values,
                    raw_path=fetch_plan.raw_path,
                    lake_path_prefix=fetch_plan.lake_path_prefix,
                    existing_status=existing_status,
                    planned_action=action,
                )
            )
        return BackfillPlan(
            plan_id="plan_" + now_utc().replace("-", "").replace(":", "").replace(".", "").replace("Z", ""),
            api_name=api_name,
            date_field=date_field,
            dates=normalized_dates,
            total_candidate_jobs=len(normalized_dates),
            max_jobs=max_jobs,
            planned_jobs=planned_jobs,
            skipped_jobs=sum(1 for job in planned_jobs if job.planned_action == "skip_existing"),
            blocked_jobs=sum(1 for job in planned_jobs if job.planned_action == "blocked_quarantined"),
            rejected_reason=None,
            dry_run=dry_run,
            warnings=warnings,
        )

    def _existing_status_and_action(self, api_name: str, job_key: str, active_exists: bool) -> tuple[str, str]:
        if active_exists:
            return "active_exists", "skip_existing"
        if self.catalog.quarantine_exists_for_job(job_key):
            return "quarantined_exists", "blocked_quarantined"
        statuses = self.catalog.file_statuses_for_job(job_key, api_name)
        if "staged" in statuses:
            return "staged_exists", "retry_failed"
        job = self.catalog.get_job(job_key)
        if job and job.get("status") == "failed":
            return "failed_exists", "retry_failed"
        if job:
            return "unknown", "fetch"
        return "missing", "fetch"


class BackfillExecutor:
    def __init__(self, root: Path | str, catalog: CatalogStore, store: FileLakeStore | None = None):
        self.root = Path(root)
        self.catalog = catalog
        self.store = store or FileLakeStore(root, catalog)

    def execute(
        self,
        plan: BackfillPlan,
        client,
        *,
        validate_latest: bool = False,
        stop_on_error: bool = False,
    ) -> BackfillExecutionResult:
        run_id = self.catalog.create_run("backfill")
        started_at = now_utc()
        results: list[BackfillJobResult] = []
        for job in plan.planned_jobs:
            if job.planned_action == "skip_existing":
                existing = self.catalog.get_job(job.job_key) or {}
                results.append(
                    BackfillJobResult(job.date, job.job_key, job.planned_action, "skipped", existing.get("record_count"), existing.get("raw_event_count"), None)
                )
                continue
            if job.planned_action == "blocked_quarantined":
                results.append(BackfillJobResult(job.date, job.job_key, job.planned_action, "blocked", None, None, None, ErrorType.SCHEMA_INCOMPATIBLE.value))
                if stop_on_error:
                    break
                continue
            try:
                result = self.store.fetch(plan.api_name, job.params, client, run_id=run_id, finish_run=False)
                row = self.catalog.get_job(job.job_key) or {}
                status = "succeeded" if result.snapshot_id else "failed"
                results.append(
                    BackfillJobResult(
                        date=job.date,
                        job_key=job.job_key,
                        action=job.planned_action,
                        status=status,
                        record_count=row.get("record_count"),
                        raw_event_count=row.get("raw_event_count"),
                        snapshot_id=result.snapshot_id,
                        error_type=row.get("last_error_type"),
                        error=row.get("last_error"),
                    )
                )
                if status == "failed" and stop_on_error:
                    break
            except Exception as exc:
                err = classify_exception(exc).value
                row = self.catalog.get_job(job.job_key) or {}
                results.append(
                    BackfillJobResult(
                        date=job.date,
                        job_key=job.job_key,
                        action=job.planned_action,
                        status="failed",
                        record_count=row.get("record_count"),
                        raw_event_count=row.get("raw_event_count"),
                        snapshot_id=None,
                        error_type=row.get("last_error_type") or err,
                        error=row.get("last_error") or str(exc),
                    )
                )
                if stop_on_error:
                    break
        validation_report = None
        if validate_latest:
            validation_report = Validator(self.root, self.catalog).validate_snapshot_report("latest", plan.api_name)
        summary = self._summary(plan, results, started_at)
        status = "failed" if summary["failed_jobs"] or summary["blocked_jobs"] else "succeeded"
        self.catalog.finish_run(run_id, status, None if status == "succeeded" else "backfill had failed or blocked jobs", None, summary)
        return BackfillExecutionResult(run_id, status, results, summary, validation_report)

    def _summary(self, plan: BackfillPlan, results: list[BackfillJobResult], started_at: str) -> dict[str, Any]:
        failed = [row for row in results if row.status == "failed"]
        blocked = [row for row in results if row.status == "blocked"]
        skipped = [row for row in results if row.status == "skipped"]
        succeeded = [row for row in results if row.status == "succeeded"]
        quarantined = [row for row in results if row.error_type == ErrorType.SCHEMA_INCOMPATIBLE.value or row.status == "blocked"]
        return {
            "api_name": plan.api_name,
            "date_field": plan.date_field,
            "requested_dates": plan.dates,
            "total_candidate_jobs": plan.total_candidate_jobs,
            "planned_jobs": len(plan.planned_jobs),
            "skipped_jobs": len(skipped),
            "succeeded_jobs": len(succeeded),
            "failed_jobs": len(failed),
            "blocked_jobs": len(blocked),
            "quarantined_jobs": len(quarantined),
            "started_at": started_at,
            "finished_at": now_utc(),
        }


def plan_to_rows(plan: BackfillPlan) -> list[dict[str, Any]]:
    rows = []
    for job in plan.planned_jobs:
        rows.append(
            {
                "api_name": job.api_name,
                "date": job.date,
                "job_key": job.job_key,
                "existing_status": job.existing_status,
                "planned_action": job.planned_action,
                "partition": json.dumps(job.partition_values, ensure_ascii=False, sort_keys=True),
                "raw_path": job.raw_path,
                "lake_path_prefix": job.lake_path_prefix,
            }
        )
    return rows


def execution_to_rows(result: BackfillExecutionResult) -> list[dict[str, Any]]:
    return [
        {
            "date": row.date,
            "job_key": row.job_key,
            "action": row.action,
            "status": row.status,
            "record_count": row.record_count,
            "raw_event_count": row.raw_event_count,
            "snapshot_id": row.snapshot_id,
            "error_type": row.error_type,
        }
        for row in result.results
    ]
