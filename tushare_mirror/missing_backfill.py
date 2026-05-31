from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .backfill import BackfillPlan, BackfillPlanner, PHASE21_EXECUTE_MAX_JOBS, TRADING_DAY_BACKFILL_APIS
from .catalog import CatalogStore
from .coverage import CoverageItem, CoverageReporter

EXECUTABLE_MISSING_STATUSES = {"missing"}
BLOCKED_MISSING_STATUSES = {"quarantined_exists", "staged_exists"}


@dataclass(frozen=True)
class MissingBackfillItem:
    date: str
    existing_status: str
    planned_action: str
    will_execute: bool
    job_key: str
    snapshot_id: str | None
    record_count: int | None
    raw_event_count: int | None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MissingBackfillPlan:
    api_name: str
    date_field: str
    coverage: dict[str, Any]
    items: list[MissingBackfillItem]
    backfill_plan: BackfillPlan
    candidate_jobs: int
    planned_jobs: int
    blocked_jobs: int
    max_jobs: int
    dry_run: bool
    execute: bool
    retry_failed: bool
    truncated_by_max_jobs: bool
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "api_name": self.api_name,
            "date_field": self.date_field,
            "coverage": self.coverage,
            "items": [item.to_dict() for item in self.items],
            "backfill_plan": self.backfill_plan.to_dict(),
            "candidate_jobs": self.candidate_jobs,
            "planned_jobs": self.planned_jobs,
            "blocked_jobs": self.blocked_jobs,
            "max_jobs": self.max_jobs,
            "dry_run": self.dry_run,
            "execute": self.execute,
            "retry_failed": self.retry_failed,
            "truncated_by_max_jobs": self.truncated_by_max_jobs,
            "warnings": self.warnings,
        }

    def summary(self) -> dict[str, Any]:
        data = {
            "api_name": self.api_name,
            "total_dates": self.coverage["total_dates"],
            "covered_dates": self.coverage["covered_dates"],
            "missing_dates": self.coverage["missing_dates"],
            "failed_dates": self.coverage["failed_dates"],
            "quarantined_dates": self.coverage["quarantined_dates"],
            "candidate_jobs": self.candidate_jobs,
            "planned_jobs": self.planned_jobs,
            "blocked_jobs": self.blocked_jobs,
            "max_jobs": self.max_jobs,
            "dry_run": self.dry_run,
            "execute": self.execute,
            "retry_failed": self.retry_failed,
            "truncated_by_max_jobs": self.truncated_by_max_jobs,
            "warnings": self.warnings,
        }
        for key in [
            "calendar_source",
            "calendar_exchange",
            "requested_start_date",
            "requested_end_date",
            "natural_days",
            "trading_days",
            "filtered_non_trading_days",
            "filtered_non_trading_dates",
        ]:
            if self.coverage.get(key) is not None:
                data[key] = self.coverage.get(key)
        return data


class MissingBackfillPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        api_name: str,
        *,
        max_jobs: int,
        dates: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        trading_days_only: bool = False,
        calendar_exchange: str = "SSE",
        retry_failed: bool = False,
        execute: bool = False,
    ) -> MissingBackfillPlan:
        if max_jobs <= 0:
            raise ValueError("--max-jobs must be positive")
        if execute and max_jobs > PHASE21_EXECUTE_MAX_JOBS:
            raise ValueError(f"Refusing to execute {max_jobs} jobs in Phase 2.7. max allowed: {PHASE21_EXECUTE_MAX_JOBS}.")
        if trading_days_only and api_name not in TRADING_DAY_BACKFILL_APIS:
            raise ValueError("trading-days-only is only supported for daily-like endpoints in Phase 2.4")
        report = CoverageReporter(self.root, self.catalog).report(
            api_name,
            dates=dates,
            start_date=start_date,
            end_date=end_date,
            trading_days_only=trading_days_only,
            calendar_exchange=calendar_exchange,
        )
        coverage_data = self._coverage_summary(report)
        candidate_statuses = set(EXECUTABLE_MISSING_STATUSES)
        if retry_failed:
            candidate_statuses.add("failed_exists")
        candidates = [item for item in report.items if item.existing_status in candidate_statuses]
        blocked = [item for item in report.items if item.existing_status in BLOCKED_MISSING_STATUSES]
        selected = candidates[:max_jobs]
        selected_dates = [item.date for item in selected]
        metadata = self._calendar_metadata(report)
        backfill_plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(
            api_name,
            selected_dates,
            max_jobs=max(1, max_jobs),
            dry_run=not execute,
            calendar_metadata=metadata,
        )
        plan_jobs_by_date = {job.date: job for job in backfill_plan.planned_jobs}
        selected_dates_set = set(selected_dates)
        items = [self._item(item, item.date in selected_dates_set, plan_jobs_by_date.get(item.date)) for item in report.items]
        warnings: list[str] = []
        truncated = len(candidates) > max_jobs
        if truncated:
            warnings.append(f"truncated candidate jobs from {len(candidates)} to max_jobs={max_jobs}")
        if report.failed_dates and not retry_failed:
            warnings.append("failed_exists dates are not executable unless --retry-failed is set")
        if blocked:
            warnings.append("quarantined or staged dates are blocked and will not be executed")
        return MissingBackfillPlan(
            api_name=api_name,
            date_field=report.date_field,
            coverage=coverage_data,
            items=items,
            backfill_plan=backfill_plan,
            candidate_jobs=len(candidates),
            planned_jobs=len(backfill_plan.planned_jobs),
            blocked_jobs=len(blocked),
            max_jobs=max_jobs,
            dry_run=not execute,
            execute=execute,
            retry_failed=retry_failed,
            truncated_by_max_jobs=truncated,
            warnings=warnings,
        )

    def _item(self, coverage_item: CoverageItem, will_execute: bool, planned_job) -> MissingBackfillItem:
        notes = coverage_item.notes
        if coverage_item.existing_status == "failed_exists" and not will_execute:
            notes = "failed job; pass --retry-failed to execute"
        elif coverage_item.existing_status in BLOCKED_MISSING_STATUSES:
            notes = "blocked; manual review required"
        return MissingBackfillItem(
            date=coverage_item.date,
            existing_status=coverage_item.existing_status,
            planned_action=planned_job.planned_action if planned_job else coverage_item.planned_action,
            will_execute=will_execute,
            job_key=coverage_item.job_key,
            snapshot_id=coverage_item.snapshot_id,
            record_count=coverage_item.record_count,
            raw_event_count=coverage_item.raw_event_count,
            notes=notes,
        )

    def _coverage_summary(self, report) -> dict[str, Any]:
        return {
            "api_name": report.api_name,
            "date_field": report.date_field,
            "requested_start_date": report.requested_start_date,
            "requested_end_date": report.requested_end_date,
            "calendar_source": report.calendar_source,
            "calendar_exchange": report.calendar_exchange,
            "natural_days": report.natural_days,
            "trading_days": report.trading_days,
            "filtered_non_trading_days": report.filtered_non_trading_days,
            "filtered_non_trading_dates": report.filtered_non_trading_dates,
            "total_dates": report.total_dates,
            "covered_dates": report.covered_dates,
            "missing_dates": report.missing_dates,
            "failed_dates": report.failed_dates,
            "quarantined_dates": report.quarantined_dates,
            "coverage_ratio": report.coverage_ratio,
        }

    def _calendar_metadata(self, report) -> dict[str, Any] | None:
        if not report.calendar_source:
            return None
        return {
            "calendar_source": report.calendar_source,
            "exchange": report.calendar_exchange,
            "requested_start_date": report.requested_start_date,
            "requested_end_date": report.requested_end_date,
            "natural_days": report.natural_days,
            "trading_days": report.trading_days,
            "filtered_non_trading_days": report.filtered_non_trading_days,
            "filtered_non_trading_dates": report.filtered_non_trading_dates,
        }
