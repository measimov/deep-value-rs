from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .backfill import BackfillPlanner, DatePlanner, SUPPORTED_DATE_BACKFILL_APIS, TRADING_DAY_BACKFILL_APIS
from .catalog import CatalogStore


@dataclass(frozen=True)
class CoverageItem:
    date: str
    existing_status: str
    planned_action: str
    job_key: str
    snapshot_id: str | None
    record_count: int | None
    raw_event_count: int | None
    file_count: int
    last_job_status: str | None
    last_error_type: str | None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CoverageReport:
    api_name: str
    date_field: str
    requested_start_date: str | None
    requested_end_date: str | None
    calendar_source: str | None
    calendar_exchange: str | None
    natural_days: int | None
    trading_days: int | None
    filtered_non_trading_days: int | None
    filtered_non_trading_dates: list[str] | None
    total_dates: int
    covered_dates: int
    missing_dates: int
    failed_dates: int
    quarantined_dates: int
    coverage_ratio: float
    items: list[CoverageItem]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["items"] = [item.to_dict() for item in self.items]
        return data


class CoverageReporter:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def report(
        self,
        api_name: str,
        *,
        dates: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        trading_days_only: bool = False,
        calendar_exchange: str = "SSE",
    ) -> CoverageReport:
        if api_name not in SUPPORTED_DATE_BACKFILL_APIS:
            supported = ", ".join(sorted(SUPPORTED_DATE_BACKFILL_APIS))
            raise ValueError(f"coverage does not support {api_name}; supported: {supported}")
        if trading_days_only and api_name not in TRADING_DAY_BACKFILL_APIS:
            raise ValueError("trading-days-only is only supported for daily-like endpoints in Phase 2.4")
        planned_dates, calendar_metadata = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(
            dates=dates,
            start_date=start_date,
            end_date=end_date,
            trading_days_only=trading_days_only,
            calendar_exchange=calendar_exchange,
        )
        max_jobs = max(1, len(planned_dates))
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(
            api_name,
            planned_dates,
            max_jobs=max_jobs,
            dry_run=True,
            calendar_metadata=calendar_metadata,
        )
        items = [self._item(api_name, job.date, job.job_key, job.existing_status, job.planned_action) for job in plan.planned_jobs]
        covered = sum(1 for item in items if item.existing_status == "active_exists")
        missing = sum(1 for item in items if item.existing_status == "missing")
        failed = sum(1 for item in items if item.existing_status == "failed_exists")
        quarantined = sum(1 for item in items if item.existing_status == "quarantined_exists")
        total = len(items)
        ratio = round(covered / total, 4) if total else 0.0
        return CoverageReport(
            api_name=api_name,
            date_field=plan.date_field,
            requested_start_date=plan.requested_start_date or (planned_dates[0] if planned_dates else None),
            requested_end_date=plan.requested_end_date or (planned_dates[-1] if planned_dates else None),
            calendar_source=plan.calendar_source,
            calendar_exchange=plan.exchange,
            natural_days=plan.natural_days,
            trading_days=plan.trading_days,
            filtered_non_trading_days=plan.filtered_non_trading_days,
            filtered_non_trading_dates=plan.filtered_non_trading_dates,
            total_dates=total,
            covered_dates=covered,
            missing_dates=missing,
            failed_dates=failed,
            quarantined_dates=quarantined,
            coverage_ratio=ratio,
            items=items,
        )

    def _item(self, api_name: str, date: str, job_key: str, existing_status: str, planned_action: str) -> CoverageItem:
        job = self.catalog.get_job(job_key) or {}
        files = self.catalog.active_files_for_job(job_key, api_name)
        snapshot = self.catalog.latest_snapshot(api_name) if files else None
        snapshot_id = snapshot["snapshot_id"] if snapshot else None
        record_count = self._sum(files, "lake", "record_count")
        raw_event_count = self._sum(files, "raw", "raw_event_count")
        last_status = job.get("status")
        if last_status == "done":
            last_status = "succeeded"
        notes = {
            "active_exists": "covered by latest snapshot",
            "missing": "no active data",
            "failed_exists": "last job failed; retry would be planned",
            "staged_exists": "staged files exist; retry would be planned",
            "quarantined_exists": "quarantined job is blocked",
        }.get(existing_status)
        return CoverageItem(
            date=date,
            existing_status=existing_status,
            planned_action=planned_action,
            job_key=job_key,
            snapshot_id=snapshot_id,
            record_count=record_count,
            raw_event_count=raw_event_count,
            file_count=len(files),
            last_job_status=last_status,
            last_error_type=job.get("last_error_type"),
            notes=notes,
        )

    def _sum(self, files: list[dict[str, Any]], content_type: str, column: str) -> int | None:
        values = [int(row.get(column) or 0) for row in files if row.get("content_type") == content_type]
        return sum(values) if values else None
