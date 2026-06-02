from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


PILOT_REFERENCE_SIZE_BYTES = 35_699_847
PILOT_REFERENCE_RAW_FILES = 81
PILOT_REFERENCE_LAKE_FILES = 81
PILOT_REFERENCE_JOBS = 81


@dataclass(frozen=True)
class StorageEstimate:
    scope: str | None
    category: str | None
    api_name: str | None
    freq: str | None
    date_range: dict[str, str]
    estimated_jobs: int
    estimated_raw_files: int
    estimated_lake_files: int
    estimated_size_class: str
    estimated_size_bytes: int | None
    assumptions: list[str]
    warnings: list[str]
    confidence: str
    dry_run: bool
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        return data


class StorageEstimator:
    def estimate(
        self,
        *,
        scope: str | None,
        category: str | None,
        api_name: str | None,
        freq: str | None,
        start_date: str | None,
        end_date: str | None,
        bucket_count: int | None,
    ) -> StorageEstimate:
        warnings: list[str] = []
        blocking_errors: list[str] = []
        date_range: dict[str, str] = {}
        start: datetime | None = None
        end: datetime | None = None
        try:
            start, end, date_range = _validate_date_range(start_date, end_date)
        except ValueError as exc:
            blocking_errors.append(str(exc))

        if scope and category:
            blocking_errors.append("choose either scope or category, not both")
        if not scope and not category:
            blocking_errors.append("scope or category is required")
        if scope and scope != "low-risk-a-share":
            blocking_errors.append(f"unsupported scope: {scope}")

        if blocking_errors:
            return StorageEstimate(
                scope=scope,
                category=category,
                api_name=api_name,
                freq=freq,
                date_range=date_range,
                estimated_jobs=0,
                estimated_raw_files=0,
                estimated_lake_files=0,
                estimated_size_class="unknown",
                estimated_size_bytes=None,
                assumptions=[],
                warnings=warnings,
                confidence="low",
                dry_run=True,
                blocking_errors=blocking_errors,
            )

        assert start is not None and end is not None
        if scope == "low-risk-a-share":
            months = _inclusive_months(start, end)
            return StorageEstimate(
                scope=scope,
                category=None,
                api_name=None,
                freq=None,
                date_range=date_range,
                estimated_jobs=PILOT_REFERENCE_JOBS * months,
                estimated_raw_files=PILOT_REFERENCE_RAW_FILES * months,
                estimated_lake_files=PILOT_REFERENCE_LAKE_FILES * months,
                estimated_size_class=_size_class(PILOT_REFERENCE_SIZE_BYTES * months),
                estimated_size_bytes=PILOT_REFERENCE_SIZE_BYTES * months,
                assumptions=[
                    "uses January 2025 low-risk pilot artifact as reference",
                    "assumes similar trading-day density and file layout per month",
                    "does not include disabled object, intraday, financial, PIT, or remote backup data",
                ],
                warnings=[
                    "estimate is not a capacity guarantee",
                    "future schema or endpoint mix can change file count and size",
                ],
                confidence="medium",
                dry_run=True,
                blocking_errors=[],
            )

        days = (end - start).days + 1
        bucket = bucket_count or 64
        if category != "intraday":
            blocking_errors.append(f"unsupported category: {category}")
            return StorageEstimate(
                scope=scope,
                category=category,
                api_name=api_name,
                freq=freq,
                date_range=date_range,
                estimated_jobs=0,
                estimated_raw_files=0,
                estimated_lake_files=0,
                estimated_size_class="unknown",
                estimated_size_bytes=None,
                assumptions=[],
                warnings=warnings,
                confidence="low",
                dry_run=True,
                blocking_errors=blocking_errors,
            )
        return StorageEstimate(
            scope=None,
            category="intraday",
            api_name=api_name,
            freq=freq,
            date_range=date_range,
            estimated_jobs=days,
            estimated_raw_files=days * bucket,
            estimated_lake_files=days * bucket,
            estimated_size_class="potentially_large",
            estimated_size_bytes=None,
            assumptions=[
                "intraday estimate is warning-level only",
                "assumes one raw and one lake file per date bucket before compaction",
                "actual size depends on endpoint permission, symbols, frequency, and exchange activity",
            ],
            warnings=[
                "low confidence estimate",
                "bucketed intraday execution and compaction are not implemented",
            ],
            confidence="low",
            dry_run=True,
            blocking_errors=[],
        )


def _validate_date_range(start_date: str | None, end_date: str | None) -> tuple[datetime, datetime, dict[str, str]]:
    if not start_date or not end_date:
        raise ValueError("start-date and end-date are required")
    start = _parse_yyyymmdd(start_date, "start-date")
    end = _parse_yyyymmdd(end_date, "end-date")
    if start > end:
        raise ValueError("start-date must be <= end-date")
    return start, end, {"start_date": start_date, "end_date": end_date}


def _parse_yyyymmdd(value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {value}") from exc


def _inclusive_months(start: datetime, end: datetime) -> int:
    return (end.year - start.year) * 12 + end.month - start.month + 1


def _size_class(size_bytes: int) -> str:
    if size_bytes < 100 * 1024 * 1024:
        return "tens_of_mb"
    if size_bytes < 1024 * 1024 * 1024:
        return "hundreds_of_mb"
    return "gb_plus"
