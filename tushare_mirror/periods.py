from __future__ import annotations

import re
from dataclasses import asdict, dataclass


MAX_PERIODS = 20
QUARTER_ENDS = {
    1: "0331",
    2: "0630",
    3: "0930",
    4: "1231",
}
END_TO_QUARTER = {value: key for key, value in QUARTER_ENDS.items()}


@dataclass(frozen=True)
class Period:
    raw: str
    normalized: str
    year: int
    quarter: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PeriodRangePlan:
    periods: list[str]
    total_periods: int
    planned_periods: int
    max_periods: int
    truncated_by_max_periods: bool
    frequency: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def parse_period(value: str) -> Period:
    raw = str(value or "").strip().upper()
    quarter_match = re.fullmatch(r"(\d{4})Q([1-4])", raw)
    if quarter_match:
        year = int(quarter_match.group(1))
        quarter = int(quarter_match.group(2))
        return Period(raw=raw, normalized=f"{year}{QUARTER_ENDS[quarter]}", year=year, quarter=quarter)

    date_match = re.fullmatch(r"(\d{4})(\d{4})", raw)
    if date_match:
        year = int(date_match.group(1))
        suffix = date_match.group(2)
        quarter = END_TO_QUARTER.get(suffix)
        if quarter is None:
            raise ValueError(f"unsupported period end date: {value}")
        return Period(raw=raw, normalized=f"{year}{suffix}", year=year, quarter=quarter)

    raise ValueError(f"invalid period: {value}")


def normalize_period(value: str) -> str:
    return parse_period(value).normalized


def period_year(value: str) -> int:
    return parse_period(value).year


def period_sort_key(value: str) -> tuple[int, int]:
    period = parse_period(value)
    return period.year, period.quarter


def compare_periods(left: str, right: str) -> int:
    lkey = period_sort_key(left)
    rkey = period_sort_key(right)
    return (lkey > rkey) - (lkey < rkey)


def normalize_period_list(values: str | list[str]) -> list[str]:
    if isinstance(values, str):
        raw_values = [item.strip() for item in values.split(",") if item.strip()]
    else:
        raw_values = [str(item).strip() for item in values if str(item).strip()]
    normalized = {normalize_period(item) for item in raw_values}
    return sorted(normalized, key=period_sort_key)


def generate_period_range(start_period: str, end_period: str, frequency: str) -> list[str]:
    start = parse_period(start_period)
    end = parse_period(end_period)
    if (start.year, start.quarter) > (end.year, end.quarter):
        raise ValueError("start_period must be <= end_period")
    if frequency not in {"quarterly", "annual"}:
        raise ValueError("period-frequency must be quarterly or annual")

    periods: list[str] = []
    year = start.year
    quarter = start.quarter
    while (year, quarter) <= (end.year, end.quarter):
        if frequency == "quarterly":
            periods.append(f"{year}{QUARTER_ENDS[quarter]}")
        elif quarter == 4:
            periods.append(f"{year}1231")
        quarter += 1
        if quarter == 5:
            year += 1
            quarter = 1
    return periods


class PeriodRangePlanner:
    def plan(
        self,
        *,
        periods: str | list[str] | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        period_frequency: str = "quarterly",
        max_periods: int = MAX_PERIODS,
    ) -> PeriodRangePlan:
        if max_periods <= 0:
            raise ValueError("max_periods must be positive")
        if max_periods > MAX_PERIODS:
            raise ValueError(f"max_periods exceeds phase limit: {MAX_PERIODS}")
        if periods:
            planned = normalize_period_list(periods)
            frequency = "explicit"
        else:
            if not start_period or not end_period:
                raise ValueError("period planning requires --periods or --start-period/--end-period")
            planned = generate_period_range(start_period, end_period, period_frequency)
            frequency = period_frequency
        total = len(planned)
        selected = planned[:max_periods]
        return PeriodRangePlan(
            periods=selected,
            total_periods=total,
            planned_periods=len(selected),
            max_periods=max_periods,
            truncated_by_max_periods=total > len(selected),
            frequency=frequency,
        )
