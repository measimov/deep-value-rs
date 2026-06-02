from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


MAX_CODE_DATE_MATRIX_CODES = 20
MAX_CODE_DATE_MATRIX_DATES = 20
MAX_CODE_DATE_MATRIX_CANDIDATES = 100


@dataclass(frozen=True)
class CodeDateMatrixItem:
    api_name: str
    ts_code: str
    date: str
    params: dict[str, Any]
    job_key: str | None
    existing_status: str
    planned_action: str
    would_require_real_request: bool = True
    execution_allowed: bool = False
    blocked_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CodeDateMatrixSummary:
    api_name: str
    universe: str
    source_snapshot_id: str | None
    total_codes: int
    planned_codes: int
    total_dates: int
    planned_dates: int
    candidate_jobs: int
    planned_jobs: int
    truncated_by_code_limit: bool
    truncated_by_date_limit: bool
    truncated_by_candidate_limit: bool
    execution_allowed: bool
    dry_run: bool
    warnings: list[str]
    blocking_errors: list[str]

    @classmethod
    def from_candidate_counts(
        cls,
        *,
        api_name: str,
        universe: str,
        source_snapshot_id: str | None,
        total_codes: int,
        total_dates: int,
        limit_codes: int,
        max_dates: int,
        max_candidate_jobs: int = MAX_CODE_DATE_MATRIX_CANDIDATES,
        warnings: list[str] | None = None,
        blocking_errors: list[str] | None = None,
    ) -> "CodeDateMatrixSummary":
        planned_codes_before_candidate = min(max(total_codes, 0), max(limit_codes, 0), MAX_CODE_DATE_MATRIX_CODES)
        planned_dates_before_candidate = min(max(total_dates, 0), max(max_dates, 0), MAX_CODE_DATE_MATRIX_DATES)
        candidate_jobs = max(total_codes, 0) * max(total_dates, 0)
        truncated_by_code_limit = max(total_codes, 0) > planned_codes_before_candidate
        truncated_by_date_limit = max(total_dates, 0) > planned_dates_before_candidate
        planned_codes = planned_codes_before_candidate
        planned_dates = planned_dates_before_candidate
        max_jobs = max(max_candidate_jobs, 0)
        potential_jobs = planned_codes * planned_dates
        truncated_by_candidate_limit = potential_jobs > max_jobs
        if truncated_by_candidate_limit:
            if planned_codes == 0:
                planned_dates = 0
            else:
                planned_dates = max_jobs // planned_codes
        planned_jobs = planned_codes * planned_dates
        return cls(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=source_snapshot_id,
            total_codes=max(total_codes, 0),
            planned_codes=planned_codes,
            total_dates=max(total_dates, 0),
            planned_dates=planned_dates,
            candidate_jobs=candidate_jobs,
            planned_jobs=planned_jobs,
            truncated_by_code_limit=truncated_by_code_limit,
            truncated_by_date_limit=truncated_by_date_limit,
            truncated_by_candidate_limit=truncated_by_candidate_limit,
            execution_allowed=False,
            dry_run=True,
            warnings=list(warnings or []),
            blocking_errors=list(blocking_errors or []),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CodeDateMatrixPlan:
    summary: CodeDateMatrixSummary
    items: list[CodeDateMatrixItem]

    @property
    def blocked(self) -> bool:
        return bool(self.summary.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = self.summary.to_dict()
        data["blocked"] = self.blocked
        data["items"] = [item.to_dict() for item in self.items]
        return data
