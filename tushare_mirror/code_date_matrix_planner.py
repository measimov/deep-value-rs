from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .backfill import DatePlanner
from .catalog import CatalogStore
from .code_universe import CodeUniverseProvider
from .endpoints import load_inventory_configs
from .planner import JobPlanner


MAX_CODE_DATE_MATRIX_CODES = 20
MAX_CODE_DATE_MATRIX_DATES = 20
MAX_CODE_DATE_MATRIX_CANDIDATES = 100
PHASE2_CODE_DATE_MATRIX_PLAN_APIS = {"namechange", "stk_managers", "stk_rewards"}


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


class CodeDateMatrixPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        *,
        api_name: str,
        universe: str,
        limit_codes: int | None,
        dates: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        max_dates: int | None = None,
        max_candidate_jobs: int = MAX_CODE_DATE_MATRIX_CANDIDATES,
        trading_days_only: bool = False,
        calendar_exchange: str = "SSE",
    ) -> CodeDateMatrixPlan:
        blocking_errors = self._validate_limits(limit_codes, max_dates, max_candidate_jobs)
        bounded_max_dates = max_dates if max_dates is not None else MAX_CODE_DATE_MATRIX_DATES
        if api_name not in PHASE2_CODE_DATE_MATRIX_PLAN_APIS:
            blocking_errors.append(f"code_date_matrix_plan_not_supported_for_api:{api_name}")

        endpoint = self.catalog.get_endpoint(api_name)
        if not endpoint:
            inventory = {item["api_name"]: item for item in load_inventory_configs()}
            reason = "endpoint_disabled_inventory" if api_name in inventory else "endpoint_not_found"
            blocking_errors.append(reason)
            return self._blocked(
                api_name=api_name,
                universe=universe,
                blocking_errors=blocking_errors,
            )

        cfg = self.catalog.get_endpoint_config(api_name)
        execution_status = str(cfg.get("execution_status") or "enabled")
        if execution_status != "enabled":
            blocking_errors.append("endpoint_not_enabled")
        supported_params = set(cfg.get("supported_params") or [])
        if "ts_code" not in supported_params:
            blocking_errors.append("endpoint_does_not_support_ts_code")
        planner_kind = str(cfg.get("planner_kind") or "single_snapshot")
        if planner_kind not in {"single_snapshot", "code_list", "code_date_matrix"}:
            blocking_errors.append(f"planner_kind_not_code_date_matrix_compatible:{planner_kind}")

        try:
            planned_dates, calendar_metadata = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(
                dates=dates,
                start_date=start_date,
                end_date=end_date,
                trading_days_only=trading_days_only,
                calendar_exchange=calendar_exchange,
            )
        except ValueError as exc:
            planned_dates = []
            calendar_metadata = None
            blocking_errors.append(str(exc))

        if blocking_errors:
            return self._blocked(
                api_name=api_name,
                universe=universe,
                total_dates=len(planned_dates),
                blocking_errors=blocking_errors,
            )

        universe_result = CodeUniverseProvider(self.root, self.catalog).get(universe, limit=limit_codes)
        if universe_result.blocked:
            return self._blocked(
                api_name=api_name,
                universe=universe,
                source_snapshot_id=universe_result.source_snapshot_id,
                total_dates=len(planned_dates),
                warnings=list(universe_result.warnings),
                blocking_errors=[str(universe_result.blocked_reason)],
            )

        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=universe_result.source_snapshot_id,
            total_codes=universe_result.code_count,
            total_dates=len(planned_dates),
            limit_codes=int(limit_codes or 0),
            max_dates=int(bounded_max_dates),
            max_candidate_jobs=max_candidate_jobs,
            warnings=self._warnings(calendar_metadata),
        )
        selected_codes = universe_result.codes[: summary.planned_codes]
        selected_dates = planned_dates[: summary.planned_dates]
        job_planner = JobPlanner(self.root, self.catalog)
        items: list[CodeDateMatrixItem] = []
        for ts_code in selected_codes:
            for date in selected_dates:
                params = self._params_for_date(cfg, ts_code, date)
                fetch_plan = job_planner.plan_single_fetch(api_name, params)
                existing_status = "active_exists" if fetch_plan.existing_active_data else "missing"
                planned_action = "skip_existing" if fetch_plan.existing_active_data else "fetch"
                items.append(
                    CodeDateMatrixItem(
                        api_name=api_name,
                        ts_code=ts_code,
                        date=date,
                        params=fetch_plan.params,
                        job_key=fetch_plan.job_key,
                        existing_status=existing_status,
                        planned_action=planned_action,
                        would_require_real_request=not fetch_plan.existing_active_data,
                        execution_allowed=False,
                    )
                )
        return CodeDateMatrixPlan(summary=summary, items=items)

    def _validate_limits(self, limit_codes: int | None, max_dates: int | None, max_candidate_jobs: int) -> list[str]:
        errors: list[str] = []
        if limit_codes is None:
            errors.append("limit_codes_required")
        elif limit_codes <= 0:
            errors.append("limit_codes_must_be_positive")
        elif limit_codes > MAX_CODE_DATE_MATRIX_CODES:
            errors.append(f"limit_codes_exceeds_phase_limit:{MAX_CODE_DATE_MATRIX_CODES}")
        if max_dates is not None:
            if max_dates <= 0:
                errors.append("max_dates_must_be_positive")
            elif max_dates > MAX_CODE_DATE_MATRIX_DATES:
                errors.append(f"max_dates_exceeds_phase_limit:{MAX_CODE_DATE_MATRIX_DATES}")
        if max_candidate_jobs <= 0:
            errors.append("max_candidate_jobs_must_be_positive")
        elif max_candidate_jobs > MAX_CODE_DATE_MATRIX_CANDIDATES:
            errors.append(f"max_candidate_jobs_exceeds_phase_limit:{MAX_CODE_DATE_MATRIX_CANDIDATES}")
        return errors

    def _blocked(
        self,
        *,
        api_name: str,
        universe: str,
        source_snapshot_id: str | None = None,
        total_dates: int = 0,
        warnings: list[str] | None = None,
        blocking_errors: list[str] | None = None,
    ) -> CodeDateMatrixPlan:
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=source_snapshot_id,
            total_codes=0,
            total_dates=total_dates,
            limit_codes=0,
            max_dates=0,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )
        return CodeDateMatrixPlan(summary=summary, items=[])

    def _params_for_date(self, cfg: dict[str, Any], ts_code: str, date: str) -> dict[str, Any]:
        supported_params = set(cfg.get("supported_params") or [])
        params: dict[str, Any] = {"ts_code": ts_code}
        if "trade_date" in supported_params:
            params["trade_date"] = date
        elif "ann_date" in supported_params:
            params["ann_date"] = date
        elif "end_date" in supported_params and "start_date" not in supported_params:
            params["end_date"] = date
        elif {"start_date", "end_date"}.issubset(supported_params):
            params["start_date"] = date
            params["end_date"] = date
        elif "end_date" in supported_params:
            params["end_date"] = date
        else:
            params["date"] = date
        return params

    def _warnings(self, calendar_metadata: dict[str, Any] | None) -> list[str]:
        if not calendar_metadata:
            return []
        return [
            f"calendar_source={calendar_metadata.get('calendar_source')}",
            f"filtered_non_trading_days={calendar_metadata.get('filtered_non_trading_days')}",
        ]
