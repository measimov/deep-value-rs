from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .code_date_matrix_planner import MAX_CODE_DATE_MATRIX_CANDIDATES, MAX_CODE_DATE_MATRIX_CODES
from .code_universe import CodeUniverseProvider
from .endpoints import load_bundled_endpoint_configs, load_inventory_configs
from .hashing import job_key as make_job_key
from .periods import MAX_PERIODS, PeriodRangePlanner
from .pit import validate_pit_safety
from .planner import JobPlanner


MAX_CODE_PERIOD_CODES = MAX_CODE_DATE_MATRIX_CODES
MAX_CODE_PERIOD_PERIODS = MAX_PERIODS
MAX_CODE_PERIOD_CANDIDATES = MAX_CODE_DATE_MATRIX_CANDIDATES


@dataclass(frozen=True)
class CodePeriodPlanItem:
    api_name: str
    ts_code: str
    period: str
    params: dict[str, Any]
    job_key: str | None
    existing_status: str
    planned_action: str
    pit_required: bool
    pit_safety_status: str
    would_require_real_request: bool
    execution_allowed: bool
    blocked_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CodePeriodPlanSummary:
    api_name: str
    universe: str
    source_snapshot_id: str | None
    total_codes: int
    planned_codes: int
    total_periods: int
    planned_periods: int
    candidate_jobs: int
    planned_jobs: int
    truncated_by_code_limit: bool
    truncated_by_period_limit: bool
    truncated_by_candidate_limit: bool
    execution_allowed: bool
    dry_run: bool
    pit_required: bool
    pit_safety_status: str
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
        total_periods: int,
        limit_codes: int,
        max_periods: int,
        pit_required: bool,
        pit_safety_status: str,
        max_candidate_jobs: int = MAX_CODE_PERIOD_CANDIDATES,
        warnings: list[str] | None = None,
        blocking_errors: list[str] | None = None,
    ) -> "CodePeriodPlanSummary":
        planned_codes_before_candidate = min(max(total_codes, 0), max(limit_codes, 0), MAX_CODE_PERIOD_CODES)
        planned_periods_before_candidate = min(max(total_periods, 0), max(max_periods, 0), MAX_CODE_PERIOD_PERIODS)
        candidate_jobs = max(total_codes, 0) * max(total_periods, 0)
        truncated_by_code_limit = max(total_codes, 0) > planned_codes_before_candidate
        truncated_by_period_limit = max(total_periods, 0) > planned_periods_before_candidate
        planned_codes = planned_codes_before_candidate
        planned_periods = planned_periods_before_candidate
        max_jobs = max(max_candidate_jobs, 0)
        potential_jobs = planned_codes * planned_periods
        truncated_by_candidate_limit = potential_jobs > max_jobs
        if truncated_by_candidate_limit:
            planned_periods = max_jobs // planned_codes if planned_codes else 0
        planned_jobs = planned_codes * planned_periods
        return cls(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=source_snapshot_id,
            total_codes=max(total_codes, 0),
            planned_codes=planned_codes,
            total_periods=max(total_periods, 0),
            planned_periods=planned_periods,
            candidate_jobs=candidate_jobs,
            planned_jobs=planned_jobs,
            truncated_by_code_limit=truncated_by_code_limit,
            truncated_by_period_limit=truncated_by_period_limit,
            truncated_by_candidate_limit=truncated_by_candidate_limit,
            execution_allowed=False,
            dry_run=True,
            pit_required=pit_required,
            pit_safety_status=pit_safety_status,
            warnings=list(warnings or []),
            blocking_errors=list(blocking_errors or []),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CodePeriodPlan:
    summary: CodePeriodPlanSummary
    items: list[CodePeriodPlanItem]

    @property
    def blocked(self) -> bool:
        return bool(self.summary.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = self.summary.to_dict()
        data["blocked"] = self.blocked
        data["items"] = [item.to_dict() for item in self.items]
        return data


class CodePeriodPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        *,
        api_name: str,
        universe: str,
        limit_codes: int | None,
        periods: str | list[str] | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        period_frequency: str = "quarterly",
        max_periods: int = MAX_CODE_PERIOD_PERIODS,
        max_candidate_jobs: int = MAX_CODE_PERIOD_CANDIDATES,
    ) -> CodePeriodPlan:
        blocking_errors = self._validate_limits(limit_codes, max_periods, max_candidate_jobs)
        cfg = self._endpoint_config(api_name)
        catalog_enabled = self._catalog_has_endpoint(api_name)
        if cfg is None:
            blocking_errors.append("endpoint_not_found")
            return self._blocked(api_name, universe, blocking_errors=blocking_errors)

        planner_kind = str(cfg.get("planner_kind") or "unsupported")
        if planner_kind not in {"code_period_matrix", "period"}:
            blocking_errors.append(f"planner_kind_not_code_period_compatible:{planner_kind}")
        pit_result = validate_pit_safety(cfg)
        if pit_result.blocked:
            blocking_errors.extend(f"pit:{error}" for error in pit_result.errors)

        try:
            period_plan = PeriodRangePlanner().plan(
                periods=periods,
                start_period=start_period,
                end_period=end_period,
                period_frequency=period_frequency,
                max_periods=max_periods,
            )
            planned_periods = period_plan.periods
            total_period_count = period_plan.total_periods
        except ValueError as exc:
            planned_periods = []
            total_period_count = 0
            blocking_errors.append(str(exc))

        universe_result = CodeUniverseProvider(self.root, self.catalog).get(universe, limit=limit_codes or 0)
        if universe_result.blocked:
            blocking_errors.append(str(universe_result.blocked_reason))
            return self._blocked(
                api_name,
                universe,
                source_snapshot_id=universe_result.source_snapshot_id,
                total_periods=total_period_count,
                pit_required=pit_result.pit_required,
                pit_safety_status=pit_result.status,
                warnings=list(universe_result.warnings) + list(pit_result.warnings),
                blocking_errors=blocking_errors,
            )

        summary = CodePeriodPlanSummary.from_candidate_counts(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=universe_result.source_snapshot_id,
            total_codes=universe_result.code_count,
            total_periods=total_period_count,
            limit_codes=int(limit_codes or 0),
            max_periods=max_periods,
            max_candidate_jobs=max_candidate_jobs,
            pit_required=pit_result.pit_required,
            pit_safety_status=pit_result.status,
            warnings=list(universe_result.warnings) + list(pit_result.warnings),
            blocking_errors=blocking_errors,
        )
        if blocking_errors:
            return CodePeriodPlan(summary=summary, items=[])

        selected_codes = universe_result.codes[: summary.planned_codes]
        selected_periods = planned_periods[: summary.planned_periods]
        items: list[CodePeriodPlanItem] = []
        planner = JobPlanner(self.root, self.catalog) if catalog_enabled else None
        for ts_code in selected_codes:
            for period in selected_periods:
                params = self._params_for_period(cfg, ts_code, period)
                if planner:
                    fetch_plan = planner.plan_single_fetch(api_name, params)
                    job_key = fetch_plan.job_key
                    params = fetch_plan.params
                else:
                    job_key = make_job_key(api_name, params, [], f"inventory_{api_name}_code_period_v1")
                items.append(
                    CodePeriodPlanItem(
                        api_name=api_name,
                        ts_code=ts_code,
                        period=period,
                        params=params,
                        job_key=job_key,
                        existing_status="missing",
                        planned_action="fetch",
                        pit_required=pit_result.pit_required,
                        pit_safety_status=pit_result.status,
                        would_require_real_request=True,
                        execution_allowed=False,
                        blocked_reason=None,
                    )
                )
        return CodePeriodPlan(summary=summary, items=items)

    def _validate_limits(self, limit_codes: int | None, max_periods: int, max_candidate_jobs: int) -> list[str]:
        errors: list[str] = []
        if limit_codes is None:
            errors.append("limit_codes_required")
        elif limit_codes <= 0:
            errors.append("limit_codes_must_be_positive")
        elif limit_codes > MAX_CODE_PERIOD_CODES:
            errors.append(f"limit_codes_exceeds_phase_limit:{MAX_CODE_PERIOD_CODES}")
        if max_periods <= 0:
            errors.append("max_periods_must_be_positive")
        elif max_periods > MAX_CODE_PERIOD_PERIODS:
            errors.append(f"max_periods_exceeds_phase_limit:{MAX_CODE_PERIOD_PERIODS}")
        if max_candidate_jobs <= 0:
            errors.append("max_candidate_jobs_must_be_positive")
        elif max_candidate_jobs > MAX_CODE_PERIOD_CANDIDATES:
            errors.append(f"max_candidate_jobs_exceeds_phase_limit:{MAX_CODE_PERIOD_CANDIDATES}")
        return errors

    def _endpoint_config(self, api_name: str) -> dict[str, Any] | None:
        if self.catalog.db_path.exists():
            try:
                cfg = self.catalog.get_endpoint_config(api_name)
            except KeyError:
                cfg = None
            if cfg:
                return dict(cfg)
        for cfg in load_bundled_endpoint_configs():
            if cfg.get("api_name") == api_name:
                return dict(cfg)
        for cfg in load_inventory_configs():
            if cfg.get("api_name") == api_name:
                return dict(cfg)
        return None

    def _catalog_has_endpoint(self, api_name: str) -> bool:
        return bool(self.catalog.db_path.exists() and self.catalog.get_endpoint(api_name))

    def _params_for_period(self, cfg: dict[str, Any], ts_code: str, period: str) -> dict[str, Any]:
        period_field = cfg.get("period_field") or (cfg.get("pit_safety") or {}).get("period_field") or "period"
        return {"ts_code": ts_code, str(period_field): period}

    def _blocked(
        self,
        api_name: str,
        universe: str,
        *,
        source_snapshot_id: str | None = None,
        total_periods: int = 0,
        pit_required: bool = False,
        pit_safety_status: str = "not_required",
        warnings: list[str] | None = None,
        blocking_errors: list[str] | None = None,
    ) -> CodePeriodPlan:
        summary = CodePeriodPlanSummary.from_candidate_counts(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=source_snapshot_id,
            total_codes=0,
            total_periods=total_periods,
            limit_codes=0,
            max_periods=0,
            pit_required=pit_required,
            pit_safety_status=pit_safety_status,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )
        return CodePeriodPlan(summary=summary, items=[])
