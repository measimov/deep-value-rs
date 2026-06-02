from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .endpoints import load_bundled_endpoint_configs, load_inventory_configs
from .periods import MAX_PERIODS, PeriodRangePlanner
from .pit import validate_pit_safety


@dataclass(frozen=True)
class PeriodPlan:
    api_name: str
    planner_kind: str | None
    periods: list[str]
    period_count: int
    max_periods: int
    candidate_jobs: int
    execution_allowed: bool
    dry_run: bool
    pit_required: bool
    pit_safety_status: str
    blocked_reason: str | None
    warnings: list[str]
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        return data


class PeriodPlanner:
    def __init__(self, root: Path | str | None = None, catalog: CatalogStore | None = None):
        self.root = Path(root) if root is not None else None
        self.catalog = catalog

    def plan(
        self,
        *,
        api_name: str,
        periods: str | list[str] | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        period_frequency: str = "quarterly",
        max_periods: int = MAX_PERIODS,
    ) -> PeriodPlan:
        warnings: list[str] = []
        blocking_errors: list[str] = []
        try:
            period_plan = PeriodRangePlanner().plan(
                periods=periods,
                start_period=start_period,
                end_period=end_period,
                period_frequency=period_frequency,
                max_periods=max_periods,
            )
        except ValueError as exc:
            period_plan = None
            blocking_errors.append(str(exc))

        cfg = self._endpoint_config(api_name)
        if cfg is None:
            blocking_errors.append("endpoint_not_found")
            return self._result(
                api_name=api_name,
                planner_kind=None,
                periods=[],
                max_periods=max_periods,
                pit_required=False,
                pit_safety_status="not_required",
                warnings=warnings,
                blocking_errors=blocking_errors,
            )

        planner_kind = str(cfg.get("planner_kind") or "unsupported")
        if planner_kind not in {"period", "code_period_matrix"} and str(cfg.get("period_strategy") or "none") == "none":
            blocking_errors.append(f"planner_kind_not_period_compatible:{planner_kind}")

        pit_result = validate_pit_safety(cfg)
        if pit_result.blocked:
            blocking_errors.extend(f"pit:{error}" for error in pit_result.errors)
        warnings.extend(pit_result.warnings)

        planned_periods = period_plan.periods if period_plan else []
        return self._result(
            api_name=api_name,
            planner_kind=planner_kind,
            periods=planned_periods,
            max_periods=max_periods,
            pit_required=pit_result.pit_required,
            pit_safety_status=pit_result.status,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    def _endpoint_config(self, api_name: str) -> dict[str, Any] | None:
        if self.catalog is not None and self.catalog.db_path.exists():
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

    def _result(
        self,
        *,
        api_name: str,
        planner_kind: str | None,
        periods: list[str],
        max_periods: int,
        pit_required: bool,
        pit_safety_status: str,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> PeriodPlan:
        return PeriodPlan(
            api_name=api_name,
            planner_kind=planner_kind,
            periods=periods,
            period_count=len(periods),
            max_periods=max_periods,
            candidate_jobs=len(periods),
            execution_allowed=False,
            dry_run=True,
            pit_required=pit_required,
            pit_safety_status=pit_safety_status,
            blocked_reason=";".join(blocking_errors) if blocking_errors else None,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )
