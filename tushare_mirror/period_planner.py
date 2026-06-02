from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .endpoints import load_bundled_endpoint_configs, load_inventory_configs
from .hashing import job_key as make_job_key
from .periods import MAX_PERIODS, PeriodRangePlanner
from .pit import validate_pit_safety
from .planner import JobPlanner


@dataclass(frozen=True)
class PeriodPlanItem:
    api_name: str
    period: str
    params: dict[str, Any]
    job_key: str | None
    existing_status: str
    planned_action: str
    execution_allowed: bool
    blocked_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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
    items: list[PeriodPlanItem]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        data["items"] = [item.to_dict() for item in self.items]
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
        catalog_enabled = self._catalog_has_endpoint(api_name)
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
        items = self._plan_items(api_name, cfg, planned_periods, catalog_enabled) if not blocking_errors else []
        return self._result(
            api_name=api_name,
            planner_kind=planner_kind,
            periods=planned_periods,
            max_periods=max_periods,
            pit_required=pit_result.pit_required,
            pit_safety_status=pit_result.status,
            warnings=warnings,
            blocking_errors=blocking_errors,
            items=items,
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

    def _catalog_has_endpoint(self, api_name: str) -> bool:
        if self.catalog is None or not self.catalog.db_path.exists():
            return False
        return bool(self.catalog.get_endpoint(api_name))

    def _plan_items(self, api_name: str, cfg: dict[str, Any], periods: list[str], catalog_enabled: bool) -> list[PeriodPlanItem]:
        items: list[PeriodPlanItem] = []
        planner = JobPlanner(self.root, self.catalog) if catalog_enabled and self.root is not None and self.catalog else None
        for period in periods:
            params = self._params_for_period(cfg, period)
            if planner:
                fetch_plan = planner.plan_single_fetch(api_name, params)
                job_key = fetch_plan.job_key
                params = fetch_plan.params
                existing_status, planned_action = self._existing_status_and_action(api_name, job_key, fetch_plan.existing_active_data)
            else:
                job_key = make_job_key(api_name, params, [], f"inventory_{api_name}_period_v1")
                existing_status, planned_action = "missing", "fetch"
            items.append(
                PeriodPlanItem(
                    api_name=api_name,
                    period=period,
                    params=params,
                    job_key=job_key,
                    existing_status=existing_status,
                    planned_action=planned_action,
                    execution_allowed=False,
                    blocked_reason=self._blocked_reason_for_action(planned_action),
                )
            )
        return items

    def _params_for_period(self, cfg: dict[str, Any], period: str) -> dict[str, Any]:
        period_field = cfg.get("period_field") or (cfg.get("pit_safety") or {}).get("period_field") or "period"
        return {str(period_field): period}

    def _existing_status_and_action(self, api_name: str, job_key: str, active_exists: bool) -> tuple[str, str]:
        if active_exists:
            return "active_exists", "skip_existing"
        if self.catalog and self.catalog.quarantine_exists_for_job(job_key):
            return "quarantined_exists", "blocked_quarantined"
        statuses = self.catalog.file_statuses_for_job(job_key, api_name) if self.catalog else set()
        if "staged" in statuses:
            return "staged_exists", "blocked_staged"
        job = self.catalog.get_job(job_key) if self.catalog else None
        if job and job.get("status") == "failed":
            return "failed_exists", "retry_failed"
        if job:
            return "unknown", "fetch"
        return "missing", "fetch"

    def _blocked_reason_for_action(self, planned_action: str) -> str | None:
        if planned_action == "blocked_quarantined":
            return "quarantined_exists"
        if planned_action == "blocked_staged":
            return "staged_exists"
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
        items: list[PeriodPlanItem] | None = None,
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
            items=list(items or []),
        )
