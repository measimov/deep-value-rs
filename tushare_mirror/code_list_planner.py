from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .code_universe import CodeUniverseProvider
from .endpoints import load_inventory_configs
from .planner import JobPlanner

MAX_CODE_LIST_PLAN_CODES = 20


@dataclass(frozen=True)
class CodeListPlanItem:
    api_name: str
    ts_code: str | None
    params: dict[str, Any]
    job_key: str | None
    existing_status: str
    planned_action: str
    blocked_reason: str | None
    would_require_real_request: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CodeListPlan:
    api_name: str
    universe: str
    source_snapshot_id: str | None
    total_codes: int
    planned_codes: int
    candidate_jobs: int
    blocked: bool
    blocked_reason: str | None
    warnings: list[str]
    dry_run: bool
    execution_allowed: bool
    items: list[CodeListPlanItem]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["items"] = [item.to_dict() for item in self.items]
        return data


class CodeListPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        api_name: str,
        universe: str,
        limit_codes: int | None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> CodeListPlan:
        if limit_codes is None:
            return self._blocked(api_name, universe, "limit_codes_required")
        if limit_codes <= 0:
            return self._blocked(api_name, universe, "limit_codes_must_be_positive")
        if limit_codes > MAX_CODE_LIST_PLAN_CODES:
            return self._blocked(api_name, universe, f"limit_codes_exceeds_phase_limit:{MAX_CODE_LIST_PLAN_CODES}")

        endpoint = self.catalog.get_endpoint(api_name)
        if not endpoint:
            inventory = {item["api_name"]: item for item in load_inventory_configs()}
            if api_name in inventory:
                return self._blocked(api_name, universe, "endpoint_disabled_inventory")
            return self._blocked(api_name, universe, "endpoint_not_found")
        cfg = self.catalog.get_endpoint_config(api_name)
        execution_status = str(cfg.get("execution_status") or "enabled")
        if execution_status != "enabled":
            return self._blocked(api_name, universe, "endpoint_not_enabled")
        supported_params = set(cfg.get("supported_params") or [])
        if "ts_code" not in supported_params:
            return self._blocked(api_name, universe, "endpoint_does_not_support_ts_code")
        planner_kind = str(cfg.get("planner_kind") or "single_snapshot")
        if planner_kind not in {"single_snapshot", "code_list", "code_date_matrix"}:
            return self._blocked(api_name, universe, f"planner_kind_not_code_list_compatible:{planner_kind}")

        universe_result = CodeUniverseProvider(self.root, self.catalog).get(universe, limit=limit_codes)
        if universe_result.blocked:
            return CodeListPlan(
                api_name=api_name,
                universe=universe,
                source_snapshot_id=universe_result.source_snapshot_id,
                total_codes=0,
                planned_codes=0,
                candidate_jobs=0,
                blocked=True,
                blocked_reason=universe_result.blocked_reason,
                warnings=list(universe_result.warnings),
                dry_run=True,
                execution_allowed=False,
                items=[],
            )

        planner = JobPlanner(self.root, self.catalog)
        selected_codes = universe_result.codes[:limit_codes]
        items: list[CodeListPlanItem] = []
        for ts_code in selected_codes:
            params: dict[str, Any] = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            fetch_plan = planner.plan_single_fetch(api_name, params)
            if fetch_plan.existing_active_data:
                existing_status = "active_exists"
                planned_action = "skip_existing"
                would_require = False
            else:
                existing_status = "missing"
                planned_action = "fetch"
                would_require = True
            items.append(
                CodeListPlanItem(
                    api_name=api_name,
                    ts_code=ts_code,
                    params=fetch_plan.params,
                    job_key=fetch_plan.job_key,
                    existing_status=existing_status,
                    planned_action=planned_action,
                    blocked_reason=None,
                    would_require_real_request=would_require,
                )
            )
        return CodeListPlan(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=universe_result.source_snapshot_id,
            total_codes=universe_result.code_count,
            planned_codes=len(selected_codes),
            candidate_jobs=len(items),
            blocked=False,
            blocked_reason=None,
            warnings=list(universe_result.warnings),
            dry_run=True,
            execution_allowed=False,
            items=items,
        )

    def _blocked(self, api_name: str, universe: str, reason: str) -> CodeListPlan:
        return CodeListPlan(
            api_name=api_name,
            universe=universe,
            source_snapshot_id=None,
            total_codes=0,
            planned_codes=0,
            candidate_jobs=0,
            blocked=True,
            blocked_reason=reason,
            warnings=[],
            dry_run=True,
            execution_allowed=False,
            items=[],
        )
