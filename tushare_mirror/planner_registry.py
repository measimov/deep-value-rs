from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from .backfill import BackfillPlanner
from .capabilities import PLANNER_KIND_VALUES
from .catalog import CatalogStore
from .code_date_matrix_planner import CodeDateMatrixPlanner
from .planner import JobPlanner


SUPPORTED_PLANNER_KINDS = {
    "single_snapshot",
    "date_backfill",
    "calendar_backfill",
    "explicit_dates",
    "code_date_matrix",
}

BLOCKED_PLANNER_INFRA = {
    "code_list": "bounded code-list planner with explicit list source and max-code guardrails",
    "code_date_matrix": "bounded code/date matrix planner with explicit code-list guardrails",
    "period": "period planner with accounting-period boundaries and max-period guardrails",
    "code_period_matrix": "bounded code/period matrix planner with code-list and period guardrails",
    "object_index": "object index planner with object metadata policy",
    "object_download": "object download planner with local object store and size limits",
    "bucketed_intraday": "intraday bucket planner with bucket storage and compaction policy",
    "realtime_poll": "realtime polling policy with rate limits and retention rules",
    "unsupported": "endpoint-specific planner support",
}

REAL_REQUEST_PLANNERS = PLANNER_KIND_VALUES - {"unsupported"}


@dataclass(frozen=True)
class PlannerRegistryRequest:
    api_name: str
    planner_kind: str
    params: dict[str, Any] | None = None
    dates: list[str] | None = None
    max_jobs: int = 1
    fields: list[str] | None = None
    calendar_metadata: dict[str, Any] | None = None
    universe: str | None = None
    limit_codes: int | None = None
    max_dates: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    trading_days_only: bool = False
    calendar_exchange: str = "SSE"


@dataclass(frozen=True)
class PlannerRegistryResult:
    api_name: str
    planner_kind: str
    status: str
    plan_type: str
    planned_jobs: int
    blocked_reason: str | None
    missing_infrastructure: str | None
    requires_real_requests: bool
    requires_user_confirmation: bool
    plan: dict[str, Any] | None
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PlannerRegistry:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(self, request: PlannerRegistryRequest) -> PlannerRegistryResult:
        kind = request.planner_kind
        if kind not in PLANNER_KIND_VALUES:
            return self._blocked(
                request.api_name,
                kind,
                "unknown_planner_kind",
                "planner kind is not registered in the capability taxonomy",
            )
        if kind not in SUPPORTED_PLANNER_KINDS:
            return self._blocked(
                request.api_name,
                kind,
                "planner_infrastructure_missing",
                BLOCKED_PLANNER_INFRA.get(kind, "planner infrastructure is missing"),
            )
        if kind == "single_snapshot":
            plan = JobPlanner(self.root, self.catalog).plan_single_fetch(
                request.api_name,
                request.params or {},
                request.fields,
            )
            return PlannerRegistryResult(
                api_name=request.api_name,
                planner_kind=kind,
                status="supported",
                plan_type="single_snapshot",
                planned_jobs=0 if plan.existing_active_data else 1,
                blocked_reason=None,
                missing_infrastructure=None,
                requires_real_requests=not plan.existing_active_data,
                requires_user_confirmation=not plan.existing_active_data,
                plan=plan.to_dict(),
                warnings=[],
            )
        dates = list(request.dates or [])
        if kind == "code_date_matrix":
            plan = CodeDateMatrixPlanner(self.root, self.catalog).plan(
                api_name=request.api_name,
                universe=request.universe or "",
                limit_codes=request.limit_codes,
                dates=dates,
                start_date=request.start_date,
                end_date=request.end_date,
                max_dates=request.max_dates,
                trading_days_only=request.trading_days_only,
                calendar_exchange=request.calendar_exchange,
            )
            return PlannerRegistryResult(
                api_name=request.api_name,
                planner_kind=kind,
                status="plan_only" if not plan.blocked else "blocked",
                plan_type=kind if not plan.blocked else "blocked",
                planned_jobs=plan.summary.planned_jobs if not plan.blocked else 0,
                blocked_reason=";".join(plan.summary.blocking_errors) if plan.blocked else None,
                missing_infrastructure=None if not plan.blocked else ";".join(plan.summary.blocking_errors),
                requires_real_requests=not plan.blocked and plan.summary.planned_jobs > 0,
                requires_user_confirmation=not plan.blocked and plan.summary.planned_jobs > 0,
                plan=plan.to_dict(),
                warnings=list(plan.summary.warnings),
            )
        if kind in {"date_backfill", "calendar_backfill", "explicit_dates"}:
            plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(
                request.api_name,
                dates,
                max_jobs=request.max_jobs,
                fields=request.fields,
                calendar_metadata=request.calendar_metadata,
            )
            planned_fetches = sum(1 for item in plan.planned_jobs if item.planned_action in {"fetch", "retry_failed"})
            return PlannerRegistryResult(
                api_name=request.api_name,
                planner_kind=kind,
                status="supported",
                plan_type=kind,
                planned_jobs=planned_fetches,
                blocked_reason=None,
                missing_infrastructure=None,
                requires_real_requests=planned_fetches > 0,
                requires_user_confirmation=planned_fetches > 0,
                plan=plan.to_dict(),
                warnings=list(plan.warnings),
            )
        return self._blocked(request.api_name, kind, "unsupported_planner_kind", "planner kind is unsupported")

    def blocked_plan(self, api_name: str, planner_kind: str) -> PlannerRegistryResult:
        return self._blocked(
            api_name,
            planner_kind,
            "planner_infrastructure_missing" if planner_kind in BLOCKED_PLANNER_INFRA else "unknown_planner_kind",
            BLOCKED_PLANNER_INFRA.get(planner_kind, "planner kind is not registered in the capability taxonomy"),
        )

    def _blocked(self, api_name: str, planner_kind: str, reason: str, missing_infrastructure: str) -> PlannerRegistryResult:
        return PlannerRegistryResult(
            api_name=api_name,
            planner_kind=planner_kind,
            status="blocked",
            plan_type="blocked",
            planned_jobs=0,
            blocked_reason=reason,
            missing_infrastructure=missing_infrastructure,
            requires_real_requests=planner_kind in REAL_REQUEST_PLANNERS,
            requires_user_confirmation=planner_kind in PLANNER_KIND_VALUES,
            plan=None,
            warnings=[],
        )


def planner_registry_summary() -> dict[str, Any]:
    blocked = sorted(kind for kind in PLANNER_KIND_VALUES if kind not in SUPPORTED_PLANNER_KINDS)
    return {
        "supported_planner_kinds": sorted(SUPPORTED_PLANNER_KINDS),
        "blocked_planner_kinds": blocked,
        "blocked_missing_infrastructure": {kind: BLOCKED_PLANNER_INFRA.get(kind) for kind in blocked},
    }
