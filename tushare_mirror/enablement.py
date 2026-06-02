from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .endpoints import load_bundled_endpoint_configs, load_inventory_configs
from .policy import EndpointExecutionPolicy, ExecutionPolicyRequest


@dataclass(frozen=True)
class EndpointEnablementChecklist:
    api_name: str
    endpoint_kind: str | None
    planner_kind: str | None
    current_execution_status: str | None
    required_infra: list[str]
    required_tests: list[str]
    required_smoke_steps: list[str]
    allowed_next_action: str
    forbidden_actions: list[str]
    risk_level: str
    execution_allowed: bool
    warnings: list[str]
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        return data


class EndpointEnablementChecklistReporter:
    def report(self, api_name: str) -> EndpointEnablementChecklist:
        cfg = _endpoint_config(api_name)
        if cfg is None:
            return EndpointEnablementChecklist(
                api_name=api_name,
                endpoint_kind=None,
                planner_kind=None,
                current_execution_status=None,
                required_infra=[],
                required_tests=[],
                required_smoke_steps=[],
                allowed_next_action="none",
                forbidden_actions=["fetch", "mirror-run --execute", "backfill"],
                risk_level="unknown",
                execution_allowed=False,
                warnings=[],
                blocking_errors=["endpoint_not_found"],
            )
        decision = EndpointExecutionPolicy().decide(
            ExecutionPolicyRequest(
                endpoint_config=cfg,
                scope="low-risk-a-share",
                mode="pilot",
                user_command="fetch",
                max_jobs=1,
            )
        )
        execution_status = str(cfg.get("execution_status") or "enabled")
        endpoint_kind = str(cfg.get("endpoint_kind") or "unknown")
        planner_kind = str(cfg.get("planner_kind") or "unsupported")
        low_risk_enabled = execution_status == "enabled" and decision.execution_allowed
        required_infra = [str(item) for item in cfg.get("required_infra") or []]
        required_infra.extend(decision.missing_infrastructure)
        return EndpointEnablementChecklist(
            api_name=api_name,
            endpoint_kind=endpoint_kind,
            planner_kind=planner_kind,
            current_execution_status=execution_status,
            required_infra=sorted(set(required_infra)),
            required_tests=_required_tests(endpoint_kind, planner_kind, low_risk_enabled),
            required_smoke_steps=_required_smoke_steps(endpoint_kind, planner_kind, low_risk_enabled),
            allowed_next_action="use existing bounded low-risk command" if low_risk_enabled else "complete infrastructure and fake tests before enablement",
            forbidden_actions=_forbidden_actions(endpoint_kind, planner_kind, low_risk_enabled),
            risk_level=str(cfg.get("risk_level") or ("low" if low_risk_enabled else "unknown")),
            execution_allowed=low_risk_enabled,
            warnings=decision.warnings,
            blocking_errors=[] if low_risk_enabled else [decision.reason or decision.blocked_reason or "execution_blocked"],
        )


def _endpoint_config(api_name: str) -> dict[str, Any] | None:
    for cfg in load_bundled_endpoint_configs():
        if cfg.get("api_name") == api_name:
            return dict(cfg)
    for cfg in load_inventory_configs():
        if cfg.get("api_name") == api_name:
            return dict(cfg)
    return None


def _required_tests(endpoint_kind: str, planner_kind: str, low_risk_enabled: bool) -> list[str]:
    base = ["endpoint config validation", "policy decision JSON", "no side effects for plan commands"]
    if low_risk_enabled:
        return base + ["existing low-risk fetch/backfill fake tests", "backup and restore-check regression"]
    if endpoint_kind in {"financial_statement", "financial_indicator"} or planner_kind == "code_period_matrix":
        return base + ["PIT metadata tests", "period-plan tests", "code-period-plan tests", "tiny fake client contract"]
    if endpoint_kind in {"object_document", "text_news", "research_report", "announcement", "html_text"}:
        return base + ["object metadata tests", "object-plan tests", "object storage policy tests"]
    if endpoint_kind in {"minute_bar", "tick", "order", "realtime"} or planner_kind == "bucketed_intraday":
        return base + ["intraday bucket metadata tests", "intraday-plan tests", "storage estimate tests", "compaction readiness tests"]
    return base + ["planner-specific fake tests", "small user-confirmed smoke plan"]


def _required_smoke_steps(endpoint_kind: str, planner_kind: str, low_risk_enabled: bool) -> list[str]:
    if low_risk_enabled:
        return ["run mirror-plan", "run user-confirmed bounded mirror-run or scoped backfill", "validate --no-record", "backup and restore-check"]
    if endpoint_kind in {"object_document", "text_news", "research_report", "announcement", "html_text"}:
        return ["object-plan only", "user-confirmed metadata/index smoke later", "no object download until object store exists"]
    if endpoint_kind in {"minute_bar", "tick", "order", "realtime"} or planner_kind == "bucketed_intraday":
        return ["intraday-plan only", "storage-estimate", "rate-policy review", "no intraday fetch until bucket/compaction policy exists"]
    if endpoint_kind in {"financial_statement", "financial_indicator"}:
        return ["pit-readiness", "period-plan", "code-period-plan for 1-3 codes x 1-3 periods", "user-confirmed tiny real smoke later"]
    return ["plan-only command", "fake tests", "explicit user confirmation before any real request"]


def _forbidden_actions(endpoint_kind: str, planner_kind: str, low_risk_enabled: bool) -> list[str]:
    common = ["bulk enablement", "full mirror", "stock loop without explicit bounded limits"]
    if low_risk_enabled:
        return common
    if endpoint_kind in {"object_document", "text_news", "research_report", "announcement", "html_text"}:
        return common + ["object download", "PDF/news/research content fetch"]
    if endpoint_kind in {"minute_bar", "tick", "order", "realtime"} or planner_kind == "bucketed_intraday":
        return common + ["minute/tick/order fetch", "realtime polling", "compaction execution"]
    if endpoint_kind in {"financial_statement", "financial_indicator"}:
        return common + ["financial fetch", "PIT execution", "PostgreSQL loader"]
    return common + ["real request before fake tests"]
