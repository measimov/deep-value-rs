from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


EXECUTION_DECISIONS = {"allow", "dry_run_only", "blocked", "requires_user_confirmation", "unsupported"}
SUPPORTED_EXECUTABLE_PLANNER_KINDS = {
    "single_snapshot",
    "date_backfill",
    "calendar_backfill",
    "explicit_dates",
}
GUARDED_FINANCIAL_RAW_COMMANDS = {"financial-raw-fetch"}
FINANCIAL_RAW_SCOPES = {"a-share-financial-raw", "hk-financial-raw", "us-financial-raw"}
MAX_GUARDED_FINANCIAL_RAW_CODES = 20
MAX_GUARDED_FINANCIAL_RAW_JOBS = 100

BLOCKED_ENDPOINT_KINDS = {
    "financial_statement": "financial PIT infrastructure is required before financial statement execution",
    "financial_indicator": "financial PIT infrastructure is required before financial indicator execution",
    "minute_bar": "bucketed intraday storage and compaction policy are required before minute execution",
    "tick": "bucketed intraday storage and compaction policy are required before tick execution",
    "order": "bucketed intraday storage and compaction policy are required before order execution",
    "object_document": "object index/store infrastructure is required before object document execution",
    "text_news": "text/object retention policy is required before news or research text execution",
    "research_report": "text/object retention policy is required before research report execution",
    "announcement": "object index/store infrastructure is required before announcement execution",
    "html_text": "text/object retention policy is required before HTML text execution",
    "unknown_object_text": "object/text infrastructure is required before execution",
    "realtime": "realtime polling policy is required before realtime execution",
}

BLOCKED_PLANNER_KINDS = {
    "code_list": "explicit code-list guardrails are required before code-list execution",
    "code_date_matrix": "explicit code-list guardrails are required before code/date matrix execution",
    "period": "period planner guardrails are required before period execution",
    "code_period_matrix": "PIT-safe code/period matrix infrastructure is required before execution",
    "object_index": "object index infrastructure is required before execution",
    "object_download": "object store and object download policy are required before execution",
    "bucketed_intraday": "bucketed intraday storage and compaction policy are required before execution",
    "realtime_poll": "realtime polling policy is required before execution",
    "unsupported": "planner kind is unsupported",
}


@dataclass(frozen=True)
class ExecutionPolicyRequest:
    endpoint_config: Mapping[str, Any]
    scope: str | None = None
    mode: str | None = None
    user_command: str = "fetch"
    max_jobs: int | None = None
    requires_code_loop: bool | None = None
    requires_date_loop: bool | None = None
    requires_period_loop: bool | None = None
    requires_real_requests: bool = True
    requires_object_download: bool | None = None
    requires_pit_handling: bool | None = None
    requires_compaction_execution: bool | None = None
    max_codes_required: int | None = None


@dataclass(frozen=True)
class ExecutionPolicyDecision:
    decision: str
    api_name: str
    endpoint_kind: str | None
    planner_kind: str | None
    reason: str | None
    requires_real_requests: bool
    requires_user_confirmation: bool
    user_confirmation_required: bool
    requires_code_loop: bool
    requires_date_loop: bool
    requires_period_loop: bool
    requires_compaction_execution: bool
    max_codes_required: int | None
    execution_allowed: bool
    blocked_reason: str | None
    missing_infrastructure: list[str]
    warnings: list[str]

    @property
    def allowed(self) -> bool:
        return self.execution_allowed

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EndpointExecutionPolicy:
    DRY_RUN_COMMANDS = {
        "probe",
        "fetch-dry-run",
        "backfill-plan",
        "coverage",
        "mirror-plan",
        "mirror-review",
        "mirror-readiness",
        "mirror-batch-plan",
        "api-infra-readiness",
        "code-universe",
        "code-list-plan",
        "code-date-matrix-plan",
        "period-plan",
        "code-period-plan",
        "pit-readiness",
        "object-plan",
        "intraday-plan",
        "storage-estimate",
        "compaction-plan",
        "rate-policy",
        "endpoint-enable-checklist",
    }

    def decide(self, request: ExecutionPolicyRequest) -> ExecutionPolicyDecision:
        cfg = request.endpoint_config
        api_name = str(cfg.get("api_name") or "<unknown>")
        endpoint_kind = cfg.get("endpoint_kind")
        planner_kind = cfg.get("planner_kind")
        execution_status = str(cfg.get("execution_status") or "enabled")
        requires_code_loop = request.requires_code_loop
        if requires_code_loop is None:
            requires_code_loop = planner_kind in {"code_list", "code_date_matrix", "code_period_matrix"}
        requires_date_loop = request.requires_date_loop
        if requires_date_loop is None:
            requires_date_loop = planner_kind in {"code_date_matrix", "code_period_matrix"}
        requires_period_loop = request.requires_period_loop
        if requires_period_loop is None:
            requires_period_loop = planner_kind in {"period", "code_period_matrix"}
        requires_compaction_execution = request.requires_compaction_execution
        if requires_compaction_execution is None:
            requires_compaction_execution = False
        missing: list[str] = []
        warnings: list[str] = []

        if request.user_command in self.DRY_RUN_COMMANDS:
            return ExecutionPolicyDecision(
                decision="dry_run_only",
                api_name=api_name,
                endpoint_kind=str(endpoint_kind) if endpoint_kind else None,
                planner_kind=str(planner_kind) if planner_kind else None,
                reason="read_only_command",
                requires_real_requests=False,
                requires_user_confirmation=False,
                user_confirmation_required=False,
                requires_code_loop=bool(requires_code_loop),
                requires_date_loop=bool(requires_date_loop),
                requires_period_loop=bool(requires_period_loop),
                requires_compaction_execution=bool(requires_compaction_execution),
                max_codes_required=request.max_codes_required,
                execution_allowed=False,
                blocked_reason=None,
                missing_infrastructure=[],
                warnings=[],
            )

        if execution_status == "disabled":
            missing.extend(_list_required_infra(cfg))
            return self._decision(
                "blocked",
                api_name,
                endpoint_kind,
                planner_kind,
                "endpoint_disabled",
                request.requires_real_requests,
                missing,
                warnings,
                requires_code_loop=bool(requires_code_loop),
                requires_date_loop=bool(requires_date_loop),
                requires_period_loop=bool(requires_period_loop),
                requires_compaction_execution=bool(requires_compaction_execution),
                max_codes_required=request.max_codes_required,
            )
        if execution_status == "unsupported":
            return self._decision(
                "unsupported",
                api_name,
                endpoint_kind,
                planner_kind,
                "endpoint_unsupported",
                request.requires_real_requests,
                _list_required_infra(cfg),
                warnings,
                requires_code_loop=bool(requires_code_loop),
                requires_date_loop=bool(requires_date_loop),
                requires_period_loop=bool(requires_period_loop),
                requires_compaction_execution=bool(requires_compaction_execution),
                max_codes_required=request.max_codes_required,
            )
        if execution_status != "enabled":
            return self._decision(
                "blocked",
                api_name,
                endpoint_kind,
                planner_kind,
                f"unknown_execution_status:{execution_status}",
                request.requires_real_requests,
                _list_required_infra(cfg),
                warnings,
                requires_code_loop=bool(requires_code_loop),
                requires_date_loop=bool(requires_date_loop),
                requires_period_loop=bool(requires_period_loop),
                requires_compaction_execution=bool(requires_compaction_execution),
                max_codes_required=request.max_codes_required,
            )

        if request.user_command in GUARDED_FINANCIAL_RAW_COMMANDS:
            return self._financial_raw_decision(
                request,
                api_name,
                endpoint_kind,
                planner_kind,
                requires_code_loop=bool(requires_code_loop),
                requires_period_loop=bool(requires_period_loop),
                requires_compaction_execution=bool(requires_compaction_execution),
            )

        if endpoint_kind in BLOCKED_ENDPOINT_KINDS:
            missing.append(BLOCKED_ENDPOINT_KINDS[str(endpoint_kind)])
        if planner_kind in BLOCKED_PLANNER_KINDS:
            missing.append(BLOCKED_PLANNER_KINDS[str(planner_kind)])
        if planner_kind and planner_kind not in SUPPORTED_EXECUTABLE_PLANNER_KINDS:
            missing.append(f"planner kind is not executable: {planner_kind}")

        if requires_code_loop:
            missing.append("explicit code-list guardrails are required")
        if requires_date_loop:
            missing.append("explicit date-loop guardrails are required")
        if requires_period_loop:
            missing.append("explicit period-loop guardrails are required")

        requires_object_download = request.requires_object_download
        if requires_object_download is None:
            requires_object_download = endpoint_kind in {"object_document", "text_news"} or planner_kind in {"object_index", "object_download"}
        if requires_object_download:
            missing.append("object index/store policy is required")

        requires_pit_handling = request.requires_pit_handling
        if requires_pit_handling is None:
            requires_pit_handling = endpoint_kind in {"financial_statement", "financial_indicator"} or planner_kind == "code_period_matrix"
        if requires_pit_handling:
            missing.append("PIT safety infrastructure is required")
        if requires_compaction_execution:
            missing.append("compaction executor is required")

        if missing:
            deduped = sorted(set(missing))
            return self._decision(
                "blocked",
                api_name,
                endpoint_kind,
                planner_kind,
                "missing_required_infrastructure",
                request.requires_real_requests,
                deduped,
                warnings,
                requires_code_loop=bool(requires_code_loop),
                requires_date_loop=bool(requires_date_loop),
                requires_period_loop=bool(requires_period_loop),
                requires_compaction_execution=bool(requires_compaction_execution),
                max_codes_required=request.max_codes_required,
            )

        return ExecutionPolicyDecision(
            decision="allow",
            api_name=api_name,
            endpoint_kind=str(endpoint_kind) if endpoint_kind else None,
            planner_kind=str(planner_kind) if planner_kind else None,
            reason=None,
            requires_real_requests=request.requires_real_requests,
            requires_user_confirmation=request.requires_real_requests,
            user_confirmation_required=request.requires_real_requests,
            requires_code_loop=bool(requires_code_loop),
            requires_date_loop=bool(requires_date_loop),
            requires_period_loop=bool(requires_period_loop),
            requires_compaction_execution=bool(requires_compaction_execution),
            max_codes_required=request.max_codes_required,
            execution_allowed=True,
            blocked_reason=None,
            missing_infrastructure=[],
            warnings=warnings,
        )

    def _financial_raw_decision(
        self,
        request: ExecutionPolicyRequest,
        api_name: str,
        endpoint_kind: Any,
        planner_kind: Any,
        *,
        requires_code_loop: bool,
        requires_period_loop: bool,
        requires_compaction_execution: bool,
    ) -> ExecutionPolicyDecision:
        missing: list[str] = []
        warnings = [
            "financial raw execution requires an explicit guarded command",
            "raw financial output is not strategy-safe unless PIT usable-after fields are present",
        ]
        if request.scope not in FINANCIAL_RAW_SCOPES:
            missing.append(f"financial raw scope required: {', '.join(sorted(FINANCIAL_RAW_SCOPES))}")
        if endpoint_kind not in {"financial_statement", "financial_indicator"}:
            missing.append(f"financial endpoint kind required: {endpoint_kind}")
        if planner_kind not in {"code_period_matrix", "period"}:
            missing.append(f"code_period_matrix or period planner required: {planner_kind}")
        if requires_code_loop and request.max_codes_required is None:
            missing.append("max_codes_required is required for guarded financial raw execution")
        elif request.max_codes_required is not None and request.max_codes_required > MAX_GUARDED_FINANCIAL_RAW_CODES:
            missing.append(f"max_codes_required exceeds guarded limit: {MAX_GUARDED_FINANCIAL_RAW_CODES}")
        if request.max_jobs is not None and request.max_jobs > MAX_GUARDED_FINANCIAL_RAW_JOBS:
            missing.append(f"max_jobs exceeds guarded financial raw limit: {MAX_GUARDED_FINANCIAL_RAW_JOBS}")
        if requires_compaction_execution:
            missing.append("compaction execution is not allowed for guarded financial raw execution")
        if missing:
            return self._decision(
                "blocked",
                api_name,
                endpoint_kind,
                planner_kind,
                "financial_raw_guardrails_not_satisfied",
                request.requires_real_requests,
                sorted(set(missing)),
                warnings,
                requires_code_loop=requires_code_loop,
                requires_date_loop=False,
                requires_period_loop=requires_period_loop,
                requires_compaction_execution=requires_compaction_execution,
                max_codes_required=request.max_codes_required,
            )
        return ExecutionPolicyDecision(
            decision="allow",
            api_name=api_name,
            endpoint_kind=str(endpoint_kind) if endpoint_kind else None,
            planner_kind=str(planner_kind) if planner_kind else None,
            reason="guarded_financial_raw_execution",
            requires_real_requests=request.requires_real_requests,
            requires_user_confirmation=request.requires_real_requests,
            user_confirmation_required=request.requires_real_requests,
            requires_code_loop=requires_code_loop,
            requires_date_loop=False,
            requires_period_loop=requires_period_loop,
            requires_compaction_execution=requires_compaction_execution,
            max_codes_required=request.max_codes_required,
            execution_allowed=True,
            blocked_reason=None,
            missing_infrastructure=[],
            warnings=warnings,
        )

    def _decision(
        self,
        decision: str,
        api_name: str,
        endpoint_kind: Any,
        planner_kind: Any,
        reason: str,
        requires_real_requests: bool,
        missing_infrastructure: list[str],
        warnings: list[str],
        requires_code_loop: bool = False,
        requires_date_loop: bool = False,
        requires_period_loop: bool = False,
        requires_compaction_execution: bool = False,
        max_codes_required: int | None = None,
    ) -> ExecutionPolicyDecision:
        return ExecutionPolicyDecision(
            decision=decision,
            api_name=api_name,
            endpoint_kind=str(endpoint_kind) if endpoint_kind else None,
            planner_kind=str(planner_kind) if planner_kind else None,
            reason=reason,
            requires_real_requests=requires_real_requests,
            requires_user_confirmation=True,
            user_confirmation_required=True,
            requires_code_loop=requires_code_loop,
            requires_date_loop=requires_date_loop,
            requires_period_loop=requires_period_loop,
            requires_compaction_execution=requires_compaction_execution,
            max_codes_required=max_codes_required,
            execution_allowed=False,
            blocked_reason=reason,
            missing_infrastructure=missing_infrastructure,
            warnings=warnings,
        )


def _list_required_infra(cfg: Mapping[str, Any]) -> list[str]:
    value = cfg.get("required_infra") or []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]
