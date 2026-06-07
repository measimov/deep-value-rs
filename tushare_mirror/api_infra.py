from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .capabilities import ENDPOINT_KIND_VALUES, normalize_endpoint_capability
from .code_date_matrix_planner import (
    MAX_CODE_DATE_MATRIX_CANDIDATES,
    MAX_CODE_DATE_MATRIX_CODES,
    MAX_CODE_DATE_MATRIX_DATES,
)
from .code_list_planner import MAX_CODE_LIST_PLAN_CODES
from .code_period_planner import MAX_CODE_PERIOD_CANDIDATES, MAX_CODE_PERIOD_CODES, MAX_CODE_PERIOD_PERIODS
from .endpoints import load_bundled_endpoint_configs, load_inventory_configs
from .periods import MAX_PERIODS
from .planner_registry import planner_registry_summary


READINESS_CATEGORIES = [
    "low_risk_ready",
    "needs_code_loop",
    "needs_period_planner",
    "needs_pit",
    "needs_object_store",
    "needs_intraday_bucket",
    "needs_compaction",
    "needs_realtime_policy",
    "object_store_index",
    "object_download_validation",
    "text_dedup_policy",
    "intraday_bucket_partition",
    "compaction_executor",
    "query_benchmark",
    "storage_capacity_plan",
    "remote_disaster_recovery",
    "realtime_policy",
    "postgres_derived_layer",
    "unsupported",
]

LOW_RISK_A_SHARE_EXECUTABLE_APIS = {
    "stock_basic",
    "trade_cal",
    "hs_const",
    "daily",
    "adj_factor",
    "daily_basic",
    "weekly",
    "monthly",
    "suspend_d",
    "namechange",
    "stk_managers",
    "stk_rewards",
}

A_SHARE_LOW_RISK_EXECUTABLE_APIS = LOW_RISK_A_SHARE_EXECUTABLE_APIS | {
    "stock_company",
    "index_basic",
    "index_weekly",
    "index_monthly",
    "ths_index",
    "index_classify",
}

A_SHARE_LOW_RISK_PLAN_ONLY_APIS = {
    "top10_holders",
    "top10_floatholders",
    "stk_holdernumber",
    "stk_holdertrade",
    "pledge_stat",
    "pledge_detail",
    "repurchase",
    "concept",
    "concept_detail",
    "index_daily",
    "index_weight",
    "index_member",
    "ths_member",
}

HK_LOW_RISK_EXECUTABLE_APIS = {
    "hk_basic",
    "hk_tradecal",
    "hk_daily",
    "hk_daily_adj",
    "hk_adjfactor",
}

US_LOW_RISK_EXECUTABLE_APIS = {
    "us_basic",
    "us_tradecal",
    "us_daily",
    "us_daily_adj",
    "us_adjfactor",
}

HK_LOW_RISK_PLAN_ONLY_APIS = {
    "hk_mins",
    "rt_hk_k",
    "hk_income",
    "hk_balancesheet",
    "hk_cashflow",
    "hk_fina_indicator",
}

US_LOW_RISK_PLAN_ONLY_APIS = {
    "us_income",
    "us_balancesheet",
    "us_cashflow",
    "us_fina_indicator",
}

SCOPE_EXECUTABLE_APIS = {
    "low-risk-a-share": LOW_RISK_A_SHARE_EXECUTABLE_APIS,
    "a-share-low-risk": A_SHARE_LOW_RISK_EXECUTABLE_APIS,
    "hk-low-risk": HK_LOW_RISK_EXECUTABLE_APIS,
    "us-low-risk": US_LOW_RISK_EXECUTABLE_APIS,
    "global-equity-low-risk": A_SHARE_LOW_RISK_EXECUTABLE_APIS | HK_LOW_RISK_EXECUTABLE_APIS | US_LOW_RISK_EXECUTABLE_APIS,
}

SCOPE_PLAN_ONLY_APIS = {
    "low-risk-a-share": set(),
    "a-share-low-risk": A_SHARE_LOW_RISK_PLAN_ONLY_APIS,
    "hk-low-risk": HK_LOW_RISK_PLAN_ONLY_APIS,
    "us-low-risk": US_LOW_RISK_PLAN_ONLY_APIS,
    "global-equity-low-risk": A_SHARE_LOW_RISK_PLAN_ONLY_APIS | HK_LOW_RISK_PLAN_ONLY_APIS | US_LOW_RISK_PLAN_ONLY_APIS,
}

SCOPE_EXCLUDED_HIGH_RISK_FAMILIES = {
    "minute",
    "tick",
    "order_book",
    "realtime",
    "financial_pit",
    "object_download",
    "news_research",
    "postgres_loader",
    "remote_backup",
    "restore_into",
    "compaction_executor",
    "scheduler",
    "parallel_execution",
}


@dataclass(frozen=True)
class ApiInfraReadinessReport:
    scope: str
    scope_supported: bool
    supported_endpoint_kinds: list[str]
    supported_planner_kinds: list[str]
    blocked_planner_kinds: list[str]
    disabled_inventory_endpoint_count: int
    enabled_executable_endpoint_count: int
    missing_infrastructure_by_category: dict[str, list[str]]
    next_recommended_infra_phases: list[str]
    executable_api_names: list[str]
    disabled_inventory_api_names: list[str]
    code_universe_provider: str
    code_list_planner: str
    code_date_matrix_planner: str
    code_date_matrix_existing_status: str
    period_planner: str
    code_period_matrix_planner: str
    pit_safety_metadata: str
    pit_readiness_report: str
    object_text_planner: str
    object_download_execution: bool
    intraday_bucket_planner: str
    intraday_execution: bool
    compaction_planner: str
    compaction_execution: bool
    storage_estimate: str
    rate_policy_report: str
    endpoint_enable_checklist: str
    executable_code_loop: bool
    executable_code_date_matrix: bool
    executable_period_loop: bool
    executable_code_period_loop: bool
    financial_execution: bool
    max_safe_code_plan_limit: int
    max_safe_code_limit: int
    max_safe_date_limit: int
    max_safe_period_limit: int
    max_safe_candidate_jobs: int
    missing_for_execution: list[str]
    plan_only_api_names: list[str]
    excluded_high_risk_families: list[str]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ApiInfrastructureReadinessReporter:
    def report(self, scope: str = "all") -> ApiInfraReadinessReport:
        if scope in {"", None}:  # type: ignore[comparison-overlap]
            scope = "all"
        if scope not in {"all", *SCOPE_EXECUTABLE_APIS}:
            supported = ", ".join(["all", *sorted(SCOPE_EXECUTABLE_APIS)])
            raise ValueError(f"unsupported api-infra-readiness scope {scope!r}; supported scopes: {supported}")

        enabled = [normalize_endpoint_capability(cfg) for cfg in load_bundled_endpoint_configs()]
        inventory = load_inventory_configs()
        registry = planner_registry_summary()
        plan_only_api_names: list[str] = []
        excluded_high_risk_families: list[str] = []

        if scope != "all":
            executable_filter = SCOPE_EXECUTABLE_APIS[scope]
            inventory_filter = SCOPE_PLAN_ONLY_APIS[scope]
            enabled = [cfg for cfg in enabled if str(cfg["api_name"]) in executable_filter]
            inventory = [item for item in inventory if str(item["api_name"]) in inventory_filter]
            plan_only_api_names = sorted(inventory_filter)
            excluded_high_risk_families = sorted(SCOPE_EXCLUDED_HIGH_RISK_FAMILIES)

        missing = {category: [] for category in READINESS_CATEGORIES}
        executable_api_names = sorted(cfg["api_name"] for cfg in enabled)
        disabled_api_names = sorted(item["api_name"] for item in inventory)
        missing["low_risk_ready"] = executable_api_names
        for item in inventory:
            for category in self._categories_for_inventory_item(item):
                missing[category].append(str(item["api_name"]))
        for key in missing:
            missing[key] = sorted(set(missing[key]))
        warnings = [
            "inventory endpoints are disabled and not executable",
            "all-api readiness is infrastructure-only; it does not run mirror-run, fetch, or backfill",
            "remote disaster recovery, compaction execution, PIT derived layers, and PostgreSQL loaders remain out of scope",
        ]
        if scope != "all":
            warnings = [
                f"{scope} readiness is scoped; excluded high-risk families are not executable",
                "plan-only endpoints require separate bounded loop guardrails before execution",
                "api-infra-readiness is read-only and does not run mirror-run, fetch, or backfill",
            ]
        return ApiInfraReadinessReport(
            scope=scope,
            scope_supported=True,
            supported_endpoint_kinds=sorted(set(str(cfg["endpoint_kind"]) for cfg in enabled)),
            supported_planner_kinds=list(registry["supported_planner_kinds"]),
            blocked_planner_kinds=list(registry["blocked_planner_kinds"]),
            disabled_inventory_endpoint_count=len(inventory),
            enabled_executable_endpoint_count=len(enabled),
            missing_infrastructure_by_category=missing,
            next_recommended_infra_phases=[
                "code-list and bounded code/date planner",
                "PIT-safe period and code/period execution enablement",
                "object index/store policy for PDF, news, and research endpoints",
                "intraday bucket storage and compaction policy",
                "realtime polling policy",
            ],
            executable_api_names=executable_api_names,
            disabled_inventory_api_names=disabled_api_names,
            code_universe_provider="implemented",
            code_list_planner="plan_only",
            code_date_matrix_planner="plan_only",
            code_date_matrix_existing_status="implemented",
            period_planner="plan_only",
            code_period_matrix_planner="plan_only",
            pit_safety_metadata="implemented",
            pit_readiness_report="implemented",
            object_text_planner="plan_only",
            object_download_execution=False,
            intraday_bucket_planner="plan_only",
            intraday_execution=False,
            compaction_planner="plan_only",
            compaction_execution=False,
            storage_estimate="implemented",
            rate_policy_report="implemented",
            endpoint_enable_checklist="implemented",
            executable_code_loop=False,
            executable_code_date_matrix=False,
            executable_period_loop=False,
            executable_code_period_loop=False,
            financial_execution=False,
            max_safe_code_plan_limit=MAX_CODE_LIST_PLAN_CODES,
            max_safe_code_limit=min(MAX_CODE_DATE_MATRIX_CODES, MAX_CODE_PERIOD_CODES),
            max_safe_date_limit=MAX_CODE_DATE_MATRIX_DATES,
            max_safe_period_limit=min(MAX_PERIODS, MAX_CODE_PERIOD_PERIODS),
            max_safe_candidate_jobs=min(MAX_CODE_DATE_MATRIX_CANDIDATES, MAX_CODE_PERIOD_CANDIDATES),
            missing_for_execution=[
                "endpoint enablement",
                "PIT safe usable_after generation",
                "per-endpoint fake tests",
                "small real smoke",
                "rate-limit policy",
                "resume strategy for code/date loops",
                "failure aggregation",
                "strategy-safe derived layer",
            ],
            plan_only_api_names=plan_only_api_names,
            excluded_high_risk_families=excluded_high_risk_families,
            warnings=warnings,
        )

    def _categories_for_inventory_item(self, item: dict[str, Any]) -> list[str]:
        endpoint_kind = str(item.get("endpoint_kind") or "unknown")
        planner_kind = str(item.get("planner_kind") or "unsupported")
        categories: set[str] = set()
        if planner_kind in {"code_list", "code_date_matrix"}:
            categories.add("needs_code_loop")
        if planner_kind in {"period", "code_period_matrix"}:
            categories.add("needs_period_planner")
        if endpoint_kind in {"financial_statement", "financial_indicator"} or planner_kind == "code_period_matrix":
            categories.add("needs_pit")
        if endpoint_kind in {"object_document", "text_news"} or planner_kind in {"object_index", "object_download"}:
            categories.add("needs_object_store")
            categories.add("object_store_index")
            categories.add("object_download_validation")
            categories.add("text_dedup_policy")
        if endpoint_kind in {"minute_bar", "tick"} or planner_kind == "bucketed_intraday":
            categories.add("needs_intraday_bucket")
            categories.add("needs_compaction")
            categories.add("intraday_bucket_partition")
            categories.add("compaction_executor")
            categories.add("query_benchmark")
            categories.add("storage_capacity_plan")
        if endpoint_kind == "realtime" or planner_kind == "realtime_poll":
            categories.add("needs_realtime_policy")
            categories.add("realtime_policy")
        if endpoint_kind not in ENDPOINT_KIND_VALUES or planner_kind == "unsupported":
            categories.add("unsupported")
        if not categories:
            categories.add("unsupported")
        return sorted(categories)
