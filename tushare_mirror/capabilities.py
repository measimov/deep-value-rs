from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


ENDPOINT_KIND_VALUES = {
    "reference_snapshot",
    "calendar",
    "daily_bar",
    "daily_metric",
    "event",
    "constituent",
    "company_governance",
    "financial_statement",
    "financial_indicator",
    "macro",
    "fund",
    "index",
    "futures",
    "option",
    "hk_us",
    "text_news",
    "object_document",
    "research_report",
    "announcement",
    "html_text",
    "unknown_object_text",
    "minute_bar",
    "tick",
    "realtime",
    "unknown",
}

PLANNER_KIND_VALUES = {
    "single_snapshot",
    "date_backfill",
    "calendar_backfill",
    "explicit_dates",
    "code_list",
    "code_date_matrix",
    "period",
    "code_period_matrix",
    "object_index",
    "object_download",
    "bucketed_intraday",
    "realtime_poll",
    "unsupported",
}

PAGINATION_MODE_VALUES = {"none", "paged", "cursor", "offset", "unknown"}
EXECUTION_STATUS_VALUES = {"enabled", "disabled", "unsupported"}


@dataclass(frozen=True)
class EndpointCapability:
    api_name: str
    family: str
    market: str
    domain: str
    endpoint_kind: str
    volume_class: str
    planner_kind: str
    permission_class: str
    partition_template: str | None
    primary_date_field: str | None
    supported_params: list[str]
    default_fields: list[str]
    probe_params: dict[str, Any]
    probe_fields: list[str]
    pagination_mode: str
    date_strategy: str
    code_strategy: str
    period_strategy: str
    object_strategy: str
    pit_safety: dict[str, Any]
    execution_status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CapabilityValidationError(ValueError):
    pass


def normalize_endpoint_capability(cfg: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(cfg)
    normalized["endpoint_kind"] = normalized.get("endpoint_kind") or infer_endpoint_kind(normalized)
    normalized["planner_kind"] = normalized.get("planner_kind") or infer_planner_kind(normalized)
    normalized["permission_class"] = normalized.get("permission_class") or "regular"
    normalized["partition_template"] = normalized.get("partition_template") or infer_partition_template(normalized)
    normalized["primary_date_field"] = normalized.get("primary_date_field", infer_primary_date_field(normalized))
    normalized["supported_params"] = list(normalized.get("supported_params") or infer_supported_params(normalized))
    normalized["default_fields"] = list(normalized.get("default_fields") or [])
    normalized["pagination_mode"] = normalized.get("pagination_mode") or infer_pagination_mode(normalized)
    normalized["date_strategy"] = normalized.get("date_strategy") or infer_date_strategy(normalized)
    normalized["code_strategy"] = normalized.get("code_strategy") or infer_code_strategy(normalized)
    normalized["period_strategy"] = normalized.get("period_strategy") or infer_period_strategy(normalized)
    normalized["object_strategy"] = normalized.get("object_strategy") or "none"
    normalized["pit_safety"] = dict(normalized.get("pit_safety") or {"requires_disclosure_date": False})
    normalized["execution_status"] = normalized.get("execution_status") or ("enabled" if normalized.get("enabled", True) else "disabled")
    capability_from_config(normalized)
    return normalized


def capability_from_config(cfg: Mapping[str, Any]) -> EndpointCapability:
    required = ["api_name", "family", "market", "domain", "volume_class"]
    missing = [name for name in required if name not in cfg or cfg.get(name) in (None, "")]
    if missing:
        raise CapabilityValidationError(f"endpoint capability missing required fields: {', '.join(missing)}")
    endpoint_kind = str(cfg.get("endpoint_kind") or "")
    planner_kind = str(cfg.get("planner_kind") or "")
    pagination_mode = str(cfg.get("pagination_mode") or "")
    execution_status = str(cfg.get("execution_status") or "")
    if endpoint_kind not in ENDPOINT_KIND_VALUES:
        raise CapabilityValidationError(f"unsupported endpoint_kind for {cfg.get('api_name')}: {endpoint_kind}")
    if planner_kind not in PLANNER_KIND_VALUES:
        raise CapabilityValidationError(f"unsupported planner_kind for {cfg.get('api_name')}: {planner_kind}")
    if pagination_mode not in PAGINATION_MODE_VALUES:
        raise CapabilityValidationError(f"unsupported pagination_mode for {cfg.get('api_name')}: {pagination_mode}")
    if execution_status not in EXECUTION_STATUS_VALUES:
        raise CapabilityValidationError(f"unsupported execution_status for {cfg.get('api_name')}: {execution_status}")
    probe = cfg.get("probe") or {}
    probe_params = probe.get("params") or {}
    probe_fields = probe.get("fields") or []
    if not isinstance(probe_params, Mapping):
        raise CapabilityValidationError(f"probe params must be a mapping for {cfg.get('api_name')}")
    if not isinstance(probe_fields, list):
        raise CapabilityValidationError(f"probe fields must be a list for {cfg.get('api_name')}")
    return EndpointCapability(
        api_name=str(cfg["api_name"]),
        family=str(cfg["family"]),
        market=str(cfg["market"]),
        domain=str(cfg["domain"]),
        endpoint_kind=endpoint_kind,
        volume_class=str(cfg["volume_class"]),
        planner_kind=planner_kind,
        permission_class=str(cfg.get("permission_class") or "regular"),
        partition_template=cfg.get("partition_template"),
        primary_date_field=cfg.get("primary_date_field"),
        supported_params=[str(item) for item in (cfg.get("supported_params") or [])],
        default_fields=[str(item) for item in (cfg.get("default_fields") or [])],
        probe_params=dict(probe_params),
        probe_fields=[str(item) for item in probe_fields],
        pagination_mode=pagination_mode,
        date_strategy=str(cfg.get("date_strategy") or "none"),
        code_strategy=str(cfg.get("code_strategy") or "none"),
        period_strategy=str(cfg.get("period_strategy") or "none"),
        object_strategy=str(cfg.get("object_strategy") or "none"),
        pit_safety=dict(cfg.get("pit_safety") or {}),
        execution_status=execution_status,
    )


def infer_endpoint_kind(cfg: Mapping[str, Any]) -> str:
    api = str(cfg.get("api_name") or "")
    family = str(cfg.get("family") or "")
    if api == "stock_basic":
        return "reference_snapshot"
    if api == "trade_cal":
        return "calendar"
    if api in {"daily", "weekly", "monthly"}:
        return "daily_bar"
    if api in {"adj_factor", "daily_basic"}:
        return "daily_metric"
    if api == "suspend_d" or family == "stock_event":
        return "event"
    if api == "hs_const" or "constituent" in family:
        return "constituent"
    if family == "company_governance":
        return "company_governance"
    return "unknown"


def infer_planner_kind(cfg: Mapping[str, Any]) -> str:
    api = str(cfg.get("api_name") or "")
    if api in {"daily", "adj_factor", "daily_basic", "suspend_d"}:
        return "calendar_backfill"
    if api in {"weekly", "monthly"}:
        return "explicit_dates"
    return "single_snapshot"


def infer_partition_template(cfg: Mapping[str, Any]) -> str | None:
    if cfg.get("partition_template"):
        return str(cfg["partition_template"])
    part = cfg.get("partition") or {}
    name = str(part.get("name") or "")
    template = str(part.get("template") or "")
    combined = f"{name} {template}"
    for value in ("snapshot_date", "exchange_year", "event_year_month", "family_code_snapshot", "period_year"):
        if value in combined:
            return value
    if "year" in combined and "month" in combined:
        return "year_month"
    if part.get("date_field"):
        return "year_month"
    return None


def infer_primary_date_field(cfg: Mapping[str, Any]) -> str | None:
    part = cfg.get("partition") or {}
    return part.get("date_field")


def infer_supported_params(cfg: Mapping[str, Any]) -> list[str]:
    params: list[str] = []
    default_params = cfg.get("default_params") or {}
    if isinstance(default_params, Mapping):
        params.extend(str(key) for key in default_params)
    probe = cfg.get("probe") or {}
    probe_params = probe.get("params") or {}
    if isinstance(probe_params, Mapping):
        params.extend(str(key) for key in probe_params)
    date_field = cfg.get("primary_date_field") or infer_primary_date_field(cfg)
    if date_field:
        params.append(str(date_field))
    return sorted(set(params))


def infer_pagination_mode(cfg: Mapping[str, Any]) -> str:
    return "paged" if cfg.get("page_size") else "unknown"


def infer_date_strategy(cfg: Mapping[str, Any]) -> str:
    planner = str(cfg.get("planner_kind") or infer_planner_kind(cfg))
    if planner == "calendar_backfill":
        return "local_trade_cal"
    if planner in {"date_backfill", "explicit_dates"}:
        return "explicit_dates"
    if cfg.get("primary_date_field") or infer_primary_date_field(cfg):
        return "single_date_or_range"
    return "none"


def infer_code_strategy(cfg: Mapping[str, Any]) -> str:
    params = set(cfg.get("supported_params") or infer_supported_params(cfg))
    if "ts_code" in params:
        return "explicit_ts_code_only"
    return "none"


def infer_period_strategy(cfg: Mapping[str, Any]) -> str:
    template = cfg.get("partition_template") or infer_partition_template(cfg)
    if template == "period_year":
        return "single_period"
    return "none"
