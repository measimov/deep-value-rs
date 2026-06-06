from __future__ import annotations

import json
from importlib import resources
from pathlib import Path
from typing import Any

import yaml


SOURCE_MAP_PACKAGE = "tushare_mirror.endpoint_configs.source_maps"
HK_US_LOW_RISK_SOURCE_MAP = "hk_us_low_risk.yaml"
SOURCE_MAP_REQUIRED_FIELDS = {
    "api_name",
    "market",
    "category",
    "doc_url",
    "documented_params",
    "documented_fields",
    "documented_row_limit",
    "permission_notes",
    "update_cadence",
    "pagination_hints",
    "recommended_pagination_strategy",
    "recommended_planner_kind",
    "recommended_partition_template",
    "recommendation",
    "real_probe_status",
    "missing_metadata",
    "safety_notes",
}
RECOMMENDATION_VALUES = {"executable_candidate", "plan_only", "disabled"}
HIGH_RISK_HK_US_APIS = {
    "hk_mins",
    "rt_hk_k",
    "hk_income",
    "hk_balancesheet",
    "hk_cashflow",
    "hk_fina_indicator",
    "us_income",
    "us_balancesheet",
    "us_cashflow",
    "us_fina_indicator",
}


def bundled_source_map_files() -> list[Path]:
    root = resources.files(SOURCE_MAP_PACKAGE)
    out: list[Path] = []
    for item in root.iterdir():
        if item.name.endswith((".yaml", ".yml")):
            with resources.as_file(item) as path:
                out.append(Path(path))
    return out


def load_source_map(filename: str) -> dict[str, Any]:
    root = resources.files(SOURCE_MAP_PACKAGE)
    item = root.joinpath(filename)
    with resources.as_file(item) as path:
        data = yaml.safe_load(path.read_text()) or {}
    return dict(data)


def load_hk_us_low_risk_source_map() -> dict[str, Any]:
    return load_source_map(HK_US_LOW_RISK_SOURCE_MAP)


def hk_us_low_risk_source_endpoints() -> list[dict[str, Any]]:
    data = load_hk_us_low_risk_source_map()
    return [dict(item) for item in data.get("endpoints", [])]


def hk_us_low_risk_source_map_json() -> str:
    return json.dumps(load_hk_us_low_risk_source_map(), sort_keys=True, separators=(",", ":"))


def validate_hk_us_low_risk_source_map() -> list[str]:
    data = load_hk_us_low_risk_source_map()
    errors: list[str] = []
    if data.get("source_map_version") != "hk-us-low-risk-source-map/v1":
        errors.append("source_map_version must be hk-us-low-risk-source-map/v1")
    endpoints = data.get("endpoints")
    if not isinstance(endpoints, list) or not endpoints:
        errors.append("endpoints must be a non-empty list")
        return errors

    seen: set[str] = set()
    for index, endpoint in enumerate(endpoints):
        if not isinstance(endpoint, dict):
            errors.append(f"endpoint at index {index} must be a mapping")
            continue
        api_name = str(endpoint.get("api_name") or f"<index:{index}>")
        if api_name in seen:
            errors.append(f"duplicate endpoint source metadata: {api_name}")
        seen.add(api_name)
        missing = sorted(field for field in SOURCE_MAP_REQUIRED_FIELDS if field not in endpoint)
        if missing:
            errors.append(f"{api_name} missing required source metadata: {', '.join(missing)}")
        doc_url = str(endpoint.get("doc_url") or "")
        if not doc_url.startswith("https://tushare.pro/document/2?doc_id="):
            errors.append(f"{api_name} doc_url must be an official Tushare document URL")
        recommendation = str(endpoint.get("recommendation") or "")
        if recommendation not in RECOMMENDATION_VALUES:
            errors.append(f"{api_name} has unsupported recommendation: {recommendation}")
        if api_name in HIGH_RISK_HK_US_APIS and recommendation == "executable_candidate":
            errors.append(f"{api_name} is high-risk for this goal and cannot be executable_candidate")
        if not endpoint.get("documented_params"):
            errors.append(f"{api_name} documented_params must not be empty")
        if not endpoint.get("documented_fields"):
            errors.append(f"{api_name} documented_fields must not be empty")
    return errors

