from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from .endpoints import load_bundled_endpoint_configs, load_inventory_configs
from .object_text import OBJECT_TEXT_ENDPOINT_KINDS, validate_object_text_metadata


@dataclass(frozen=True)
class ObjectPlan:
    api_name: str
    endpoint_kind: str | None
    planner_kind: str | None
    date_range: dict[str, str]
    object_strategy: dict[str, Any]
    required_infra: list[str]
    execution_allowed: bool
    dry_run: bool
    would_require_real_request: bool
    would_download_objects: bool
    blocked_reason: str | None
    warnings: list[str]
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors) or self.blocked_reason is not None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        return data


class ObjectPlanner:
    def plan(self, *, api_name: str, start_date: str | None, end_date: str | None) -> ObjectPlan:
        warnings: list[str] = []
        blocking_errors: list[str] = []
        date_range: dict[str, str] = {}
        try:
            date_range = _validate_date_range(start_date, end_date)
        except ValueError as exc:
            blocking_errors.append(str(exc))

        cfg = _endpoint_config(api_name)
        if cfg is None:
            blocking_errors.append("endpoint_not_found")
            return _result(
                api_name=api_name,
                endpoint_kind=None,
                planner_kind=None,
                date_range=date_range,
                object_strategy={},
                required_infra=[],
                warnings=warnings,
                blocking_errors=blocking_errors,
                blocked_reason="endpoint_not_found",
            )

        endpoint_kind = str(cfg.get("endpoint_kind") or "unknown")
        planner_kind = str(cfg.get("planner_kind") or "unsupported")
        if endpoint_kind not in OBJECT_TEXT_ENDPOINT_KINDS and planner_kind not in {"object_index", "object_download"}:
            blocking_errors.append(f"endpoint_not_object_text:{endpoint_kind}")
        metadata_result = validate_object_text_metadata(cfg)
        warnings.extend(metadata_result.warnings)
        required_infra = [str(item) for item in cfg.get("required_infra") or []]
        for error in metadata_result.errors:
            if error not in required_infra:
                required_infra.append(error)
        blocked_reason = "object_index_store_policy_missing"
        return _result(
            api_name=api_name,
            endpoint_kind=endpoint_kind,
            planner_kind=planner_kind,
            date_range=date_range,
            object_strategy=metadata_result.metadata.to_dict(),
            required_infra=required_infra,
            warnings=warnings,
            blocking_errors=blocking_errors,
            blocked_reason=blocked_reason,
        )


def _result(
    *,
    api_name: str,
    endpoint_kind: str | None,
    planner_kind: str | None,
    date_range: dict[str, str],
    object_strategy: dict[str, Any],
    required_infra: list[str],
    warnings: list[str],
    blocking_errors: list[str],
    blocked_reason: str | None,
) -> ObjectPlan:
    return ObjectPlan(
        api_name=api_name,
        endpoint_kind=endpoint_kind,
        planner_kind=planner_kind,
        date_range=date_range,
        object_strategy=object_strategy,
        required_infra=sorted(set(required_infra)),
        execution_allowed=False,
        dry_run=True,
        would_require_real_request=True,
        would_download_objects=False,
        blocked_reason=blocked_reason,
        warnings=warnings,
        blocking_errors=blocking_errors,
    )


def _endpoint_config(api_name: str) -> dict[str, Any] | None:
    for cfg in load_bundled_endpoint_configs():
        if cfg.get("api_name") == api_name:
            return dict(cfg)
    for cfg in load_inventory_configs():
        if cfg.get("api_name") == api_name:
            return dict(cfg)
    return None


def _validate_date_range(start_date: str | None, end_date: str | None) -> dict[str, str]:
    if not start_date or not end_date:
        raise ValueError("start-date and end-date are required")
    start = _parse_yyyymmdd(start_date, "start-date")
    end = _parse_yyyymmdd(end_date, "end-date")
    if start > end:
        raise ValueError("start-date must be <= end-date")
    return {"start_date": start_date, "end_date": end_date}


def _parse_yyyymmdd(value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {value}") from exc
