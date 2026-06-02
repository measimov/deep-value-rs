from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from .endpoints import load_bundled_endpoint_configs, load_inventory_configs
from .intraday import INTRADAY_ENDPOINT_KINDS, validate_intraday_metadata


@dataclass(frozen=True)
class IntradayPlan:
    api_name: str
    endpoint_kind: str | None
    planner_kind: str | None
    freq: str | None
    date_range: dict[str, str]
    bucket_count: int | None
    estimated_partition_strategy: str | None
    required_infra: list[str]
    execution_allowed: bool
    dry_run: bool
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


class IntradayPlanner:
    def plan(
        self,
        *,
        api_name: str,
        freq: str | None,
        start_date: str | None,
        end_date: str | None,
        bucket_count: int | None,
    ) -> IntradayPlan:
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
                freq=freq,
                date_range=date_range,
                bucket_count=bucket_count,
                estimated_partition_strategy=None,
                required_infra=[],
                warnings=warnings,
                blocking_errors=blocking_errors,
                blocked_reason="endpoint_not_found",
            )
        cfg = dict(cfg)
        strategy = dict(cfg.get("intraday_strategy") or {})
        if freq:
            strategy["freq"] = freq
        if bucket_count is not None:
            strategy["bucket_count"] = bucket_count
        cfg["intraday_strategy"] = strategy

        endpoint_kind = str(cfg.get("endpoint_kind") or "unknown")
        planner_kind = str(cfg.get("planner_kind") or "unsupported")
        if endpoint_kind not in INTRADAY_ENDPOINT_KINDS and planner_kind != "bucketed_intraday":
            blocking_errors.append(f"endpoint_not_intraday:{endpoint_kind}")
        metadata_result = validate_intraday_metadata(cfg)
        warnings.extend(metadata_result.warnings)
        required_infra = [str(item) for item in cfg.get("required_infra") or []]
        for error in metadata_result.errors:
            if error not in required_infra:
                required_infra.append(error)
        required_infra.extend(["bucket partition policy", "compaction policy", "storage estimate", "rate-limit policy"])
        return _result(
            api_name=api_name,
            endpoint_kind=endpoint_kind,
            planner_kind=planner_kind,
            freq=metadata_result.metadata.freq,
            date_range=date_range,
            bucket_count=metadata_result.metadata.bucket_count,
            estimated_partition_strategy=f"{metadata_result.metadata.partition_template}:{metadata_result.metadata.bucket_strategy}",
            required_infra=required_infra,
            warnings=warnings,
            blocking_errors=blocking_errors,
            blocked_reason="bucket_policy_missing",
        )


def _result(
    *,
    api_name: str,
    endpoint_kind: str | None,
    planner_kind: str | None,
    freq: str | None,
    date_range: dict[str, str],
    bucket_count: int | None,
    estimated_partition_strategy: str | None,
    required_infra: list[str],
    warnings: list[str],
    blocking_errors: list[str],
    blocked_reason: str | None,
) -> IntradayPlan:
    return IntradayPlan(
        api_name=api_name,
        endpoint_kind=endpoint_kind,
        planner_kind=planner_kind,
        freq=freq,
        date_range=date_range,
        bucket_count=bucket_count,
        estimated_partition_strategy=estimated_partition_strategy,
        required_infra=sorted(set(required_infra)),
        execution_allowed=False,
        dry_run=True,
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
