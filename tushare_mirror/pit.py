from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from .endpoints import load_inventory_configs


PIT_ENDPOINT_KINDS = {"financial_statement", "financial_indicator"}


@dataclass(frozen=True)
class PITSafetyMetadata:
    pit_required: bool
    period_field: str | None
    announcement_date_fields: list[str]
    usable_after_field: str | None
    fallback_usable_after_policy: str | None
    allow_without_disclosure_date: bool
    lookahead_risk: bool
    strategy_safe_default: bool
    blocked_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PITSafetyValidationResult:
    status: str
    pit_required: bool
    metadata: PITSafetyMetadata
    errors: list[str]
    warnings: list[str]

    @property
    def blocked(self) -> bool:
        return self.status == "blocked"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PITReadinessReport:
    financial_endpoint_count: int
    pit_metadata_complete_count: int
    pit_metadata_incomplete_count: int
    execution_enabled_count: int
    execution_blocked_count: int
    missing_period_field: list[str]
    missing_announcement_date_fields: list[str]
    missing_usable_after_strategy: list[str]
    strategy_safe_count: int
    strategy_unsafe_count: int
    next_required_infra: list[str]
    items: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PITReadinessReporter:
    def report(self) -> PITReadinessReport:
        financial = [
            item
            for item in load_inventory_configs()
            if str(item.get("endpoint_kind") or "") in PIT_ENDPOINT_KINDS or pit_required_for_endpoint(item)
        ]
        items: list[dict[str, Any]] = []
        missing_period: list[str] = []
        missing_announcement: list[str] = []
        missing_strategy: list[str] = []
        complete = 0
        execution_enabled = 0
        strategy_safe = 0
        for item in financial:
            result = validate_pit_safety(item)
            api_name = str(item.get("api_name"))
            if result.status == "complete":
                complete += 1
            if item.get("execution_status") == "enabled":
                execution_enabled += 1
            if result.metadata.strategy_safe_default:
                strategy_safe += 1
            if "missing_period_field" in result.errors:
                missing_period.append(api_name)
            if "missing_announcement_date_fields" in result.errors:
                missing_announcement.append(api_name)
            if "missing_usable_after_strategy" in result.errors:
                missing_strategy.append(api_name)
            items.append(
                {
                    "api_name": api_name,
                    "endpoint_kind": item.get("endpoint_kind"),
                    "planner_kind": item.get("planner_kind"),
                    "execution_status": item.get("execution_status"),
                    "pit_required": result.pit_required,
                    "pit_safety_status": result.status,
                    "period_field": result.metadata.period_field,
                    "announcement_date_fields": result.metadata.announcement_date_fields,
                    "usable_after_field": result.metadata.usable_after_field,
                    "strategy_safe_default": result.metadata.strategy_safe_default,
                    "errors": result.errors,
                    "warnings": result.warnings,
                }
            )
        total = len(financial)
        return PITReadinessReport(
            financial_endpoint_count=total,
            pit_metadata_complete_count=complete,
            pit_metadata_incomplete_count=total - complete,
            execution_enabled_count=execution_enabled,
            execution_blocked_count=total - execution_enabled,
            missing_period_field=sorted(missing_period),
            missing_announcement_date_fields=sorted(missing_announcement),
            missing_usable_after_strategy=sorted(missing_strategy),
            strategy_safe_count=strategy_safe,
            strategy_unsafe_count=total - strategy_safe,
            next_required_infra=[
                "PIT-safe usable_after generation",
                "per-endpoint fake tests",
                "small user-confirmed real smoke",
                "rate-limit policy",
                "resume and failure aggregation",
                "strategy-safe derived layer",
            ],
            items=sorted(items, key=lambda row: str(row["api_name"])),
        )


def pit_required_for_endpoint(endpoint_config: Mapping[str, Any]) -> bool:
    pit_safety = endpoint_config.get("pit_safety")
    if isinstance(pit_safety, Mapping) and "pit_required" in pit_safety:
        return bool(pit_safety.get("pit_required"))
    endpoint_kind = str(endpoint_config.get("endpoint_kind") or "")
    return endpoint_kind in PIT_ENDPOINT_KINDS


def pit_metadata_from_config(endpoint_config: Mapping[str, Any]) -> PITSafetyMetadata:
    pit_safety_raw = endpoint_config.get("pit_safety")
    pit_safety = dict(pit_safety_raw) if isinstance(pit_safety_raw, Mapping) else {}
    pit_required = pit_required_for_endpoint(endpoint_config)
    announcement = pit_safety.get("announcement_date_fields")
    if announcement is None:
        announcement = endpoint_config.get("announcement_date_fields") or []
    if isinstance(announcement, str):
        announcement_fields = [announcement]
    else:
        announcement_fields = [str(item) for item in (announcement or [])]
    period_field = pit_safety.get("period_field") or endpoint_config.get("period_field")
    usable_after_field = pit_safety.get("usable_after_field") or endpoint_config.get("usable_after_field")
    fallback_policy = pit_safety.get("fallback_usable_after_policy") or endpoint_config.get("fallback_usable_after_policy")
    allow_without = pit_safety.get("allow_without_disclosure_date", False)
    lookahead_risk = pit_safety.get("lookahead_risk", pit_required)
    strategy_safe_default = pit_safety.get("strategy_safe_default", False)
    blocked_reason = pit_safety.get("blocked_reason")
    return PITSafetyMetadata(
        pit_required=pit_required,
        period_field=str(period_field) if period_field else None,
        announcement_date_fields=announcement_fields,
        usable_after_field=str(usable_after_field) if usable_after_field else None,
        fallback_usable_after_policy=str(fallback_policy) if fallback_policy else None,
        allow_without_disclosure_date=bool(allow_without),
        lookahead_risk=bool(lookahead_risk),
        strategy_safe_default=bool(strategy_safe_default),
        blocked_reason=str(blocked_reason) if blocked_reason else None,
    )


def validate_pit_safety(endpoint_config: Mapping[str, Any]) -> PITSafetyValidationResult:
    metadata = pit_metadata_from_config(endpoint_config)
    errors: list[str] = []
    warnings: list[str] = []
    pit_safety_raw = endpoint_config.get("pit_safety")
    endpoint_kind = str(endpoint_config.get("endpoint_kind") or "")

    if not metadata.pit_required:
        return PITSafetyValidationResult(
            status="not_required",
            pit_required=False,
            metadata=metadata,
            errors=[],
            warnings=[],
        )

    if endpoint_kind in PIT_ENDPOINT_KINDS and not isinstance(pit_safety_raw, Mapping):
        errors.append("unknown_pit_safety")
    if not metadata.period_field:
        errors.append("missing_period_field")
    if not metadata.announcement_date_fields:
        errors.append("missing_announcement_date_fields")
    if not metadata.usable_after_field and not metadata.fallback_usable_after_policy:
        errors.append("missing_usable_after_strategy")
    if metadata.allow_without_disclosure_date:
        warnings.append("allow_without_disclosure_date=true increases lookahead risk")
    if metadata.strategy_safe_default:
        warnings.append("strategy_safe_default=true must be backed by endpoint-specific tests")
    status = "blocked" if errors else "complete"
    return PITSafetyValidationResult(
        status=status,
        pit_required=True,
        metadata=metadata,
        errors=errors,
        warnings=warnings,
    )
