from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


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
