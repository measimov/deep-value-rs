from __future__ import annotations

from dataclasses import asdict, dataclass, field
from importlib import resources
from typing import Any, Mapping

import yaml


SOURCE_MAP_PACKAGE = "tushare_mirror.endpoint_configs.source_maps"
DISCLOSURE_EVENT_SCHEMA_FILE = "disclosure_event_schema.yaml"
FINANCIAL_DISCLOSURE_SOURCES_FILE = "financial_disclosure_sources.yaml"

PIT_STRENGTH_VALUES = {"raw_only", "availability_only", "as_filed_verified"}
DISCLOSURE_MATCH_STATUS_VALUES = {
    "exact",
    "near",
    "period_only",
    "candidate",
    "unmatched",
    "source_unavailable",
    "manual_review_required",
}
DISCLOSURE_SOURCE_STATUS_VALUES = {
    "stable_public_json",
    "tentative_manual_audit",
    "future_vendor_placeholder",
    "disabled",
}


@dataclass(frozen=True)
class DisclosureEvent:
    event_id: str
    market: str
    source: str
    source_status: str
    source_doc_id: str | None
    source_url: str | None
    ticker: str | None
    ts_code: str | None
    external_id: str | None
    cik: str | None
    period: str
    end_date: str
    report_type: str | None
    form_type: str | None
    filing_date: str | None
    accepted_at: str | None
    disclosure_date: str | None
    announcement_title: str | None
    language: str | None
    match_status: str
    match_confidence: float
    pit_strength: str
    as_filed_value_verified: bool
    limitations: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        validate_pit_strength(self.pit_strength)
        validate_match_status(self.match_status)
        if self.source_status not in DISCLOSURE_SOURCE_STATUS_VALUES:
            raise ValueError(f"unsupported disclosure source_status: {self.source_status}")
        if not 0.0 <= float(self.match_confidence) <= 1.0:
            raise ValueError("match_confidence must be between 0 and 1")
        if self.pit_strength == "as_filed_verified" and not self.as_filed_value_verified:
            raise ValueError("as_filed_verified requires as_filed_value_verified=true")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DisclosureSource:
    source_id: str
    market: str
    source: str
    source_status: str
    automation_status: str
    supports_automated_metadata: bool
    supports_value_verification: bool
    base_url: str | None
    limitations: list[str]
    safety_notes: list[str]

    def __post_init__(self) -> None:
        if self.source_status not in DISCLOSURE_SOURCE_STATUS_VALUES:
            raise ValueError(f"unsupported disclosure source_status: {self.source_status}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_pit_strength(value: str) -> str:
    if value not in PIT_STRENGTH_VALUES:
        raise ValueError(f"unsupported pit_strength: {value}")
    return value


def validate_match_status(value: str) -> str:
    if value not in DISCLOSURE_MATCH_STATUS_VALUES:
        raise ValueError(f"unsupported disclosure match_status: {value}")
    return value


def load_disclosure_event_schema() -> dict[str, Any]:
    return _load_source_map(DISCLOSURE_EVENT_SCHEMA_FILE)


def load_financial_disclosure_sources() -> dict[str, Any]:
    return _load_source_map(FINANCIAL_DISCLOSURE_SOURCES_FILE)


def disclosure_sources() -> list[DisclosureSource]:
    data = load_financial_disclosure_sources()
    return [DisclosureSource(**_source_args(item)) for item in data.get("sources", [])]


def validate_disclosure_event_schema() -> list[str]:
    data = load_disclosure_event_schema()
    errors: list[str] = []
    if data.get("schema_version") != "financial-disclosure-event-schema/v1":
        errors.append("schema_version must be financial-disclosure-event-schema/v1")
    required_fields = data.get("required_fields")
    field_definitions = data.get("field_definitions")
    if not isinstance(required_fields, list) or not required_fields:
        errors.append("required_fields must be a non-empty list")
        required_fields = []
    if not isinstance(field_definitions, Mapping):
        errors.append("field_definitions must be a mapping")
        field_definitions = {}
    missing_definitions = [field for field in required_fields if field not in field_definitions]
    if missing_definitions:
        errors.append(f"required fields missing definitions: {', '.join(sorted(missing_definitions))}")
    if set(data.get("pit_strength_values") or []) != PIT_STRENGTH_VALUES:
        errors.append("pit_strength_values must match supported disclosure PIT strengths")
    if set(data.get("match_status_values") or []) != DISCLOSURE_MATCH_STATUS_VALUES:
        errors.append("match_status_values must match supported disclosure match statuses")
    if data.get("durable_storage_enabled") is not False:
        errors.append("durable_storage_enabled must be false for this goal")
    return errors


def validate_financial_disclosure_sources() -> list[str]:
    data = load_financial_disclosure_sources()
    errors: list[str] = []
    if data.get("source_inventory_version") != "financial-disclosure-sources/v1":
        errors.append("source_inventory_version must be financial-disclosure-sources/v1")
    sources = data.get("sources")
    if not isinstance(sources, list) or not sources:
        errors.append("sources must be a non-empty list")
        return errors
    seen: set[str] = set()
    for index, item in enumerate(sources):
        if not isinstance(item, Mapping):
            errors.append(f"source at index {index} must be a mapping")
            continue
        source_id = str(item.get("source_id") or f"<index:{index}>")
        if source_id in seen:
            errors.append(f"duplicate disclosure source_id: {source_id}")
        seen.add(source_id)
        for field_name in [
            "source_id",
            "market",
            "source",
            "source_status",
            "automation_status",
            "supports_automated_metadata",
            "supports_value_verification",
            "limitations",
            "safety_notes",
        ]:
            if field_name not in item:
                errors.append(f"{source_id} missing required source field: {field_name}")
        source_status = str(item.get("source_status") or "")
        if source_status not in DISCLOSURE_SOURCE_STATUS_VALUES:
            errors.append(f"{source_id} has unsupported source_status: {source_status}")
        if item.get("market") == "hk" and bool(item.get("supports_automated_metadata")):
            errors.append(f"{source_id} HK automation must remain disabled until stable metadata is proven")
    source_ids = {str(item.get("source_id")) for item in sources if isinstance(item, Mapping)}
    if "sec_edgar_submissions" not in source_ids:
        errors.append("SEC EDGAR submissions source is required")
    if "hkexnews_advanced_search" not in source_ids:
        errors.append("HKEXnews advanced search source is required")
    return errors


def _load_source_map(filename: str) -> dict[str, Any]:
    root = resources.files(SOURCE_MAP_PACKAGE)
    item = root.joinpath(filename)
    with resources.as_file(item) as path:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(data)


def _source_args(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_id": str(item.get("source_id") or ""),
        "market": str(item.get("market") or ""),
        "source": str(item.get("source") or ""),
        "source_status": str(item.get("source_status") or ""),
        "automation_status": str(item.get("automation_status") or ""),
        "supports_automated_metadata": bool(item.get("supports_automated_metadata")),
        "supports_value_verification": bool(item.get("supports_value_verification")),
        "base_url": str(item.get("base_url")) if item.get("base_url") else None,
        "limitations": [str(value) for value in (item.get("limitations") or [])],
        "safety_notes": [str(value) for value in (item.get("safety_notes") or [])],
    }
