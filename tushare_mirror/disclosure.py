from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
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
    "blocked",
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


@dataclass(frozen=True)
class HKEXDisclosureAutomationGate:
    report_version: str
    stock_code: str
    period: str
    source_status: str
    automation_status: str
    manual_audit_required: bool
    can_auto_match_disclosure_date: bool
    match_status: str
    max_requests: int
    real_requests_sent: bool
    limitations: list[str]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DisclosureMatchResult:
    match_status: str
    match_confidence: float
    pit_strength_candidate: str
    feature_eligible: bool
    date_delta_days: int | None
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PITFeatureGateResult:
    status: str
    pit_strength: str
    feature_eligible: bool
    strong_feature_eligible: bool
    require_as_filed: bool
    warnings: list[str]
    blocking_errors: list[str]

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


def hkex_disclosure_automation_gate(
    *,
    stock_code: str,
    period: str,
    max_requests: int,
    announcement_title: str | None = None,
) -> HKEXDisclosureAutomationGate:
    blocking_errors: list[str] = []
    warnings = [
        "HKEX disclosure automation remains manual-audit-only until a stable documented metadata path is proven",
        "title-only evidence cannot upgrade HK financial data to availability_only",
    ]
    if max_requests < 1:
        blocking_errors.append("max_requests_must_be_positive")
    if max_requests > 2:
        blocking_errors.append("max_requests_exceeds_hkex_gate_limit:2")
    if len("".join(ch for ch in str(period) if ch.isdigit())) != 8:
        blocking_errors.append("period_must_be_YYYYMMDD")
    match_status = "candidate" if announcement_title else "source_unavailable"
    return HKEXDisclosureAutomationGate(
        report_version="hkex-disclosure-metadata-probe/v1",
        stock_code=str(stock_code),
        period=str(period),
        source_status="tentative_manual_audit",
        automation_status="manual_audit_only",
        manual_audit_required=True,
        can_auto_match_disclosure_date=False,
        match_status=match_status,
        max_requests=max_requests,
        real_requests_sent=False,
        limitations=[
            "No stable documented HKEX JSON metadata API is assumed.",
            "No PDF download, bulk crawl, or document parsing is allowed in this goal.",
            "Operator-provided disclosure dates may be reviewed later but are not trusted automatically.",
        ],
        warnings=warnings,
        blocking_errors=blocking_errors,
    )


def classify_disclosure_match(
    *,
    identifier_match: bool,
    period_match: bool,
    report_type_match: bool,
    source_doc_id_present: bool,
    external_disclosure_date: str | None,
    tushare_notice_date: str | None,
    source_available: bool = True,
    unsafe_source: bool = False,
    title_only: bool = False,
    operator_audited: bool = False,
    value_reconciled: bool = False,
    near_tolerance_days: int = 7,
) -> DisclosureMatchResult:
    warnings: list[str] = []
    blocking_errors: list[str] = []
    if not source_available:
        blocking_errors.append("disclosure_source_unavailable")
    if unsafe_source:
        blocking_errors.append("disclosure_source_unsafe")
    if blocking_errors:
        return DisclosureMatchResult(
            match_status="blocked",
            match_confidence=0.0,
            pit_strength_candidate="raw_only",
            feature_eligible=False,
            date_delta_days=None,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    if title_only:
        warnings.append("title_only_match_requires_manual_audit")
        return DisclosureMatchResult(
            match_status="candidate",
            match_confidence=0.25,
            pit_strength_candidate="raw_only",
            feature_eligible=False,
            date_delta_days=None,
            warnings=warnings,
            blocking_errors=[],
        )

    if not identifier_match or not period_match:
        return DisclosureMatchResult(
            match_status="unmatched",
            match_confidence=0.0,
            pit_strength_candidate="raw_only",
            feature_eligible=False,
            date_delta_days=None,
            warnings=warnings,
            blocking_errors=[],
        )

    external_date = _parse_yyyymmdd(external_disclosure_date)
    tushare_date = _parse_yyyymmdd(tushare_notice_date)
    date_delta = abs((tushare_date - external_date).days) if external_date and tushare_date else None
    if report_type_match and source_doc_id_present and date_delta == 0:
        strength = "as_filed_verified" if value_reconciled else "availability_only"
        return DisclosureMatchResult(
            match_status="exact",
            match_confidence=1.0,
            pit_strength_candidate=strength,
            feature_eligible=True,
            date_delta_days=0,
            warnings=["values are not as-filed verified"] if not value_reconciled else [],
            blocking_errors=[],
        )
    if report_type_match and source_doc_id_present and date_delta is not None and date_delta <= near_tolerance_days:
        strength = "as_filed_verified" if value_reconciled else "availability_only"
        return DisclosureMatchResult(
            match_status="near",
            match_confidence=0.8,
            pit_strength_candidate=strength,
            feature_eligible=True,
            date_delta_days=date_delta,
            warnings=["near disclosure-date match requires review before feature promotion"],
            blocking_errors=[],
        )
    if operator_audited and source_doc_id_present and external_date:
        return DisclosureMatchResult(
            match_status="period_only",
            match_confidence=0.6,
            pit_strength_candidate="availability_only",
            feature_eligible=True,
            date_delta_days=date_delta,
            warnings=["operator-audited period-only match is availability-only, not as-filed verified"],
            blocking_errors=[],
        )
    warnings.append("identifier and period align but report type or disclosure date confidence is insufficient")
    return DisclosureMatchResult(
        match_status="period_only",
        match_confidence=0.5,
        pit_strength_candidate="raw_only",
        feature_eligible=False,
        date_delta_days=date_delta,
        warnings=warnings,
        blocking_errors=[],
    )


def evaluate_pit_feature_gate(pit_strength: str, *, require_as_filed: bool = False) -> PITFeatureGateResult:
    validate_pit_strength(pit_strength)
    warnings: list[str] = []
    blocking_errors: list[str] = []
    if pit_strength == "raw_only":
        blocking_errors.append("raw_only_not_feature_eligible")
        return PITFeatureGateResult(
            status="blocked",
            pit_strength=pit_strength,
            feature_eligible=False,
            strong_feature_eligible=False,
            require_as_filed=require_as_filed,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )
    if pit_strength == "availability_only":
        warnings.append("availability_only gates by disclosure date but values are not as-filed verified")
        if require_as_filed:
            blocking_errors.append("as_filed_verified_required")
            return PITFeatureGateResult(
                status="blocked",
                pit_strength=pit_strength,
                feature_eligible=False,
                strong_feature_eligible=False,
                require_as_filed=True,
                warnings=warnings,
                blocking_errors=blocking_errors,
            )
        return PITFeatureGateResult(
            status="warning",
            pit_strength=pit_strength,
            feature_eligible=True,
            strong_feature_eligible=False,
            require_as_filed=False,
            warnings=warnings,
            blocking_errors=[],
        )
    return PITFeatureGateResult(
        status="passed",
        pit_strength=pit_strength,
        feature_eligible=True,
        strong_feature_eligible=True,
        require_as_filed=require_as_filed,
        warnings=warnings,
        blocking_errors=[],
    )


def _parse_yyyymmdd(value: str | None) -> date | None:
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) != 8:
        return None
    try:
        return date(int(digits[:4]), int(digits[4:6]), int(digits[6:]))
    except ValueError:
        return None


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
