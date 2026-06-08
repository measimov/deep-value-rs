from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .disclosure import validate_disclosure_event_schema, validate_financial_disclosure_sources


DISCLOSURE_CONTRACT_REPORT_VERSION = "disclosure-contract-report/v1"


@dataclass(frozen=True)
class DisclosureContractReport:
    report_version: str
    sec_probe: str
    cross_check: str
    source_status: str
    schema_status: str
    sec_status: str
    tushare_status: str
    match_status: str
    pit_strength_candidate: str
    can_mark_availability_only: bool
    can_mark_as_filed_verified: bool
    sec_disclosure_date: str | None
    tushare_notice_date: str | None
    date_delta_days: int | None
    match_confidence: float
    limitations: list[str]
    warnings: list[str]
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


class DisclosureContractReporter:
    def report(self, *, sec_probe: str | Path, cross_check: str | Path) -> DisclosureContractReport:
        warnings = ["disclosure-contract-report is read-only and does not fetch or write catalog state"]
        blocking_errors: list[str] = []
        source_errors = validate_financial_disclosure_sources()
        schema_errors = validate_disclosure_event_schema()
        source_status = "passed" if not source_errors else "blocked"
        schema_status = "passed" if not schema_errors else "blocked"
        blocking_errors.extend(f"source:{error}" for error in source_errors)
        blocking_errors.extend(f"schema:{error}" for error in schema_errors)

        sec_payload, sec_errors = _read_json_report(Path(sec_probe), "sec-disclosure-probe/v1")
        cross_payload, cross_errors = _read_json_report(Path(cross_check), "sec-tushare-disclosure-cross-check/v1")
        blocking_errors.extend(sec_errors)
        blocking_errors.extend(cross_errors)

        sec_status = str(cross_payload.get("sec_status") or sec_payload.get("overall_status") or "unknown")
        tushare_status = str(cross_payload.get("tushare_status") or "unknown")
        match_status = str(cross_payload.get("match_status") or "unknown")
        pit_strength = str(cross_payload.get("pit_strength_candidate") or "raw_only")
        sec_disclosure_date = _optional_str(cross_payload.get("sec_disclosure_date"))
        tushare_notice_date = _optional_str(cross_payload.get("tushare_notice_date"))
        date_delta_days = cross_payload.get("date_delta_days")
        date_delta = int(date_delta_days) if isinstance(date_delta_days, int) else None
        match_confidence_raw = cross_payload.get("match_confidence")
        match_confidence = float(match_confidence_raw) if isinstance(match_confidence_raw, (int, float)) else 0.0
        limitations = [str(item) for item in (cross_payload.get("limitations") or [])]
        limitations.append("as_filed_verified is false until value-level reconciliation exists")

        if sec_payload.get("token_plaintext_found") or cross_payload.get("token_plaintext_found"):
            blocking_errors.append("probe_report_token_plaintext_found")
        if sec_status == "blocked":
            blocking_errors.append("sec_status_blocked")
        if tushare_status == "blocked":
            blocking_errors.append("tushare_status_blocked")
        if not tushare_notice_date:
            warnings.append("missing Tushare notice_date prevents availability_only")
        if match_status in {"candidate", "period_only"}:
            warnings.append(f"{match_status} match cannot enter the feature layer")

        can_mark_availability_only = (
            not blocking_errors
            and sec_status == "passed"
            and tushare_status == "passed"
            and match_status in {"exact", "near"}
            and pit_strength == "availability_only"
            and bool(sec_disclosure_date)
            and bool(tushare_notice_date)
        )
        return DisclosureContractReport(
            report_version=DISCLOSURE_CONTRACT_REPORT_VERSION,
            sec_probe=str(sec_probe),
            cross_check=str(cross_check),
            source_status=source_status,
            schema_status=schema_status,
            sec_status=sec_status,
            tushare_status=tushare_status,
            match_status=match_status,
            pit_strength_candidate=pit_strength,
            can_mark_availability_only=can_mark_availability_only,
            can_mark_as_filed_verified=False,
            sec_disclosure_date=sec_disclosure_date,
            tushare_notice_date=tushare_notice_date,
            date_delta_days=date_delta,
            match_confidence=match_confidence,
            limitations=limitations,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )


def _read_json_report(path: Path, expected_version: str) -> tuple[dict[str, Any], list[str]]:
    if not path.exists():
        return {}, [f"report_not_found:{path}"]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, [f"report_invalid_json:{path}:{exc}"]
    if payload.get("report_version") != expected_version:
        return payload, [f"unsupported_report_version:{path}:{payload.get('report_version') or 'missing'}"]
    return payload, []


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
