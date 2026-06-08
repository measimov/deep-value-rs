from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .disclosure import disclosure_sources, evaluate_pit_feature_gate
from .periods import PeriodRangePlanner
from .source_metadata import hk_us_low_risk_source_endpoints


DISCLOSURE_FINANCIAL_SCOPES = {"hk-financial-raw": "hk", "us-financial-raw": "us"}
DISCLOSURE_FINANCIAL_CATEGORIES = {"financial_statement", "financial_indicator"}


@dataclass(frozen=True)
class DisclosureReportResult:
    report_version: str
    scope: str
    root: str | None
    raw_only_count: int
    availability_only_count: int
    as_filed_verified_count: int
    candidate_count: int
    blocked_count: int
    feature_eligible_count: int
    items: list[dict[str, Any]]
    warnings: list[str]
    blocking_errors: list[str]
    summary_fields: dict[str, Any] | None = None

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        summary = data.pop("summary_fields") or {}
        data.update(summary)
        return data

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


class DisclosureSourceReporter:
    REPORT_VERSION = "disclosure-source-report/v1"

    def report(self) -> DisclosureReportResult:
        sources = [source.to_dict() for source in disclosure_sources()]
        blocked = [item for item in sources if item["source_status"] in {"disabled", "future_vendor_placeholder"}]
        candidates = [item for item in sources if item["source_status"] == "tentative_manual_audit"]
        return DisclosureReportResult(
            report_version=self.REPORT_VERSION,
            scope="all",
            root=None,
            raw_only_count=0,
            availability_only_count=0,
            as_filed_verified_count=0,
            candidate_count=len(candidates),
            blocked_count=len(blocked),
            feature_eligible_count=0,
            items=sources,
            warnings=["disclosure-source-report is read-only and does not fetch or write catalog state"],
            blocking_errors=[],
            summary_fields={"source_count": len(sources)},
        )


class DisclosurePlanReporter:
    REPORT_VERSION = "disclosure-plan/v1"

    def report(
        self,
        *,
        scope: str,
        from_period: str,
        to_period: str,
        limit_codes: int,
        max_periods: int = 20,
    ) -> DisclosureReportResult:
        endpoints, errors = _source_endpoints(scope)
        warnings = ["disclosure-plan is read-only and does not fetch or write catalog state"]
        if limit_codes <= 0:
            errors.append("limit_codes_must_be_positive")
        if limit_codes > 20:
            errors.append("limit_codes_exceeds_guarded_limit:20")
        try:
            periods = PeriodRangePlanner().plan(start_period=from_period, end_period=to_period, max_periods=max_periods)
            planned_periods = periods.planned_periods
            period_sample = periods.periods[:5]
        except ValueError as exc:
            planned_periods = 0
            period_sample = []
            errors.append(str(exc))
        items = [_endpoint_state(item) for item in endpoints]
        return _result(
            report_version=self.REPORT_VERSION,
            scope=scope,
            root=None,
            items=items,
            warnings=warnings,
            blocking_errors=errors,
            summary_fields={
                "from_period": from_period,
                "to_period": to_period,
                "limit_codes": limit_codes,
                "max_periods": max_periods,
                "planned_periods": planned_periods,
                "period_sample": period_sample,
                "planned_disclosure_checks": limit_codes * planned_periods * len([item for item in items if item["state"] in {"raw_only", "candidate"}]) if not errors else 0,
            },
        )


class DisclosureAvailabilityReporter:
    REPORT_VERSION = "disclosure-availability/v1"

    def report(self, *, scope: str, root: str | Path | None = None) -> DisclosureReportResult:
        endpoints, errors = _source_endpoints(scope)
        warnings = ["disclosure-availability is read-only and does not fetch or write catalog state"]
        if root is not None and not Path(root).exists():
            warnings.append(f"root path does not exist: {root}")
        items = [_endpoint_state(item) for item in endpoints]
        return _result(
            report_version=self.REPORT_VERSION,
            scope=scope,
            root=str(root) if root is not None else None,
            items=items,
            warnings=warnings,
            blocking_errors=errors,
        )


class DisclosureGateReporter:
    REPORT_VERSION = "disclosure-gate/v1"

    def report(self, *, scope: str, api_name: str, ts_code: str, period: str) -> DisclosureReportResult:
        endpoints, errors = _source_endpoints(scope)
        warnings = ["disclosure-gate is read-only and does not fetch or write catalog state"]
        endpoint = next((item for item in endpoints if item.get("api_name") == api_name), None)
        if endpoint is None:
            errors.append(f"api_not_in_scope:{api_name}:{scope}")
            items: list[dict[str, Any]] = []
        else:
            item = _endpoint_state(endpoint)
            item.update({"ts_code": ts_code, "period": period, "feature_eligible": False})
            feature_gate = evaluate_pit_feature_gate(item["pit_strength"])
            item["feature_gate_status"] = feature_gate.status
            item["feature_gate_blocking_errors"] = feature_gate.blocking_errors
            if item["state"] == "candidate":
                warnings.append("candidate disclosure state is not feature-eligible without an exact or approved near match")
            items = [item]
        return _result(
            report_version=self.REPORT_VERSION,
            scope=scope,
            root=None,
            items=items,
            warnings=warnings,
            blocking_errors=errors,
            summary_fields={"api_name": api_name, "ts_code": ts_code, "period": period},
        )


def _source_endpoints(scope: str) -> tuple[list[dict[str, Any]], list[str]]:
    market = DISCLOSURE_FINANCIAL_SCOPES.get(scope)
    if market is None:
        return [], [f"unsupported_disclosure_scope:{scope}"]
    endpoints = [
        dict(item)
        for item in hk_us_low_risk_source_endpoints()
        if item.get("market") == market and item.get("category") in DISCLOSURE_FINANCIAL_CATEGORIES
    ]
    return endpoints, []


def _endpoint_state(item: dict[str, Any]) -> dict[str, Any]:
    api_name = str(item.get("api_name"))
    raw_ready = bool(item.get("raw_mirror_candidate")) and item.get("real_probe_status") == "passed"
    pit_candidate = bool(item.get("pit_safe_candidate")) and raw_ready
    if pit_candidate:
        state = "candidate"
        strength = "raw_only"
        feature_eligible = False
    elif raw_ready:
        state = "raw_only"
        strength = "raw_only"
        feature_eligible = False
    else:
        state = "blocked"
        strength = "raw_only"
        feature_eligible = False
    return {
        "api_name": api_name,
        "state": state,
        "raw_ready": raw_ready,
        "pit_safe_candidate": pit_candidate,
        "pit_strength": strength,
        "feature_eligible": feature_eligible,
        "reason": _state_reason(item, state),
    }


def _state_reason(item: dict[str, Any], state: str) -> str:
    if state == "candidate":
        return "Tushare notice_date exists, but no external disclosure event match has been accepted"
    if state == "raw_only":
        return "raw financial endpoint is not feature-eligible without a reliable disclosure-date match"
    return str(item.get("pit_disclosure_concern") or item.get("real_probe_status") or "not raw-ready")


def _result(
    *,
    report_version: str,
    scope: str,
    root: str | None,
    items: list[dict[str, Any]],
    warnings: list[str],
    blocking_errors: list[str],
    summary_fields: dict[str, Any] | None = None,
) -> DisclosureReportResult:
    raw_only = [item for item in items if item["state"] == "raw_only"]
    candidate = [item for item in items if item["state"] == "candidate"]
    blocked = [item for item in items if item["state"] == "blocked"]
    availability = [item for item in items if item["pit_strength"] == "availability_only"]
    as_filed = [item for item in items if item["pit_strength"] == "as_filed_verified"]
    feature_eligible = [item for item in items if item["feature_eligible"]]
    return DisclosureReportResult(
        report_version=report_version,
        scope=scope,
        root=root,
        raw_only_count=len(raw_only),
        availability_only_count=len(availability),
        as_filed_verified_count=len(as_filed),
        candidate_count=len(candidate),
        blocked_count=len(blocked),
        feature_eligible_count=len(feature_eligible),
        items=items,
        warnings=warnings,
        blocking_errors=blocking_errors,
        summary_fields=summary_fields,
    )
