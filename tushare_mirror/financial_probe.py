from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .source_metadata import hk_us_low_risk_source_endpoints


FINANCIAL_PROBE_REPORT_VERSION = "hk-us-financial-probe-report/v1"
DISCLOSURE_FIELDS = {"ann_date", "f_ann_date", "notice_date", "disclosure_date", "publish_date"}
RAW_READY_PROBE_STATUSES = {"passed"}


@dataclass(frozen=True)
class HKUSFinancialProbeEndpointReport:
    api_name: str
    probe_status: str
    documented_fields: list[str]
    observed_fields: list[str]
    inventory_assumed_pit_fields: list[str]
    missing_assumed_pit_fields: list[str]
    observed_disclosure_fields: list[str]
    raw_executable_candidate: bool
    pit_safe_candidate: bool
    pit_usable_after_status: str
    recommended_execution_status: str
    blocking_errors: list[str]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HKUSFinancialProbeReport:
    report_version: str
    input: str
    source_report_version: str | None
    raw_executable_candidates: list[str]
    pit_safe_candidates: list[str]
    blocked_without_disclosure_date: list[str]
    permission_blocked: list[str]
    contract_blocked: list[str]
    endpoint_count: int
    blocking_errors: list[str]
    warnings: list[str]
    endpoints: list[HKUSFinancialProbeEndpointReport]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["endpoints"] = [item.to_dict() for item in self.endpoints]
        return data


class HKUSFinancialProbeReporter:
    def report(self, *, input_path: Path | str) -> HKUSFinancialProbeReport:
        path = Path(input_path)
        blocking_errors: list[str] = []
        warnings: list[str] = []
        if not path.exists():
            return HKUSFinancialProbeReport(
                report_version=FINANCIAL_PROBE_REPORT_VERSION,
                input=str(path),
                source_report_version=None,
                raw_executable_candidates=[],
                pit_safe_candidates=[],
                blocked_without_disclosure_date=[],
                permission_blocked=[],
                contract_blocked=[],
                endpoint_count=0,
                blocking_errors=[f"probe_input_not_found:{path}"],
                warnings=[],
                endpoints=[],
            )
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            return HKUSFinancialProbeReport(
                report_version=FINANCIAL_PROBE_REPORT_VERSION,
                input=str(path),
                source_report_version=None,
                raw_executable_candidates=[],
                pit_safe_candidates=[],
                blocked_without_disclosure_date=[],
                permission_blocked=[],
                contract_blocked=[],
                endpoint_count=0,
                blocking_errors=[f"probe_input_invalid_json:{exc}"],
                warnings=[],
                endpoints=[],
            )

        if payload.get("report_version") != "hk-us-financial-pit-probe/v1":
            blocking_errors.append("unsupported_probe_report_version")
        if payload.get("token_plaintext_found"):
            blocking_errors.append("probe_report_token_plaintext_found")

        source_by_api = {
            str(item["api_name"]): item
            for item in hk_us_low_risk_source_endpoints()
            if str(item.get("api_name")) in {
                "hk_income",
                "hk_balancesheet",
                "hk_cashflow",
                "hk_fina_indicator",
                "us_income",
                "us_balancesheet",
                "us_cashflow",
                "us_fina_indicator",
            }
        }
        probes_by_api = {
            str(item.get("api_name")): item
            for item in payload.get("endpoints", [])
            if isinstance(item, dict) and item.get("api_name")
        }
        endpoint_reports: list[HKUSFinancialProbeEndpointReport] = []
        for api_name in sorted(source_by_api):
            source = source_by_api[api_name]
            probe = probes_by_api.get(api_name, {})
            endpoint_reports.append(self._endpoint_report(api_name, source, probe))
        missing_probe = sorted(set(source_by_api) - set(probes_by_api))
        for api_name in missing_probe:
            blocking_errors.append(f"missing_probe_result:{api_name}")

        raw = [item.api_name for item in endpoint_reports if item.raw_executable_candidate]
        pit = [item.api_name for item in endpoint_reports if item.pit_safe_candidate]
        no_disclosure = [item.api_name for item in endpoint_reports if item.pit_usable_after_status == "blocked_without_disclosure_date"]
        permission = [item.api_name for item in endpoint_reports if item.pit_usable_after_status == "permission_blocked"]
        contract = [item.api_name for item in endpoint_reports if item.pit_usable_after_status == "contract_blocked"]
        if any(item.probe_status == "empty_but_authorized" for item in endpoint_reports):
            warnings.append("one or more endpoints were authorized but returned no rows for the bounded probe parameters")
        return HKUSFinancialProbeReport(
            report_version=FINANCIAL_PROBE_REPORT_VERSION,
            input=str(path),
            source_report_version=str(payload.get("report_version") or ""),
            raw_executable_candidates=raw,
            pit_safe_candidates=pit,
            blocked_without_disclosure_date=no_disclosure,
            permission_blocked=permission,
            contract_blocked=contract,
            endpoint_count=len(endpoint_reports),
            blocking_errors=blocking_errors,
            warnings=warnings,
            endpoints=endpoint_reports,
        )

    def _endpoint_report(self, api_name: str, source: dict[str, Any], probe: dict[str, Any]) -> HKUSFinancialProbeEndpointReport:
        documented = [str(item) for item in (source.get("documented_output_fields") or source.get("documented_fields") or [])]
        observed = [str(item) for item in (probe.get("observed_fields") or [])]
        assumed = [str(item) for item in (source.get("assumed_pit_fields") or [])]
        observed_disclosure = [str(item) for item in (probe.get("observed_disclosure_fields") or []) if str(item) in DISCLOSURE_FIELDS]
        missing_assumed = [field for field in assumed if field not in observed]
        status = str(probe.get("probe_status") or "missing")
        blocking_errors: list[str] = []
        warnings: list[str] = []
        raw_candidate = status in RAW_READY_PROBE_STATUSES
        if status == "permission_denied":
            pit_status = "permission_blocked"
            blocking_errors.append("permission_denied")
        elif status == "contract_changed":
            pit_status = "contract_blocked"
            blocking_errors.append("contract_changed")
        elif status in {"failed", "missing"}:
            pit_status = "probe_pending"
            blocking_errors.append("probe_not_passed")
        elif observed_disclosure:
            pit_status = "complete"
        else:
            pit_status = "blocked_without_disclosure_date"
            warnings.append("observed probe fields do not include a disclosure date")
        if status == "empty_but_authorized":
            warnings.append("bounded probe returned no rows; keep raw execution plan-only until a non-empty contract is proven")
        pit_candidate = raw_candidate and pit_status == "complete"
        if raw_candidate:
            recommended = "raw_executable_candidate"
        elif status == "empty_but_authorized":
            recommended = "plan_only_empty_probe"
        elif status == "permission_denied":
            recommended = "plan_only_permission_blocked"
        elif status == "contract_changed":
            recommended = "plan_only_contract_blocked"
        else:
            recommended = "plan_only_probe_pending"
        return HKUSFinancialProbeEndpointReport(
            api_name=api_name,
            probe_status=status,
            documented_fields=documented,
            observed_fields=observed,
            inventory_assumed_pit_fields=assumed,
            missing_assumed_pit_fields=missing_assumed,
            observed_disclosure_fields=observed_disclosure,
            raw_executable_candidate=raw_candidate,
            pit_safe_candidate=pit_candidate,
            pit_usable_after_status=pit_status,
            recommended_execution_status=recommended,
            blocking_errors=blocking_errors,
            warnings=warnings,
        )
