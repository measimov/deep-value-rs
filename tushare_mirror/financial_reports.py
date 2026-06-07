from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .code_universe import CodeUniverseProvider
from .hashing import job_key as make_job_key
from .periods import PeriodRangePlanner, normalize_period_list
from .planner import JobPlanner
from .source_metadata import hk_us_low_risk_source_endpoints


FINANCIAL_RAW_SCOPE_MARKETS = {
    "hk-financial-raw": "hk",
    "us-financial-raw": "us",
}
FINANCIAL_REPORT_CATEGORIES = {"financial_statement", "financial_indicator"}


@dataclass(frozen=True)
class FinancialReportResult:
    report_version: str
    scope: str
    root: str | None
    items: list[dict[str, Any]]
    warnings: list[str]
    blocking_errors: list[str]
    summary_fields: dict[str, Any]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.update(self.summary_fields)
        data.pop("summary_fields", None)
        return data

    def summary(self) -> dict[str, Any]:
        data = {
            "report_version": self.report_version,
            "scope": self.scope,
            "root": self.root,
            **self.summary_fields,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }
        return data


class FinancialReadinessReporter:
    REPORT_VERSION = "financial-readiness/v1"

    def report(self, *, scope: str, root: str | Path | None = None) -> FinancialReportResult:
        endpoints, errors = _financial_source_endpoints(scope)
        warnings = ["financial-readiness is read-only and does not fetch or write catalog state"]
        if root is not None and not Path(root).exists():
            warnings.append(f"root path does not exist: {root}")
        items = [_endpoint_readiness_item(item) for item in endpoints]
        raw_ready = [item["api_name"] for item in items if item["raw_ready"]]
        pit_safe = [item["api_name"] for item in items if item["pit_safe_ready"]]
        permission_blocked = [item["api_name"] for item in items if item["permission_blocked"]]
        contract_blocked = [item["api_name"] for item in items if item["contract_blocked"]]
        summary_fields = {
            "endpoint_count": len(items),
            "raw_ready_count": len(raw_ready),
            "pit_safe_ready_count": len(pit_safe),
            "permission_blocked_count": len(permission_blocked),
            "contract_blocked_count": len(contract_blocked),
            "raw_ready": raw_ready,
            "pit_safe_ready": pit_safe,
            "permission_blocked": permission_blocked,
            "contract_blocked": contract_blocked,
            "not_a_full_pull": True,
            "not_pit_safe_unless_disclosure_date_present": True,
        }
        return FinancialReportResult(
            report_version=self.REPORT_VERSION,
            scope=scope,
            root=str(root) if root is not None else None,
            items=items,
            warnings=warnings,
            blocking_errors=errors,
            summary_fields=summary_fields,
        )


class FinancialRequestEstimateReporter:
    REPORT_VERSION = "financial-request-estimate/v1"

    def report(
        self,
        *,
        scope: str,
        from_period: str,
        to_period: str,
        limit_codes: int,
        max_periods: int = 20,
    ) -> FinancialReportResult:
        endpoints, errors = _financial_source_endpoints(scope)
        warnings = ["financial-request-estimate is read-only and does not call Tushare or inspect token quota"]
        if limit_codes <= 0:
            errors.append("limit_codes_must_be_positive")
        if limit_codes > 20:
            errors.append("limit_codes_exceeds_guarded_limit:20")
        resolved_to = _resolve_to_period(to_period)
        try:
            period_plan = PeriodRangePlanner().plan(
                start_period=from_period,
                end_period=resolved_to,
                max_periods=max_periods,
            )
            periods = period_plan.periods
            total_periods = period_plan.total_periods
            planned_periods = period_plan.planned_periods
            truncated_by_max_periods = period_plan.truncated_by_max_periods
        except ValueError as exc:
            periods = []
            total_periods = 0
            planned_periods = 0
            truncated_by_max_periods = False
            errors.append(str(exc))

        raw_ready_endpoints = [item for item in endpoints if bool(item.get("raw_mirror_candidate")) and item.get("real_probe_status") == "passed"]
        estimated_by_api: dict[str, int] = {}
        for item in endpoints:
            api_name = str(item.get("api_name"))
            estimated_by_api[api_name] = (limit_codes * planned_periods) if item in raw_ready_endpoints and not errors else 0
        estimated_total = sum(estimated_by_api.values())
        items = [
            {
                "api_name": str(item.get("api_name")),
                "raw_ready": bool(item.get("raw_mirror_candidate")) and item.get("real_probe_status") == "passed",
                "pit_safe_ready": bool(item.get("pit_safe_candidate")),
                "estimated_requests": estimated_by_api[str(item.get("api_name"))],
                "not_a_quota_guarantee": True,
            }
            for item in endpoints
        ]
        summary_fields = {
            "from_period": from_period,
            "to_period": to_period,
            "resolved_to_period": resolved_to,
            "limit_codes": limit_codes,
            "max_periods": max_periods,
            "planned_periods": planned_periods,
            "total_periods": total_periods,
            "truncated_by_max_periods": truncated_by_max_periods,
            "estimated_requests_by_api": estimated_by_api,
            "estimated_total_requests": estimated_total,
            "not_a_quota_guarantee": True,
            "period_sample": periods[:5],
        }
        return FinancialReportResult(
            report_version=self.REPORT_VERSION,
            scope=scope,
            root=None,
            items=items,
            warnings=warnings,
            blocking_errors=errors,
            summary_fields=summary_fields,
        )


class FinancialCoverageMatrixReporter:
    REPORT_VERSION = "financial-coverage-matrix/v1"

    def report(
        self,
        *,
        root: str | Path,
        scope: str,
        periods: str,
        limit_codes: int,
        universe: str | None = None,
    ) -> FinancialReportResult:
        endpoints, errors = _financial_source_endpoints(scope)
        mirror_root = Path(root)
        warnings = ["financial-coverage-matrix is read-only and does not fetch, backfill, or write catalog state"]
        if not (mirror_root / "_catalog" / "catalog.sqlite").exists():
            errors.append(f"catalog not found: {mirror_root / '_catalog' / 'catalog.sqlite'}")
        try:
            planned_periods = normalize_period_list(periods)
        except ValueError as exc:
            planned_periods = []
            errors.append(str(exc))

        catalog = CatalogStore(mirror_root, read_only=True)
        universe_name = universe or _default_universe_for_scope(scope)
        universe_result = CodeUniverseProvider(mirror_root, catalog).get(universe_name, limit=limit_codes)
        codes = universe_result.codes[: max(limit_codes, 0)] if not universe_result.blocked else []
        if universe_result.blocked:
            errors.append(str(universe_result.blocked_reason))
        items: list[dict[str, Any]] = []
        raw_ready_endpoints = [item for item in endpoints if bool(item.get("raw_mirror_candidate")) and item.get("real_probe_status") == "passed"]
        for endpoint in endpoints:
            api_name = str(endpoint.get("api_name"))
            raw_ready = endpoint in raw_ready_endpoints
            total = len(codes) * len(planned_periods) if raw_ready else 0
            covered = _covered_code_period_count(catalog, mirror_root, api_name, codes, planned_periods) if raw_ready and not errors else 0
            missing = max(total - covered, 0)
            items.append(
                {
                    "api_name": api_name,
                    "coverage_class": "code_period",
                    "raw_ready": raw_ready,
                    "pit_safe_ready": bool(endpoint.get("pit_safe_candidate")),
                    "total_code_periods": total,
                    "covered_code_periods": covered,
                    "missing_code_periods": missing,
                    "coverage_ratio": round(covered / total, 6) if total else 0.0,
                    "status": "complete" if total and covered == total else ("not_raw_ready" if not raw_ready else "missing"),
                }
            )
        summary_fields = {
            "universe": universe_name,
            "source_snapshot_id": universe_result.source_snapshot_id,
            "planned_codes": len(codes),
            "planned_periods": len(planned_periods),
            "coverage_by_code_period": True,
        }
        return FinancialReportResult(
            report_version=self.REPORT_VERSION,
            scope=scope,
            root=str(root),
            items=items,
            warnings=warnings + list(universe_result.warnings),
            blocking_errors=errors,
            summary_fields=summary_fields,
        )


def _financial_source_endpoints(scope: str) -> tuple[list[dict[str, Any]], list[str]]:
    market = FINANCIAL_RAW_SCOPE_MARKETS.get(scope)
    if market is None:
        return [], [f"unsupported_financial_scope:{scope}"]
    endpoints = [
        dict(item)
        for item in hk_us_low_risk_source_endpoints()
        if item.get("market") == market and item.get("category") in FINANCIAL_REPORT_CATEGORIES
    ]
    return endpoints, []


def _endpoint_readiness_item(item: dict[str, Any]) -> dict[str, Any]:
    api_name = str(item.get("api_name"))
    probe_status = str(item.get("real_probe_status") or "pending")
    raw_ready = bool(item.get("raw_mirror_candidate")) and probe_status == "passed"
    pit_safe = bool(item.get("pit_safe_candidate")) and raw_ready
    permission_blocked = probe_status == "blocked_by_permission"
    contract_blocked = not raw_ready and probe_status in {"empty_but_accessible", "contract_changed", "failed", "pending"}
    if pit_safe:
        pit_status = "complete"
    elif raw_ready:
        pit_status = "blocked_without_disclosure_date"
    elif permission_blocked:
        pit_status = "permission_blocked"
    else:
        pit_status = "probe_or_contract_pending"
    observed_fields = list((item.get("real_probe_observed") or {}).get("fields") or [])
    return {
        "api_name": api_name,
        "probe_status": probe_status,
        "raw_ready": raw_ready,
        "pit_safe_ready": pit_safe,
        "permission_blocked": permission_blocked,
        "contract_blocked": contract_blocked,
        "pit_usable_after_status": pit_status,
        "observed_fields": observed_fields,
        "observed_disclosure_fields": _observed_disclosure_fields(observed_fields),
        "recommended_execution_status": "guarded_raw" if raw_ready else "plan_only",
    }


def _observed_disclosure_fields(fields: list[str]) -> list[str]:
    disclosure = {"ann_date", "f_ann_date", "notice_date", "disclosure_date", "publish_date"}
    return [field for field in fields if field in disclosure]


def _default_universe_for_scope(scope: str) -> str:
    return "hk_listed" if scope == "hk-financial-raw" else "us_equity"


def _covered_code_period_count(catalog: CatalogStore, root: Path, api_name: str, codes: list[str], periods: list[str]) -> int:
    covered = 0
    planner: JobPlanner | None
    try:
        catalog.get_endpoint_config(api_name)
        planner = JobPlanner(root, catalog)
    except KeyError:
        planner = None
    for ts_code in codes:
        for period in periods:
            params = {"ts_code": ts_code, "period": period}
            if planner is not None:
                fetch_plan = planner.plan_single_fetch(api_name, params)
                job_key = fetch_plan.job_key
            else:
                job_key = make_job_key(api_name, params, [], f"inventory_{api_name}_code_period_v1")
            job = catalog.get_job(job_key)
            if job and job.get("status") == "done":
                covered += 1
    return covered


def _resolve_to_period(value: str) -> str:
    if value != "latest":
        return value
    today = date.today()
    quarter = (today.month - 1) // 3 + 1
    latest_quarter = quarter - 1
    year = today.year
    if latest_quarter == 0:
        latest_quarter = 4
        year -= 1
    return {
        1: f"{year}0331",
        2: f"{year}0630",
        3: f"{year}0930",
        4: f"{year}1231",
    }[latest_quarter]
