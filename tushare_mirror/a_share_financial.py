from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from .catalog import CatalogStore, loads
from .errors import ErrorType, MirrorError
from .periods import MAX_PERIODS, PeriodRangePlanner
from .pit import validate_pit_safety
from .planner import JobPlanner
from .policy import EndpointExecutionPolicy, ExecutionPolicyRequest, MAX_GUARDED_FINANCIAL_RAW_JOBS
from .reader import LakeReader
from .store import FileLakeStore


A_SHARE_FINANCIAL_RAW_SCOPE = "a-share-financial-raw"
A_SHARE_FINANCIAL_APIS = (
    "income_vip",
    "balancesheet_vip",
    "cashflow_vip",
    "fina_indicator_vip",
    "disclosure_date",
)
A_SHARE_VALUE_REQUIRED_APIS = ("income_vip", "balancesheet_vip", "disclosure_date")
A_SHARE_DISCLOSURE_API = "disclosure_date"


@dataclass(frozen=True)
class AShareFinancialPlanItem:
    api_name: str
    period: str
    params: dict[str, Any]
    job_key: str | None
    existing_status: str
    planned_action: str
    pit_required: bool
    pit_safety_status: str
    would_require_real_request: bool
    execution_allowed: bool
    blocked_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AShareFinancialPlan:
    scope: str
    apis: list[str]
    periods: list[str]
    api_count: int
    period_count: int
    candidate_jobs: int
    planned_jobs: int
    max_jobs: int
    dry_run: bool
    execution_allowed: bool
    truncated_by_max_jobs: bool
    warnings: list[str]
    blocking_errors: list[str]
    items: list[AShareFinancialPlanItem]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        data["items"] = [item.to_dict() for item in self.items]
        return data


@dataclass(frozen=True)
class AShareFinancialExecutionResult:
    run_id: str | None
    requested_jobs: int
    executed_jobs: int
    skipped_jobs: int
    failed_jobs: int
    errors: list[dict[str, Any]]
    results: list[dict[str, Any]]

    @property
    def succeeded(self) -> bool:
        return self.failed_jobs == 0

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["succeeded"] = self.succeeded
        return data


@dataclass(frozen=True)
class ASharePitAvailabilityPeriod:
    period: str
    api_coverage: dict[str, bool]
    disclosure_rows: int
    disclosure_actual_date_rows: int
    disclosure_missing_actual_date_rows: int
    pit_safe: bool
    blocked_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ASharePitAvailabilityReport:
    periods: list[str]
    required_apis: list[str]
    latest_snapshots: dict[str, str | None]
    active_lake_files: dict[str, int]
    missing_apis: list[str]
    pit_safe_periods: list[str]
    blocked_periods: list[str]
    feature_layer_allowed: bool
    strict_exit_conditions: list[str]
    periods_detail: list[ASharePitAvailabilityPeriod]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["periods_detail"] = [item.to_dict() for item in self.periods_detail]
        return data


class AShareFinancialPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        *,
        apis: str | list[str] | None = None,
        periods: str | list[str] | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        period_frequency: str = "annual",
        max_periods: int = MAX_PERIODS,
        max_jobs: int = 20,
    ) -> AShareFinancialPlan:
        warnings: list[str] = []
        blocking_errors: list[str] = []
        if max_jobs <= 0:
            blocking_errors.append("max_jobs must be positive")
        if max_jobs > MAX_GUARDED_FINANCIAL_RAW_JOBS:
            blocking_errors.append(f"max_jobs exceeds guarded financial raw limit:{MAX_GUARDED_FINANCIAL_RAW_JOBS}")

        try:
            period_plan = PeriodRangePlanner().plan(
                periods=periods,
                start_period=start_period,
                end_period=end_period,
                period_frequency=period_frequency,
                max_periods=max_periods,
            )
            planned_periods = period_plan.periods
            if period_plan.truncated_by_max_periods:
                warnings.append("periods_truncated_by_max_periods")
        except ValueError as exc:
            planned_periods = []
            blocking_errors.append(str(exc))

        api_names = self._normalize_apis(apis)
        if not api_names:
            blocking_errors.append("no_apis_requested")

        items: list[AShareFinancialPlanItem] = []
        planned_real_jobs = 0
        fetch_candidates = 0
        if not blocking_errors:
            planner = JobPlanner(self.root, self.catalog)
            for api_name in api_names:
                cfg = self._endpoint_config(api_name, blocking_errors)
                if cfg is None:
                    continue
                api_policy_allowed, api_warnings, api_errors = self._api_execution_gate(cfg, max_jobs)
                warnings.extend(api_warnings)
                blocking_errors.extend(api_errors)
                pit_result = validate_pit_safety(cfg)
                if pit_result.blocked:
                    blocking_errors.extend(f"{api_name}:pit:{error}" for error in pit_result.errors)
                warnings.extend(f"{api_name}:{warning}" for warning in pit_result.warnings)
                for period in planned_periods:
                    params = self._params_for_api(api_name, cfg, period)
                    fetch_plan = planner.plan_single_fetch(api_name, params)
                    existing_status, planned_action = self._existing_status_and_action(api_name, fetch_plan.job_key, fetch_plan.existing_active_data)
                    would_request = planned_action in {"fetch", "retry_failed"}
                    if would_request:
                        fetch_candidates += 1
                    within_limit = fetch_candidates <= max_jobs
                    execution_allowed = bool(api_policy_allowed and would_request and within_limit)
                    if execution_allowed:
                        planned_real_jobs += 1
                    items.append(
                        AShareFinancialPlanItem(
                            api_name=api_name,
                            period=period,
                            params=fetch_plan.params,
                            job_key=fetch_plan.job_key,
                            existing_status=existing_status,
                            planned_action=planned_action,
                            pit_required=pit_result.pit_required,
                            pit_safety_status=pit_result.status,
                            would_require_real_request=would_request,
                            execution_allowed=execution_allowed,
                            blocked_reason=self._blocked_reason_for_action(planned_action, api_policy_allowed, within_limit),
                        )
                    )

        truncated_by_jobs = fetch_candidates > max_jobs
        if truncated_by_jobs:
            warnings.append("fetch_jobs_truncated_by_max_jobs")
        return AShareFinancialPlan(
            scope=A_SHARE_FINANCIAL_RAW_SCOPE,
            apis=api_names,
            periods=planned_periods,
            api_count=len(api_names),
            period_count=len(planned_periods),
            candidate_jobs=len(api_names) * len(planned_periods),
            planned_jobs=planned_real_jobs,
            max_jobs=max_jobs,
            dry_run=True,
            execution_allowed=planned_real_jobs > 0 and not blocking_errors,
            truncated_by_max_jobs=truncated_by_jobs,
            warnings=sorted(set(warnings)),
            blocking_errors=sorted(set(blocking_errors)),
            items=items if not blocking_errors else [],
        )

    def _normalize_apis(self, apis: str | list[str] | None) -> list[str]:
        if apis is None:
            return list(A_SHARE_FINANCIAL_APIS)
        values = [item.strip() for item in apis.split(",")] if isinstance(apis, str) else [str(item).strip() for item in apis]
        seen: set[str] = set()
        out: list[str] = []
        for api_name in values:
            if not api_name:
                continue
            if api_name not in A_SHARE_FINANCIAL_APIS:
                raise ValueError(f"unsupported A-share financial api: {api_name}")
            if api_name not in seen:
                out.append(api_name)
                seen.add(api_name)
        return out

    def _endpoint_config(self, api_name: str, blocking_errors: list[str]) -> dict[str, Any] | None:
        try:
            cfg = self.catalog.get_endpoint_config(api_name)
        except KeyError:
            blocking_errors.append(f"endpoint_not_found:{api_name}")
            return None
        if str(cfg.get("market") or "") != "a":
            blocking_errors.append(f"endpoint_market_not_a:{api_name}")
        if str(cfg.get("planner_kind") or "") != "period":
            blocking_errors.append(f"endpoint_planner_not_period:{api_name}")
        return dict(cfg)

    def _api_execution_gate(self, cfg: Mapping[str, Any], max_jobs: int) -> tuple[bool, list[str], list[str]]:
        decision = EndpointExecutionPolicy().decide(
            ExecutionPolicyRequest(
                endpoint_config=cfg,
                scope=A_SHARE_FINANCIAL_RAW_SCOPE,
                user_command="financial-raw-fetch",
                max_jobs=max_jobs,
                requires_real_requests=True,
                requires_pit_handling=False,
                max_codes_required=None,
            )
        )
        return decision.allowed, list(decision.warnings), list(decision.missing_infrastructure)

    def _params_for_api(self, api_name: str, cfg: Mapping[str, Any], period: str) -> dict[str, Any]:
        period_field = str(cfg.get("period_field") or (cfg.get("pit_safety") or {}).get("period_field") or "period")
        if api_name == A_SHARE_DISCLOSURE_API:
            period_field = "end_date"
        return {period_field: period}

    def _existing_status_and_action(self, api_name: str, job_key: str, active_exists: bool) -> tuple[str, str]:
        if active_exists:
            return "active_exists", "skip_existing"
        if self.catalog.quarantine_exists_for_job(job_key):
            return "quarantined_exists", "blocked_quarantined"
        statuses = self.catalog.file_statuses_for_job(job_key, api_name)
        if "staged" in statuses:
            return "staged_exists", "blocked_staged"
        job = self.catalog.get_job(job_key)
        if job and job.get("status") == "failed":
            return "failed_exists", "retry_failed"
        if job:
            return "unknown", "fetch"
        return "missing", "fetch"

    def _blocked_reason_for_action(self, planned_action: str, policy_allowed: bool, within_limit: bool) -> str | None:
        if not policy_allowed:
            return "financial_raw_guardrails_not_satisfied"
        if not within_limit:
            return "max_jobs_exceeded"
        if planned_action == "blocked_quarantined":
            return "quarantined_exists"
        if planned_action == "blocked_staged":
            return "staged_exists"
        return None


class AShareFinancialExecutor:
    def __init__(self, root: Path | str, catalog: CatalogStore, store: FileLakeStore | None = None):
        self.root = Path(root)
        self.catalog = catalog
        self.store = store or FileLakeStore(root, catalog)

    def execute(self, plan: AShareFinancialPlan, client, *, max_attempts: int = 3) -> AShareFinancialExecutionResult:
        executable = [item for item in plan.items if item.execution_allowed]
        if plan.blocked:
            raise MirrorError(ErrorType.INVALID_ENDPOINT, f"A-share financial plan is blocked: {plan.blocking_errors}")
        if not executable:
            return AShareFinancialExecutionResult(
                run_id=None,
                requested_jobs=0,
                executed_jobs=0,
                skipped_jobs=0,
                failed_jobs=0,
                errors=[],
                results=[],
            )

        run_id = self.catalog.create_run("financial-raw-fetch")
        results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        skipped = 0
        executed = 0
        for item in executable:
            try:
                result = self.store.fetch(
                    item.api_name,
                    item.params,
                    client,
                    max_attempts=max_attempts,
                    run_id=run_id,
                    finish_run=False,
                    run_type="financial-raw-fetch",
                    scope=A_SHARE_FINANCIAL_RAW_SCOPE,
                    max_codes_required=None,
                    requires_pit_handling=False,
                )
                if result.skipped:
                    skipped += 1
                else:
                    executed += 1
                results.append(
                    {
                        "api_name": item.api_name,
                        "period": item.period,
                        "job_key": result.job_key,
                        "snapshot_id": result.snapshot_id,
                        "record_count": result.record_count,
                        "skipped": result.skipped,
                    }
                )
            except Exception as exc:
                errors.append(
                    {
                        "api_name": item.api_name,
                        "period": item.period,
                        "job_key": item.job_key,
                        "error": str(exc),
                    }
                )
        status = "failed" if errors else "succeeded"
        self.catalog.finish_run(run_id, status, summary={"executed_jobs": executed, "skipped_jobs": skipped, "failed_jobs": len(errors)})
        return AShareFinancialExecutionResult(
            run_id=run_id,
            requested_jobs=len(executable),
            executed_jobs=executed,
            skipped_jobs=skipped,
            failed_jobs=len(errors),
            errors=errors,
            results=results,
        )


class ASharePitAvailabilityReporter:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog
        self.reader = LakeReader(root, catalog)

    def report(
        self,
        *,
        periods: str | list[str],
        required_apis: list[str] | None = None,
    ) -> ASharePitAvailabilityReport:
        api_names = list(required_apis or A_SHARE_VALUE_REQUIRED_APIS)
        period_plan = PeriodRangePlanner().plan(periods=periods, max_periods=MAX_PERIODS)
        active_files = {api: self.reader.list_active_files(api) for api in api_names}
        latest = {
            api: ((self.catalog.latest_snapshot(api) or {}).get("snapshot_id"))
            for api in api_names
        }
        missing_apis = sorted(api for api in api_names if not active_files.get(api))

        detail: list[ASharePitAvailabilityPeriod] = []
        pit_safe_periods: list[str] = []
        blocked_periods: list[str] = []
        for period in period_plan.periods:
            coverage = {api: self._has_period(active_files.get(api, []), period) for api in api_names}
            disclosure_rows, actual_rows, missing_actual_rows = self._disclosure_actual_counts(period)
            pit_safe = all(coverage.values()) and disclosure_rows > 0 and missing_actual_rows == 0
            blocked_reason = None
            if not all(coverage.values()):
                blocked_reason = "missing_required_period_coverage"
            elif disclosure_rows == 0:
                blocked_reason = "missing_disclosure_date_rows"
            elif missing_actual_rows > 0:
                blocked_reason = "missing_disclosure_actual_date"
            if pit_safe:
                pit_safe_periods.append(period)
            else:
                blocked_periods.append(period)
            detail.append(
                ASharePitAvailabilityPeriod(
                    period=period,
                    api_coverage=coverage,
                    disclosure_rows=disclosure_rows,
                    disclosure_actual_date_rows=actual_rows,
                    disclosure_missing_actual_date_rows=missing_actual_rows,
                    pit_safe=pit_safe,
                    blocked_reason=blocked_reason,
                )
            )

        return ASharePitAvailabilityReport(
            periods=period_plan.periods,
            required_apis=api_names,
            latest_snapshots={api: (str(value) if value else None) for api, value in latest.items()},
            active_lake_files={api: len(files) for api, files in active_files.items()},
            missing_apis=missing_apis,
            pit_safe_periods=pit_safe_periods,
            blocked_periods=blocked_periods,
            feature_layer_allowed=bool(period_plan.periods) and len(pit_safe_periods) == len(period_plan.periods),
            strict_exit_conditions=[
                "income_vip, balancesheet_vip, and disclosure_date have current lake files",
                "each requested period has active lake coverage for all required APIs",
                "disclosure_date rows exist for each period",
                "every disclosure_date row has a non-empty actual_date",
            ],
            periods_detail=detail,
        )

    def _has_period(self, files: list[dict[str, Any]], period: str) -> bool:
        for row in files:
            values = loads(row.get("partition_values_json")) or {}
            if str(values.get("period_date") or "") == period:
                return True
        return False

    def _disclosure_actual_counts(self, period: str) -> tuple[int, int, int]:
        try:
            table = self.reader.scan_partition(A_SHARE_DISCLOSURE_API, {"period_date": period})
        except Exception:
            return 0, 0, 0
        rows = table.num_rows
        if rows == 0 or "actual_date" not in table.column_names:
            return rows, 0, rows
        actual_values = table["actual_date"].to_pylist()
        actual_rows = sum(1 for value in actual_values if value is not None and str(value).strip())
        return rows, actual_rows, rows - actual_rows
