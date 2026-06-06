from __future__ import annotations

import os
import json
import hashlib
import re
import shutil
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .api_infra import ApiInfrastructureReadinessReporter
from .backup import BackupExecutor, BackupInspector, BackupPlanner, RestoreChecker
from .backfill import BackfillExecutor, BackfillPlanner, DatePlanner
from .catalog import CatalogStore, loads
from .coverage import CoverageReporter
from .client import classify_probe_response
from .endpoints import load_bundled_endpoint_configs, load_inventory_configs, load_into_catalog
from .errors import classify_exception, retry_delay_seconds, should_retry
from .hashing import token_hash
from .io_utils import now_utc
from .planner import JobPlanner
from .reader import LakeReader
from .source_metadata import hk_us_low_risk_source_endpoints
from .store import FileLakeStore
from .validation import Validator

LOW_RISK_A_SHARE_ENDPOINTS = [
    "stock_basic",
    "trade_cal",
    "hs_const",
    "daily",
    "adj_factor",
    "daily_basic",
    "weekly",
    "monthly",
    "suspend_d",
    "namechange",
    "stk_managers",
    "stk_rewards",
]

A_SHARE_LOW_RISK_ENDPOINTS = [
    "stock_basic",
    "stock_company",
    "trade_cal",
    "namechange",
    "hs_const",
    "daily",
    "weekly",
    "monthly",
    "adj_factor",
    "daily_basic",
    "suspend_d",
    "stk_managers",
    "stk_rewards",
    "top10_holders",
    "top10_floatholders",
    "stk_holdernumber",
    "stk_holdertrade",
    "pledge_stat",
    "pledge_detail",
    "repurchase",
    "concept",
    "concept_detail",
    "index_basic",
    "index_daily",
    "index_weekly",
    "index_monthly",
    "index_weight",
    "index_member",
    "ths_index",
    "ths_member",
    "index_classify",
]

A_SHARE_LOW_RISK_CATEGORIES = {
    "reference_snapshot": ["stock_basic", "stock_company", "trade_cal", "namechange", "hs_const"],
    "daily_periodic_market_data": ["daily", "weekly", "monthly", "adj_factor", "daily_basic", "suspend_d"],
    "event_governance": [
        "stk_managers",
        "stk_rewards",
        "top10_holders",
        "top10_floatholders",
        "stk_holdernumber",
        "stk_holdertrade",
        "pledge_stat",
        "pledge_detail",
        "repurchase",
    ],
    "concept_industry_index_metadata": [
        "concept",
        "concept_detail",
        "index_basic",
        "index_daily",
        "index_weekly",
        "index_monthly",
        "index_weight",
        "index_member",
        "ths_index",
        "ths_member",
        "index_classify",
    ],
}

PROHIBITED_SCOPE_ENDPOINT_PATTERNS = [
    "minute",
    "tick",
    "order",
    "realtime",
    "income",
    "balancesheet",
    "cashflow",
    "fina_indicator",
    "express",
    "forecast",
    "anns",
    "news",
    "report",
]

SMOKE_REFERENCE_FETCHES: dict[str, dict[str, Any]] = {
    "stock_basic": {"list_status": "L"},
    "trade_cal": {"exchange": "SSE", "start_date": "20250101", "end_date": "20250110"},
    "hs_const": {"hs_type": "SH", "is_new": "1"},
    "namechange": {"ts_code": "000001.SZ"},
    "stk_managers": {"ts_code": "000001.SZ"},
    "stk_rewards": {"ts_code": "000001.SZ"},
}

A_SHARE_LOW_RISK_REFERENCE_FETCHES: dict[str, dict[str, Any]] = {
    "stock_basic": {"list_status": "L"},
    "stock_company": {"exchange": "SSE"},
    "trade_cal": {"exchange": "SSE", "start_date": "20250101", "end_date": "20250110"},
    "hs_const": {"hs_type": "SH", "is_new": "1"},
    "concept": {"src": "ts"},
    "index_basic": {"market": "SSE"},
    "ths_index": {"exchange": "A", "type": "N"},
    "index_classify": {"src": "SW2021", "level": "L1"},
}

A_SHARE_LOW_RISK_STOCK_CODE_SMOKE_FETCHES: dict[str, dict[str, Any]] = {
    "namechange": {"ts_code": "000001.SZ"},
    "stk_managers": {"ts_code": "000001.SZ"},
    "stk_rewards": {"ts_code": "000001.SZ"},
}

SMOKE_CALENDAR_BACKFILL_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d"]
A_SHARE_LOW_RISK_CALENDAR_BACKFILL_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d", "index_daily"]
SMOKE_EXPLICIT_DATE_APIS: dict[str, list[str]] = {
    "weekly": ["20250103", "20250110"],
    "monthly": ["20250127", "20250228"],
}
A_SHARE_LOW_RISK_EXPLICIT_DATE_APIS: dict[str, list[str]] = {
    **SMOKE_EXPLICIT_DATE_APIS,
    "index_weekly": ["20250103", "20250110"],
    "index_monthly": ["20250127", "20250228"],
}

PILOT_JAN_2025_WEEKLY_DATES = ["20250103", "20250110", "20250117", "20250124", "20250127"]
PILOT_JAN_2025_MONTHLY_DATES = ["20250127"]

PILOT_BACKFILL_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d", "weekly", "monthly"]
DAILY_LIKE_MIRROR_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d"]
A_SHARE_LOW_RISK_DAILY_LIKE_APIS = ["daily", "adj_factor", "daily_basic", "suspend_d", "index_daily"]
A_SHARE_LOW_RISK_EXPLICIT_PERIODIC_APIS = ["weekly", "monthly", "index_weekly", "index_monthly"]
A_SHARE_LOW_RISK_PLAN_ONLY_APIS = [
    "namechange",
    "stk_managers",
    "stk_rewards",
    "top10_holders",
    "top10_floatholders",
    "stk_holdernumber",
    "stk_holdertrade",
    "pledge_stat",
    "pledge_detail",
    "repurchase",
    "concept_detail",
    "index_weight",
    "index_member",
    "ths_member",
]
HK_LOW_RISK_SCOPE = "hk-low-risk"
US_LOW_RISK_SCOPE = "us-low-risk"
GLOBAL_EQUITY_LOW_RISK_SCOPE = "global-equity-low-risk"
SUPPORTED_MIRROR_SCOPES = {
    "low-risk-a-share",
    "a-share-low-risk",
    HK_LOW_RISK_SCOPE,
    US_LOW_RISK_SCOPE,
    GLOBAL_EQUITY_LOW_RISK_SCOPE,
}
HK_US_SCOPE_MARKETS = {HK_LOW_RISK_SCOPE: "hk", US_LOW_RISK_SCOPE: "us"}
GLOBAL_EQUITY_CHILD_SCOPES = ["a-share-low-risk", HK_LOW_RISK_SCOPE, US_LOW_RISK_SCOPE]
MODE_MAX_JOBS = {"smoke": 3, "pilot": 20}


def _valid_until_for(status: str) -> str:
    now = datetime.now(timezone.utc)
    if status in {"accessible", "empty_but_accessible"}:
        delta = timedelta(days=7)
    elif status in {"rate_limited", "network_error", "server_error", "unknown_error"}:
        delta = timedelta(days=1)
    else:
        delta = timedelta(days=30)
    return (now + delta).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class MirrorPlanItem:
    endpoint: str
    category: str
    requires_trade_cal: bool
    plan_status: str
    planned_jobs: int
    max_jobs: int
    existing_coverage: str | None
    missing_jobs: int
    blocked_reason: str | None
    will_execute: bool
    params: dict[str, Any] | None = None
    dates: list[str] | None = None
    permission_status: str | None = None
    planned_action: str | None = None
    required_by: list[str] | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MirrorPlan:
    scope: str
    mode: str
    root: str
    start_date: str | None
    end_date: str | None
    max_jobs_per_api: int
    endpoint_count: int
    planned_endpoint_count: int
    blocked_endpoint_count: int
    total_planned_jobs: int
    requires_real_requests: bool
    dry_run: bool
    items: list[MirrorPlanItem]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["items"] = [item.to_dict() for item in self.items]
        return data

    def summary(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "mode": self.mode,
            "root": self.root,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "max_jobs_per_api": self.max_jobs_per_api,
            "endpoint_count": self.endpoint_count,
            "planned_endpoint_count": self.planned_endpoint_count,
            "blocked_endpoint_count": self.blocked_endpoint_count,
            "total_planned_jobs": self.total_planned_jobs,
            "requires_real_requests": self.requires_real_requests,
            "dry_run": self.dry_run,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class MirrorRunResult:
    run_id: str | None
    status: str
    summary: dict[str, Any]
    validation: dict[str, Any] | None = None
    backup: dict[str, Any] | None = None
    restore_check: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "summary": self.summary,
            "validation": self.validation,
            "backup": self.backup,
            "restore_check": self.restore_check,
        }


@dataclass(frozen=True)
class MirrorReviewResult:
    root: str
    backup: str
    scope: str
    mode: str
    start_date: str
    end_date: str
    calendar_exchange: str
    root_status: str
    backup_status: str
    catalog_status: dict[str, Any]
    latest_snapshots: list[dict[str, Any]]
    endpoint_summary: list[dict[str, Any]]
    coverage_summary: list[dict[str, Any]]
    validation_status: str
    validation_results: list[dict[str, Any]]
    backup_inspect: dict[str, Any] | None
    backup_restore_check: dict[str, Any] | None
    backup_catalog_checksum_status: str | None
    backup_possible_mutation: bool
    artifact_size: dict[str, Any]
    token_plaintext_found: bool
    ready_for_next_batch: bool
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "root_status": self.root_status,
            "backup_status": self.backup_status,
            "validation_status": self.validation_status,
            "backup_catalog_checksum_status": self.backup_catalog_checksum_status,
            "backup_possible_mutation": self.backup_possible_mutation,
            "token_plaintext_found": self.token_plaintext_found,
            "ready_for_next_batch": self.ready_for_next_batch,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class MirrorReadinessResult:
    root: str
    backup: str
    scope: str
    readiness_status: str
    ready_for_controlled_full_backfill: bool
    checks: dict[str, Any]
    warnings: list[str]
    blocking_errors: list[str]
    review: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "readiness_status": self.readiness_status,
            "ready_for_controlled_full_backfill": self.ready_for_controlled_full_backfill,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class MirrorStatusResult:
    report_version: str
    root: str
    backup: str
    scope: str
    catalog_status: dict[str, Any]
    backup_status: str
    restore_check_status: str
    readiness_status: str
    ready_for_controlled_full_backfill: bool
    latest_snapshot_count: int
    enabled_executable_endpoint_count: int
    disabled_inventory_endpoint_count: int
    daily_like_coverage_summary: list[dict[str, Any]]
    backup_possible_mutation: bool
    token_plaintext_found: bool
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "root": self.root,
            "backup": self.backup,
            "backup_status": self.backup_status,
            "restore_check_status": self.restore_check_status,
            "readiness_status": self.readiness_status,
            "ready_for_controlled_full_backfill": self.ready_for_controlled_full_backfill,
            "latest_snapshot_count": self.latest_snapshot_count,
            "enabled_executable_endpoint_count": self.enabled_executable_endpoint_count,
            "disabled_inventory_endpoint_count": self.disabled_inventory_endpoint_count,
            "backup_possible_mutation": self.backup_possible_mutation,
            "token_plaintext_found": self.token_plaintext_found,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class MirrorAuditResult:
    report_version: str
    root: str
    backup: str | None
    scope: str
    since: str | None
    limit: int
    run_count_by_type: dict[str, int]
    succeeded_run_count: int
    failed_run_count: int
    job_count_by_status: dict[str, int]
    validation_status_counts: dict[str, int]
    snapshot_count_by_api: dict[str, int]
    failed_jobs: list[dict[str, Any]]
    quarantined_count: int
    latest_run_id: str | None
    backup_summary: dict[str, Any] | None
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "root": self.root,
            "backup": self.backup,
            "scope": self.scope,
            "since": self.since,
            "limit": self.limit,
            "run_count_by_type": self.run_count_by_type,
            "succeeded_run_count": self.succeeded_run_count,
            "failed_run_count": self.failed_run_count,
            "job_count_by_status": self.job_count_by_status,
            "validation_status_counts": self.validation_status_counts,
            "quarantined_count": self.quarantined_count,
            "latest_run_id": self.latest_run_id,
            "backup_summary": self.backup_summary,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class MirrorNextBatchResult:
    report_version: str
    root: str
    scope: str
    current_completed_months: list[str]
    last_complete_month: str | None
    recommended_next_start_date: str | None
    recommended_next_end_date: str | None
    reason: str
    required_trade_cal_range: dict[str, Any] | None
    estimated_request_count: int
    recommended_max_jobs_per_api: int
    plan_command_preview: str | None
    execute_command_preview: dict[str, str] | None
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "root": self.root,
            "scope": self.scope,
            "current_completed_months": self.current_completed_months,
            "last_complete_month": self.last_complete_month,
            "recommended_next_start_date": self.recommended_next_start_date,
            "recommended_next_end_date": self.recommended_next_end_date,
            "reason": self.reason,
            "required_trade_cal_range": self.required_trade_cal_range,
            "estimated_request_count": self.estimated_request_count,
            "recommended_max_jobs_per_api": self.recommended_max_jobs_per_api,
            "plan_command_preview": self.plan_command_preview,
            "execute_command_preview": self.execute_command_preview,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class MirrorBatchBundleResult:
    report_version: str
    root: str
    backup: str
    output: str
    scope: str
    start_date: str
    end_date: str
    max_jobs_per_api: int
    status: str
    overwritten: bool
    files: list[str]
    commands_execute_guard: str
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorBatchBundleVerifyResult:
    report_version: str
    status: str
    bundle: str
    bundle_id: str | None
    manifest_present: bool
    manifest_valid: bool
    pre_manifest_bundle_detected: bool
    recommended_action: str | None
    file_count: int
    checked_file_count: int
    missing_file_count: int
    checksum_failure_count: int
    command_guard_status: str
    token_plaintext_found: bool
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class CommandSafetyCheckResult:
    report_version: str
    file: str
    status: str
    execute_commands_found: list[str]
    guarded_execute_commands: list[str]
    unguarded_execute_commands: list[str]
    destructive_commands_found: list[str]
    network_commands_found: list[str]
    token_plaintext_found: bool
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorBatchRehearsalResult:
    report_version: str
    root: str
    backup: str
    bundle: str
    rehearsal_status: str
    steps: list[dict[str, Any]]
    would_execute_real_requests: bool
    estimated_request_count: int
    blocked_by: list[str]
    warnings: list[str]
    user_confirmation_required: bool
    next_safe_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorBatchLedgerResult:
    report_version: str
    root: str
    scope: str
    ledger_status: str
    batches: list[dict[str, Any]]
    inferred_batches: list[dict[str, Any]]
    planned_batches: list[dict[str, Any]]
    latest_completed_batch: dict[str, Any] | None
    next_recommended_batch: dict[str, Any] | None
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorBatchCertificateResult:
    report_version: str
    status: str
    output: str
    files: list[str]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorOperatorChecklistResult:
    report_version: str
    root: str
    backup: str
    scope: str
    start_date: str
    end_date: str
    paths_valid: bool
    backup_not_nested: bool
    restore_check_passed: bool
    backup_not_mutated: bool
    readiness_not_blocked: bool
    no_schema_quarantine: bool
    no_failed_validation: bool
    token_available: bool
    max_jobs_guardrail: dict[str, Any]
    batch_plan_available: bool
    disk_space_warning: str | None
    stop_conditions: dict[str, Any]
    exact_plan_command: str
    exact_execute_command: dict[str, str]
    ready: bool
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class StopPolicyResult:
    report_version: str
    category: str
    execution_blocked: bool
    stop_immediately: list[str]
    continue_with_warning: list[str]
    retryable_failures: list[str]
    non_retryable_failures: list[str]
    backup_required_conditions: list[str]
    user_confirmation_required_conditions: list[str]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorFailureDrillResult:
    report_version: str
    scenario: str
    severity: str
    stop_condition: bool
    retry_allowed: bool
    continue_allowed: bool
    required_operator_action: str
    commands_to_inspect: list[str]
    commands_not_to_run: list[str]
    recovery_steps: list[str]
    escalation_notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class PathDiagnosticsResult:
    report_version: str
    status: str
    root: str
    backup: str
    root_exists: bool
    backup_exists: bool
    root_size: int
    backup_size: int
    root_file_count: int
    backup_file_count: int
    parent_free_bytes: dict[str, int | None]
    backup_inside_root: bool
    root_inside_backup: bool
    same_device: bool | None
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class TokenHygieneResult:
    report_version: str
    status: str
    path: str
    scanned_file_count: int
    skipped_file_count: int
    suspicious_match_count: int
    suspicious_paths: list[str]
    token_plaintext_found: bool
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MonthlyPromotionChecklistResult:
    report_version: str
    status: str
    root: str
    backup: str
    scope: str
    from_month: str
    to_month: str
    from_range: dict[str, str]
    to_range: dict[str, str]
    ready_to_promote: bool
    hard_blockers: list[str]
    dependency_stage: dict[str, Any]
    ready_for_dependency_stage: bool
    ready_for_batch_after_dependency: bool | str
    next_safe_action: str
    checks: dict[str, Any]
    warnings: list[str]
    blocking_errors: list[str]
    required_user_confirmation: bool
    next_commands: list[dict[str, str]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorOpsReportResult:
    report_version: str
    overall_status: str
    ready_for_next_user_confirmed_batch: bool
    hard_blockers: list[str]
    dependency_stage: dict[str, Any]
    bundle_status: dict[str, Any]
    promotion_status: str
    daily_like_coverage: list[dict[str, Any]]
    weekly_monthly_advisory_coverage: list[dict[str, Any]]
    backup_status: dict[str, Any]
    token_hygiene: dict[str, Any]
    schema_status: dict[str, Any]
    next_safe_action: str
    root: str
    backup: str
    scope: str
    start_date: str
    end_date: str
    next_start_date: str
    next_end_date: str
    sections: dict[str, Any]
    warnings: list[str]
    blocking_errors: list[str]
    recommended_next_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class FinalGateCheck:
    name: str
    status: str
    passed: bool
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FinalGateCommandPreview:
    command: str
    confirmation: str
    confirmation_phrase: str
    would_execute_real_requests: bool
    requires_user_confirmation: bool
    do_not_run_automatically: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FinalGateResult:
    report_version: str
    gate_status: str
    ready_for_user_confirmed_execute: bool
    ready_for_dependency_stage: bool
    ready_for_full_batch_after_dependency: bool | str
    root: str
    backup: str
    bundle: str
    scope: str
    requested_range: dict[str, str]
    max_jobs_per_api: int
    estimated_request_count: int
    dependency_stage: dict[str, Any]
    checks: list[dict[str, Any]]
    sections: dict[str, Any]
    command_preview: dict[str, Any]
    final_command_preview: dict[str, Any]
    confirmation_phrase: str
    blocking_errors: list[str]
    warnings: list[str]
    safety_boundaries: list[str]
    do_not_run_automatically: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "gate_status": self.gate_status,
            "ready_for_user_confirmed_execute": self.ready_for_user_confirmed_execute,
            "ready_for_dependency_stage": self.ready_for_dependency_stage,
            "ready_for_full_batch_after_dependency": self.ready_for_full_batch_after_dependency,
            "root": self.root,
            "backup": self.backup,
            "bundle": self.bundle,
            "scope": self.scope,
            "requested_range": self.requested_range,
            "max_jobs_per_api": self.max_jobs_per_api,
            "estimated_request_count": self.estimated_request_count,
            "dependency_stage": self.dependency_stage,
            "confirmation_phrase": self.confirmation_phrase,
            "command_preview": self.command_preview,
            "safety_boundaries": self.safety_boundaries,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
            "do_not_run_automatically": self.do_not_run_automatically,
        }


@dataclass(frozen=True)
class ExecuteScriptResult:
    report_version: str
    status: str
    output: str
    root: str
    backup: str
    bundle: str
    scope: str
    start_date: str
    end_date: str
    max_jobs_per_api: int
    overwritten: bool
    confirmation_phrase: str
    command_safety_status: str | None
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class ExecuteReadinessResult:
    report_version: str
    execute_readiness_status: str
    may_execute_after_user_confirmation: bool
    must_not_execute_automatically: bool
    final_gate_status: str
    bundle_status: str | None
    command_safety_status: str | None
    rehearsal_status: str | None
    promotion_status: str | None
    backup_status: str | None
    token_hygiene_status: str
    estimated_request_count: int
    confirmation_phrase: str
    exact_user_confirmed_command: str
    final_gate: dict[str, Any]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "execute_readiness_status": self.execute_readiness_status,
            "may_execute_after_user_confirmation": self.may_execute_after_user_confirmation,
            "must_not_execute_automatically": self.must_not_execute_automatically,
            "final_gate_status": self.final_gate_status,
            "bundle_status": self.bundle_status,
            "command_safety_status": self.command_safety_status,
            "rehearsal_status": self.rehearsal_status,
            "promotion_status": self.promotion_status,
            "backup_status": self.backup_status,
            "token_hygiene_status": self.token_hygiene_status,
            "estimated_request_count": self.estimated_request_count,
            "confirmation_phrase": self.confirmation_phrase,
            "exact_user_confirmed_command": self.exact_user_confirmed_command,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class SchemaStatusResult:
    report_version: str
    root: str
    schema_count_by_api: dict[str, int]
    latest_schema_by_api: dict[str, dict[str, Any]]
    schema_change_count: int
    incompatible_schema_count: int
    pending_schema_change_count: int
    quarantine_count: int
    quarantined_apis: list[str]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorPullCommandResult:
    report_version: str
    status: str
    scope: str
    root: str
    backup: str
    date_range: dict[str, str]
    max_jobs_per_api: int
    commands: list[dict[str, Any]]
    user_confirmation_required: bool
    estimated_requests: dict[str, Any]
    warnings: list[str]
    stop_conditions: dict[str, Any]
    output: str | None
    files: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorAutoSyncResult:
    report_version: str
    status: str
    execute: bool
    root: str
    backup: str
    scope: str
    from_date: str
    to_date: str
    resolved_to_date: str
    window_days: int
    max_jobs_per_api: int
    state_path: str | None
    resume_from_state: bool
    next_start_date: str | None
    planned_window_count: int
    executed_window_count: int
    succeeded_window_count: int
    failed_window_count: int
    max_attempts: int
    windows: list[dict[str, Any]]
    safety_boundaries: list[str]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "status": self.status,
            "execute": self.execute,
            "root": self.root,
            "backup": self.backup,
            "scope": self.scope,
            "from_date": self.from_date,
            "to_date": self.to_date,
            "resolved_to_date": self.resolved_to_date,
            "planned_window_count": self.planned_window_count,
            "executed_window_count": self.executed_window_count,
            "succeeded_window_count": self.succeeded_window_count,
            "failed_window_count": self.failed_window_count,
            "next_start_date": self.next_start_date,
            "state_path": self.state_path,
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class BackupStatusResult:
    report_version: str
    backup: str
    manifest_valid: bool
    backup_id: str | None
    created_at: str | None
    snapshot_scope: str | None
    file_count: int
    raw_file_count: int
    lake_file_count: int
    catalog_checksum_status: str | None
    possible_mutation: bool
    restore_check_status: str
    recommended_action: str
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorCoverageMatrixResult:
    report_version: str
    root: str
    scope: str
    start_date: str
    end_date: str
    items: list[dict[str, Any]]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "root": self.root,
            "scope": self.scope,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "api_count": len(self.items),
            "warnings": self.warnings,
            "blocking_errors": self.blocking_errors,
        }


@dataclass(frozen=True)
class RequestEstimateResult:
    report_version: str
    root: str
    scope: str
    start_date: str
    end_date: str
    estimated_requests_by_api: dict[str, int]
    estimated_total_requests: int
    planned_trade_cal_requests: int
    daily_like_requests: int
    weekly_monthly_requests: int
    reference_refresh_requests: int
    dependency_requests: int
    executable_after_dependency_requests: int
    currently_unblocked_requests: int
    dependency_status: str
    dependency_action: str | None
    trade_cal_params: dict[str, str]
    daily_like_status: str
    natural_day_fallback: bool
    risk_level: str
    assumptions: list[str]
    warnings: list[str]
    not_a_quota_guarantee: bool
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class MirrorBatchEndpointPlan:
    endpoint: str
    category: str
    requires_trade_cal: bool
    plan_status: str
    planned_action: str
    total_candidate_jobs: int
    planned_jobs: int
    missing_jobs: int
    skipped_jobs: int
    blocked_jobs: int
    max_jobs: int
    truncated: bool
    dates: list[str]
    refresh_strategy: str | None = None
    blocked_reason: str | None = None
    warnings: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MirrorBatchPlan:
    batch_id: str
    scope: str
    root: str
    start_date: str
    end_date: str
    calendar_exchange: str
    max_jobs_per_api: int
    endpoint_plans: list[MirrorBatchEndpointPlan]
    total_candidate_jobs: int
    total_planned_jobs: int
    blocked_endpoints: int
    warnings: list[str]
    estimated_request_count: int
    requires_execute_confirmation: bool
    trade_cal_dependency_status: str
    dependency_status: str
    dependency_action: str | None
    trade_cal_params: dict[str, str]
    daily_like_status: str
    natural_day_fallback: bool
    dependency_requests: int
    executable_after_dependency_requests: int
    currently_unblocked_requests: int

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["endpoint_plans"] = [item.to_dict() for item in self.endpoint_plans]
        return data

    def summary(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "scope": self.scope,
            "root": self.root,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "calendar_exchange": self.calendar_exchange,
            "max_jobs_per_api": self.max_jobs_per_api,
            "total_candidate_jobs": self.total_candidate_jobs,
            "total_planned_jobs": self.total_planned_jobs,
            "blocked_endpoints": self.blocked_endpoints,
            "estimated_request_count": self.estimated_request_count,
            "requires_execute_confirmation": self.requires_execute_confirmation,
            "trade_cal_dependency_status": self.trade_cal_dependency_status,
            "dependency_status": self.dependency_status,
            "dependency_action": self.dependency_action,
            "trade_cal_params": self.trade_cal_params,
            "daily_like_status": self.daily_like_status,
            "natural_day_fallback": self.natural_day_fallback,
            "dependency_requests": self.dependency_requests,
            "executable_after_dependency_requests": self.executable_after_dependency_requests,
            "currently_unblocked_requests": self.currently_unblocked_requests,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class MirrorScopeReportResult:
    report_version: str
    scope: str
    endpoints_in_scope: list[str]
    executable_now: list[str]
    plan_only: list[str]
    disabled: list[str]
    blocked_reason: dict[str, str]
    missing_metadata: list[str]
    next_enablement_step: dict[str, str]
    categories: dict[str, list[str]]
    excluded_high_risk_patterns: list[str]
    warnings: list[str]
    real_probe_status: dict[str, str] = field(default_factory=dict)
    pagination_strategy: dict[str, str] = field(default_factory=dict)
    child_scopes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "scope": self.scope,
            "endpoints_in_scope": len(self.endpoints_in_scope),
            "executable_now": len(self.executable_now),
            "plan_only": len(self.plan_only),
            "disabled": len(self.disabled),
            "missing_metadata": len(self.missing_metadata),
            "warnings": self.warnings,
        }


def ensure_mirror_scope(scope: str) -> None:
    if scope not in SUPPORTED_MIRROR_SCOPES:
        supported = ", ".join(sorted(SUPPORTED_MIRROR_SCOPES))
        raise ValueError("unknown mirror scope: %s; supported: %s" % (scope, supported))


def ensure_mirror_mode(mode: str) -> None:
    if mode not in MODE_MAX_JOBS:
        raise ValueError("unknown mirror mode: %s; supported: smoke, pilot" % mode)


def mirror_scope_endpoints(scope: str) -> list[str]:
    ensure_mirror_scope(scope)
    if scope == "a-share-low-risk":
        return list(A_SHARE_LOW_RISK_ENDPOINTS)
    if scope in HK_US_SCOPE_MARKETS:
        return [item["api_name"] for item in _hk_us_source_endpoints_for_scope(scope)]
    if scope == GLOBAL_EQUITY_LOW_RISK_SCOPE:
        return [*A_SHARE_LOW_RISK_ENDPOINTS, *mirror_scope_endpoints(HK_LOW_RISK_SCOPE), *mirror_scope_endpoints(US_LOW_RISK_SCOPE)]
    return list(LOW_RISK_A_SHARE_ENDPOINTS)


def daily_like_apis_for_scope(scope: str) -> list[str]:
    ensure_mirror_scope(scope)
    if scope == "a-share-low-risk":
        return list(A_SHARE_LOW_RISK_DAILY_LIKE_APIS)
    if scope in HK_US_SCOPE_MARKETS:
        return [
            item["api_name"]
            for item in _hk_us_source_endpoints_for_scope(scope)
            if item.get("category") == "daily_market_data" and item.get("recommendation") == "executable_candidate"
        ]
    if scope == GLOBAL_EQUITY_LOW_RISK_SCOPE:
        return [*A_SHARE_LOW_RISK_DAILY_LIKE_APIS, *daily_like_apis_for_scope(HK_LOW_RISK_SCOPE), *daily_like_apis_for_scope(US_LOW_RISK_SCOPE)]
    return list(DAILY_LIKE_MIRROR_APIS)


def coverage_matrix_apis_for_scope(scope: str) -> list[str]:
    ensure_mirror_scope(scope)
    if scope == "a-share-low-risk":
        return [*A_SHARE_LOW_RISK_DAILY_LIKE_APIS, *A_SHARE_LOW_RISK_EXPLICIT_PERIODIC_APIS]
    if scope in HK_US_SCOPE_MARKETS:
        return daily_like_apis_for_scope(scope)
    if scope == GLOBAL_EQUITY_LOW_RISK_SCOPE:
        return [
            *A_SHARE_LOW_RISK_DAILY_LIKE_APIS,
            *A_SHARE_LOW_RISK_EXPLICIT_PERIODIC_APIS,
            *coverage_matrix_apis_for_scope(HK_LOW_RISK_SCOPE),
            *coverage_matrix_apis_for_scope(US_LOW_RISK_SCOPE),
        ]
    return [*DAILY_LIKE_MIRROR_APIS, "weekly", "monthly"]


def reference_refresh_apis_for_scope(scope: str) -> list[str]:
    ensure_mirror_scope(scope)
    if scope == "a-share-low-risk":
        return ["stock_basic", "stock_company", "hs_const", "concept", "index_basic", "ths_index", "index_classify"]
    if scope in HK_US_SCOPE_MARKETS:
        return [
            item["api_name"]
            for item in _hk_us_source_endpoints_for_scope(scope)
            if item.get("category") in {"reference_snapshot", "calendar"} and item.get("recommendation") == "executable_candidate"
        ]
    if scope == GLOBAL_EQUITY_LOW_RISK_SCOPE:
        return [
            *reference_refresh_apis_for_scope("a-share-low-risk"),
            *reference_refresh_apis_for_scope(HK_LOW_RISK_SCOPE),
            *reference_refresh_apis_for_scope(US_LOW_RISK_SCOPE),
        ]
    return ["stock_basic", "hs_const"]


def _hk_us_source_endpoints_for_scope(scope: str) -> list[dict[str, Any]]:
    market = HK_US_SCOPE_MARKETS[scope]
    return [item for item in hk_us_low_risk_source_endpoints() if item.get("market") == market]


class MirrorScopeReporter:
    REPORT_VERSION = "mirror-scope/v1"

    def report(self, *, scope: str) -> MirrorScopeReportResult:
        ensure_mirror_scope(scope)
        if scope == "low-risk-a-share":
            return self._legacy_low_risk_report(scope)
        if scope == "a-share-low-risk":
            return self._a_share_low_risk_report(scope)
        if scope in HK_US_SCOPE_MARKETS:
            return self._hk_us_low_risk_report(scope)
        return self._global_equity_low_risk_report(scope)

    def _legacy_low_risk_report(self, scope: str) -> MirrorScopeReportResult:
        endpoints = mirror_scope_endpoints(scope)
        return MirrorScopeReportResult(
            report_version=self.REPORT_VERSION,
            scope=scope,
            endpoints_in_scope=endpoints,
            executable_now=endpoints,
            plan_only=[],
            disabled=[],
            blocked_reason={},
            missing_metadata=[],
            next_enablement_step={},
            categories={"legacy_low_risk_a_share": endpoints},
            excluded_high_risk_patterns=PROHIBITED_SCOPE_ENDPOINT_PATTERNS,
            warnings=["mirror-scope is read-only and does not fetch, backfill, or write catalog state"],
        )

    def _a_share_low_risk_report(self, scope: str) -> MirrorScopeReportResult:
        executable_configs = {cfg["api_name"]: cfg for cfg in load_bundled_endpoint_configs()}
        inventory_configs = {cfg["api_name"]: cfg for cfg in load_inventory_configs()}
        executable_now: list[str] = []
        plan_only: list[str] = []
        disabled: list[str] = []
        missing_metadata: list[str] = []
        blocked_reason: dict[str, str] = {}
        next_enablement_step: dict[str, str] = {}

        for api_name in A_SHARE_LOW_RISK_ENDPOINTS:
            cfg = executable_configs.get(api_name)
            if cfg and cfg.get("execution_status", "enabled") != "disabled":
                executable_now.append(api_name)
                continue
            inventory = inventory_configs.get(api_name)
            if inventory:
                disabled.append(api_name)
                reason = str(inventory.get("reason_disabled") or "disabled inventory endpoint")
                blocked_reason[api_name] = reason
                next_enablement_step[api_name] = self._next_step(inventory)
                continue
            plan_only.append(api_name)
            missing_metadata.append(api_name)
            blocked_reason[api_name] = "metadata is not yet declared in executable configs or disabled inventory"
            next_enablement_step[api_name] = "add endpoint config or disabled inventory entry with explicit planner, fields, risk, and guardrail metadata"

        return MirrorScopeReportResult(
            report_version=self.REPORT_VERSION,
            scope=scope,
            endpoints_in_scope=list(A_SHARE_LOW_RISK_ENDPOINTS),
            executable_now=executable_now,
            plan_only=plan_only,
            disabled=disabled,
            blocked_reason=blocked_reason,
            missing_metadata=missing_metadata,
            next_enablement_step=next_enablement_step,
            categories={key: list(value) for key, value in A_SHARE_LOW_RISK_CATEGORIES.items()},
            excluded_high_risk_patterns=PROHIBITED_SCOPE_ENDPOINT_PATTERNS,
            warnings=["mirror-scope is read-only and does not fetch, backfill, or write catalog state"],
        )

    def _hk_us_low_risk_report(self, scope: str) -> MirrorScopeReportResult:
        executable_configs = {cfg["api_name"]: cfg for cfg in load_bundled_endpoint_configs()}
        endpoints = _hk_us_source_endpoints_for_scope(scope)
        executable_now: list[str] = []
        plan_only: list[str] = []
        disabled: list[str] = []
        missing_metadata: list[str] = []
        blocked_reason: dict[str, str] = {}
        next_enablement_step: dict[str, str] = {}
        categories: dict[str, list[str]] = {}
        real_probe_status: dict[str, str] = {}
        pagination_strategy: dict[str, str] = {}

        for item in endpoints:
            api_name = str(item["api_name"])
            category = str(item.get("category") or "uncategorized")
            categories.setdefault(category, []).append(api_name)
            real_probe_status[api_name] = str(item.get("real_probe_status") or "pending")
            pagination_strategy[api_name] = str(item.get("recommended_pagination_strategy") or "unknown")
            recommendation = str(item.get("recommendation") or "")
            if recommendation == "disabled":
                disabled.append(api_name)
                blocked_reason[api_name] = self._hk_us_blocked_reason(item)
                next_enablement_step[api_name] = "keep disabled; outside this low-risk historical mirror goal"
                continue
            if recommendation == "plan_only":
                plan_only.append(api_name)
                blocked_reason[api_name] = self._hk_us_blocked_reason(item)
                next_enablement_step[api_name] = "keep plan-only until a separate financial/PIT execution goal adds disclosure-safe code-period infrastructure"
                continue

            cfg = executable_configs.get(api_name)
            if cfg and cfg.get("execution_status", "enabled") != "disabled":
                executable_now.append(api_name)
                continue
            plan_only.append(api_name)
            missing_metadata.append(api_name)
            blocked_reason[api_name] = "real probe passed, but executable endpoint config, fake fixture, planner, and report integration are not complete yet"
            next_enablement_step[api_name] = "add endpoint config, partition strategy, fake fetch/write/read/validate tests, and guarded command/report integration"

        return MirrorScopeReportResult(
            report_version=self.REPORT_VERSION,
            scope=scope,
            endpoints_in_scope=[str(item["api_name"]) for item in endpoints],
            executable_now=executable_now,
            plan_only=plan_only,
            disabled=disabled,
            blocked_reason=blocked_reason,
            missing_metadata=missing_metadata,
            next_enablement_step=next_enablement_step,
            categories={key: list(value) for key, value in categories.items()},
            excluded_high_risk_patterns=PROHIBITED_SCOPE_ENDPOINT_PATTERNS,
            warnings=[
                "mirror-scope is read-only and does not fetch, backfill, or write catalog state",
                "HK/US executable candidates remain plan-only until endpoint configs and fake tests are added",
            ],
            real_probe_status=real_probe_status,
            pagination_strategy=pagination_strategy,
        )

    def _global_equity_low_risk_report(self, scope: str) -> MirrorScopeReportResult:
        child_reports = {child: self.report(scope=child) for child in GLOBAL_EQUITY_CHILD_SCOPES}
        child_payloads = {child: report.to_dict() for child, report in child_reports.items()}

        def combined_list(field: str) -> list[str]:
            out: list[str] = []
            for report in child_reports.values():
                out.extend(list(getattr(report, field)))
            return out

        blocked_reason: dict[str, str] = {}
        missing_metadata: list[str] = []
        next_enablement_step: dict[str, str] = {}
        real_probe_status: dict[str, str] = {}
        pagination_strategy: dict[str, str] = {}
        categories: dict[str, list[str]] = {}
        warnings: list[str] = [
            "global-equity-low-risk is explicit composition of a-share-low-risk, hk-low-risk, and us-low-risk; it is not a wildcard over all Tushare APIs"
        ]
        for child, report in child_reports.items():
            blocked_reason.update(report.blocked_reason)
            missing_metadata.extend(report.missing_metadata)
            next_enablement_step.update(report.next_enablement_step)
            real_probe_status.update(report.real_probe_status)
            pagination_strategy.update(report.pagination_strategy)
            categories[child] = list(report.endpoints_in_scope)
            warnings.extend(report.warnings)

        return MirrorScopeReportResult(
            report_version=self.REPORT_VERSION,
            scope=scope,
            endpoints_in_scope=combined_list("endpoints_in_scope"),
            executable_now=combined_list("executable_now"),
            plan_only=combined_list("plan_only"),
            disabled=combined_list("disabled"),
            blocked_reason=blocked_reason,
            missing_metadata=missing_metadata,
            next_enablement_step=next_enablement_step,
            categories=categories,
            excluded_high_risk_patterns=PROHIBITED_SCOPE_ENDPOINT_PATTERNS,
            warnings=sorted(set(warnings)),
            real_probe_status=real_probe_status,
            pagination_strategy=pagination_strategy,
            child_scopes=child_payloads,
        )

    def _hk_us_blocked_reason(self, item: Mapping[str, Any]) -> str:
        notes = item.get("safety_notes") or []
        if notes:
            return "; ".join(str(note) for note in notes)
        return str(item.get("recommendation") or "not executable")

    def _next_step(self, cfg: Mapping[str, Any]) -> str:
        required = cfg.get("required_infra") or []
        if required:
            return "; ".join(str(item) for item in required)
        return "complete endpoint contract, fake fixture, planner, and guardrail review before enabling"


@dataclass(frozen=True)
class MirrorPreflightResult:
    status: str
    ready_to_execute: bool
    mirror_root: str
    backup_target: str
    scope: str
    mode: str
    start_date: str | None
    end_date: str | None
    max_jobs_per_api: int
    token_available: bool
    mirror_root_status: str
    backup_target_status: str
    path_relationship_status: str
    disk_space: dict[str, Any]
    existing_catalog: dict[str, Any]
    existing_backup: dict[str, Any]
    warnings: list[str]
    blocking_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


def _resolve_path(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_under_tmp(path: Path) -> bool:
    resolved = _resolve_path(path)
    tmp = Path('/tmp').resolve(strict=False)
    return resolved == tmp or _is_relative_to(resolved, tmp)


def _natural_dates_between(start_date: str, end_date: str) -> list[str]:
    current = datetime.strptime(start_date, "%Y%m%d")
    stop = datetime.strptime(end_date, "%Y%m%d")
    out: list[str] = []
    while current <= stop:
        out.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return out


def _calendar_weekly_dates(start_date: str, end_date: str) -> list[str]:
    return [date for date in _natural_dates_between(start_date, end_date) if datetime.strptime(date, "%Y%m%d").weekday() == 4]


def _calendar_monthly_dates(start_date: str, end_date: str) -> list[str]:
    by_month: dict[str, str] = {}
    for date in _natural_dates_between(start_date, end_date):
        by_month[date[:6]] = date
    return sorted(by_month.values())


def _trade_calendar_period_dates(root: Path, catalog: CatalogStore, start_date: str, end_date: str, *, period: str) -> list[str] | None:
    expanded_start, expanded_end = _period_bounds(start_date, end_date, period=period)
    overlap = _trade_calendar_range_overlap(root, catalog, start_date, end_date)
    if overlap is False:
        return None
    try:
        open_dates, _ = DatePlanner(root, catalog).plan_dates_with_metadata(
            start_date=expanded_start,
            end_date=expanded_end,
            trading_days_only=True,
            calendar_exchange="SSE",
        )
    except Exception:
        return None
    if not open_dates:
        return []
    by_period: dict[str, str] = {}
    for date in open_dates:
        parsed = datetime.strptime(date, "%Y%m%d")
        if period == "weekly":
            iso = parsed.isocalendar()
            key = f"{iso.year:04d}-W{iso.week:02d}"
        elif period == "monthly":
            key = date[:6]
        else:
            raise ValueError(f"unknown period: {period}")
        by_period[key] = date
    return sorted(
        date
        for date in by_period.values()
        if start_date <= date <= end_date and _period_candidate_is_complete(date, end_date, period=period)
    )


def _period_bounds(start_date: str, end_date: str, *, period: str) -> tuple[str, str]:
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    if period == "weekly":
        expanded_start = start - timedelta(days=start.weekday())
        expanded_end = end + timedelta(days=6 - end.weekday())
        return expanded_start.strftime("%Y%m%d"), expanded_end.strftime("%Y%m%d")
    if period == "monthly":
        expanded_start = start.replace(day=1)
        if end.month == 12:
            next_month = end.replace(year=end.year + 1, month=1, day=1)
        else:
            next_month = end.replace(month=end.month + 1, day=1)
        expanded_end = next_month - timedelta(days=1)
        return expanded_start.strftime("%Y%m%d"), expanded_end.strftime("%Y%m%d")
    raise ValueError(f"unknown period: {period}")


def _trade_calendar_range_overlap(root: Path, catalog: CatalogStore, start_date: str, end_date: str) -> bool | None:
    if not catalog.latest_snapshot("trade_cal"):
        return None
    try:
        table = LakeReader(root, catalog).scan_api("trade_cal", columns=["exchange", "cal_date"])
    except Exception:
        return None
    if table.num_rows == 0:
        return False
    exchanges = table["exchange"].to_pylist()
    cal_dates = table["cal_date"].to_pylist()
    for exchange, cal_date in zip(exchanges, cal_dates):
        if str(exchange).upper() != "SSE":
            continue
        text = str(cal_date).strip()
        try:
            value = DatePlanner(root, catalog)._normalize_date(text)
        except ValueError:
            continue
        if start_date <= value <= end_date:
            return True
    return False


def _period_candidate_is_complete(candidate: str, requested_end_date: str, *, period: str) -> bool:
    parsed = datetime.strptime(candidate, "%Y%m%d")
    requested_end = datetime.strptime(requested_end_date, "%Y%m%d")
    if period == "weekly":
        if parsed.weekday() == 4:
            return True
        week_end = parsed + timedelta(days=6 - parsed.weekday())
        return requested_end >= week_end
    if period == "monthly":
        if parsed.month == 12:
            next_month = parsed.replace(year=parsed.year + 1, month=1, day=1)
        else:
            next_month = parsed.replace(month=parsed.month + 1, day=1)
        month_end = next_month - timedelta(days=1)
        if parsed.date() == month_end.date():
            return True
        return requested_end >= month_end
    raise ValueError(f"unknown period: {period}")


def _pilot_compatible_period_dates(root: Path, catalog: CatalogStore, start_date: str, end_date: str, *, period: str) -> list[str]:
    if period == "weekly":
        trade_dates = _trade_calendar_period_dates(root, catalog, start_date, end_date, period=period)
        fallback = trade_dates if trade_dates is not None else _calendar_weekly_dates(start_date, end_date)
        pilot_dates = PILOT_JAN_2025_WEEKLY_DATES
    elif period == "monthly":
        trade_dates = _trade_calendar_period_dates(root, catalog, start_date, end_date, period=period)
        fallback = trade_dates if trade_dates is not None else _calendar_monthly_dates(start_date, end_date)
        pilot_dates = PILOT_JAN_2025_MONTHLY_DATES
    else:
        raise ValueError(f"unknown period: {period}")
    expected = {date for date in fallback if not ("20250101" <= date <= "20250131")}
    expected.update(date for date in pilot_dates if start_date <= date <= end_date)
    return sorted(expected)


def _nearest_existing_parent(path: Path) -> Path | None:
    current = _resolve_path(path)
    if current.exists():
        return current if current.is_dir() else current.parent
    for parent in [current.parent, *current.parents]:
        if parent.exists():
            return parent
    return None


def _disk_free(path: Path) -> tuple[int | None, str | None]:
    parent = _nearest_existing_parent(path.parent)
    if parent is None:
        return None, f'no existing parent for {path}'
    try:
        return int(shutil.disk_usage(parent).free), None
    except Exception as exc:  # pragma: no cover - platform specific
        return None, str(exc)


def _token_available_from_env() -> bool:
    if os.environ.get('TUSHARE_TOKEN'):
        return True
    env_path = Path('.env')
    if not env_path.exists():
        return False
    try:
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            if key.strip() == 'TUSHARE_TOKEN' and value.strip():
                return True
    except OSError:
        return False
    return False


def _token_value_from_env() -> str | None:
    if os.environ.get("TUSHARE_TOKEN"):
        return os.environ.get("TUSHARE_TOKEN")
    env_path = Path(".env")
    if not env_path.exists():
        return None
    try:
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "TUSHARE_TOKEN" and value.strip():
                return value.strip()
    except OSError:
        return None
    return None


def _contains_token_plaintext(paths: list[Path], token: str | None = None) -> bool:
    secret = token if token is not None else _token_value_from_env()
    if not secret or len(secret) < 8:
        return False
    needle = secret.encode()
    for root in paths:
        if not root.exists():
            continue
        files = [root] if root.is_file() else (path for path in root.rglob("*") if path.is_file())
        for path in files:
            try:
                with path.open("rb") as handle:
                    carry = b""
                    while True:
                        chunk = handle.read(1024 * 1024)
                        if not chunk:
                            break
                        data = carry + chunk
                        if needle in data:
                            return True
                        carry = data[-max(len(needle) - 1, 0):]
            except OSError:
                continue
    return False


def _artifact_size(paths: list[Path]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for label, root in paths:
        file_count = 0
        total_size = 0
        if root.exists():
            files = [root] if root.is_file() else (path for path in root.rglob("*") if path.is_file())
            for path in files:
                try:
                    stat = path.stat()
                except OSError:
                    continue
                file_count += 1
                total_size += stat.st_size
        summary[f"{label}_file_count"] = file_count
        summary[f"{label}_total_size_bytes"] = total_size
    return summary


def _catalog_counts_readonly(catalog_path: Path) -> dict[str, Any]:
    uri = f"file:{catalog_path.resolve().as_posix()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.row_factory = sqlite3.Row
        version = conn.execute("select value from catalog_meta where key='catalog_schema_version'").fetchone()
        latest_count = conn.execute("select count(*) from snapshots where status='current'").fetchone()[0]
        active_file_count = conn.execute("select count(*) from files where status='current'").fetchone()[0]
        return {
            'present': True,
            'schema_version': int(version[0]) if version else None,
            'endpoint_count': conn.execute('select count(*) from endpoints').fetchone()[0],
            'snapshot_count': conn.execute('select count(*) from snapshots').fetchone()[0],
            'latest_snapshots_count': latest_count,
            'active_file_count': active_file_count,
            'has_active_data': active_file_count > 0,
        }
    finally:
        conn.close()


class MirrorReviewer:
    def review(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str = "low-risk-a-share",
        mode: str = "pilot",
        start_date: str = "20250101",
        end_date: str = "20250131",
        calendar_exchange: str = "SSE",
    ) -> MirrorReviewResult:
        ensure_mirror_scope(scope)
        ensure_mirror_mode(mode)
        mirror_root = Path(root)
        backup_root = Path(backup)
        warnings: list[str] = []
        blocking_errors: list[str] = []
        catalog_status: dict[str, Any] = {"present": False}
        latest_snapshots: list[dict[str, Any]] = []
        endpoint_summary: list[dict[str, Any]] = []
        coverage_summary: list[dict[str, Any]] = []
        validation_status = "not_checked"
        validation_results: list[dict[str, Any]] = []
        backup_inspect_dict: dict[str, Any] | None = None
        backup_restore_dict: dict[str, Any] | None = None
        backup_checksum_status: str | None = None
        backup_possible_mutation = False

        root_status = "missing"
        catalog = CatalogStore(mirror_root, read_only=True)
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}")
        else:
            root_status = "existing_catalog"
            try:
                catalog_status = catalog.inspect_summary()
                latest_snapshots = catalog.list_snapshots(latest=True, limit=100)
                endpoint_summary = self._endpoint_summary(catalog, scope)
                ok, validation_results = Validator(mirror_root, catalog).validate_latest_snapshots(record=False)
                validation_status = "succeeded" if ok else "failed"
                if not ok:
                    blocking_errors.append("validate --snapshot latest --no-record failed")
                coverage_summary = self._coverage_summary(mirror_root, catalog, scope, start_date, end_date, calendar_exchange, warnings, blocking_errors)
            except Exception as exc:
                blocking_errors.append(f"catalog review failed: {exc}")

        backup_status = "missing"
        if not backup_root.exists():
            blocking_errors.append(f"backup not found: {backup_root}")
        else:
            backup_status = "present"
            try:
                inspect = BackupInspector().inspect(backup_root)
                restore = RestoreChecker().check(backup_root)
                backup_inspect_dict = inspect.to_dict()
                backup_restore_dict = restore.to_dict()
                backup_checksum_status = restore.catalog_checksum_status or inspect.catalog_checksum_status
                backup_possible_mutation = bool(restore.possible_mutation or inspect.possible_mutation)
                if inspect.status != "succeeded":
                    blocking_errors.append("backup-inspect failed")
                if restore.status != "succeeded":
                    blocking_errors.append("restore-check failed")
                if backup_possible_mutation:
                    blocking_errors.append("backup catalog may have been modified after backup creation")
            except Exception as exc:
                blocking_errors.append(f"backup review failed: {exc}")

        token_plaintext_found = _contains_token_plaintext([mirror_root, backup_root])
        if token_plaintext_found:
            blocking_errors.append("token plaintext found in mirror or backup artifact")

        artifact_size = _artifact_size([("root", mirror_root), ("backup", backup_root)])
        ready = not blocking_errors and not any("missing trading dates" in warning for warning in warnings)
        return MirrorReviewResult(
            root=str(mirror_root),
            backup=str(backup_root),
            scope=scope,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            calendar_exchange=calendar_exchange,
            root_status=root_status,
            backup_status=backup_status,
            catalog_status=catalog_status,
            latest_snapshots=latest_snapshots,
            endpoint_summary=endpoint_summary,
            coverage_summary=coverage_summary,
            validation_status=validation_status,
            validation_results=validation_results,
            backup_inspect=backup_inspect_dict,
            backup_restore_check=backup_restore_dict,
            backup_catalog_checksum_status=backup_checksum_status,
            backup_possible_mutation=backup_possible_mutation,
            artifact_size=artifact_size,
            token_plaintext_found=token_plaintext_found,
            ready_for_next_batch=ready,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    def _endpoint_summary(self, catalog: CatalogStore, scope: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for endpoint in mirror_scope_endpoints(scope):
            snapshot = catalog.latest_snapshot(endpoint)
            if not snapshot:
                rows.append({
                    "endpoint": endpoint,
                    "status": "missing_snapshot",
                    "snapshot_id": None,
                    "record_count": 0,
                    "raw_event_count": 0,
                    "raw_files": 0,
                    "lake_files": 0,
                })
                continue
            files = catalog.files_for_snapshot(str(snapshot["snapshot_id"]))
            rows.append({
                "endpoint": endpoint,
                "status": "current",
                "snapshot_id": snapshot.get("snapshot_id"),
                "record_count": sum(int(row.get("record_count") or 0) for row in files if row.get("content_type") == "lake"),
                "raw_event_count": sum(int(row.get("raw_event_count") or 0) for row in files if row.get("content_type") == "raw"),
                "raw_files": sum(1 for row in files if row.get("content_type") == "raw"),
                "lake_files": sum(1 for row in files if row.get("content_type") == "lake"),
            })
        return rows

    def _coverage_summary(
        self,
        root: Path,
        catalog: CatalogStore,
        scope: str,
        start_date: str,
        end_date: str,
        calendar_exchange: str,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for api in daily_like_apis_for_scope(scope):
            try:
                report = CoverageReporter(root, catalog).report(
                    api,
                    start_date=start_date,
                    end_date=end_date,
                    trading_days_only=True,
                    calendar_exchange=calendar_exchange,
                )
                data = report.to_dict()
                data.pop("items", None)
                data["active_exists_dates"] = [item.date for item in report.items if item.existing_status == "active_exists"]
                data["missing_trading_dates"] = [item.date for item in report.items if item.existing_status == "missing"]
                rows.append(data)
                if report.missing_dates:
                    warnings.append(f"{api} has {report.missing_dates} missing trading dates in review range")
            except Exception as exc:
                rows.append({"api_name": api, "status": "failed", "error": str(exc)})
                blocking_errors.append(f"{api} coverage failed: {exc}")
        return rows


class MirrorReadinessReporter:
    CONTROLLED_FULL_BACKFILL_WARNINGS = [
        "only 2025-01 pilot coverage has been proven; this is not a full mirror",
        "event/company endpoints are not stock-looped",
        "weekly/monthly do not use trading-days-only",
        "financial/PIT/minute/tick/object/postgres datasets are not covered",
        "remote disaster recovery is not implemented",
        "compaction is not implemented",
    ]

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str = "low-risk-a-share",
    ) -> MirrorReadinessResult:
        ensure_mirror_scope(scope)
        review = MirrorReviewer().review(
            root=root,
            backup=backup,
            scope=scope,
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            calendar_exchange="SSE",
        )
        checks = self._checks(review, Path(root), Path(backup), scope)
        blocking_errors = list(review.blocking_errors)
        for name, check in checks.items():
            if check.get("required") and not check.get("passed"):
                message = check.get("message") or f"{name} failed"
                if message not in blocking_errors:
                    blocking_errors.append(message)
        warnings = list(dict.fromkeys([*review.warnings, *self.CONTROLLED_FULL_BACKFILL_WARNINGS]))
        readiness_status = "blocked" if blocking_errors else ("warning" if warnings else "ready")
        return MirrorReadinessResult(
            root=str(root),
            backup=str(backup),
            scope=scope,
            readiness_status=readiness_status,
            ready_for_controlled_full_backfill=not blocking_errors,
            checks=checks,
            warnings=warnings,
            blocking_errors=blocking_errors,
            review=review.to_dict(),
        )

    def _checks(self, review: MirrorReviewResult, root: Path, backup: Path, scope: str) -> dict[str, Any]:
        endpoint_status = {row["endpoint"]: row for row in review.endpoint_summary}
        required_coverage_apis = set(DAILY_LIKE_MIRROR_APIS) if scope == "a-share-low-risk" else set(daily_like_apis_for_scope(scope))
        required_coverage_rows = [row for row in review.coverage_summary if row.get("api_name") in required_coverage_apis]
        coverage_complete = bool(required_coverage_rows) and all(
            int(row.get("total_dates") or 0) > 0
            and int(row.get("missing_dates") or 0) == 0
            and int(row.get("failed_dates") or 0) == 0
            and int(row.get("quarantined_dates") or 0) == 0
            for row in required_coverage_rows
        )
        relationship = MirrorPreflightChecker(token_available=True)._path_relationship(_resolve_path(root), _resolve_path(backup))
        schema_version = review.catalog_status.get("schema_version")
        checks = {
            "catalog_opens": {
                "required": True,
                "passed": review.root_status == "existing_catalog" and bool(review.catalog_status.get("schema_version")),
                "message": "catalog cannot be opened",
            },
            "supported_schema": {
                "required": True,
                "passed": isinstance(schema_version, int) and schema_version >= 2,
                "message": "catalog schema version is unsupported",
            },
            "latest_snapshots_exist": {
                "required": True,
                "passed": bool(review.latest_snapshots),
                "message": "latest snapshots are missing",
            },
            "trade_cal_latest_exists": {
                "required": True,
                "passed": (endpoint_status.get("trade_cal") or {}).get("status") == "current",
                "message": "trade_cal latest snapshot is missing",
            },
            "backup_exists": {
                "required": True,
                "passed": review.backup_status == "present",
                "message": "backup artifact is missing",
            },
            "restore_check_succeeds": {
                "required": True,
                "passed": (review.backup_restore_check or {}).get("status") == "succeeded",
                "message": "restore-check failed",
            },
            "backup_possible_mutation_false": {
                "required": True,
                "passed": not review.backup_possible_mutation,
                "message": "backup possible_mutation is true",
            },
            "validate_no_record_succeeds": {
                "required": True,
                "passed": review.validation_status == "succeeded",
                "message": "validate --snapshot latest --no-record failed",
            },
            "pilot_coverage_complete": {
                "required": True,
                "passed": coverage_complete,
                "message": "pilot coverage is incomplete for one or more daily-like endpoints",
            },
            "token_plaintext_not_found": {
                "required": True,
                "passed": not review.token_plaintext_found,
                "message": "token plaintext found in mirror or backup artifact",
            },
            "backup_not_nested_inside_mirror": {
                "required": True,
                "passed": relationship == "ok",
                "message": f"unsafe backup path relationship: {relationship}",
            },
            "cli_guardrails_exist": {
                "required": True,
                "passed": MODE_MAX_JOBS.get("smoke") == 3 and MODE_MAX_JOBS.get("pilot") == 20,
                "message": "mirror CLI max-jobs guardrails are missing",
            },
        }
        return checks


def _dedupe_messages(messages: list[str]) -> list[str]:
    return list(dict.fromkeys(message for message in messages if message))


class MirrorStatusReporter:
    REPORT_VERSION = "mirror-status/v1"

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str = "low-risk-a-share",
    ) -> MirrorStatusResult:
        ensure_mirror_scope(scope)
        review = MirrorReviewer().review(
            root=root,
            backup=backup,
            scope=scope,
            mode="pilot",
            start_date="20250101",
            end_date="20250131",
            calendar_exchange="SSE",
        )
        readiness = MirrorReadinessReporter().report(root=root, backup=backup, scope=scope)
        api_infra = ApiInfrastructureReadinessReporter().report()
        catalog_status = dict(review.catalog_status)
        catalog_status.setdefault("present", review.root_status == "existing_catalog")
        backup_status = self._backup_status(review)
        restore_check_status = (review.backup_restore_check or {}).get("status") or "not_checked"
        warnings = _dedupe_messages([*review.warnings, *readiness.warnings])
        blocking_errors = _dedupe_messages([*review.blocking_errors, *readiness.blocking_errors])
        return MirrorStatusResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            backup=str(backup),
            scope=scope,
            catalog_status=catalog_status,
            backup_status=backup_status,
            restore_check_status=str(restore_check_status),
            readiness_status=readiness.readiness_status,
            ready_for_controlled_full_backfill=readiness.ready_for_controlled_full_backfill,
            latest_snapshot_count=len(review.latest_snapshots),
            enabled_executable_endpoint_count=api_infra.enabled_executable_endpoint_count,
            disabled_inventory_endpoint_count=api_infra.disabled_inventory_endpoint_count,
            daily_like_coverage_summary=review.coverage_summary,
            backup_possible_mutation=review.backup_possible_mutation,
            token_plaintext_found=review.token_plaintext_found,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    def _backup_status(self, review: MirrorReviewResult) -> str:
        if review.backup_status != "present":
            return review.backup_status
        return str((review.backup_inspect or {}).get("status") or "present")


class MirrorAuditReporter:
    REPORT_VERSION = "mirror-audit/v1"

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str | None = None,
        scope: str = "low-risk-a-share",
        since: str | None = None,
        limit: int = 20,
    ) -> MirrorAuditResult:
        ensure_mirror_scope(scope)
        if limit <= 0:
            raise ValueError("--limit must be positive")
        since_bound = self._since_bound(since)
        mirror_root = Path(root)
        warnings: list[str] = []
        blocking_errors: list[str] = []
        catalog = CatalogStore(mirror_root, read_only=True)
        empty = {
            "run_count_by_type": {},
            "succeeded_run_count": 0,
            "failed_run_count": 0,
            "job_count_by_status": {},
            "validation_status_counts": {},
            "snapshot_count_by_api": {},
            "failed_jobs": [],
            "quarantined_count": 0,
            "latest_run_id": None,
        }
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
            backup_summary = self._backup_summary(backup, warnings, blocking_errors) if backup else None
            return self._result(root, backup, scope, since, limit, backup_summary, warnings, blocking_errors, **empty)
        try:
            with catalog.connect() as conn:
                values = self._catalog_audit(conn, since_bound, limit)
        except Exception as exc:
            blocking_errors.append(f"catalog audit failed: {exc}")
            values = empty
        backup_summary = self._backup_summary(backup, warnings, blocking_errors) if backup else None
        return self._result(root, backup, scope, since, limit, backup_summary, warnings, blocking_errors, **values)

    def _result(
        self,
        root: Path | str,
        backup: Path | str | None,
        scope: str,
        since: str | None,
        limit: int,
        backup_summary: dict[str, Any] | None,
        warnings: list[str],
        blocking_errors: list[str],
        *,
        run_count_by_type: dict[str, int],
        succeeded_run_count: int,
        failed_run_count: int,
        job_count_by_status: dict[str, int],
        validation_status_counts: dict[str, int],
        snapshot_count_by_api: dict[str, int],
        failed_jobs: list[dict[str, Any]],
        quarantined_count: int,
        latest_run_id: str | None,
    ) -> MirrorAuditResult:
        return MirrorAuditResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            backup=str(backup) if backup is not None else None,
            scope=scope,
            since=since,
            limit=limit,
            run_count_by_type=run_count_by_type,
            succeeded_run_count=succeeded_run_count,
            failed_run_count=failed_run_count,
            job_count_by_status=job_count_by_status,
            validation_status_counts=validation_status_counts,
            snapshot_count_by_api=snapshot_count_by_api,
            failed_jobs=failed_jobs,
            quarantined_count=quarantined_count,
            latest_run_id=latest_run_id,
            backup_summary=backup_summary,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _catalog_audit(self, conn: sqlite3.Connection, since_bound: str | None, limit: int) -> dict[str, Any]:
        return {
            "run_count_by_type": self._count_by(conn, "ingestion_runs", "run_type", "started_at", since_bound),
            "succeeded_run_count": self._count(conn, "ingestion_runs", "status='succeeded'", "started_at", since_bound),
            "failed_run_count": self._count(conn, "ingestion_runs", "status='failed'", "started_at", since_bound),
            "job_count_by_status": self._job_count_by_status(conn, since_bound),
            "validation_status_counts": self._count_by(conn, "validation_runs", "status", "started_at", since_bound),
            "snapshot_count_by_api": self._count_by(conn, "snapshots", "api_name", "created_at", since_bound),
            "failed_jobs": self._failed_jobs(conn, since_bound, limit),
            "quarantined_count": self._count(conn, "quarantine_files", "1=1", "created_at", since_bound),
            "latest_run_id": self._latest_run_id(conn, since_bound),
        }

    def _since_bound(self, since: str | None) -> str | None:
        if since is None:
            return None
        try:
            return datetime.strptime(since, "%Y%m%d").strftime("%Y-%m-%dT00:00:00")
        except ValueError as exc:
            raise ValueError("--since must be in YYYYMMDD format") from exc

    def _count_by(self, conn: sqlite3.Connection, table: str, field: str, time_field: str, since_bound: str | None) -> dict[str, int]:
        sql = f"select {field} as key, count(*) as count from {table}"
        args: list[Any] = []
        if since_bound:
            sql += f" where {time_field}>=?"
            args.append(since_bound)
        sql += f" group by {field} order by {field}"
        rows = conn.execute(sql, args).fetchall()
        return {str(row["key"]): int(row["count"]) for row in rows if row["key"] is not None}

    def _count(self, conn: sqlite3.Connection, table: str, condition: str, time_field: str, since_bound: str | None) -> int:
        sql = f"select count(*) from {table} where {condition}"
        args: list[Any] = []
        if since_bound:
            sql += f" and {time_field}>=?"
            args.append(since_bound)
        return int(conn.execute(sql, args).fetchone()[0])

    def _job_count_by_status(self, conn: sqlite3.Connection, since_bound: str | None) -> dict[str, int]:
        sql = "select j.status as key, count(*) as count from jobs j left join ingestion_runs r on r.run_id=j.run_id"
        args: list[Any] = []
        if since_bound:
            sql += " where coalesce(r.started_at,j.created_at)>=?"
            args.append(since_bound)
        sql += " group by j.status order by j.status"
        rows = conn.execute(sql, args).fetchall()
        return {str(row["key"]): int(row["count"]) for row in rows if row["key"] is not None}

    def _failed_jobs(self, conn: sqlite3.Connection, since_bound: str | None, limit: int) -> list[dict[str, Any]]:
        sql = """
            select j.job_key,j.run_id,j.api_name,j.status,j.params_json,j.last_error_type,j.last_error,
                   coalesce(r.started_at,j.created_at) as started_at
            from jobs j left join ingestion_runs r on r.run_id=j.run_id
            where j.status='failed'
        """
        args: list[Any] = []
        if since_bound:
            sql += " and coalesce(r.started_at,j.created_at)>=?"
            args.append(since_bound)
        sql += " order by started_at desc, j.job_key limit ?"
        args.append(limit)
        rows = conn.execute(sql, args).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["params"] = loads(item.pop("params_json")) or {}
            out.append(item)
        return out

    def _latest_run_id(self, conn: sqlite3.Connection, since_bound: str | None) -> str | None:
        sql = "select run_id from ingestion_runs"
        args: list[Any] = []
        if since_bound:
            sql += " where started_at>=?"
            args.append(since_bound)
        sql += " order by started_at desc limit 1"
        row = conn.execute(sql, args).fetchone()
        return str(row[0]) if row else None

    def _backup_summary(self, backup: Path | str | None, warnings: list[str], blocking_errors: list[str]) -> dict[str, Any] | None:
        if backup is None:
            return None
        backup_root = Path(backup)
        if not backup_root.exists():
            blocking_errors.append(f"backup not found: {backup_root}")
            return {"path": str(backup_root), "status": "missing"}
        inspect = BackupInspector().inspect(backup_root)
        restore = RestoreChecker().check(backup_root)
        if inspect.status != "succeeded":
            blocking_errors.append("backup-inspect failed")
        if restore.status != "succeeded":
            blocking_errors.append("restore-check failed")
        if inspect.possible_mutation or restore.possible_mutation:
            blocking_errors.append("backup catalog may have been modified after backup creation")
        if inspect.manifest_warning_count:
            warnings.append("backup manifest has warnings")
        return {
            "path": str(backup_root),
            "backup_id": inspect.backup_id or restore.backup_id,
            "backup_status": inspect.status,
            "restore_check_status": restore.status,
            "catalog_checksum_status": restore.catalog_checksum_status or inspect.catalog_checksum_status,
            "possible_mutation": bool(inspect.possible_mutation or restore.possible_mutation),
            "file_count": inspect.file_count,
            "raw_file_count": inspect.raw_file_count,
            "lake_file_count": inspect.lake_file_count,
            "manifest_warning_count": inspect.manifest_warning_count,
            "manifest_error_count": inspect.manifest_error_count,
        }


class MirrorNextBatchReporter:
    REPORT_VERSION = "mirror-next-batch/v1"
    FIRST_CONTROLLED_MONTH = "202501"
    MAX_MONTHS_TO_SCAN = 48
    RECOMMENDED_MAX_JOBS_PER_API = 20

    def report(
        self,
        *,
        root: Path | str,
        scope: str = "low-risk-a-share",
    ) -> MirrorNextBatchResult:
        ensure_mirror_scope(scope)
        mirror_root = Path(root)
        warnings = ["mirror-next-batch is read-only; it does not execute mirror-run, fetch, or backfill"]
        blocking_errors: list[str] = []
        catalog = CatalogStore(mirror_root, read_only=True)
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
            return self._result(mirror_root, scope, [], None, None, None, "catalog missing", None, 0, warnings, blocking_errors)

        completed: list[str] = []
        month = self.FIRST_CONTROLLED_MONTH
        incomplete: dict[str, Any] | None = None
        try:
            for _ in range(self.MAX_MONTHS_TO_SCAN):
                status = self._month_status(mirror_root, catalog, scope, month)
                if status["complete"]:
                    completed.append(month)
                    month = self._next_month(month)
                    continue
                incomplete = status
                break
        except Exception as exc:
            blocking_errors.append(f"next batch inspection failed: {exc}")
            return self._result(mirror_root, scope, completed, completed[-1] if completed else None, None, None, "inspection failed", None, 0, warnings, blocking_errors)

        target_month = (incomplete or {}).get("month") or month
        start, end = self._month_range(target_month)
        reason = self._reason(completed, incomplete)
        required_trade_cal_range = (incomplete or {}).get("calendar")
        estimated = self._estimated_request_count(mirror_root, catalog, scope, start, end, warnings)
        return self._result(
            mirror_root,
            scope,
            completed,
            completed[-1] if completed else None,
            start,
            end,
            reason,
            required_trade_cal_range,
            estimated,
            warnings,
            blocking_errors,
        )

    def _result(
        self,
        root: Path,
        scope: str,
        completed: list[str],
        last_complete: str | None,
        start: str | None,
        end: str | None,
        reason: str,
        required_trade_cal_range: dict[str, Any] | None,
        estimated_request_count: int,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> MirrorNextBatchResult:
        plan_command = None
        execute_preview = None
        if start and end:
            plan_command = (
                f"python3 -m tushare_mirror mirror-batch-plan --root {root} --scope {scope} "
                f"--start-date {start} --end-date {end} --max-jobs-per-api {self.RECOMMENDED_MAX_JOBS_PER_API}"
            )
            execute_preview = {
                "confirmation": "USER_CONFIRMATION_REQUIRED",
                "command": (
                    f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} --mode pilot "
                    f"--start-date {start} --end-date {end} --max-jobs-per-api {self.RECOMMENDED_MAX_JOBS_PER_API} --execute"
                ),
            }
        return MirrorNextBatchResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            scope=scope,
            current_completed_months=completed,
            last_complete_month=last_complete,
            recommended_next_start_date=start,
            recommended_next_end_date=end,
            reason=reason,
            required_trade_cal_range=required_trade_cal_range,
            estimated_request_count=estimated_request_count,
            recommended_max_jobs_per_api=self.RECOMMENDED_MAX_JOBS_PER_API,
            plan_command_preview=plan_command,
            execute_command_preview=execute_preview,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _month_status(self, root: Path, catalog: CatalogStore, scope: str, month: str) -> dict[str, Any]:
        start, end = self._month_range(month)
        planner = MirrorBatchPlanner(root, catalog)
        calendar = planner._calendar_range(start, end, "SSE")
        calendar_summary = {
            "start_date": start,
            "end_date": end,
            "exchange": "SSE",
            "status": calendar.get("status"),
            "missing_calendar_dates": calendar.get("missing_calendar_dates") or [],
            "trading_dates": calendar.get("open_dates") or [],
        }
        if calendar.get("status") != "covered":
            return {"month": month, "complete": False, "reason": "missing_trade_cal_range", "calendar": calendar_summary}
        coverage = []
        for api_name in daily_like_apis_for_scope(scope):
            report = CoverageReporter(root, catalog).report(
                api_name,
                start_date=start,
                end_date=end,
                trading_days_only=True,
                calendar_exchange="SSE",
            )
            data = report.to_dict()
            data.pop("items", None)
            coverage.append(data)
        complete = bool(coverage) and all(
            int(row.get("total_dates") or 0) > 0
            and int(row.get("covered_dates") or 0) == int(row.get("total_dates") or 0)
            and int(row.get("missing_dates") or 0) == 0
            and int(row.get("failed_dates") or 0) == 0
            and int(row.get("quarantined_dates") or 0) == 0
            for row in coverage
        )
        return {
            "month": month,
            "complete": complete,
            "reason": "complete" if complete else "partial_daily_like_coverage",
            "calendar": calendar_summary,
            "coverage": coverage,
        }

    def _reason(self, completed: list[str], incomplete: dict[str, Any] | None) -> str:
        if not completed and incomplete and incomplete.get("reason") == "missing_trade_cal_range":
            return "no completed month found; recommend first controlled month and required local trade_cal range"
        if not completed:
            return f"partial coverage detected for {incomplete.get('month') if incomplete else self.FIRST_CONTROLLED_MONTH}; recommend completing that month"
        if incomplete and incomplete.get("reason") == "partial_daily_like_coverage":
            return f"partial coverage detected for {incomplete['month']}; recommend completing that month before advancing"
        return f"latest complete month is {completed[-1]}; recommend next bounded month"

    def _estimated_request_count(self, root: Path, catalog: CatalogStore, scope: str, start: str, end: str, warnings: list[str]) -> int:
        try:
            plan = MirrorBatchPlanner(root, catalog).plan(
                scope=scope,
                start_date=start,
                end_date=end,
                calendar_exchange="SSE",
                max_jobs_per_api=self.RECOMMENDED_MAX_JOBS_PER_API,
            )
            return plan.estimated_request_count
        except Exception as exc:
            warnings.append(f"request estimate unavailable from mirror-batch-plan: {exc}")
            return 0

    def _month_range(self, month: str) -> tuple[str, str]:
        start = datetime.strptime(month + "01", "%Y%m%d")
        next_month = datetime.strptime(self._next_month(month) + "01", "%Y%m%d")
        end = next_month - timedelta(days=1)
        return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

    def _next_month(self, month: str) -> str:
        year = int(month[:4])
        month_num = int(month[4:])
        if month_num == 12:
            return f"{year + 1}01"
        return f"{year}{month_num + 1:02d}"


class MirrorBatchBundleReporter:
    REPORT_VERSION = "mirror-batch-bundle/v1"
    MANIFEST_VERSION = "mirror-batch-bundle-manifest/v1"
    MANIFEST_FILE = "bundle_manifest.json"
    REPORT_FILES = [
        "README.md",
        "batch_plan.json",
        "readiness.json",
        "review.json",
        "status.json",
        "audit.json",
        "stop_policy.json",
        "operator_checklist.json",
        "final_gate.json",
        "execute_script_preview.sh",
        "final_operator_summary.md",
        "command_safety.json",
        "commands.sh",
    ]
    BUNDLE_FILES = [*REPORT_FILES, MANIFEST_FILE]

    def create(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
        output: Path | str,
        overwrite: bool = False,
    ) -> MirrorBatchBundleResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        output_root = _resolve_path(Path(output))
        warnings = ["mirror-batch-bundle writes only the requested output bundle and does not execute generated commands"]
        blocking_errors = self._preflight(mirror_root, backup_root, output_root, overwrite)
        if blocking_errors:
            return self._result(mirror_root, backup_root, output_root, scope, start_date, end_date, max_jobs_per_api, "blocked", overwrite, [], warnings, blocking_errors)

        catalog = CatalogStore(mirror_root, read_only=True)
        try:
            batch_plan = MirrorBatchPlanner(mirror_root, catalog).plan(
                scope=scope,
                start_date=start_date,
                end_date=end_date,
                calendar_exchange="SSE",
                max_jobs_per_api=max_jobs_per_api,
            )
            readiness = MirrorReadinessReporter().report(root=mirror_root, backup=backup_root, scope=scope)
            review = MirrorReviewer().review(root=mirror_root, backup=backup_root, scope=scope, mode="pilot", start_date="20250101", end_date="20250131")
            status = MirrorStatusReporter().report(root=mirror_root, backup=backup_root, scope=scope)
            audit = MirrorAuditReporter().report(root=mirror_root, backup=backup_root, scope=scope)
            operator_checklist = MirrorOperatorChecklistReporter().report(
                root=mirror_root,
                backup=backup_root,
                scope=scope,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            return self._result(mirror_root, backup_root, output_root, scope, start_date, end_date, max_jobs_per_api, "blocked", overwrite, [], warnings, [f"bundle source report failed: {exc}"])

        if output_root.exists() and overwrite:
            if output_root.is_dir():
                shutil.rmtree(output_root)
            else:
                output_root.unlink()
        output_root.mkdir(parents=True, exist_ok=False)
        payloads: dict[str, Any] = {
            "batch_plan.json": batch_plan.to_dict(),
            "readiness.json": readiness.to_dict(),
            "review.json": review.to_dict(),
            "status.json": status.to_dict(),
            "audit.json": audit.to_dict(),
            "stop_policy.json": StopPolicyReporter().report(scope=scope).to_dict(),
            "operator_checklist.json": operator_checklist.to_dict(),
        }
        for filename, payload in payloads.items():
            (output_root / filename).write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        (output_root / "README.md").write_text(self._readme(scope, start_date, end_date), encoding="utf-8")
        commands = output_root / "commands.sh"
        commands.write_text(
            self._commands(mirror_root, backup_root, scope, start_date, end_date, max_jobs_per_api),
            encoding="utf-8",
        )
        commands.chmod(0o644)
        command_safety = CommandSafetyAnalyzer().analyze(file=commands).to_dict()
        payloads["command_safety.json"] = command_safety
        (output_root / "command_safety.json").write_text(json.dumps(command_safety, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        base_report_files = [relative_path for relative_path in self.REPORT_FILES if relative_path not in {"final_gate.json", "execute_script_preview.sh", "final_operator_summary.md"}]
        base_manifest = self._manifest(
            output_root=output_root,
            mirror_root=mirror_root,
            backup_root=backup_root,
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs_per_api,
            input_reports=payloads,
            report_files=base_report_files,
        )
        (output_root / self.MANIFEST_FILE).write_text(json.dumps(base_manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        final_gate = FinalGateReporter().report(
            root=mirror_root,
            backup=backup_root,
            bundle=output_root,
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs_per_api,
        ).to_dict()
        payloads["final_gate.json"] = final_gate
        (output_root / "final_gate.json").write_text(json.dumps(final_gate, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        script_preview = output_root / "execute_script_preview.sh"
        script_preview.write_text(
            ExecuteScriptReporter()._script(mirror_root, backup_root, output_root, scope, start_date, end_date, max_jobs_per_api, confirmation_phrase(scope, start_date, end_date, max_jobs_per_api)),
            encoding="utf-8",
        )
        script_preview.chmod(0o644)
        (output_root / "final_operator_summary.md").write_text(
            self._final_operator_summary(final_gate),
            encoding="utf-8",
        )
        manifest = self._manifest(
            output_root=output_root,
            mirror_root=mirror_root,
            backup_root=backup_root,
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs_per_api,
            input_reports=payloads,
        )
        (output_root / self.MANIFEST_FILE).write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return self._result(mirror_root, backup_root, output_root, scope, start_date, end_date, max_jobs_per_api, "created", overwrite, self.BUNDLE_FILES, warnings, [])

    def _preflight(self, mirror_root: Path, backup_root: Path, output_root: Path, overwrite: bool) -> list[str]:
        blocking_errors: list[str] = []
        catalog_path = mirror_root / "_catalog" / "catalog.sqlite"
        if not catalog_path.exists():
            blocking_errors.append(f"catalog not found: {catalog_path}; run init-catalog first")
        if not backup_root.exists():
            blocking_errors.append(f"backup not found: {backup_root}")
        if output_root == mirror_root or _is_relative_to(output_root, mirror_root):
            blocking_errors.append("output path must not be inside mirror root")
        if output_root == backup_root or _is_relative_to(output_root, backup_root):
            blocking_errors.append("output path must not be inside backup root")
        if output_root.exists() and not overwrite:
            manifest_path = output_root / self.MANIFEST_FILE
            if output_root.is_dir() and not manifest_path.exists():
                blocking_errors.append("existing bundle output is not a valid manifest-bearing bundle; rerun with --overwrite or choose another output path")
            else:
                blocking_errors.append("output path already exists; pass --overwrite to replace it")
        return blocking_errors

    def _result(
        self,
        root: Path,
        backup: Path,
        output: Path,
        scope: str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
        status: str,
        overwritten: bool,
        files: list[str],
        warnings: list[str],
        blocking_errors: list[str],
    ) -> MirrorBatchBundleResult:
        return MirrorBatchBundleResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            backup=str(backup),
            output=str(output),
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs_per_api,
            status=status,
            overwritten=overwritten,
            files=files,
            commands_execute_guard="USER_CONFIRMATION_REQUIRED",
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _readme(self, scope: str, start_date: str, end_date: str) -> str:
        return "\n".join(
            [
                "# Tushare Mirror Batch Dry-run Bundle",
                "",
                f"Scope: {scope}",
                f"Window: {start_date}-{end_date}",
                "",
                "This bundle is generated from local read-only reports. It does not execute mirror-run, fetch data, backfill dates, or write catalog state.",
                "No command has been executed, the February batch has not started, and this bundle is only a plan artifact.",
                "Review every JSON report before using any command preview.",
                "commands.sh contains staged previews only; any mirror-run --execute line is commented and marked USER_CONFIRMATION_REQUIRED.",
                "",
            ]
        )

    def _commands(self, root: Path, backup: Path, scope: str, start_date: str, end_date: str, max_jobs_per_api: int) -> str:
        plan = (
            f"python3 -m tushare_mirror mirror-batch-plan --root {root} --scope {scope} "
            f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {max_jobs_per_api} --json"
        )
        execute = (
            f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} --mode pilot "
            f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {max_jobs_per_api} "
            f"--backup-target {backup} --execute --json"
        )
        validate = f"python3 -m tushare_mirror --root {root} validate --latest-all --no-record --json"
        backup_command = f"python3 -m tushare_mirror --root {root} backup --target {backup} --json"
        restore_check = f"python3 -m tushare_mirror restore-check --backup {backup} --json"
        review = (
            f"python3 -m tushare_mirror mirror-review --root {root} --backup {backup} --scope {scope} "
            f"--start-date {start_date} --end-date {end_date} --json"
        )
        return "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'echo "This bundle is a dry-run artifact. No command in it has been executed."',
                "",
                "# Stage 1: trade_cal dependency staging",
                plan,
                "",
                "# USER_CONFIRMATION_REQUIRED: Run the single monthly mirror-run only after user confirms.",
                "# The orchestrator will fetch trade_cal before daily-like endpoints; there is no natural-day fallback.",
                "# If the current system cannot execute trade_cal separately, do not invent a separate command.",
                f"# {execute}",
                "",
                "# Stage 2: post-dependency and post-execution checks",
                "# Rerun mirror-batch-plan after trade_cal is local and before interpreting daily-like readiness.",
                f"# {plan}",
                "# validate --no-record would run only after a successful user-confirmed execution.",
                f"# {validate}",
                "# backup would run only after successful validation/review of the user-confirmed execution.",
                f"# {backup_command}",
                "# restore-check would run only after backup completes.",
                f"# {restore_check}",
                "# post-batch review would run after backup and restore-check.",
                f"# {review}",
                "",
                "# February batch has not started; this file is documentation, not an automation script.",
                "# Do not run commands.sh directly.",
                "",
            ]
        )

    def _manifest(
        self,
        *,
        output_root: Path,
        mirror_root: Path,
        backup_root: Path,
        scope: str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
        input_reports: dict[str, Any],
        report_files: list[str] | None = None,
    ) -> dict[str, Any]:
        commands_path = output_root / "commands.sh"
        command_text = commands_path.read_text(encoding="utf-8")
        commands = self._command_manifest_items(command_text)
        relative_paths = report_files or self.REPORT_FILES
        return {
            "manifest_version": self.MANIFEST_VERSION,
            "bundle_id": f"{scope}_{start_date}_{end_date}_{max_jobs_per_api}",
            "created_at": now_utc(),
            "source_root": str(mirror_root),
            "backup_root": str(backup_root),
            "scope": scope,
            "start_date": start_date,
            "end_date": end_date,
            "max_jobs_per_api": max_jobs_per_api,
            "generated_by": "python3 -m tushare_mirror mirror-batch-bundle",
            "input_reports": [
                {
                    "relative_path": filename,
                    "report_version": payload.get("report_version") if isinstance(payload, dict) else None,
                }
                for filename, payload in sorted(input_reports.items())
            ],
            "files": [self._manifest_file_item(output_root, relative_path) for relative_path in relative_paths],
            "commands": commands,
            "safety_boundaries": [
                "bundle generation is read-only except for the requested output directory",
                "commands.sh must not be auto-executed",
                "mirror-run --execute requires explicit user confirmation",
                "no real Tushare requests are made during bundle generation",
            ],
            "requires_user_confirmation": any(item["requires_user_confirmation"] for item in commands),
            "execute_command_present": any(item["would_execute_real_requests"] for item in commands),
            "commands_guarded": all((not item["would_execute_real_requests"]) or item["guarded"] for item in commands),
            "token_plaintext_found": self._token_plaintext_found(output_root, relative_paths),
        }

    def _final_operator_summary(self, final_gate: dict[str, Any]) -> str:
        return "\n".join(
            [
                "# Final Operator Summary",
                "",
                f"Gate status: {final_gate.get('gate_status')}",
                f"Ready for user-confirmed execute: {final_gate.get('ready_for_user_confirmed_execute')}",
                f"Ready for dependency stage: {final_gate.get('ready_for_dependency_stage')}",
                f"Ready for full batch after dependency: {final_gate.get('ready_for_full_batch_after_dependency')}",
                f"Estimated requests: {final_gate.get('estimated_request_count')}",
                f"Confirmation phrase: {final_gate.get('confirmation_phrase')}",
                "",
                "This summary is generated from local read-only reports. It does not execute February.",
                "Do not run commands.sh or execute_script_preview.sh automatically.",
                "Any mirror-run --execute command requires explicit user confirmation.",
                "The confirmation phrase is operator friction, not security.",
                "",
            ]
        )

    def _manifest_file_item(self, output_root: Path, relative_path: str) -> dict[str, Any]:
        path = output_root / relative_path
        return {
            "relative_path": relative_path,
            "size_bytes": path.stat().st_size,
            "sha256": self._sha256(path),
            "file_kind": self._file_kind(relative_path),
            "required": True,
        }

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _file_kind(self, relative_path: str) -> str:
        if relative_path.endswith(".json"):
            return "json_report"
        if relative_path.endswith(".md"):
            return "readme"
        if relative_path.endswith(".sh"):
            return "command_preview"
        return "artifact"

    def _command_manifest_items(self, content: str) -> list[dict[str, Any]]:
        marker_present = "USER_CONFIRMATION_REQUIRED" in content
        items: list[dict[str, Any]] = []
        for raw_line in content.splitlines():
            stripped = raw_line.strip()
            if "python3 -m tushare_mirror" not in stripped:
                continue
            guarded = stripped.startswith("#") or marker_present
            command_text = stripped.lstrip("#").strip()
            parts = command_text.split()
            command_name = self._mirror_command_name(parts)
            would_execute = (
                (command_name == "mirror-run" and "--execute" in parts)
                or (command_name in {"backfill", "backfill-missing"} and "--execute" in parts)
            )
            allowed_commands = {
                "mirror-batch-plan",
                "mirror-run",
                "validate",
                "backup",
                "restore-check",
                "mirror-review",
            }
            items.append(
                {
                    "command_name": command_name,
                    "command_text": command_text,
                    "would_execute_real_requests": would_execute,
                    "requires_user_confirmation": would_execute,
                    "guarded": guarded,
                    "allowed_in_bundle": command_name in allowed_commands and ((not would_execute) or guarded),
                }
            )
        return items

    def _mirror_command_name(self, parts: list[str]) -> str:
        try:
            index = parts.index("tushare_mirror") + 1
        except (ValueError, IndexError):
            return "unknown"
        options_with_values = {"--root"}
        while index < len(parts):
            token = parts[index]
            if token in options_with_values:
                index += 2
                continue
            if token.startswith("-"):
                index += 1
                continue
            return token
        return "unknown"

    def _token_plaintext_found(self, output_root: Path, relative_paths: list[str]) -> bool:
        token = os.environ.get("TUSHARE_TOKEN")
        if not token:
            return False
        for relative_path in relative_paths:
            path = output_root / relative_path
            if not path.exists() or not path.is_file():
                continue
            try:
                if token in path.read_text(encoding="utf-8", errors="ignore"):
                    return True
            except OSError:
                continue
        return False


class MirrorBatchBundleVerifier:
    REPORT_VERSION = "mirror-batch-bundle-verify/v1"
    TOKEN_PATTERN = re.compile(r"(?i)(?:TUSHARE_TOKEN|token)\s*[:=]\s*['\"]?[A-Za-z0-9][A-Za-z0-9_\-]{10,}")
    JSON_REPORTS = [
        "batch_plan.json",
        "readiness.json",
        "review.json",
        "status.json",
        "audit.json",
        "stop_policy.json",
        "operator_checklist.json",
        "final_gate.json",
        "command_safety.json",
    ]

    def verify(self, *, bundle: Path | str) -> MirrorBatchBundleVerifyResult:
        bundle_root = _resolve_path(Path(bundle))
        warnings = ["mirror-batch-bundle-verify is read-only and does not execute commands"]
        blocking_errors: list[str] = []
        checked_file_count = 0
        missing_file_count = 0
        checksum_failure_count = 0
        command_guard_status = "blocked"
        bundle_id: str | None = None
        manifest_present = False
        manifest_valid = False
        pre_manifest_bundle_detected = False
        recommended_action: str | None = None

        if not bundle_root.exists() or not bundle_root.is_dir():
            return self._result(bundle_root, None, False, False, False, None, 0, 0, 0, 0, "blocked", False, warnings, [f"bundle not found: {bundle_root}"])

        file_count = self._file_count(bundle_root)
        manifest_path = bundle_root / MirrorBatchBundleReporter.MANIFEST_FILE
        manifest: dict[str, Any] | None = None
        if not manifest_path.exists():
            pre_manifest_bundle_detected = self._looks_like_pre_manifest_bundle(bundle_root)
            recommended_action = "Regenerate bundle with mirror-batch-bundle --overwrite"
            blocking_errors.append("missing bundle_manifest.json")
        else:
            manifest_present = True
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                recommended_action = "Regenerate bundle with mirror-batch-bundle --overwrite"
                blocking_errors.append(f"bundle_manifest.json is invalid JSON: {exc}")
            else:
                bundle_id = manifest.get("bundle_id")
                if manifest.get("manifest_version") != MirrorBatchBundleReporter.MANIFEST_VERSION:
                    recommended_action = "Regenerate bundle with mirror-batch-bundle --overwrite"
                    blocking_errors.append(f"unsupported manifest_version: {manifest.get('manifest_version')}")
                else:
                    manifest_valid = True

        if manifest:
            for item in manifest.get("files") or []:
                relative_path = str(item.get("relative_path") or "")
                path = bundle_root / relative_path
                if item.get("required") and not path.exists():
                    missing_file_count += 1
                    blocking_errors.append(f"required bundle file missing: {relative_path}")
                    continue
                if not path.exists():
                    continue
                checked_file_count += 1
                expected_size = item.get("size_bytes")
                expected_sha = item.get("sha256")
                actual_size = path.stat().st_size
                actual_sha = self._sha256(path)
                if expected_size != actual_size or expected_sha != actual_sha:
                    checksum_failure_count += 1
                    blocking_errors.append(f"bundle file checksum mismatch: {relative_path}")

        for relative_path in ["README.md", "commands.sh", *self.JSON_REPORTS]:
            path = bundle_root / relative_path
            if not path.exists():
                if relative_path not in {str((item or {}).get("relative_path")) for item in ((manifest or {}).get("files") or [])}:
                    missing_file_count += 1
                    blocking_errors.append(f"required bundle file missing: {relative_path}")
                continue
            if relative_path.endswith(".json"):
                try:
                    json.loads(path.read_text(encoding="utf-8"))
                except json.JSONDecodeError as exc:
                    blocking_errors.append(f"{relative_path} is invalid JSON: {exc}")

        commands_path = bundle_root / "commands.sh"
        command_guard_status = self._command_guard_status(commands_path, warnings, blocking_errors)
        execute_script = bundle_root / "execute_script_preview.sh"
        if execute_script.exists():
            script_safety = CommandSafetyAnalyzer().analyze(file=execute_script)
            if script_safety.status == "blocked":
                blocking_errors.extend(script_safety.blocking_errors)
                blocking_errors.append("execute_script_preview.sh failed command safety check")
        token_plaintext_found = self._token_plaintext_found(bundle_root)
        if token_plaintext_found:
            blocking_errors.append("token-like plaintext found in bundle")

        status = "blocked" if blocking_errors else ("warning" if warnings[1:] or command_guard_status == "warning" else "passed")
        return self._result(
            bundle_root,
            bundle_id,
            manifest_present,
            manifest_valid,
            pre_manifest_bundle_detected,
            recommended_action,
            file_count,
            checked_file_count,
            missing_file_count,
            checksum_failure_count,
            command_guard_status,
            token_plaintext_found,
            warnings,
            blocking_errors,
            status=status,
        )

    def _result(
        self,
        bundle: Path,
        bundle_id: str | None,
        manifest_present: bool,
        manifest_valid: bool,
        pre_manifest_bundle_detected: bool,
        recommended_action: str | None,
        file_count: int,
        checked_file_count: int,
        missing_file_count: int,
        checksum_failure_count: int,
        command_guard_status: str,
        token_plaintext_found: bool,
        warnings: list[str],
        blocking_errors: list[str],
        *,
        status: str | None = None,
    ) -> MirrorBatchBundleVerifyResult:
        final_status = status or ("blocked" if blocking_errors else ("warning" if warnings[1:] else "passed"))
        return MirrorBatchBundleVerifyResult(
            report_version=self.REPORT_VERSION,
            status=final_status,
            bundle=str(bundle),
            bundle_id=bundle_id,
            manifest_present=manifest_present,
            manifest_valid=manifest_valid,
            pre_manifest_bundle_detected=pre_manifest_bundle_detected,
            recommended_action=recommended_action,
            file_count=file_count,
            checked_file_count=checked_file_count,
            missing_file_count=missing_file_count,
            checksum_failure_count=checksum_failure_count,
            command_guard_status=command_guard_status,
            token_plaintext_found=token_plaintext_found,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _file_count(self, bundle_root: Path) -> int:
        return sum(1 for path in bundle_root.iterdir() if path.is_file())

    def _looks_like_pre_manifest_bundle(self, bundle_root: Path) -> bool:
        marker_files = {"README.md", "commands.sh", "batch_plan.json", "readiness.json", "review.json", "status.json", "audit.json", "stop_policy.json"}
        return any((bundle_root / relative_path).exists() for relative_path in marker_files)

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _command_guard_status(self, commands_path: Path, warnings: list[str], blocking_errors: list[str]) -> str:
        if not commands_path.exists():
            return "blocked"
        content = commands_path.read_text(encoding="utf-8", errors="ignore")
        marker_present = "USER_CONFIRMATION_REQUIRED" in content
        if not marker_present:
            blocking_errors.append("commands.sh missing USER_CONFIRMATION_REQUIRED marker")
        unguarded_execute = []
        for raw_line in content.splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "mirror-run" in stripped and "--execute" in stripped:
                unguarded_execute.append(stripped)
            if "backfill" in stripped and "--execute" in stripped:
                unguarded_execute.append(stripped)
        if unguarded_execute:
            blocking_errors.append("commands.sh contains unguarded execute command")
        if commands_path.stat().st_mode & 0o111:
            warnings.append("commands.sh is executable; keep bundle command previews non-executable by default")
            return "warning" if marker_present and not unguarded_execute else "blocked"
        return "passed" if marker_present and not unguarded_execute else "blocked"

    def _token_plaintext_found(self, bundle_root: Path) -> bool:
        env_token = os.environ.get("TUSHARE_TOKEN")
        for path in bundle_root.iterdir():
            if not path.is_file():
                continue
            if path.suffix not in {".json", ".md", ".sh", ".txt", ".yaml", ".yml"}:
                continue
            try:
                content = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if env_token and env_token in content:
                return True
            if self.TOKEN_PATTERN.search(content):
                return True
        return False


class CommandSafetyAnalyzer:
    REPORT_VERSION = "command-safety-check/v1"
    TOKEN_PATTERN = MirrorBatchBundleVerifier.TOKEN_PATTERN

    def analyze(self, *, file: Path | str) -> CommandSafetyCheckResult:
        path = _resolve_path(Path(file))
        warnings = ["command-safety-check is read-only and does not execute command files"]
        blocking_errors: list[str] = []
        execute_commands: list[str] = []
        guarded_execute: list[str] = []
        unguarded_execute: list[str] = []
        destructive: list[str] = []
        network: list[str] = []

        if not path.exists() or not path.is_file():
            return self._result(path, "blocked", [], [], [], [], [], False, warnings, [f"command file not found: {path}"])

        content = path.read_text(encoding="utf-8", errors="ignore")
        marker_present = "USER_CONFIRMATION_REQUIRED" in content
        token_plaintext_found = self._token_plaintext_found(content)
        if token_plaintext_found:
            blocking_errors.append("token-like plaintext found in command file")

        for raw_line in content.splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#!") or stripped.startswith("set ") or stripped.startswith("echo "):
                continue
            command_text = stripped.lstrip("#").strip()
            is_comment = stripped.startswith("#")
            if self._is_execute_command(command_text):
                execute_commands.append(command_text)
                if is_comment and marker_present:
                    guarded_execute.append(command_text)
                else:
                    unguarded_execute.append(command_text)
            if self._is_destructive_rm(command_text):
                destructive.append(command_text)
            if self._is_network_command(command_text):
                network.append(command_text)
            self._detect_path_relationship_risks(command_text, blocking_errors)
            self._detect_python_fetch_risk(command_text, is_comment, marker_present, blocking_errors)
            self._detect_unknown_high_risk(command_text, is_comment, blocking_errors)

        if execute_commands:
            warnings.append("execute command previews were found; explicit user confirmation is required before any execution")
        if execute_commands and not marker_present:
            blocking_errors.append("USER_CONFIRMATION_REQUIRED marker missing for execute command preview")
        if unguarded_execute:
            blocking_errors.append("unguarded execute command found")
        if destructive:
            blocking_errors.append("destructive rm -rf command found")
        if network:
            blocking_errors.append("network command or URL found")

        status = "blocked" if blocking_errors else ("warning" if warnings[1:] else "passed")
        return self._result(path, status, execute_commands, guarded_execute, unguarded_execute, destructive, network, token_plaintext_found, warnings, blocking_errors)

    def _result(
        self,
        path: Path,
        status: str,
        execute_commands: list[str],
        guarded_execute: list[str],
        unguarded_execute: list[str],
        destructive: list[str],
        network: list[str],
        token_plaintext_found: bool,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> CommandSafetyCheckResult:
        return CommandSafetyCheckResult(
            report_version=self.REPORT_VERSION,
            file=str(path),
            status=status,
            execute_commands_found=_dedupe_messages(execute_commands),
            guarded_execute_commands=_dedupe_messages(guarded_execute),
            unguarded_execute_commands=_dedupe_messages(unguarded_execute),
            destructive_commands_found=_dedupe_messages(destructive),
            network_commands_found=_dedupe_messages(network),
            token_plaintext_found=token_plaintext_found,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _is_execute_command(self, command_text: str) -> bool:
        parts = command_text.split()
        return (
            ("mirror-run" in parts and "--execute" in parts)
            or ("backfill" in parts and "--execute" in parts)
            or ("backfill-missing" in parts and "--execute" in parts)
        )

    def _is_destructive_rm(self, command_text: str) -> bool:
        parts = command_text.split()
        return bool(parts and parts[0] == "rm" and any(flag in {"-rf", "-fr", "-r", "-R"} for flag in parts[1:]))

    def _is_network_command(self, command_text: str) -> bool:
        parts = command_text.split()
        return bool(parts and parts[0] in {"curl", "wget"}) or "http://" in command_text or "https://" in command_text

    def _token_plaintext_found(self, content: str) -> bool:
        env_token = os.environ.get("TUSHARE_TOKEN")
        return bool((env_token and env_token in content) or self.TOKEN_PATTERN.search(content))

    def _detect_path_relationship_risks(self, command_text: str, blocking_errors: list[str]) -> None:
        parts = command_text.split()
        root = self._option_value(parts, "--root")
        output = self._option_value(parts, "--output")
        backup = self._option_value(parts, "--backup") or self._option_value(parts, "--backup-target")
        if root and output:
            try:
                root_path = _resolve_path(Path(root))
                output_path = _resolve_path(Path(output))
            except OSError:
                return
            if output_path == root_path or _is_relative_to(output_path, root_path):
                blocking_errors.append("output path is inside mirror root")
        if root and backup:
            try:
                root_path = _resolve_path(Path(root))
                backup_path = _resolve_path(Path(backup))
            except OSError:
                return
            if backup_path == root_path or _is_relative_to(backup_path, root_path):
                blocking_errors.append("backup path is inside mirror root")

    def _detect_python_fetch_risk(self, command_text: str, is_comment: bool, marker_present: bool, blocking_errors: list[str]) -> None:
        parts = command_text.split()
        if not parts:
            return
        if parts[0].startswith("python") and "tushare_real_smoke.py" in command_text:
            blocking_errors.append("python command would run real smoke requests")
            return
        if parts[0].startswith("python") and "tushare_mirror" in parts:
            command_name = self._mirror_command_name(parts)
            if command_name == "fetch" and "--dry-run" not in parts:
                blocking_errors.append("python command would fetch real Tushare data")
            if command_name in {"backfill", "backfill-missing", "mirror-run"} and "--execute" in parts and not (is_comment and marker_present):
                blocking_errors.append("python command would execute real Tushare requests without guard")

    def _detect_unknown_high_risk(self, command_text: str, is_comment: bool, blocking_errors: list[str]) -> None:
        if is_comment:
            return
        parts = command_text.split()
        if not parts:
            return
        if parts[0].startswith("python") and "tushare_mirror" in parts:
            command_name = self._mirror_command_name(parts)
            if command_name not in {"mirror-batch-plan", "mirror-status", "mirror-audit", "mirror-next-batch", "mirror-batch-bundle-verify", "command-safety-check"}:
                blocking_errors.append(f"unknown or high-risk active tushare_mirror command: {command_name}")
        elif parts[0] not in {"echo"}:
            if self.TOKEN_PATTERN.search(command_text):
                blocking_errors.append("unknown high-risk active command contains token-like assignment")
            else:
                blocking_errors.append(f"unknown high-risk active command: {parts[0]}")

    def _mirror_command_name(self, parts: list[str]) -> str:
        try:
            return parts[parts.index("tushare_mirror") + 1]
        except (ValueError, IndexError):
            return "unknown"

    def _option_value(self, parts: list[str], option: str) -> str | None:
        try:
            index = parts.index(option)
        except ValueError:
            return None
        if index + 1 >= len(parts):
            return None
        return parts[index + 1]


class MirrorBatchRehearsalReporter:
    REPORT_VERSION = "mirror-batch-rehearse/v1"

    def rehearse(self, *, root: Path | str, backup: Path | str, bundle: Path | str) -> MirrorBatchRehearsalResult:
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        bundle_root = _resolve_path(Path(bundle))
        warnings = ["mirror-batch-rehearse is read-only and does not execute generated commands"]
        blocked_by: list[str] = []
        steps: list[dict[str, Any]] = []
        estimated_request_count = 0

        manifest = self._read_manifest(bundle_root, warnings, blocked_by)
        verification = MirrorBatchBundleVerifier().verify(bundle=bundle_root)
        if verification.status == "blocked":
            blocked_by.append("bundle_verification")
        elif verification.status == "warning":
            warnings.extend(verification.warnings)
        steps.append(self._step("preflight", "blocked" if blocked_by else "passed", "verify bundle, root, and backup paths", verification.to_dict()))

        if not manifest:
            return self._result(mirror_root, backup_root, bundle_root, "blocked", steps, estimated_request_count, blocked_by, warnings)

        scope = str(manifest.get("scope") or "low-risk-a-share")
        start_date = str(manifest.get("start_date") or "")
        end_date = str(manifest.get("end_date") or "")
        max_jobs_per_api = int(manifest.get("max_jobs_per_api") or 20)
        ensure_mirror_scope(scope)
        self._warn_if_manifest_paths_differ(manifest, mirror_root, backup_root, warnings)

        catalog = CatalogStore(mirror_root, read_only=True)
        try:
            review = MirrorReviewer().review(root=mirror_root, backup=backup_root, scope=scope, mode="pilot", start_date="20250101", end_date="20250131")
            if review.blocking_errors:
                blocked_by.append("review")
            steps.append(self._step("review", "blocked" if review.blocking_errors else "passed", "read current mirror review", review.summary()))
        except Exception as exc:
            blocked_by.append("review")
            steps.append(self._step("review", "blocked", "read current mirror review", {"error": str(exc)}))

        try:
            readiness = MirrorReadinessReporter().report(root=mirror_root, backup=backup_root, scope=scope)
            if readiness.readiness_status == "blocked":
                blocked_by.append("readiness")
            steps.append(self._step("readiness", readiness.readiness_status, "read current mirror readiness", readiness.summary()))
        except Exception as exc:
            blocked_by.append("readiness")
            steps.append(self._step("readiness", "blocked", "read current mirror readiness", {"error": str(exc)}))

        try:
            plan = MirrorBatchPlanner(mirror_root, catalog).plan(
                scope=scope,
                start_date=start_date,
                end_date=end_date,
                calendar_exchange="SSE",
                max_jobs_per_api=max_jobs_per_api,
            )
            estimated_request_count = plan.estimated_request_count
            steps.append(self._step("batch-plan", "passed", "simulate batch planning only", plan.summary()))
        except Exception as exc:
            blocked_by.append("batch_plan")
            steps.append(self._step("batch-plan", "blocked", "simulate batch planning only", {"error": str(exc)}))

        try:
            checklist = MirrorOperatorChecklistReporter(token_available=True).report(
                root=mirror_root,
                backup=backup_root,
                scope=scope,
                start_date=start_date,
                end_date=end_date,
            )
            if checklist.blocking_errors:
                blocked_by.append("operator_checklist")
            steps.append(self._step("operator-checklist", "blocked" if checklist.blocking_errors else "passed", "read operator checklist", checklist.summary()))
        except Exception as exc:
            blocked_by.append("operator_checklist")
            steps.append(self._step("operator-checklist", "blocked", "read operator checklist", {"error": str(exc)}))

        steps.extend(
            [
                self._step("execute-command-would-run", "requires_user_confirmation", "mirror-run --execute is not run by rehearsal", {"would_execute_real_requests": True}),
                self._step("validate-no-record-would-run", "simulated", "validate --no-record would run after user-confirmed execution", {"would_write_catalog": False}),
                self._step("backup-would-run", "simulated", "backup would run after successful user-confirmed execution", {"would_write_backup": True}),
                self._step("restore-check-would-run", "simulated", "restore-check would run after backup", BackupStatusReporter().report(backup=backup_root).summary()),
                self._step("post-batch-review-would-run", "simulated", "post-batch review would run after backup and restore-check", {"would_write_catalog": False}),
            ]
        )

        status = "blocked" if blocked_by else ("warning" if verification.status == "warning" else "passed")
        return self._result(mirror_root, backup_root, bundle_root, status, steps, estimated_request_count, blocked_by, warnings)

    def _result(
        self,
        root: Path,
        backup: Path,
        bundle: Path,
        status: str,
        steps: list[dict[str, Any]],
        estimated_request_count: int,
        blocked_by: list[str],
        warnings: list[str],
    ) -> MirrorBatchRehearsalResult:
        blocked = _dedupe_messages(blocked_by)
        return MirrorBatchRehearsalResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            backup=str(backup),
            bundle=str(bundle),
            rehearsal_status=status,
            steps=steps,
            would_execute_real_requests=True,
            estimated_request_count=estimated_request_count,
            blocked_by=blocked,
            warnings=_dedupe_messages(warnings),
            user_confirmation_required=True,
            next_safe_action=(
                "resolve blocked rehearsal sections before any execution"
                if blocked
                else "review rehearsal output and obtain explicit user confirmation before mirror-run --execute"
            ),
        )

    def _read_manifest(self, bundle_root: Path, warnings: list[str], blocked_by: list[str]) -> dict[str, Any] | None:
        manifest_path = bundle_root / MirrorBatchBundleReporter.MANIFEST_FILE
        if not manifest_path.exists():
            blocked_by.append("bundle_manifest")
            return None
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            blocked_by.append("bundle_manifest")
            warnings.append(f"bundle manifest cannot be parsed: {exc}")
            return None

    def _warn_if_manifest_paths_differ(self, manifest: dict[str, Any], root: Path, backup: Path, warnings: list[str]) -> None:
        if manifest.get("source_root") and _resolve_path(Path(str(manifest["source_root"]))) != root:
            warnings.append("bundle source_root differs from rehearsal root")
        if manifest.get("backup_root") and _resolve_path(Path(str(manifest["backup_root"]))) != backup:
            warnings.append("bundle backup_root differs from rehearsal backup")

    def _step(self, name: str, status: str, description: str, details: dict[str, Any]) -> dict[str, Any]:
        return {
            "step": name,
            "status": status,
            "description": description,
            "details": details,
        }


class MirrorBatchLedgerReporter:
    REPORT_VERSION = "mirror-batch-ledger/v1"

    def report(self, *, root: Path | str, scope: str, bundle: Path | str | None = None) -> MirrorBatchLedgerResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        warnings = ["mirror-batch-ledger is read-only and infers history; no explicit batch ledger table is written"]
        blocking_errors: list[str] = []
        catalog = CatalogStore(mirror_root, read_only=True)
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
            return self._result(mirror_root, scope, "blocked", [], [], [], None, None, warnings, blocking_errors)

        batches = self._mirror_run_batches(catalog)
        next_batch = MirrorNextBatchReporter().report(root=mirror_root, scope=scope)
        inferred = [
            self._inferred_month_batch(month, mirror_root, scope)
            for month in next_batch.current_completed_months
        ]
        latest_completed = inferred[-1] if inferred else (batches[0] if batches else None)
        recommended = None
        if next_batch.recommended_next_start_date and next_batch.recommended_next_end_date:
            recommended = {
                "start_date": next_batch.recommended_next_start_date,
                "end_date": next_batch.recommended_next_end_date,
                "reason": next_batch.reason,
                "estimated_request_count": next_batch.estimated_request_count,
                "required_trade_cal_range": next_batch.required_trade_cal_range,
            }
        planned = self._planned_batches(bundle=bundle, recommended=recommended, warnings=warnings)
        if not batches and not inferred:
            warnings.append("no completed batch history could be inferred")
        elif not inferred:
            warnings.append("no complete month coverage inferred from daily-like endpoints")
        ledger_status = "blocked" if blocking_errors else ("empty" if not batches and not inferred and not planned else ("warning" if not inferred else "passed"))
        return self._result(mirror_root, scope, ledger_status, batches, inferred, planned, latest_completed, recommended, warnings, blocking_errors)

    def _result(
        self,
        root: Path,
        scope: str,
        status: str,
        batches: list[dict[str, Any]],
        inferred: list[dict[str, Any]],
        planned: list[dict[str, Any]],
        latest_completed: dict[str, Any] | None,
        recommended: dict[str, Any] | None,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> MirrorBatchLedgerResult:
        return MirrorBatchLedgerResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            scope=scope,
            ledger_status=status,
            batches=batches,
            inferred_batches=inferred,
            planned_batches=planned,
            latest_completed_batch=latest_completed,
            next_recommended_batch=recommended,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _mirror_run_batches(self, catalog: CatalogStore) -> list[dict[str, Any]]:
        batches: list[dict[str, Any]] = []
        for run in catalog.list_runs(limit=1000):
            if run.get("run_type") != "mirror":
                continue
            jobs = catalog.jobs_for_run(str(run["run_id"]))
            start_date, end_date = self._date_range_from_jobs(jobs)
            summary = run.get("summary") or {}
            batches.append(
                {
                    "batch_id": run["run_id"],
                    "source": "mirror_run",
                    "status": run.get("status"),
                    "mode": summary.get("mode"),
                    "date_range": {"start_date": start_date, "end_date": end_date},
                    "executed_endpoints": sorted({job["api_name"] for job in jobs if job.get("status") == "done"}),
                    "backup_status": summary.get("backup_status"),
                    "backup_target": summary.get("backup_target"),
                    "restore_check_status": summary.get("restore_check_status"),
                    "validation_status": summary.get("validation_status"),
                    "coverage_status": "inferred_from_run",
                    "started_at": run.get("started_at"),
                    "finished_at": run.get("finished_at"),
                }
            )
        return batches

    def _planned_batches(self, *, bundle: Path | str | None, recommended: dict[str, Any] | None, warnings: list[str]) -> list[dict[str, Any]]:
        candidates: list[Path] = []
        if bundle is not None:
            candidates.append(_resolve_path(Path(bundle)))
        planned: list[dict[str, Any]] = []
        for candidate in candidates:
            item = self._planned_batch_from_bundle(candidate, warnings)
            if item:
                planned.append(item)
        return planned

    def _planned_batch_from_bundle(self, bundle: Path, warnings: list[str]) -> dict[str, Any] | None:
        verification = MirrorBatchBundleVerifier().verify(bundle=bundle)
        manifest_path = bundle / MirrorBatchBundleReporter.MANIFEST_FILE
        if not manifest_path.exists():
            warnings.append(f"planned bundle is not manifest-bearing: {bundle}")
            return None
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            warnings.append(f"planned bundle manifest is invalid: {exc}")
            return None
        batch_plan: dict[str, Any] = {}
        plan_path = bundle / "batch_plan.json"
        if plan_path.exists():
            try:
                batch_plan = json.loads(plan_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                warnings.append(f"planned bundle batch_plan.json is invalid: {exc}")
        dependency_status = batch_plan.get("dependency_status") or "unknown"
        if dependency_status == "missing":
            batch_state = "dependency_stage_planned"
        else:
            batch_state = "planned_future_batch"
        return {
            "batch_id": manifest.get("bundle_id"),
            "source": "bundle_manifest",
            "bundle": str(bundle),
            "batch_state": batch_state,
            "execution_state": "not_executed",
            "scope": manifest.get("scope"),
            "date_range": {"start_date": manifest.get("start_date"), "end_date": manifest.get("end_date")},
            "dependency_status": dependency_status,
            "dependency_action": batch_plan.get("dependency_action"),
            "daily_like_status": batch_plan.get("daily_like_status"),
            "verification_status": verification.status,
        }

    def _date_range_from_jobs(self, jobs: list[dict[str, Any]]) -> tuple[str | None, str | None]:
        dates: list[str] = []
        for job in jobs:
            params = job.get("params") or {}
            for key in ["trade_date", "cal_date", "ann_date", "end_date"]:
                value = params.get(key)
                if isinstance(value, str) and len(value) == 8 and value.isdigit():
                    dates.append(value)
            for key in ["start_date", "end_date"]:
                value = params.get(key)
                if isinstance(value, str) and len(value) == 8 and value.isdigit():
                    dates.append(value)
        if not dates:
            return None, None
        return min(dates), max(dates)

    def _inferred_month_batch(self, month: str, root: Path, scope: str) -> dict[str, Any]:
        start_date, end_date = self._month_range(month)
        coverage = MirrorCoverageMatrixReporter().report(root=root, scope=scope, start_date=start_date, end_date=end_date)
        scoped_daily_like = set(daily_like_apis_for_scope(scope))
        complete = all(item.get("status") == "complete" for item in coverage.items if item.get("api") in scoped_daily_like)
        return {
            "batch_id": f"inferred_{month}",
            "source": "coverage_matrix",
            "month": month,
            "date_range": {"start_date": start_date, "end_date": end_date},
            "coverage_status": "complete" if complete else "partial",
            "coverage_summary": coverage.items,
            "validation_status": "inferred_not_recorded",
            "backup_status": "see_backup_status_report",
        }

    def _month_range(self, month: str) -> tuple[str, str]:
        start = datetime.strptime(month + "01", "%Y%m%d")
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1)
        else:
            next_month = start.replace(month=start.month + 1)
        end = next_month - timedelta(days=1)
        return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


class MirrorBatchCertificateReporter:
    REPORT_VERSION = "mirror-batch-certificate/v1"
    CERTIFICATE_VERSION = "mirror-batch-certificate/v1"
    FILES = ["certificate.json", "certificate.md"]

    def create(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
        output: Path | str,
        overwrite: bool = False,
    ) -> MirrorBatchCertificateResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        output_root = _resolve_path(Path(output))
        warnings = ["mirror-batch-certificate writes only the requested output bundle and does not execute commands"]
        blocking_errors = self._preflight(mirror_root, backup_root, output_root, overwrite)
        if blocking_errors:
            return self._result(output_root, "blocked", [], warnings, blocking_errors)

        try:
            certificate = self._certificate(mirror_root, backup_root, scope, start_date, end_date)
        except Exception as exc:
            return self._result(output_root, "blocked", [], warnings, [f"certificate source report failed: {exc}"])
        completion_blockers = self._completion_blockers(certificate)
        if completion_blockers:
            warnings.append("completion certificates are only generated for completed ranges; use mirror-batch-bundle for planned future batches")
            return self._result(output_root, "blocked", [], warnings, completion_blockers)

        if output_root.exists() and overwrite:
            if output_root.is_dir():
                shutil.rmtree(output_root)
            else:
                output_root.unlink()
        output_root.mkdir(parents=True, exist_ok=False)
        (output_root / "certificate.json").write_text(json.dumps(certificate, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        (output_root / "certificate.md").write_text(self._markdown(certificate), encoding="utf-8")
        return self._result(output_root, "created", self.FILES, warnings, [])

    def _preflight(self, mirror_root: Path, backup_root: Path, output_root: Path, overwrite: bool) -> list[str]:
        blocking_errors: list[str] = []
        catalog_path = mirror_root / "_catalog" / "catalog.sqlite"
        if not catalog_path.exists():
            blocking_errors.append(f"catalog not found: {catalog_path}; run init-catalog first")
        if not backup_root.exists():
            blocking_errors.append(f"backup not found: {backup_root}")
        if output_root == mirror_root or _is_relative_to(output_root, mirror_root):
            blocking_errors.append("output path must not be inside mirror root")
        if output_root == backup_root or _is_relative_to(output_root, backup_root):
            blocking_errors.append("output path must not be inside backup root")
        if output_root.exists() and not overwrite:
            blocking_errors.append("output path already exists; pass --overwrite to replace it")
        return blocking_errors

    def _certificate(self, root: Path, backup: Path, scope: str, start_date: str, end_date: str) -> dict[str, Any]:
        catalog = CatalogStore(root, read_only=True)
        coverage = MirrorCoverageMatrixReporter().report(root=root, scope=scope, start_date=start_date, end_date=end_date)
        snapshots = [
            {
                "api_name": item.get("api_name"),
                "snapshot_id": item.get("snapshot_id"),
                "status": item.get("status"),
                "created_at": item.get("created_at"),
                "file_count": item.get("file_count"),
                "record_count": item.get("record_count"),
            }
            for item in catalog.latest_snapshots()
        ]
        backup_status = BackupStatusReporter().report(backup=backup)
        validation_status = self._validation_status(catalog)
        status = MirrorStatusReporter().report(root=root, backup=backup, scope=scope)
        return {
            "certificate_version": self.CERTIFICATE_VERSION,
            "certificate_type": "completion",
            "completion_status": "completed",
            "root": str(root),
            "backup": str(backup),
            "scope": scope,
            "date_range": {"start_date": start_date, "end_date": end_date},
            "coverage_summary": coverage.items,
            "snapshot_summary": snapshots,
            "validation_status": validation_status,
            "backup_status": "valid" if backup_status.manifest_valid and not backup_status.possible_mutation else "blocked",
            "restore_check_status": backup_status.restore_check_status,
            "token_plaintext_found": status.token_plaintext_found,
            "generated_at": now_utc(),
            "limitations": [
                "certificate is generated from local catalog and backup reports",
                "financial, object/text, intraday, compaction, PostgreSQL, and full mirror automation remain out of scope",
                "certificate does not execute validation, backup, restore, mirror-run, fetch, or backfill commands",
            ],
            "not_a_full_mirror": True,
        }

    def _completion_blockers(self, certificate: dict[str, Any]) -> list[str]:
        blockers: list[str] = []
        daily_like = [item for item in certificate.get("coverage_summary", []) if item.get("coverage_class") == "daily_like"]
        if not daily_like or any(item.get("status") != "complete" for item in daily_like):
            blockers.append("requested date range is not completed; use mirror-batch-bundle for planned future batch")
        if certificate.get("backup_status") != "valid":
            blockers.append("backup status is not valid")
        if certificate.get("restore_check_status") != "succeeded":
            blockers.append("restore-check status is not succeeded")
        return blockers

    def _validation_status(self, catalog: CatalogStore) -> dict[str, Any]:
        with catalog.connect() as conn:
            rows = conn.execute("select status, count(*) as count from validation_runs group by status").fetchall()
        counts = {row["status"]: row["count"] for row in rows}
        return {
            "counts": counts,
            "failed": int(counts.get("failed") or 0),
            "status": "failed" if counts.get("failed") else "succeeded" if counts else "not_recorded",
        }

    def _markdown(self, certificate: dict[str, Any]) -> str:
        date_range = certificate["date_range"]
        return "\n".join(
            [
                "# Tushare Mirror Batch Certificate",
                "",
                f"Scope: {certificate['scope']}",
                f"Date range: {date_range['start_date']}-{date_range['end_date']}",
                f"Restore-check status: {certificate['restore_check_status']}",
                f"Token plaintext found: {certificate['token_plaintext_found']}",
                f"Not a full mirror: {certificate['not_a_full_mirror']}",
                "",
                "Limitations:",
                *[f"- {item}" for item in certificate["limitations"]],
                "",
            ]
        )

    def _result(self, output: Path, status: str, files: list[str], warnings: list[str], blocking_errors: list[str]) -> MirrorBatchCertificateResult:
        return MirrorBatchCertificateResult(
            report_version=self.REPORT_VERSION,
            status=status,
            output=str(output),
            files=files,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )


class MirrorOperatorChecklistReporter:
    REPORT_VERSION = "mirror-operator-checklist/v1"
    MAX_JOBS_PER_API = 20

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
    ) -> MirrorOperatorChecklistResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        warnings: list[str] = ["mirror-operator-checklist is read-only and does not execute generated commands"]
        blocking_errors: list[str] = []

        catalog = CatalogStore(mirror_root, read_only=True)
        paths_valid = catalog.db_path.exists() and backup_root.exists() and backup_root.is_dir()
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
        if not backup_root.exists():
            blocking_errors.append(f"backup not found: {backup_root}")
        elif not backup_root.is_dir():
            blocking_errors.append("backup path exists but is not a directory")

        relationship = MirrorPreflightChecker(token_available=True)._path_relationship(mirror_root, backup_root)
        backup_not_nested = relationship == "ok"
        if not backup_not_nested:
            blocking_errors.append(f"unsafe backup path relationship: {relationship}")

        restore_check_passed = False
        backup_not_mutated = False
        if backup_root.exists() and backup_root.is_dir():
            inspect = BackupInspector().inspect(backup_root)
            restore = RestoreChecker().check(backup_root)
            restore_check_passed = restore.status == "succeeded"
            backup_not_mutated = not bool(inspect.possible_mutation or restore.possible_mutation)
            if not restore_check_passed:
                blocking_errors.append("restore-check failed")
            if not backup_not_mutated:
                blocking_errors.append("backup catalog may have been modified after backup creation")

        readiness_not_blocked = False
        current_validation_status = None
        try:
            readiness = MirrorReadinessReporter().report(root=mirror_root, backup=backup_root, scope=scope)
            readiness_not_blocked = readiness.readiness_status != "blocked"
            current_validation_status = (readiness.review or {}).get("validation_status")
            warnings.extend(readiness.warnings)
            if not readiness_not_blocked:
                blocking_errors.append("mirror-readiness is blocked")
        except Exception as exc:
            blocking_errors.append(f"mirror-readiness failed: {exc}")

        no_schema_quarantine, no_failed_validation = self._catalog_quality_flags(catalog, blocking_errors)
        if current_validation_status is not None:
            no_failed_validation = current_validation_status != "failed"
            if not no_failed_validation:
                blocking_errors.append("current readiness validation failed")
        token_available = self._token_available()
        if not token_available:
            blocking_errors.append("TUSHARE_TOKEN is not available")
        max_jobs_guardrail = {
            "max_jobs_per_api": self.MAX_JOBS_PER_API,
            "max_allowed_for_pilot": MODE_MAX_JOBS["pilot"],
            "passed": self.MAX_JOBS_PER_API <= MODE_MAX_JOBS["pilot"],
        }
        if not max_jobs_guardrail["passed"]:
            blocking_errors.append("max-jobs-per-api guardrail failed")

        batch_plan_available = self._batch_plan_available(mirror_root, catalog, scope, start_date, end_date, blocking_errors)
        disk_warning = self._disk_warning(mirror_root, backup_root, warnings)
        plan_command, execute_command = self._commands(mirror_root, backup_root, scope, start_date, end_date)
        stop_conditions = StopPolicyReporter().report(scope=scope).to_dict()
        ready = not blocking_errors
        return MirrorOperatorChecklistResult(
            report_version=self.REPORT_VERSION,
            root=str(mirror_root),
            backup=str(backup_root),
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            paths_valid=paths_valid,
            backup_not_nested=backup_not_nested,
            restore_check_passed=restore_check_passed,
            backup_not_mutated=backup_not_mutated,
            readiness_not_blocked=readiness_not_blocked,
            no_schema_quarantine=no_schema_quarantine,
            no_failed_validation=no_failed_validation,
            token_available=token_available,
            max_jobs_guardrail=max_jobs_guardrail,
            batch_plan_available=batch_plan_available,
            disk_space_warning=disk_warning,
            stop_conditions=stop_conditions,
            exact_plan_command=plan_command,
            exact_execute_command=execute_command,
            ready=ready,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _token_available(self) -> bool:
        if self._token_available_override is not None:
            return self._token_available_override
        return _token_available_from_env()

    def _catalog_quality_flags(self, catalog: CatalogStore, blocking_errors: list[str]) -> tuple[bool, bool]:
        if not catalog.db_path.exists():
            return False, False
        try:
            with catalog.connect() as conn:
                quarantine_count = int(conn.execute("select count(*) from quarantine_files").fetchone()[0])
        except Exception as exc:
            blocking_errors.append(f"catalog quality check failed: {exc}")
            return False, False
        if quarantine_count:
            blocking_errors.append("schema quarantine is present")
        return quarantine_count == 0, True

    def _batch_plan_available(self, root: Path, catalog: CatalogStore, scope: str, start_date: str, end_date: str, blocking_errors: list[str]) -> bool:
        if not catalog.db_path.exists():
            return False
        try:
            MirrorBatchPlanner(root, catalog).plan(
                scope=scope,
                start_date=start_date,
                end_date=end_date,
                calendar_exchange="SSE",
                max_jobs_per_api=self.MAX_JOBS_PER_API,
            )
            return True
        except Exception as exc:
            blocking_errors.append(f"mirror-batch-plan unavailable: {exc}")
            return False

    def _disk_warning(self, root: Path, backup: Path, warnings: list[str]) -> str | None:
        disk_warnings: list[str] = []
        disk = MirrorPreflightChecker(token_available=True)._disk_space_summary(root, backup, disk_warnings)
        warnings.extend(disk_warnings)
        return disk.get("warning") if isinstance(disk, dict) else None

    def _commands(self, root: Path, backup: Path, scope: str, start_date: str, end_date: str) -> tuple[str, dict[str, str]]:
        plan = (
            f"python3 -m tushare_mirror mirror-batch-plan --root {root} --scope {scope} "
            f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {self.MAX_JOBS_PER_API}"
        )
        execute = (
            f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} --mode pilot "
            f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {self.MAX_JOBS_PER_API} "
            f"--backup-target {backup} --execute"
        )
        return plan, {"confirmation": "USER_CONFIRMATION_REQUIRED", "command": execute}


class StopPolicyReporter:
    REPORT_VERSION = "stop-policy/v1"
    CATEGORIES = {
        "low-risk-a-share",
        "a-share-low-risk",
        "code-loop",
        "financial",
        "object-text",
        "intraday",
        "backup",
        "mirror-orchestrator",
    }

    def report(self, *, scope: str | None = None, category: str | None = None) -> StopPolicyResult:
        if scope and category:
            raise ValueError("use either --scope or --category, not both")
        key = category or scope or "low-risk-a-share"
        if key not in self.CATEGORIES:
            supported = ", ".join(sorted(self.CATEGORIES))
            raise ValueError(f"unknown stop-policy category: {key}; supported: {supported}")
        policy = self._policy(key)
        return StopPolicyResult(report_version=self.REPORT_VERSION, category=key, **policy)

    def _policy(self, key: str) -> dict[str, Any]:
        base = {
            "execution_blocked": key in {"financial", "object-text", "intraday", "code-loop"},
            "stop_immediately": [
                "restore-check fails",
                "backup possible_mutation is true",
                "schema quarantine appears",
                "current readiness validation fails",
                "token plaintext is detected",
            ],
            "continue_with_warning": [
                "readiness is warning but not blocked",
                "request estimate is conservative",
                "batch plan has missing jobs but no blocking errors",
            ],
            "retryable_failures": [
                "network_error",
                "rate_limited",
                "server_error",
            ],
            "non_retryable_failures": [
                "permission_denied",
                "schema_incompatible",
                "unsafe_output_path",
            ],
            "backup_required_conditions": [
                "before any user-confirmed mirror-run --execute",
                "after every completed controlled batch",
            ],
            "user_confirmation_required_conditions": [
                "mirror-run --execute",
                "any command that fetches real Tushare data",
                "any command that writes outside a user-provided output path",
            ],
            "warnings": [
                "stop-policy is descriptive and read-only",
            ],
            "blocking_errors": [],
        }
        if key == "financial":
            base["stop_immediately"] = [
                "financial execution remains blocked",
                "PIT usable_after derivation is not executable",
                *base["stop_immediately"],
            ]
            base["blocking_errors"] = ["financial execution is blocked by current hard boundaries"]
        elif key == "intraday":
            base["stop_immediately"] = [
                "intraday execution remains blocked",
                "bucketed intraday storage is plan-only",
                "compaction execution remains blocked",
                *base["stop_immediately"],
            ]
            base["blocking_errors"] = ["intraday execution is blocked by current hard boundaries"]
        elif key == "object-text":
            base["stop_immediately"] = [
                "object/text execution remains blocked",
                "object downloads are not enabled",
                *base["stop_immediately"],
            ]
            base["blocking_errors"] = ["object/text execution is blocked by current hard boundaries"]
        elif key == "code-loop":
            base["stop_immediately"] = [
                "stock loops must not execute",
                "code/date and code/period planners are plan-only",
                *base["stop_immediately"],
            ]
            base["blocking_errors"] = ["code-loop execution is blocked by current hard boundaries"]
        elif key == "backup":
            base["execution_blocked"] = False
            base["stop_immediately"] = [
                "backup manifest validation fails",
                "restore-check fails",
                "backup catalog checksum mismatches manifest",
                "backup path is nested inside mirror root",
            ]
            base["continue_with_warning"] = [
                "backup manifest has warnings but restore-check succeeds",
                "disk-space estimate is unavailable",
            ]
            base["backup_required_conditions"] = [
                "before any user-confirmed controlled batch",
                "after every completed controlled batch",
                "before replacing any existing backup artifact",
            ]
        elif key == "mirror-orchestrator":
            base["execution_blocked"] = False
            base["stop_immediately"] = [
                "operator checklist is not ready",
                "mirror-readiness is blocked",
                "mirror-batch-plan is unavailable",
                *base["stop_immediately"],
            ]
        return base


class MirrorFailureDrillReporter:
    REPORT_VERSION = "mirror-failure-drill/v1"
    SUPPORTED_SCENARIOS = {
        "rate_limited",
        "permission_denied",
        "invalid_params",
        "schema_incompatible",
        "validation_failed",
        "backup_failed",
        "restore_check_failed",
        "trade_cal_missing",
        "token_missing",
        "disk_space_low",
    }

    def report(self, *, scenario: str, scope: str = "low-risk-a-share") -> MirrorFailureDrillResult:
        ensure_mirror_scope(scope)
        if scenario not in self.SUPPORTED_SCENARIOS:
            supported = ", ".join(sorted(self.SUPPORTED_SCENARIOS))
            raise ValueError(f"unknown failure drill scenario: {scenario}; supported: {supported}")
        drill = self._scenario(scenario)
        return MirrorFailureDrillResult(report_version=self.REPORT_VERSION, scenario=scenario, **drill)

    def _scenario(self, scenario: str) -> dict[str, Any]:
        common_not_to_run = [
            "commands.sh",
            "python3 -m tushare_mirror mirror-run --execute",
            "python3 -m tushare_mirror backfill --execute",
            "python3 -m tushare_mirror backfill-missing --execute",
        ]
        common_inspect = [
            "python3 -m tushare_mirror stop-policy --scope low-risk-a-share --json",
            "python3 -m tushare_mirror mirror-status --root MIRROR_ROOT --backup MIRROR_BACKUP --scope low-risk-a-share --json",
            "python3 -m tushare_mirror mirror-audit --root MIRROR_ROOT --backup MIRROR_BACKUP --scope low-risk-a-share --json",
        ]
        scenarios: dict[str, dict[str, Any]] = {
            "rate_limited": {
                "severity": "warning",
                "stop_condition": True,
                "retry_allowed": True,
                "continue_allowed": False,
                "required_operator_action": "pause execution, preserve logs, wait for the configured retry window, and lower request pace before any user-confirmed retry",
                "commands_to_inspect": [
                    *common_inspect,
                    "python3 -m tushare_mirror rate-policy --scope low-risk-a-share --json",
                    "python3 -m tushare_mirror request-estimate --scope low-risk-a-share --start-date START --end-date END --root MIRROR_ROOT --json",
                ],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "confirm the failure is rate_limited and not permission_denied or invalid_params",
                    "review rate-policy and request-estimate before retry planning",
                    "retry only after user confirmation and only with a bounded batch command",
                ],
                "escalation_notes": ["escalate if rate limits persist after the retry window or affect multiple APIs"],
            },
            "permission_denied": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": False,
                "continue_allowed": False,
                "required_operator_action": "stop the batch and verify token permissions without printing token values",
                "commands_to_inspect": [*common_inspect, "python3 -m tushare_mirror token-hygiene --path MIRROR_ROOT --json"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "verify the token is configured without exposing plaintext",
                    "confirm the requested API is permitted for the token",
                    "regenerate the bounded batch plan after permissions are corrected",
                ],
                "escalation_notes": ["requires operator or account owner action before any retry"],
            },
            "invalid_params": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": False,
                "continue_allowed": False,
                "required_operator_action": "stop and inspect the generated plan parameters before building a corrected bundle",
                "commands_to_inspect": [*common_inspect, "python3 -m tushare_mirror mirror-batch-bundle-verify --bundle BUNDLE --json"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "inspect batch_plan.json and commands.sh without executing them",
                    "correct the plan generator or date range before regenerating the bundle",
                    "rerun bundle verification and rehearsal after regeneration",
                ],
                "escalation_notes": ["treat repeated invalid parameters as a planner defect"],
            },
            "schema_incompatible": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": False,
                "continue_allowed": False,
                "required_operator_action": "stop immediately and quarantine the schema change for review",
                "commands_to_inspect": [*common_inspect, "python3 -m tushare_mirror schema-status --root MIRROR_ROOT --json"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "inspect schema-status and quarantine details",
                    "do not enable affected endpoints until compatibility is reviewed",
                    "update schema handling in a separate infrastructure commit before retry",
                ],
                "escalation_notes": ["requires schema owner review"],
            },
            "validation_failed": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": True,
                "continue_allowed": False,
                "required_operator_action": "stop promotion and inspect validation failures before retrying validation",
                "commands_to_inspect": [*common_inspect, "python3 -m tushare_mirror mirror-coverage-matrix --root MIRROR_ROOT --scope low-risk-a-share --start-date START --end-date END --json"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "identify failed validation IDs and affected snapshots",
                    "rerun only read-only validation/report commands until root cause is understood",
                    "retry execution only after a corrected bundle and user confirmation",
                ],
                "escalation_notes": ["escalate if validation failure indicates data loss or schema mismatch"],
            },
            "backup_failed": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": True,
                "continue_allowed": False,
                "required_operator_action": "stop promotion until a valid backup can be produced and inspected",
                "commands_to_inspect": [*common_inspect, "python3 -m tushare_mirror backup-status --backup MIRROR_BACKUP --json"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "inspect backup-status for manifest or checksum failures",
                    "verify path diagnostics and disk space before creating another backup",
                    "run restore-check before considering the batch complete",
                ],
                "escalation_notes": ["do not proceed to a larger batch without a clean backup and restore-check"],
            },
            "restore_check_failed": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": True,
                "continue_allowed": False,
                "required_operator_action": "treat backup as unusable until restore-check succeeds",
                "commands_to_inspect": [*common_inspect, "python3 -m tushare_mirror backup-status --backup MIRROR_BACKUP --json"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "inspect backup manifest and restore-check errors",
                    "rebuild the backup only after understanding the restore failure",
                    "regenerate completion certificate only after restore-check succeeds",
                ],
                "escalation_notes": ["possible backup corruption or unsafe path configuration"],
            },
            "trade_cal_missing": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": False,
                "continue_allowed": False,
                "required_operator_action": "stop daily-like planning until the local trade_cal coverage is available",
                "commands_to_inspect": [
                    *common_inspect,
                    "python3 -m tushare_mirror mirror-next-batch --root MIRROR_ROOT --scope low-risk-a-share --json",
                    "python3 -m tushare_mirror mirror-coverage-matrix --root MIRROR_ROOT --scope low-risk-a-share --start-date START --end-date END --json",
                ],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "confirm the missing trade_cal range using read-only reports",
                    "do not infer trading days from wall-calendar dates for execution",
                    "prepare a separate user-confirmed plan to refresh trade_cal if needed",
                ],
                "escalation_notes": ["daily-like endpoint coverage cannot be trusted without local SSE trade_cal"],
            },
            "token_missing": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": False,
                "continue_allowed": False,
                "required_operator_action": "configure token availability without printing or storing token plaintext",
                "commands_to_inspect": [*common_inspect, "python3 scripts/tushare_real_smoke.py --help"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "verify environment/configuration only through boolean token availability checks",
                    "scan mirror and backup paths for accidental token plaintext",
                    "rerun operator checklist before any user-confirmed execution",
                ],
                "escalation_notes": ["requires operator secret-management action"],
            },
            "disk_space_low": {
                "severity": "blocking",
                "stop_condition": True,
                "retry_allowed": False,
                "continue_allowed": False,
                "required_operator_action": "stop before execution and free or provision space outside the mirror and backup roots",
                "commands_to_inspect": [*common_inspect, "python3 -m tushare_mirror path-diagnostics --root MIRROR_ROOT --backup MIRROR_BACKUP --json"],
                "commands_not_to_run": common_not_to_run,
                "recovery_steps": [
                    "inspect path diagnostics and parent free bytes",
                    "verify backup is not nested under mirror root",
                    "rerun request-estimate and operator checklist after capacity is corrected",
                ],
                "escalation_notes": ["requires filesystem/operator action before execution"],
            },
        }
        return scenarios[scenario]


class PathDiagnosticsReporter:
    REPORT_VERSION = "path-diagnostics/v1"

    def report(self, *, root: Path | str, backup: Path | str) -> PathDiagnosticsResult:
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        warnings: list[str] = []
        blocking_errors: list[str] = []

        root_exists = mirror_root.exists()
        backup_exists = backup_root.exists()
        if not root_exists:
            blocking_errors.append(f"root path does not exist: {mirror_root}")
        if not backup_exists:
            blocking_errors.append(f"backup path does not exist: {backup_root}")

        root_size, root_file_count, root_warnings = self._tree_stats(mirror_root)
        backup_size, backup_file_count, backup_warnings = self._tree_stats(backup_root)
        warnings.extend(root_warnings)
        warnings.extend(backup_warnings)

        root_free, root_free_warning = _disk_free(mirror_root)
        backup_free, backup_free_warning = _disk_free(backup_root)
        if root_free_warning:
            warnings.append(f"root parent free space unavailable: {root_free_warning}")
        if backup_free_warning:
            warnings.append(f"backup parent free space unavailable: {backup_free_warning}")

        backup_inside_root = backup_root == mirror_root or _is_relative_to(backup_root, mirror_root)
        root_inside_backup = mirror_root == backup_root or _is_relative_to(mirror_root, backup_root)
        if backup_inside_root:
            blocking_errors.append("backup path is inside mirror root")
        if root_inside_backup:
            blocking_errors.append("mirror root is inside backup path")

        same_device, same_device_warning = self._same_device(mirror_root, backup_root)
        if same_device_warning:
            warnings.append(same_device_warning)

        status = "blocked" if blocking_errors else "warning" if warnings else "passed"
        return PathDiagnosticsResult(
            report_version=self.REPORT_VERSION,
            status=status,
            root=str(mirror_root),
            backup=str(backup_root),
            root_exists=root_exists,
            backup_exists=backup_exists,
            root_size=root_size,
            backup_size=backup_size,
            root_file_count=root_file_count,
            backup_file_count=backup_file_count,
            parent_free_bytes={"root_parent": root_free, "backup_parent": backup_free},
            backup_inside_root=backup_inside_root,
            root_inside_backup=root_inside_backup,
            same_device=same_device,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _tree_stats(self, path: Path) -> tuple[int, int, list[str]]:
        if not path.exists():
            return 0, 0, []
        size = 0
        count = 0
        warnings: list[str] = []
        files: Iterable[Path]
        if path.is_file():
            files = [path]
        else:
            files = (item for item in path.rglob("*") if item.is_file())
        for item in files:
            try:
                stat = item.stat()
            except OSError as exc:
                warnings.append(f"could not stat file {item}: {exc}")
                continue
            size += int(stat.st_size)
            count += 1
        return size, count, warnings

    def _same_device(self, root: Path, backup: Path) -> tuple[bool | None, str | None]:
        root_parent = _nearest_existing_parent(root)
        backup_parent = _nearest_existing_parent(backup)
        if root_parent is None or backup_parent is None:
            return None, "same_device unavailable because one or both paths have no existing parent"
        try:
            return root_parent.stat().st_dev == backup_parent.stat().st_dev, None
        except OSError as exc:
            return None, f"same_device unavailable: {exc}"


class TokenHygieneScanner:
    REPORT_VERSION = "token-hygiene/v1"
    TOKEN_PATTERN = re.compile(
        r"(?i)(?:\bTUSHARE_TOKEN\b|\btushare[\s_-]?token\b|\bapi[\s_-]?token\b|\baccess[\s_-]?token\b|\bsecret[\s_-]?token\b|\btoken\b)\s*[:=]\s*['\"]?[A-Za-z0-9][A-Za-z0-9_\-]{10,}"
    )
    TEXT_SUFFIXES = {
        ".cfg",
        ".conf",
        ".csv",
        ".env",
        ".ini",
        ".json",
        ".jsonl",
        ".log",
        ".md",
        ".sh",
        ".sql",
        ".toml",
        ".tsv",
        ".txt",
        ".yaml",
        ".yml",
    }
    TEXT_FILENAMES = {".env", "README", "README.md", "commands.sh", "manifest"}
    SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}
    BINARY_SUFFIXES = {
        ".br",
        ".bz2",
        ".gz",
        ".lz4",
        ".parquet",
        ".pickle",
        ".pkl",
        ".png",
        ".sqlite-journal",
        ".wal",
        ".zip",
        ".zst",
    }

    def scan(self, *, path: Path | str) -> TokenHygieneResult:
        root = _resolve_path(Path(path))
        warnings: list[str] = ["token-hygiene reports counts and paths only; matched token-like values are never printed"]
        blocking_errors: list[str] = []
        if not root.exists():
            blocking_errors.append(f"path does not exist: {root}")
            return self._result(root, "blocked", 0, 0, 0, [], warnings, blocking_errors)

        scanned_file_count = 0
        skipped_file_count = 0
        suspicious_match_count = 0
        suspicious_paths: list[str] = []
        files = [root] if root.is_file() else (item for item in root.rglob("*") if item.is_file())
        for file_path in files:
            if self._is_sqlite_file(file_path):
                scanned_file_count += 1
                matches, scan_warnings = self._scan_sqlite(file_path)
                warnings.extend(scan_warnings)
            elif self._is_text_file(file_path):
                scanned_file_count += 1
                matches, scan_warnings = self._scan_text_file(file_path)
                warnings.extend(scan_warnings)
            else:
                skipped_file_count += 1
                continue
            if matches:
                suspicious_match_count += matches
                suspicious_paths.append(str(file_path))

        if suspicious_match_count:
            blocking_errors.append("token-like plaintext found; paths are reported without matched values")
        status = "blocked" if blocking_errors else "passed"
        return self._result(
            root,
            status,
            scanned_file_count,
            skipped_file_count,
            suspicious_match_count,
            suspicious_paths,
            warnings,
            blocking_errors,
        )

    def _is_text_file(self, path: Path) -> bool:
        if path.name in self.TEXT_FILENAMES:
            return True
        if path.suffix.lower() in self.BINARY_SUFFIXES:
            return False
        if path.suffix.lower() in self.TEXT_SUFFIXES:
            return True
        if path.name.endswith(".manifest") or path.name.endswith(".manifest.json"):
            return True
        return False

    def _is_sqlite_file(self, path: Path) -> bool:
        name = path.name.lower()
        return path.suffix.lower() in self.SQLITE_SUFFIXES or name.endswith(".sqlite")

    def _scan_text_file(self, path: Path) -> tuple[int, list[str]]:
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError as exc:
            return 0, [f"could not scan text file {path}: {exc}"]
        return len(self.TOKEN_PATTERN.findall(content)), []

    def _scan_sqlite(self, path: Path) -> tuple[int, list[str]]:
        warnings: list[str] = []
        matches = 0
        uri = f"file:{path.resolve(strict=False).as_posix()}?mode=ro&immutable=1"
        try:
            conn = sqlite3.connect(uri, uri=True)
        except sqlite3.Error as exc:
            return 0, [f"could not open sqlite file {path}: {exc}"]
        try:
            conn.row_factory = sqlite3.Row
            tables = [
                row["name"]
                for row in conn.execute("select name from sqlite_master where type = 'table' and name not like 'sqlite_%'")
            ]
            for table in tables:
                try:
                    columns = list(conn.execute(f"pragma table_info({self._quote_identifier(table)})"))
                except sqlite3.Error as exc:
                    warnings.append(f"could not inspect sqlite table {table} in {path}: {exc}")
                    continue
                for column in columns:
                    name = str(column["name"])
                    declared_type = str(column["type"] or "").lower()
                    if not self._is_sqlite_text_column(name, declared_type):
                        continue
                    try:
                        rows = conn.execute(f"select {self._quote_identifier(name)} as value from {self._quote_identifier(table)} where {self._quote_identifier(name)} is not null")
                    except sqlite3.Error as exc:
                        warnings.append(f"could not scan sqlite column {table}.{name} in {path}: {exc}")
                        continue
                    for row in rows:
                        value = row["value"]
                        if value is None:
                            continue
                        text = str(value)
                        if self._column_name_suggests_plaintext_token(name) and len(text.strip()) >= 8:
                            matches += 1
                        matches += len(self.TOKEN_PATTERN.findall(text))
        finally:
            conn.close()
        return matches, warnings

    def _is_sqlite_text_column(self, name: str, declared_type: str) -> bool:
        if self._column_name_suggests_plaintext_token(name):
            return True
        if not declared_type:
            return True
        return any(marker in declared_type for marker in ["char", "clob", "json", "text", "varchar"])

    def _column_name_suggests_plaintext_token(self, name: str) -> bool:
        lowered = name.lower()
        return "token" in lowered and "hash" not in lowered and "hashed" not in lowered

    def _quote_identifier(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def _result(
        self,
        path: Path,
        status: str,
        scanned_file_count: int,
        skipped_file_count: int,
        suspicious_match_count: int,
        suspicious_paths: list[str],
        warnings: list[str],
        blocking_errors: list[str],
    ) -> TokenHygieneResult:
        return TokenHygieneResult(
            report_version=self.REPORT_VERSION,
            status=status,
            path=str(path),
            scanned_file_count=scanned_file_count,
            skipped_file_count=skipped_file_count,
            suspicious_match_count=suspicious_match_count,
            suspicious_paths=sorted(set(suspicious_paths)),
            token_plaintext_found=bool(suspicious_match_count),
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )


class MonthlyPromotionChecklistReporter:
    REPORT_VERSION = "monthly-promotion-checklist/v1"

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str,
        from_month: str,
        to_month: str,
        bundle: Path | str | None = None,
    ) -> MonthlyPromotionChecklistResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        from_start, from_end = self._month_range(from_month)
        to_start, to_end = self._month_range(to_month)
        warnings: list[str] = ["monthly-promotion-checklist is read-only and does not execute mirror-run or generated commands"]
        blocking_errors: list[str] = []
        hard_blockers: list[str] = []
        checks: dict[str, Any] = {}

        coverage = MirrorCoverageMatrixReporter().report(root=mirror_root, scope=scope, start_date=from_start, end_date=from_end)
        daily_like_items = [item for item in coverage.items if item.get("coverage_class") == "daily_like"]
        weekly_monthly_items = [item for item in coverage.items if item.get("coverage_class") == "weekly_monthly"]
        daily_like_complete = bool(daily_like_items) and not coverage.blocking_errors and all(item.get("status") == "complete" for item in daily_like_items)
        weekly_monthly_complete = bool(weekly_monthly_items) and all(item.get("status") == "complete" for item in weekly_monthly_items)
        checks["source_month_coverage_complete"] = {
            "passed": daily_like_complete,
            "summary": coverage.items,
            "daily_like_summary": daily_like_items,
            "weekly_monthly_advisory_summary": weekly_monthly_items,
        }
        checks["weekly_monthly_advisory_coverage"] = {
            "passed": weekly_monthly_complete,
            "summary": weekly_monthly_items,
        }
        warnings.extend(coverage.warnings)
        blocking_errors.extend(coverage.blocking_errors)
        hard_blockers.extend(coverage.blocking_errors)
        if not daily_like_complete:
            hard_blockers.append(f"source month {from_month} coverage is incomplete")
        if weekly_monthly_items and not weekly_monthly_complete:
            warnings.append(f"source month {from_month} weekly/monthly advisory coverage is partial")

        backup_status = BackupStatusReporter().report(backup=backup_root)
        backup_valid = bool(backup_status.manifest_valid and backup_status.restore_check_status == "succeeded")
        backup_not_mutated = not backup_status.possible_mutation
        checks["backup_valid"] = {"passed": backup_valid, "report": backup_status.to_dict()}
        checks["backup_not_mutated"] = {"passed": backup_not_mutated}
        warnings.extend(backup_status.warnings)
        if not backup_valid:
            hard_blockers.append("backup is not valid or restore-check did not succeed")
        if not backup_not_mutated:
            hard_blockers.append("backup catalog may have been modified after backup creation")
        blocking_errors.extend(backup_status.blocking_errors)
        hard_blockers.extend(backup_status.blocking_errors)

        schema_status = SchemaStatusReporter().report(root=mirror_root)
        schema_clear = not schema_status.blocking_errors and schema_status.incompatible_schema_count == 0 and schema_status.quarantine_count == 0
        checks["no_schema_quarantine_blockers"] = {"passed": schema_clear, "report": schema_status.to_dict()}
        warnings.extend(schema_status.warnings)
        blocking_errors.extend(schema_status.blocking_errors)
        hard_blockers.extend(schema_status.blocking_errors)
        if not schema_clear:
            hard_blockers.append("schema or quarantine blockers are present")

        plan_available, plan_report, plan_errors = self._next_plan(mirror_root, scope, to_start, to_end)
        checks["next_batch_plan_available"] = {"passed": plan_available, "report": plan_report}
        dependency_stage = self._dependency_stage(plan_report, plan_available)
        dependency_plan_present = bool(
            dependency_stage.get("dependency_status") == "missing"
            and dependency_stage.get("dependency_action") == "fetch_trade_cal_first"
            and dependency_stage.get("daily_like_status") == "blocked_until_trade_cal"
            and dependency_stage.get("natural_day_fallback") is False
        )
        if plan_errors:
            hard_blockers.extend(plan_errors)
        elif dependency_plan_present:
            warnings.append("target trade_cal is missing, but a safe dependency plan is present")

        request_estimate = RequestEstimateReporter().report(root=mirror_root, scope=scope, start_date=to_start, end_date=to_end)
        request_risk_ok = request_estimate.risk_level in {"low", "moderate"}
        checks["request_estimate_low_or_moderate"] = {"passed": request_risk_ok, "report": request_estimate.to_dict()}
        warnings.extend(request_estimate.warnings)
        blocking_errors.extend(request_estimate.blocking_errors)
        hard_blockers.extend(request_estimate.blocking_errors)
        if not request_risk_ok:
            hard_blockers.append(f"request estimate risk is {request_estimate.risk_level}")

        checklist = MirrorOperatorChecklistReporter(token_available=self._token_available_override).report(
            root=mirror_root,
            backup=backup_root,
            scope=scope,
            start_date=to_start,
            end_date=to_end,
        )
        checks["operator_checklist_ready"] = {"passed": checklist.ready, "report": checklist.to_dict()}
        warnings.extend(checklist.warnings)
        blocking_errors.extend(checklist.blocking_errors)
        hard_blockers.extend(checklist.blocking_errors)
        if not checklist.ready:
            hard_blockers.append("operator checklist is not ready")

        if bundle is not None:
            bundle_result = MirrorBatchBundleVerifier().verify(bundle=bundle)
            checks["bundle_verified"] = {"passed": bundle_result.status == "passed", "report": bundle_result.to_dict()}
            warnings.extend(bundle_result.warnings)
            blocking_errors.extend(bundle_result.blocking_errors)
            hard_blockers.extend(bundle_result.blocking_errors)
            if bundle_result.status == "blocked":
                hard_blockers.append("provided bundle verification is blocked")
        else:
            checks["bundle_verified"] = {"passed": None, "report": None}
            warnings.append("no bundle path provided; verify the February bundle before user confirmation")

        checks["explicit_user_confirmation_required"] = {"passed": True, "marker": "USER_CONFIRMATION_REQUIRED"}
        next_commands = self._next_commands(mirror_root, backup_root, scope, to_start, to_end, bundle)
        hard_blockers = _dedupe_messages(hard_blockers)
        blocking_errors = hard_blockers
        warnings = _dedupe_messages(warnings)
        dependency_missing = dependency_stage.get("dependency_status") == "missing"
        ready_for_dependency_stage = not hard_blockers and dependency_missing and dependency_plan_present
        ready_for_batch_after_dependency: bool | str = "pending" if ready_for_dependency_stage else (not hard_blockers and not dependency_missing)
        ready = not hard_blockers and not dependency_missing
        status = "ready" if ready else ("staged" if ready_for_dependency_stage else "blocked")
        return MonthlyPromotionChecklistResult(
            report_version=self.REPORT_VERSION,
            status=status,
            root=str(mirror_root),
            backup=str(backup_root),
            scope=scope,
            from_month=from_month,
            to_month=to_month,
            from_range={"start_date": from_start, "end_date": from_end},
            to_range={"start_date": to_start, "end_date": to_end},
            ready_to_promote=ready,
            hard_blockers=hard_blockers,
            dependency_stage=dependency_stage,
            ready_for_dependency_stage=ready_for_dependency_stage,
            ready_for_batch_after_dependency=ready_for_batch_after_dependency,
            next_safe_action=self._next_safe_action(ready, ready_for_dependency_stage, hard_blockers),
            checks=checks,
            warnings=warnings,
            blocking_errors=blocking_errors,
            required_user_confirmation=True,
            next_commands=next_commands,
        )

    def _month_range(self, month: str) -> tuple[str, str]:
        try:
            start = datetime.strptime(month + "01", "%Y%m%d")
        except ValueError as exc:
            raise ValueError(f"month must be YYYYMM: {month}") from exc
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1)
        else:
            next_month = start.replace(month=start.month + 1)
        end = next_month - timedelta(days=1)
        return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

    def _next_plan(self, root: Path, scope: str, start_date: str, end_date: str) -> tuple[bool, dict[str, Any] | None, list[str]]:
        catalog = CatalogStore(root, read_only=True)
        if not catalog.db_path.exists():
            return False, None, [f"catalog not found: {catalog.db_path}; run init-catalog first"]
        try:
            plan = MirrorBatchPlanner(root, catalog).plan(
                scope=scope,
                start_date=start_date,
                end_date=end_date,
                calendar_exchange="SSE",
                max_jobs_per_api=MirrorNextBatchReporter.RECOMMENDED_MAX_JOBS_PER_API,
            )
        except Exception as exc:
            return False, None, [f"next batch plan failed: {exc}"]
        report = plan.to_dict()
        if plan.blocked_endpoints:
            if (
                plan.dependency_status == "missing"
                and plan.dependency_action == "fetch_trade_cal_first"
                and plan.daily_like_status == "blocked_until_trade_cal"
                and plan.natural_day_fallback is False
            ):
                return True, report, []
            return False, report, [f"next batch plan has {plan.blocked_endpoints} blocked endpoints"]
        if plan.total_planned_jobs <= 0:
            return False, report, ["next batch plan has no planned jobs"]
        return True, report, []

    def _dependency_stage(self, plan_report: dict[str, Any] | None, plan_available: bool) -> dict[str, Any]:
        if not plan_report:
            return {
                "plan_available": plan_available,
                "dependency_status": "unknown",
                "dependency_action": None,
                "trade_cal_params": None,
                "daily_like_status": "unknown",
                "natural_day_fallback": False,
                "ready_for_dependency_stage": False,
            }
        ready = bool(
            plan_available
            and plan_report.get("dependency_status") == "missing"
            and plan_report.get("dependency_action") == "fetch_trade_cal_first"
            and plan_report.get("daily_like_status") == "blocked_until_trade_cal"
            and plan_report.get("natural_day_fallback") is False
        )
        return {
            "plan_available": plan_available,
            "dependency_status": plan_report.get("dependency_status"),
            "dependency_action": plan_report.get("dependency_action"),
            "trade_cal_params": plan_report.get("trade_cal_params"),
            "daily_like_status": plan_report.get("daily_like_status"),
            "natural_day_fallback": plan_report.get("natural_day_fallback"),
            "dependency_requests": plan_report.get("dependency_requests"),
            "currently_unblocked_requests": plan_report.get("currently_unblocked_requests"),
            "executable_after_dependency_requests": plan_report.get("executable_after_dependency_requests"),
            "ready_for_dependency_stage": ready,
        }

    def _next_safe_action(self, ready: bool, ready_for_dependency_stage: bool, hard_blockers: list[str]) -> str:
        if hard_blockers:
            return "Resolve hard blockers before regenerating or confirming any February command."
        if ready_for_dependency_stage:
            return "Regenerate verified bundle; user may confirm the bounded February command that first fetches trade_cal and then proceeds under orchestrator control."
        if ready:
            return "Review verified bundle, rehearsal, and operator checklist; only then request explicit user confirmation for the bounded February mirror-run --execute command."
        return "Rerun monthly-promotion-checklist after dependency state changes."

    def _next_commands(self, root: Path, backup: Path, scope: str, start_date: str, end_date: str, bundle: Path | str | None) -> list[dict[str, str]]:
        output = "/tmp/tushare-mirror-batch-bundle-" + start_date[:6]
        commands = [
            {
                "name": "generate_bundle",
                "command": (
                    f"python3 -m tushare_mirror mirror-batch-bundle --root {root} --backup {backup} --scope {scope} "
                    f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {MirrorNextBatchReporter.RECOMMENDED_MAX_JOBS_PER_API} --output {output} --json"
                ),
            },
        ]
        verify_target = str(bundle) if bundle is not None else output
        commands.extend(
            [
                {"name": "verify_bundle", "command": f"python3 -m tushare_mirror mirror-batch-bundle-verify --bundle {verify_target} --json"},
                {"name": "command_safety_check", "command": f"python3 -m tushare_mirror command-safety-check --file {verify_target}/commands.sh --json"},
                {"name": "rehearse", "command": f"python3 -m tushare_mirror mirror-batch-rehearse --root {root} --backup {backup} --bundle {verify_target} --json"},
                {
                    "name": "operator_checklist",
                    "command": (
                        f"python3 -m tushare_mirror mirror-operator-checklist --root {root} --backup {backup} --scope {scope} "
                        f"--start-date {start_date} --end-date {end_date} --json"
                    ),
                },
                {
                    "name": "user_confirmation_required_execute",
                    "command": (
                        "USER_CONFIRMATION_REQUIRED: "
                        f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} "
                        f"--mode pilot --start-date {start_date} --end-date {end_date} "
                        f"--max-jobs-per-api {MirrorNextBatchReporter.RECOMMENDED_MAX_JOBS_PER_API} --backup-target {backup} --execute"
                    ),
                },
            ]
        )
        return commands


class MirrorOpsReportReporter:
    REPORT_VERSION = "mirror-ops-report/v1"

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
        next_start_date: str,
        next_end_date: str,
        bundle: Path | str | None = None,
    ) -> MirrorOpsReportResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        warnings: list[str] = ["mirror-ops-report is read-only and does not execute generated commands"]
        blocking_errors: list[str] = []
        sections: dict[str, Any] = {}

        self._add_section(sections, warnings, blocking_errors, "mirror_status", MirrorStatusReporter().report(root=mirror_root, backup=backup_root, scope=scope).to_dict())
        self._add_section(sections, warnings, blocking_errors, "mirror_audit", MirrorAuditReporter().report(root=mirror_root, backup=backup_root, scope=scope).to_dict())
        self._add_section(sections, warnings, blocking_errors, "mirror_next_batch", MirrorNextBatchReporter().report(root=mirror_root, scope=scope).to_dict())
        backup_payload = BackupStatusReporter().report(backup=backup_root).to_dict()
        self._add_section(sections, warnings, blocking_errors, "backup_status", backup_payload)
        schema_payload = SchemaStatusReporter().report(root=mirror_root).to_dict()
        self._add_section(sections, warnings, blocking_errors, "schema_status", schema_payload)
        coverage_payload = MirrorCoverageMatrixReporter().report(root=mirror_root, scope=scope, start_date=start_date, end_date=end_date).to_dict()
        self._add_section(sections, warnings, blocking_errors, "coverage_matrix", coverage_payload)
        self._add_section(sections, warnings, blocking_errors, "request_estimate", RequestEstimateReporter().report(root=mirror_root, scope=scope, start_date=next_start_date, end_date=next_end_date).to_dict())
        self._add_section(
            sections,
            warnings,
            blocking_errors,
            "operator_checklist",
            MirrorOperatorChecklistReporter(token_available=self._token_available_override).report(
                root=mirror_root,
                backup=backup_root,
                scope=scope,
                start_date=next_start_date,
                end_date=next_end_date,
            ).to_dict(),
        )
        self._add_section(sections, warnings, blocking_errors, "stop_policy", StopPolicyReporter().report(scope=scope).to_dict())
        self._add_section(sections, warnings, blocking_errors, "path_diagnostics", PathDiagnosticsReporter().report(root=mirror_root, backup=backup_root).to_dict())
        token_root = TokenHygieneScanner().scan(path=mirror_root)
        token_backup = TokenHygieneScanner().scan(path=backup_root)
        token_section = {
            "root": token_root.to_dict(),
            "backup": token_backup.to_dict(),
            "token_plaintext_found": token_root.token_plaintext_found or token_backup.token_plaintext_found,
            "blocking_errors": [*token_root.blocking_errors, *token_backup.blocking_errors],
            "warnings": [*token_root.warnings, *token_backup.warnings],
        }
        self._add_section(sections, warnings, blocking_errors, "token_hygiene", token_section)
        bundle_status = self._bundle_status(next_start_date, bundle)
        sections["bundle_status"] = bundle_status
        promotion_bundle = bundle_status.get("bundle") if bundle_status.get("status") in {"passed", "warning", "blocked"} else None
        promotion = MonthlyPromotionChecklistReporter(token_available=self._token_available_override).report(
            root=mirror_root,
            backup=backup_root,
            scope=scope,
            from_month=start_date[:6],
            to_month=next_start_date[:6],
            bundle=promotion_bundle,
        )
        promotion_payload = promotion.to_dict()
        self._add_section(sections, warnings, blocking_errors, "promotion_checklist", promotion_payload)

        warnings = _dedupe_messages(warnings)
        hard_blockers = _dedupe_messages([*blocking_errors, *promotion.hard_blockers])
        blocking_errors = hard_blockers
        dependency_stage = promotion.dependency_stage
        ready_for_next = bool(promotion.ready_to_promote or promotion.ready_for_dependency_stage) and not hard_blockers
        overall_status = "blocked" if hard_blockers else ("staged" if promotion.ready_for_dependency_stage else "ready" if promotion.ready_to_promote else "warning")
        daily_like_coverage = [item for item in coverage_payload.get("items", []) if item.get("coverage_class") == "daily_like"]
        weekly_monthly_coverage = [item for item in coverage_payload.get("items", []) if item.get("coverage_class") == "weekly_monthly"]
        next_safe_action = promotion.next_safe_action if promotion.next_safe_action else self._recommended_next_action(ready_for_next)
        return MirrorOpsReportResult(
            report_version=self.REPORT_VERSION,
            overall_status=overall_status,
            ready_for_next_user_confirmed_batch=ready_for_next,
            hard_blockers=hard_blockers,
            dependency_stage=dependency_stage,
            bundle_status=bundle_status,
            promotion_status=promotion.status,
            daily_like_coverage=daily_like_coverage,
            weekly_monthly_advisory_coverage=weekly_monthly_coverage,
            backup_status=backup_payload,
            token_hygiene=token_section,
            schema_status=schema_payload,
            next_safe_action=next_safe_action,
            root=str(mirror_root),
            backup=str(backup_root),
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            next_start_date=next_start_date,
            next_end_date=next_end_date,
            sections=sections,
            warnings=warnings,
            blocking_errors=blocking_errors,
            recommended_next_action=next_safe_action,
        )

    def _add_section(
        self,
        sections: dict[str, Any],
        warnings: list[str],
        blocking_errors: list[str],
        name: str,
        payload: dict[str, Any],
    ) -> None:
        sections[name] = payload
        for warning in payload.get("warnings") or []:
            warnings.append(f"{name}: {warning}")
        for error in payload.get("blocking_errors") or []:
            blocking_errors.append(f"{name}: {error}")

    def _bundle_status(self, next_start_date: str, bundle: Path | str | None = None) -> dict[str, Any]:
        bundle_path = _resolve_path(Path(bundle)) if bundle is not None else Path("/tmp") / f"tushare-mirror-batch-bundle-{next_start_date[:6]}"
        if not bundle_path.exists():
            return {
                "status": "not_provided",
                "bundle": str(bundle_path),
                "verified": False,
                "report": None,
            }
        report = MirrorBatchBundleVerifier().verify(bundle=bundle_path).to_dict()
        return {
            "status": report.get("status"),
            "bundle": str(bundle_path),
            "verified": report.get("status") == "passed",
            "report": report,
        }

    def _recommended_next_action(self, ready: bool) -> str:
        if not ready:
            return "resolve blocking errors, regenerate or verify the February bundle, and rerun mirror-ops-report before seeking user confirmation"
        return "review bundle verification, command safety, rehearsal, and operator checklist; only then request explicit user confirmation for mirror-run --execute"


class FinalGateReporter:
    REPORT_VERSION = "mirror-final-gate/v1"
    MAX_JOBS_PER_API = 20

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        bundle: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
    ) -> FinalGateResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        bundle_root = _resolve_path(Path(bundle))
        warnings: list[str] = ["mirror-final-gate is read-only and does not execute mirror-run, commands.sh, fetch, or backfill"]
        blocking_errors: list[str] = []
        checks: list[FinalGateCheck] = []
        sections: dict[str, Any] = {}

        from_start, from_end = self._previous_month_range(start_date)
        bundle_verify = MirrorBatchBundleVerifier().verify(bundle=bundle_root)
        sections["bundle_verification"] = bundle_verify.to_dict()
        self._add_check(
            checks,
            "bundle_exists",
            bundle_root.exists() and bundle_root.is_dir(),
            {"bundle": str(bundle_root)},
            blocking_errors,
            "bundle not found",
        )
        self._add_check(
            checks,
            "bundle_verified",
            bundle_verify.status == "passed",
            bundle_verify.to_dict(),
            blocking_errors,
            "bundle verification is blocked",
        )
        warnings.extend(bundle_verify.warnings)
        blocking_errors.extend(bundle_verify.blocking_errors)

        command_safety = CommandSafetyAnalyzer().analyze(file=bundle_root / "commands.sh")
        sections["command_safety"] = command_safety.to_dict()
        command_safety_ok = command_safety.status in {"passed", "warning"} and not command_safety.blocking_errors
        self._add_check(
            checks,
            "command_safety_warning_only",
            command_safety_ok,
            command_safety.to_dict(),
            blocking_errors,
            "command safety is blocked",
            status="warning" if command_safety.status == "warning" and command_safety_ok else None,
        )
        warnings.extend(command_safety.warnings)
        blocking_errors.extend(command_safety.blocking_errors)

        rehearsal = MirrorBatchRehearsalReporter().rehearse(root=mirror_root, backup=backup_root, bundle=bundle_root)
        sections["rehearsal"] = rehearsal.to_dict()
        self._add_check(
            checks,
            "rehearsal_passed",
            rehearsal.rehearsal_status == "passed",
            rehearsal.to_dict(),
            blocking_errors,
            "rehearsal is blocked",
        )
        warnings.extend(rehearsal.warnings)
        blocking_errors.extend(rehearsal.blocked_by)

        promotion = MonthlyPromotionChecklistReporter(token_available=self._token_available_override).report(
            root=mirror_root,
            backup=backup_root,
            scope=scope,
            from_month=from_start[:6],
            to_month=start_date[:6],
            bundle=bundle_root,
        )
        sections["promotion_checklist"] = promotion.to_dict()
        self._add_check(
            checks,
            "promotion_no_hard_blockers",
            not promotion.hard_blockers,
            promotion.to_dict(),
            blocking_errors,
            "promotion checklist has hard blockers",
            status="warning" if promotion.ready_for_dependency_stage else None,
        )
        warnings.extend(promotion.warnings)
        blocking_errors.extend(promotion.hard_blockers)

        ops = MirrorOpsReportReporter(token_available=self._token_available_override).report(
            root=mirror_root,
            backup=backup_root,
            scope=scope,
            start_date=from_start,
            end_date=from_end,
            next_start_date=start_date,
            next_end_date=end_date,
            bundle=bundle_root,
        )
        sections["ops_report"] = ops.to_dict()
        self._add_check(
            checks,
            "ops_report_no_hard_blockers",
            not ops.hard_blockers,
            ops.to_dict(),
            blocking_errors,
            "ops report has hard blockers",
            status="warning" if ops.overall_status == "staged" else None,
        )
        warnings.extend(ops.warnings)
        blocking_errors.extend(ops.hard_blockers)

        backup_status = BackupStatusReporter().report(backup=backup_root)
        sections["backup_status"] = backup_status.to_dict()
        self._add_check(checks, "backup_restore_check_passed", backup_status.restore_check_status == "succeeded", backup_status.to_dict(), blocking_errors, "backup restore-check did not pass")
        self._add_check(checks, "backup_not_mutated", not backup_status.possible_mutation, backup_status.to_dict(), blocking_errors, "backup possible_mutation is true")
        warnings.extend(backup_status.warnings)
        blocking_errors.extend(backup_status.blocking_errors)

        schema_status = SchemaStatusReporter().report(root=mirror_root)
        sections["schema_status"] = schema_status.to_dict()
        schema_clear = not schema_status.blocking_errors and schema_status.incompatible_schema_count == 0 and schema_status.quarantine_count == 0
        self._add_check(checks, "schema_status_clear", schema_clear, schema_status.to_dict(), blocking_errors, "schema or quarantine blockers are present")
        warnings.extend(schema_status.warnings)
        blocking_errors.extend(schema_status.blocking_errors)

        path_diagnostics = PathDiagnosticsReporter().report(root=mirror_root, backup=backup_root)
        sections["path_diagnostics"] = path_diagnostics.to_dict()
        self._add_check(checks, "paths_safe", not path_diagnostics.blocking_errors, path_diagnostics.to_dict(), blocking_errors, "path diagnostics are blocked")
        warnings.extend(path_diagnostics.warnings)
        blocking_errors.extend(path_diagnostics.blocking_errors)

        token_root = TokenHygieneScanner().scan(path=mirror_root)
        token_backup = TokenHygieneScanner().scan(path=backup_root)
        token_hygiene = {
            "root": token_root.to_dict(),
            "backup": token_backup.to_dict(),
            "token_plaintext_found": token_root.token_plaintext_found or token_backup.token_plaintext_found,
            "blocking_errors": [*token_root.blocking_errors, *token_backup.blocking_errors],
            "warnings": [*token_root.warnings, *token_backup.warnings],
        }
        sections["token_hygiene"] = token_hygiene
        self._add_check(checks, "token_hygiene_clear", not token_hygiene["token_plaintext_found"] and not token_hygiene["blocking_errors"], token_hygiene, blocking_errors, "token plaintext found")
        warnings.extend(token_hygiene["warnings"])
        blocking_errors.extend(token_hygiene["blocking_errors"])

        operator = MirrorOperatorChecklistReporter(token_available=self._token_available_override).report(
            root=mirror_root,
            backup=backup_root,
            scope=scope,
            start_date=start_date,
            end_date=end_date,
        )
        sections["operator_checklist"] = operator.to_dict()
        self._add_check(checks, "token_available", operator.token_available, {"token_available": operator.token_available}, blocking_errors, "TUSHARE_TOKEN is not available")
        warnings.extend(operator.warnings)
        blocking_errors.extend(operator.blocking_errors)

        stop_policy = StopPolicyReporter().report(scope=scope)
        sections["stop_policy"] = stop_policy.to_dict()
        self._add_check(checks, "stop_policy_allows_low_risk", not stop_policy.execution_blocked, stop_policy.to_dict(), blocking_errors, "stop policy blocks this category")
        warnings.extend(stop_policy.warnings)
        blocking_errors.extend(stop_policy.blocking_errors)

        request_bounds_ok = self._valid_bounded_range(start_date, end_date)
        self._add_check(checks, "requested_date_range_bounded", request_bounds_ok, {"start_date": start_date, "end_date": end_date}, blocking_errors, "requested date range is not a valid bounded range")
        max_jobs_ok = max_jobs_per_api <= self.MAX_JOBS_PER_API
        self._add_check(checks, "max_jobs_per_api_guardrail", max_jobs_ok, {"max_jobs_per_api": max_jobs_per_api, "max_allowed": self.MAX_JOBS_PER_API}, blocking_errors, "max_jobs_per_api exceeds 20")
        scope_ok = scope == "low-risk-a-share"
        self._add_check(checks, "scope_low_risk_a_share", scope_ok, {"scope": scope}, blocking_errors, "scope is not low-risk-a-share")
        self._add_check(checks, "no_full_mirror", True, {"mode": "pilot", "full_mirror": False}, blocking_errors, "full mirror execution is not allowed")
        self._add_check(checks, "no_stock_loop", True, {"stock_loop": False}, blocking_errors, "stock loops are not allowed")
        prohibited_clear = self._prohibited_categories_clear(sections.get("promotion_checklist") or {})
        self._add_check(checks, "no_prohibited_api_category", prohibited_clear, {"prohibited_categories": ["financial", "pit", "object", "intraday", "compaction"]}, blocking_errors, "prohibited API category is present")
        self._add_check(checks, "no_commands_executed_by_gate", True, {"final_gate_read_only": True}, blocking_errors, "final gate executed commands")

        dependency_stage = dict(promotion.dependency_stage or {})
        ready_for_dependency_stage = bool(promotion.ready_for_dependency_stage and not blocking_errors)
        ready_for_full_batch_after_dependency = promotion.ready_for_batch_after_dependency
        ready_for_user_confirmed_execute = bool((promotion.ready_to_promote or promotion.ready_for_dependency_stage) and not blocking_errors)
        if blocking_errors:
            gate_status = "blocked"
        elif ready_for_dependency_stage:
            gate_status = "warning"
            warnings.append("February is staged: user-confirmed execution may first fetch trade_cal before daily-like endpoints proceed")
        elif warnings:
            gate_status = "warning"
        else:
            gate_status = "passed"

        command_preview = self._command_preview(mirror_root, backup_root, scope, start_date, end_date, max_jobs_per_api)
        return FinalGateResult(
            report_version=self.REPORT_VERSION,
            gate_status=gate_status,
            ready_for_user_confirmed_execute=ready_for_user_confirmed_execute,
            ready_for_dependency_stage=ready_for_dependency_stage,
            ready_for_full_batch_after_dependency=ready_for_full_batch_after_dependency,
            root=str(mirror_root),
            backup=str(backup_root),
            bundle=str(bundle_root),
            scope=scope,
            requested_range={"start_date": start_date, "end_date": end_date},
            max_jobs_per_api=max_jobs_per_api,
            estimated_request_count=int((sections.get("promotion_checklist") or {}).get("checks", {}).get("request_estimate_low_or_moderate", {}).get("report", {}).get("estimated_total_requests") or rehearsal.estimated_request_count or 0),
            dependency_stage=dependency_stage,
            checks=[check.to_dict() for check in checks],
            sections=sections,
            command_preview=command_preview.to_dict(),
            final_command_preview=command_preview.to_dict(),
            confirmation_phrase=confirmation_phrase(scope, start_date, end_date, max_jobs_per_api),
            blocking_errors=_dedupe_messages(blocking_errors),
            warnings=_dedupe_messages(warnings),
            safety_boundaries=self._safety_boundaries(),
            do_not_run_automatically=True,
        )

    def _add_check(
        self,
        checks: list[FinalGateCheck],
        name: str,
        passed: bool,
        details: dict[str, Any],
        blocking_errors: list[str],
        blocking_message: str,
        *,
        status: str | None = None,
    ) -> None:
        check_status = status or ("passed" if passed else "blocked")
        checks.append(FinalGateCheck(name=name, status=check_status, passed=passed, details=details))
        if not passed:
            blocking_errors.append(blocking_message)

    def _previous_month_range(self, start_date: str) -> tuple[str, str]:
        start = datetime.strptime(start_date, "%Y%m%d")
        first = start.replace(day=1)
        previous_end = first - timedelta(days=1)
        previous_start = previous_end.replace(day=1)
        return previous_start.strftime("%Y%m%d"), previous_end.strftime("%Y%m%d")

    def _valid_bounded_range(self, start_date: str, end_date: str) -> bool:
        try:
            start = datetime.strptime(start_date, "%Y%m%d")
            end = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            return False
        return start <= end and (end - start).days <= 31

    def _prohibited_categories_clear(self, promotion: dict[str, Any]) -> bool:
        plan = ((promotion.get("checks") or {}).get("next_batch_plan_available") or {}).get("report") or {}
        prohibited = {"financial", "pit", "object", "object-text", "intraday", "compaction"}
        for endpoint_plan in plan.get("endpoint_plans") or []:
            if str(endpoint_plan.get("category") or "").lower() in prohibited:
                return False
        return True

    def _command_preview(self, root: Path, backup: Path, scope: str, start_date: str, end_date: str, max_jobs_per_api: int) -> FinalGateCommandPreview:
        command = (
            f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} --mode pilot "
            f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {max_jobs_per_api} "
            f"--backup-target {backup} --execute"
        )
        return FinalGateCommandPreview(
            command=command,
            confirmation="USER_CONFIRMATION_REQUIRED",
            confirmation_phrase=confirmation_phrase(scope, start_date, end_date, max_jobs_per_api),
            would_execute_real_requests=True,
            requires_user_confirmation=True,
            do_not_run_automatically=True,
        )

    def _safety_boundaries(self) -> list[str]:
        return [
            "mirror-final-gate is read-only",
            "does not execute mirror-run",
            "does not run commands.sh",
            "does not fetch real Tushare data",
            "does not backfill dates",
            "does not write catalog or validation_runs",
            "does not write backup data",
            "do_not_run_automatically=true",
        ]


def confirmation_phrase(scope: str, start_date: str, end_date: str, max_jobs_per_api: int) -> str:
    return f"CONFIRM {scope.upper()} {start_date}-{end_date} MAXJOBS{max_jobs_per_api}"


class ExecuteScriptReporter:
    REPORT_VERSION = "mirror-execute-script/v1"

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def create(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        bundle: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
        output: Path | str,
        overwrite: bool = False,
    ) -> ExecuteScriptResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        bundle_root = _resolve_path(Path(bundle))
        output_path = _resolve_path(Path(output))
        warnings = ["mirror-execute-script writes only the requested output file and does not execute it"]
        blocking_errors = self._preflight(mirror_root, backup_root, output_path, overwrite)
        phrase = confirmation_phrase(scope, start_date, end_date, max_jobs_per_api)
        if blocking_errors:
            return self._result(mirror_root, backup_root, bundle_root, output_path, scope, start_date, end_date, max_jobs_per_api, overwrite, phrase, None, warnings, blocking_errors)

        final_gate = FinalGateReporter(token_available=self._token_available_override).report(
            root=mirror_root,
            backup=backup_root,
            bundle=bundle_root,
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs_per_api,
        )
        if final_gate.gate_status == "blocked":
            blocking_errors.extend(final_gate.blocking_errors)
            blocking_errors.append("final gate is blocked; regenerate or verify inputs before creating an execute script")
            return self._result(mirror_root, backup_root, bundle_root, output_path, scope, start_date, end_date, max_jobs_per_api, overwrite, phrase, None, warnings, blocking_errors)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists() and overwrite:
            output_path.unlink()
        output_path.write_text(
            self._script(mirror_root, backup_root, bundle_root, scope, start_date, end_date, max_jobs_per_api, phrase),
            encoding="utf-8",
        )
        output_path.chmod(0o644)
        command_safety = CommandSafetyAnalyzer().analyze(file=output_path)
        warnings.extend(command_safety.warnings)
        blocking_errors.extend(command_safety.blocking_errors)
        if command_safety.status == "blocked":
            blocking_errors.append("generated execute script failed command safety check")
        return self._result(
            mirror_root,
            backup_root,
            bundle_root,
            output_path,
            scope,
            start_date,
            end_date,
            max_jobs_per_api,
            overwrite,
            phrase,
            command_safety.status,
            warnings,
            blocking_errors,
            status="created" if not blocking_errors else "blocked",
        )

    def _preflight(self, mirror_root: Path, backup_root: Path, output_path: Path, overwrite: bool) -> list[str]:
        blocking_errors: list[str] = []
        if output_path == mirror_root or _is_relative_to(output_path, mirror_root):
            blocking_errors.append("output path must not be inside mirror root")
        if output_path == backup_root or _is_relative_to(output_path, backup_root):
            blocking_errors.append("output path must not be inside backup root")
        if output_path.exists() and output_path.is_dir():
            blocking_errors.append("output path exists as a directory")
        elif output_path.exists() and not overwrite:
            blocking_errors.append("output path already exists; pass --overwrite to replace it")
        return blocking_errors

    def _result(
        self,
        root: Path,
        backup: Path,
        bundle: Path,
        output: Path,
        scope: str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
        overwritten: bool,
        phrase: str,
        command_safety_status: str | None,
        warnings: list[str],
        blocking_errors: list[str],
        *,
        status: str | None = None,
    ) -> ExecuteScriptResult:
        return ExecuteScriptResult(
            report_version=self.REPORT_VERSION,
            status=status or ("blocked" if blocking_errors else "created"),
            output=str(output),
            root=str(root),
            backup=str(backup),
            bundle=str(bundle),
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs_per_api,
            overwritten=overwritten,
            confirmation_phrase=phrase,
            command_safety_status=command_safety_status,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _script(self, root: Path, backup: Path, bundle: Path, scope: str, start_date: str, end_date: str, max_jobs_per_api: int, phrase: str) -> str:
        final_gate = (
            f"python3 -m tushare_mirror mirror-final-gate --root {root} --backup {backup} --bundle {bundle} "
            f"--scope {scope} --start-date {start_date} --end-date {end_date} --max-jobs-per-api {max_jobs_per_api} --json"
        )
        execute = (
            f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} --mode pilot "
            f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {max_jobs_per_api} "
            f"--backup-target {backup} --execute --json"
        )
        validate = f"python3 -m tushare_mirror --root {root} validate --latest-all --no-record --json"
        backup_inspect = f"python3 -m tushare_mirror backup-inspect --backup {backup} --json"
        restore_check = f"python3 -m tushare_mirror restore-check --backup {backup} --json"
        review = (
            f"python3 -m tushare_mirror mirror-review --root {root} --backup {backup} --scope {scope} "
            f"--start-date {start_date} --end-date {end_date} --json"
        )
        next_batch = f"python3 -m tushare_mirror mirror-next-batch --root {root} --scope {scope} --json"
        return "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'echo "This is a guarded preview script. It has not executed the February batch."',
                "",
                "# USER_CONFIRMATION_REQUIRED",
                f"# Confirmation phrase: {phrase}",
                "# The phrase is operator friction, not a secret or security token.",
                "# Do not run this file directly until a human has reviewed the final gate output.",
                "# Commands are commented. A human must explicitly edit/uncomment the intended command.",
                "",
                "# Pre-run final gate",
                f"# {final_gate}",
                "",
                "# User-confirmed February execution command",
                f"# {execute}",
                "",
                "# Post-run read-only validation and review commands",
                f"# {validate}",
                f"# {backup_inspect}",
                f"# {restore_check}",
                f"# {review}",
                f"# {next_batch}",
                "",
                "# Safety boundaries:",
                "# - do not run commands.sh from the bundle",
                "# - do not execute mirror-run without explicit user confirmation",
                "# - do not paste or print TUSHARE_TOKEN",
                "",
            ]
        )


class ExecuteReadinessReporter:
    REPORT_VERSION = "mirror-execute-readiness/v1"

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def report(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        bundle: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
    ) -> ExecuteReadinessResult:
        final_gate = FinalGateReporter(token_available=self._token_available_override).report(
            root=root,
            backup=backup,
            bundle=bundle,
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs_per_api,
        )
        payload = final_gate.to_dict()
        sections = payload.get("sections") or {}
        bundle_status = ((sections.get("bundle_verification") or {}).get("status"))
        command_safety_status = ((sections.get("command_safety") or {}).get("status"))
        rehearsal_status = ((sections.get("rehearsal") or {}).get("rehearsal_status"))
        promotion_status = ((sections.get("promotion_checklist") or {}).get("status"))
        backup_status = ((sections.get("backup_status") or {}).get("restore_check_status"))
        token_hygiene = sections.get("token_hygiene") or {}
        token_hygiene_status = "blocked" if token_hygiene.get("token_plaintext_found") or token_hygiene.get("blocking_errors") else "passed"
        status = "blocked" if final_gate.gate_status == "blocked" else ("warning" if final_gate.gate_status == "warning" else "passed")
        command = (payload.get("final_command_preview") or {}).get("command") or ""
        return ExecuteReadinessResult(
            report_version=self.REPORT_VERSION,
            execute_readiness_status=status,
            may_execute_after_user_confirmation=bool(final_gate.ready_for_user_confirmed_execute and not final_gate.blocking_errors),
            must_not_execute_automatically=True,
            final_gate_status=final_gate.gate_status,
            bundle_status=bundle_status,
            command_safety_status=command_safety_status,
            rehearsal_status=rehearsal_status,
            promotion_status=promotion_status,
            backup_status=backup_status,
            token_hygiene_status=token_hygiene_status,
            estimated_request_count=final_gate.estimated_request_count,
            confirmation_phrase=final_gate.confirmation_phrase,
            exact_user_confirmed_command=command,
            final_gate=payload,
            warnings=final_gate.warnings,
            blocking_errors=final_gate.blocking_errors,
        )


class MirrorPullCommandReporter:
    REPORT_VERSION = "mirror-pull-command/v1"

    def create(
        self,
        *,
        scope: str,
        root: Path | str,
        backup: Path | str,
        start_date: str,
        end_date: str,
        max_jobs_per_api: int,
        output: Path | str | None = None,
        overwrite: bool = False,
    ) -> MirrorPullCommandResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        warnings = ["mirror-pull-command is read-only unless --output is provided; it does not execute generated commands"]
        blocking_errors: list[str] = []
        commands = self._commands(mirror_root, backup_root, scope, start_date, end_date, max_jobs_per_api)
        estimate = RequestEstimateReporter().report(root=mirror_root, scope=scope, start_date=start_date, end_date=end_date).to_dict()
        stop_policy = StopPolicyReporter().report(scope=scope).to_dict()
        output_path: Path | None = None
        files: list[str] = []
        if output is not None:
            output_path = _resolve_path(Path(output))
            blocking_errors.extend(self._preflight_output(mirror_root, backup_root, output_path, overwrite))
            if not blocking_errors:
                if output_path.exists() and overwrite:
                    shutil.rmtree(output_path)
                output_path.mkdir(parents=True, exist_ok=False)
                plan = {
                    "report_version": self.REPORT_VERSION,
                    "scope": scope,
                    "root": str(mirror_root),
                    "backup": str(backup_root),
                    "date_range": {"start_date": start_date, "end_date": end_date},
                    "max_jobs_per_api": max_jobs_per_api,
                    "commands": commands,
                    "estimated_requests": estimate,
                    "stop_conditions": stop_policy,
                    "user_confirmation_required": True,
                    "real_requests_sent": False,
                }
                (output_path / "README.md").write_text(self._readme(scope, start_date, end_date), encoding="utf-8")
                (output_path / "commands.sh").write_text(self._commands_sh(commands), encoding="utf-8")
                (output_path / "commands.sh").chmod(0o644)
                (output_path / "plan.json").write_text(json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
                files = ["README.md", "commands.sh", "plan.json"]
                safety = CommandSafetyAnalyzer().analyze(file=output_path / "commands.sh")
                warnings.extend(safety.warnings)
                blocking_errors.extend(safety.blocking_errors)
                if safety.status == "blocked":
                    blocking_errors.append("generated commands.sh failed command safety check")
        return MirrorPullCommandResult(
            report_version=self.REPORT_VERSION,
            status="blocked" if blocking_errors else ("created" if output_path else "planned"),
            scope=scope,
            root=str(mirror_root),
            backup=str(backup_root),
            date_range={"start_date": start_date, "end_date": end_date},
            max_jobs_per_api=max_jobs_per_api,
            commands=commands,
            user_confirmation_required=True,
            estimated_requests=estimate,
            warnings=_dedupe_messages(warnings),
            stop_conditions=stop_policy,
            output=str(output_path) if output_path else None,
            files=files,
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _preflight_output(self, root: Path, backup: Path, output: Path, overwrite: bool) -> list[str]:
        blocking_errors: list[str] = []
        if output == root or _is_relative_to(output, root):
            blocking_errors.append("output path must not be inside mirror root")
        if output == backup or _is_relative_to(output, backup):
            blocking_errors.append("output path must not be inside backup root")
        if output.exists() and not output.is_dir():
            blocking_errors.append("output path exists and is not a directory")
        elif output.exists() and not overwrite:
            blocking_errors.append("output path already exists; pass --overwrite to replace it")
        return blocking_errors

    def _commands(self, root: Path, backup: Path, scope: str, start_date: str, end_date: str, max_jobs: int) -> list[dict[str, Any]]:
        rows = [
            ("mirror-review-before", f"python3 -m tushare_mirror mirror-review --root {root} --backup {backup} --scope {scope} --start-date {start_date} --end-date {end_date} --json", False),
            ("mirror-readiness", f"python3 -m tushare_mirror mirror-readiness --root {root} --backup {backup} --scope {scope} --json", False),
            ("mirror-batch-plan", f"python3 -m tushare_mirror mirror-batch-plan --root {root} --scope {scope} --start-date {start_date} --end-date {end_date} --calendar-exchange SSE --max-jobs-per-api {max_jobs} --json", False),
            ("mirror-run-execute", f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} --mode pilot --start-date {start_date} --end-date {end_date} --max-jobs-per-api {max_jobs} --backup-target {backup} --execute --json", True),
            ("validate-no-record", f"python3 -m tushare_mirror --root {root} validate --latest-all --no-record --json", False),
            ("backup-inspect", f"python3 -m tushare_mirror backup-inspect --backup {backup} --json", False),
            ("restore-check", f"python3 -m tushare_mirror restore-check --backup {backup} --json", False),
            ("mirror-review-after", f"python3 -m tushare_mirror mirror-review --root {root} --backup {backup} --scope {scope} --start-date {start_date} --end-date {end_date} --json", False),
        ]
        return [
            {
                "command_name": name,
                "command_text": text,
                "would_execute_real_requests": would_execute,
                "user_confirmation_required": would_execute,
                "guarded": would_execute,
            }
            for name, text, would_execute in rows
        ]

    def _commands_sh(self, commands: list[dict[str, Any]]) -> str:
        lines = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "",
            "# Generated by mirror-pull-command.",
            "# This file is a guarded plan artifact. It has not been executed.",
            "# Do not run automatically. Review every command first.",
            "",
        ]
        for command in commands:
            lines.append(f"# {command['command_name']}")
            if command["would_execute_real_requests"]:
                lines.append("# USER_CONFIRMATION_REQUIRED: uncomment manually only after reviewing readiness and stop conditions.")
            lines.append(f"# {command['command_text']}")
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def _readme(self, scope: str, start_date: str, end_date: str) -> str:
        return "\n".join(
            [
                "# A-share Low-risk Pull Command Bundle",
                "",
                f"Scope: {scope}",
                f"Date range: {start_date}-{end_date}",
                "",
                "This bundle is a command preview only. It does not execute mirror-run, fetch real Tushare data, backfill dates, or write mirror catalog state.",
                "commands.sh comments every command and marks the mirror-run --execute step with USER_CONFIRMATION_REQUIRED.",
                "The operator must review readiness, stop conditions, and backups before manually running any command.",
            ]
        ) + "\n"


class MirrorAutoSyncReporter:
    REPORT_VERSION = "mirror-auto-sync/v1"
    STATE_VERSION = "mirror-auto-sync-state/v1"
    RETRYABLE_FAILURES = {"rate_limited", "network_error", "server_error", "unknown_error"}

    def create(
        self,
        *,
        root: Path | str,
        backup: Path | str,
        scope: str,
        from_date: str,
        to_date: str,
        window_days: int,
        max_jobs_per_api: int,
        state: Path | str | None = None,
        execute: bool = False,
        confirm_auto_sync: bool = False,
        max_attempts: int = 3,
        retry_backoff_seconds: int = 60,
        client: Any | None = None,
        sleep=time.sleep,
    ) -> MirrorAutoSyncResult:
        ensure_mirror_scope(scope)
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        state_path = _resolve_path(Path(state)) if state else None
        warnings = [
            "mirror-auto-sync uses bounded pilot windows and does not enable disabled or plan-only endpoints",
        ]
        blocking_errors: list[str] = []
        if scope != "a-share-low-risk":
            blocking_errors.append("mirror-auto-sync currently supports only scope a-share-low-risk")
        if window_days <= 0:
            blocking_errors.append("--window-days must be positive")
        if max_jobs_per_api <= 0:
            blocking_errors.append("--max-jobs-per-api must be positive")
        if max_jobs_per_api > MODE_MAX_JOBS["pilot"]:
            blocking_errors.append("mirror-auto-sync max-jobs-per-api cannot exceed 20")
        if window_days > max_jobs_per_api:
            blocking_errors.append("--window-days must be <= --max-jobs-per-api so no endpoint window can be silently truncated")
        if max_attempts <= 0:
            blocking_errors.append("--max-attempts must be positive")
        if retry_backoff_seconds < 0:
            blocking_errors.append("--retry-backoff-seconds must be >= 0")
        if execute and not confirm_auto_sync:
            blocking_errors.append("--execute requires --confirm-auto-sync")
        if execute and state_path is None:
            blocking_errors.append("--execute requires --state for checkpoint/resume")
        if execute and client is None:
            blocking_errors.append("--execute requires a Tushare client")
        if state_path is not None:
            blocking_errors.extend(self._preflight_state_path(mirror_root, backup_root, state_path))
        if not mirror_root.exists():
            blocking_errors.append(f"mirror root does not exist: {mirror_root}")
        if not backup_root.exists():
            blocking_errors.append(f"backup root does not exist: {backup_root}")

        normalized_from = self._normalize_date(from_date)
        resolved_to, latest_warning = self._resolve_to_date(to_date)
        if latest_warning:
            warnings.append(latest_warning)
        if normalized_from > resolved_to:
            blocking_errors.append("from-date must be <= resolved to-date")

        resume_from_state = False
        state_payload = self._read_state(state_path) if state_path and state_path.exists() else {}
        effective_start = normalized_from
        if state_payload.get("next_start_date"):
            candidate = self._normalize_date(str(state_payload["next_start_date"]))
            if candidate > effective_start:
                effective_start = candidate
                resume_from_state = True

        windows = self._planned_windows(effective_start, resolved_to, window_days) if not blocking_errors else []
        for window in windows:
            window["command_preview"] = self._mirror_run_command(mirror_root, backup_root, scope, window["start_date"], window["end_date"], max_jobs_per_api)
            window["attempts"] = 0
            window["status"] = "planned"
            window["would_execute_real_requests"] = execute
            window["user_confirmation_required"] = bool(execute)

        executed = 0
        succeeded = 0
        failed = 0
        next_start_date = windows[0]["start_date"] if windows else None
        if execute and not blocking_errors:
            catalog = CatalogStore(mirror_root)
            if not catalog.db_path.exists():
                blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
            else:
                load_into_catalog(mirror_root, catalog)
                state_payload = self._initial_state(state_payload, mirror_root, backup_root, scope, normalized_from, to_date, resolved_to, window_days, max_jobs_per_api)
                for window in windows:
                    executed += 1
                    result = self._execute_window(
                        mirror_root=mirror_root,
                        backup_root=backup_root,
                        catalog=catalog,
                        client=client,
                        scope=scope,
                        window=window,
                        max_jobs_per_api=max_jobs_per_api,
                        max_attempts=max_attempts,
                        retry_backoff_seconds=retry_backoff_seconds,
                        sleep=sleep,
                    )
                    if result["status"] == "succeeded":
                        succeeded += 1
                        next_start_date = self._next_date(window["end_date"])
                        self._write_state(state_path, state_payload, window, next_start_date)
                        continue
                    failed += 1
                    next_start_date = window["start_date"]
                    blocking_errors.extend(result.get("blocking_errors") or [])
                    break

        status = "blocked" if blocking_errors else ("succeeded" if execute and failed == 0 else "planned")
        return MirrorAutoSyncResult(
            report_version=self.REPORT_VERSION,
            status=status,
            execute=execute,
            root=str(mirror_root),
            backup=str(backup_root),
            scope=scope,
            from_date=normalized_from,
            to_date=to_date,
            resolved_to_date=resolved_to,
            window_days=window_days,
            max_jobs_per_api=max_jobs_per_api,
            state_path=str(state_path) if state_path else None,
            resume_from_state=resume_from_state,
            next_start_date=next_start_date,
            planned_window_count=len(windows),
            executed_window_count=executed,
            succeeded_window_count=succeeded,
            failed_window_count=failed,
            max_attempts=max_attempts,
            windows=windows,
            safety_boundaries=self._safety_boundaries(max_jobs_per_api, window_days),
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _preflight_state_path(self, root: Path, backup: Path, state: Path) -> list[str]:
        errors: list[str] = []
        if state == root or _is_relative_to(state, root):
            errors.append("state path must not be inside mirror root")
        if state == backup or _is_relative_to(state, backup):
            errors.append("state path must not be inside backup root")
        if state.exists() and state.is_dir():
            errors.append("state path exists and is a directory")
        if not state.parent.exists():
            errors.append(f"state path parent does not exist: {state.parent}")
        return errors

    def _resolve_to_date(self, to_date: str) -> tuple[str, str | None]:
        text = str(to_date).strip().lower()
        if text in {"latest", "latest-trade-date"}:
            resolved = datetime.now().strftime("%Y%m%d")
            return resolved, "latest-trade-date is resolved to today's calendar date for planning; execution fetches trade_cal per window and never uses natural-day fallback for daily-like endpoints"
        return self._normalize_date(to_date), None

    def _normalize_date(self, value: str) -> str:
        text = str(value).strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y%m%d")
            except ValueError:
                pass
        raise ValueError(f"invalid date: {value}")

    def _planned_windows(self, start_date: str, end_date: str, window_days: int) -> list[dict[str, Any]]:
        windows: list[dict[str, Any]] = []
        current = datetime.strptime(start_date, "%Y%m%d")
        stop = datetime.strptime(end_date, "%Y%m%d")
        while current <= stop:
            window_end = min(current + timedelta(days=window_days - 1), stop)
            windows.append(
                {
                    "start_date": current.strftime("%Y%m%d"),
                    "end_date": window_end.strftime("%Y%m%d"),
                }
            )
            current = window_end + timedelta(days=1)
        return windows

    def _mirror_run_command(self, root: Path, backup: Path, scope: str, start_date: str, end_date: str, max_jobs: int) -> str:
        return (
            f"python3 -m tushare_mirror mirror-run --root {root} --scope {scope} --mode pilot "
            f"--start-date {start_date} --end-date {end_date} --max-jobs-per-api {max_jobs} "
            f"--backup-target {backup} --execute --json"
        )

    def _execute_window(
        self,
        *,
        mirror_root: Path,
        backup_root: Path,
        catalog: CatalogStore,
        client: Any,
        scope: str,
        window: dict[str, Any],
        max_jobs_per_api: int,
        max_attempts: int,
        retry_backoff_seconds: int,
        sleep,
    ) -> dict[str, Any]:
        attempt = 1
        while attempt <= max_attempts:
            window["attempts"] = attempt
            window["status"] = "running"
            result = MirrorOrchestrator(mirror_root, catalog, client, sleep=sleep).run(
                scope=scope,
                mode="pilot",
                start_date=window["start_date"],
                end_date=window["end_date"],
                max_jobs_per_api=max_jobs_per_api,
                backup_target=str(backup_root),
                backup_overwrite=True,
                allow_quarantined_retry=True,
            )
            window["run_id"] = result.run_id
            window["mirror_run_status"] = result.status
            window["summary"] = self._window_summary(result)
            if result.status == "succeeded" and self._post_checks_passed(result):
                window["status"] = "succeeded"
                return {"status": "succeeded"}
            retryable = self._retryable_result(result)
            if not retryable or attempt >= max_attempts:
                window["status"] = "failed"
                errors = self._result_blockers(result)
                if retryable and attempt >= max_attempts:
                    errors.append("retry attempts exhausted for window %s-%s" % (window["start_date"], window["end_date"]))
                return {"status": "failed", "blocking_errors": errors}
            window["status"] = "retrying"
            sleep(retry_backoff_seconds)
            attempt += 1
        window["status"] = "failed"
        return {"status": "failed", "blocking_errors": ["retry attempts exhausted"]}

    def _window_summary(self, result: MirrorRunResult) -> dict[str, Any]:
        summary = result.summary or {}
        return {
            "status": result.status,
            "run_id": result.run_id,
            "failed_endpoints": summary.get("failed_endpoints"),
            "blocked_endpoints": summary.get("blocked_endpoints"),
            "critical_dependency_failed": summary.get("critical_dependency_failed"),
            "total_jobs_executed": summary.get("total_jobs_executed"),
            "backup_status": summary.get("backup_status"),
            "restore_check_status": summary.get("restore_check_status"),
            "validation_status": summary.get("validation_status"),
        }

    def _post_checks_passed(self, result: MirrorRunResult) -> bool:
        summary = result.summary or {}
        return (
            summary.get("validation_status") == "succeeded"
            and summary.get("backup_status") in {None, "not_requested", "succeeded"}
            and summary.get("restore_check_status") in {None, "not_requested", "succeeded"}
        )

    def _retryable_result(self, result: MirrorRunResult) -> bool:
        summary = result.summary or {}
        if summary.get("backup_status") not in {None, "not_requested", "succeeded"}:
            return False
        if summary.get("restore_check_status") not in {None, "not_requested", "succeeded"}:
            return False
        if summary.get("validation_status") not in {None, "succeeded"}:
            return False
        failed_reasons = []
        for item in summary.get("items") or []:
            if item.get("status") not in {"failed", "blocked"}:
                continue
            failed_reasons.append(item.get("error_type") or item.get("blocked_reason") or "unknown_error")
        return bool(failed_reasons) and all(reason in self.RETRYABLE_FAILURES for reason in failed_reasons)

    def _result_blockers(self, result: MirrorRunResult) -> list[str]:
        summary = result.summary or {}
        errors: list[str] = []
        if summary.get("validation_status") not in {None, "succeeded"}:
            errors.append("validation failed after mirror window")
        if summary.get("backup_status") not in {None, "not_requested", "succeeded"}:
            errors.append(f"backup failed after mirror window: {summary.get('backup_status')}")
        if summary.get("restore_check_status") not in {None, "not_requested", "succeeded"}:
            errors.append(f"restore-check failed after mirror window: {summary.get('restore_check_status')}")
        for item in summary.get("items") or []:
            if item.get("status") in {"failed", "blocked"}:
                reason = item.get("error_type") or item.get("blocked_reason") or "unknown_error"
                errors.append(f"{item.get('endpoint')} {item.get('status')}: {reason}")
        return _dedupe_messages(errors or ["mirror window failed"])

    def _initial_state(self, state: dict[str, Any], root: Path, backup: Path, scope: str, from_date: str, to_date: str, resolved_to_date: str, window_days: int, max_jobs: int) -> dict[str, Any]:
        if state:
            state.setdefault("completed_windows", [])
            return state
        return {
            "state_version": self.STATE_VERSION,
            "root": str(root),
            "backup": str(backup),
            "scope": scope,
            "from_date": from_date,
            "to_date": to_date,
            "resolved_to_date": resolved_to_date,
            "window_days": window_days,
            "max_jobs_per_api": max_jobs,
            "completed_windows": [],
            "created_at": now_utc(),
        }

    def _read_state(self, state_path: Path | None) -> dict[str, Any]:
        if state_path is None or not state_path.exists():
            return {}
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid auto-sync state file: {state_path}: {exc}") from exc
        if payload.get("state_version") != self.STATE_VERSION:
            raise ValueError(f"unsupported auto-sync state file version: {payload.get('state_version')}")
        return payload

    def _write_state(self, state_path: Path | None, payload: dict[str, Any], window: dict[str, Any], next_start_date: str) -> None:
        if state_path is None:
            return
        completed = payload.setdefault("completed_windows", [])
        completed.append(
            {
                "start_date": window["start_date"],
                "end_date": window["end_date"],
                "attempts": window.get("attempts"),
                "run_id": window.get("run_id"),
                "finished_at": now_utc(),
                "status": "succeeded",
            }
        )
        payload["last_successful_end_date"] = window["end_date"]
        payload["next_start_date"] = next_start_date
        payload["updated_at"] = now_utc()
        state_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    def _next_date(self, date: str) -> str:
        return (datetime.strptime(date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")

    def _safety_boundaries(self, max_jobs: int, window_days: int) -> list[str]:
        return [
            "scope is fixed to a-share-low-risk",
            "each execution window is bounded by start_date/end_date",
            f"window_days={window_days} and max_jobs_per_api={max_jobs}",
            "disabled and plan-only endpoints remain excluded",
            "trade_cal is fetched per window before daily-like endpoints",
            "daily-like endpoints use trading-days-only with no natural-day fallback",
            "checkpoint state is required for execution and written only to --state",
            "each failed window stops later windows after retry attempts are exhausted",
            "backup and restore-check must pass after each window",
            "auto-sync atomically refreshes the configured backup target after each successful window",
        ]


class SchemaStatusReporter:
    REPORT_VERSION = "schema-status/v1"

    def report(self, *, root: Path | str) -> SchemaStatusResult:
        mirror_root = Path(root)
        catalog = CatalogStore(mirror_root, read_only=True)
        warnings = ["schema-status is read-only and does not validate, fetch, or write catalog state"]
        blocking_errors: list[str] = []
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
            return self._result(mirror_root, {}, {}, 0, 0, 0, 0, [], warnings, blocking_errors)
        try:
            with catalog.connect() as conn:
                schema_count = self._schema_count_by_api(conn)
                latest = self._latest_schema_by_api(conn)
                schema_change_count = int(conn.execute("select count(*) from schema_changes").fetchone()[0])
                incompatible_count = int(conn.execute("select count(*) from schema_changes where change_type like '%incompatible%'").fetchone()[0])
                pending_count = int(conn.execute("select count(*) from schema_changes where approved=0").fetchone()[0])
                quarantine_count = int(conn.execute("select count(*) from quarantine_files").fetchone()[0])
                quarantined_apis = [
                    str(row[0])
                    for row in conn.execute("select distinct api_name from quarantine_files where api_name is not null order by api_name").fetchall()
                ]
        except Exception as exc:
            blocking_errors.append(f"schema-status failed: {exc}")
            return self._result(mirror_root, {}, {}, 0, 0, 0, 0, [], warnings, blocking_errors)
        if incompatible_count:
            blocking_errors.append("incompatible schema changes are present")
        if quarantine_count:
            blocking_errors.append("schema quarantine is present")
        if pending_count:
            warnings.append("pending schema changes are present")
        return self._result(
            mirror_root,
            schema_count,
            latest,
            schema_change_count,
            incompatible_count,
            pending_count,
            quarantine_count,
            quarantined_apis,
            warnings,
            blocking_errors,
        )

    def _result(
        self,
        root: Path,
        schema_count_by_api: dict[str, int],
        latest_schema_by_api: dict[str, dict[str, Any]],
        schema_change_count: int,
        incompatible_schema_count: int,
        pending_schema_change_count: int,
        quarantine_count: int,
        quarantined_apis: list[str],
        warnings: list[str],
        blocking_errors: list[str],
    ) -> SchemaStatusResult:
        return SchemaStatusResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            schema_count_by_api=schema_count_by_api,
            latest_schema_by_api=latest_schema_by_api,
            schema_change_count=schema_change_count,
            incompatible_schema_count=incompatible_schema_count,
            pending_schema_change_count=pending_schema_change_count,
            quarantine_count=quarantine_count,
            quarantined_apis=quarantined_apis,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _schema_count_by_api(self, conn: sqlite3.Connection) -> dict[str, int]:
        rows = conn.execute("select api_name, count(*) as count from schemas group by api_name order by api_name").fetchall()
        return {str(row["api_name"]): int(row["count"]) for row in rows}

    def _latest_schema_by_api(self, conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
        rows = conn.execute(
            """
            select s.api_name,s.schema_id,s.fields_json,s.logical_types_json,s.created_at
            from schemas s
            join (
                select api_name, max(created_at) as max_created_at
                from schemas group by api_name
            ) latest on latest.api_name=s.api_name and latest.max_created_at=s.created_at
            order by s.api_name,s.schema_id
            """
        ).fetchall()
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            fields = loads(row["fields_json"]) or []
            logical_types = loads(row["logical_types_json"]) or {}
            latest.setdefault(
                str(row["api_name"]),
                {
                    "schema_id": row["schema_id"],
                    "created_at": row["created_at"],
                    "field_count": len(fields),
                    "fields": fields,
                    "logical_types": logical_types,
                },
            )
        return latest


class BackupStatusReporter:
    REPORT_VERSION = "backup-status/v1"

    def report(self, *, backup: Path | str) -> BackupStatusResult:
        backup_root = Path(backup)
        warnings = ["backup-status is read-only and does not restore files or write catalog state"]
        blocking_errors: list[str] = []
        if not backup_root.exists():
            blocking_errors.append(f"backup not found: {backup_root}")
            return self._result(backup_root, False, None, None, None, 0, 0, 0, None, False, "not_checked", "create a fresh backup before controlled execution", warnings, blocking_errors)
        inspect = BackupInspector().inspect(backup_root)
        restore = RestoreChecker().check(backup_root)
        manifest_valid = inspect.manifest_validation_status == "succeeded"
        possible_mutation = bool(inspect.possible_mutation or restore.possible_mutation)
        catalog_checksum_status = restore.catalog_checksum_status or inspect.catalog_checksum_status
        if not manifest_valid:
            blocking_errors.append("backup manifest is invalid")
        if restore.status != "succeeded":
            blocking_errors.append("restore-check failed")
        if possible_mutation:
            blocking_errors.append("backup catalog may have been modified after backup creation")
        if inspect.manifest_warning_count:
            warnings.append("backup manifest has warnings")
        return self._result(
            backup_root,
            manifest_valid,
            inspect.backup_id or restore.backup_id,
            inspect.created_at,
            inspect.snapshot_scope,
            inspect.file_count,
            inspect.raw_file_count,
            inspect.lake_file_count,
            catalog_checksum_status,
            possible_mutation,
            restore.status,
            self._recommended_action(manifest_valid, restore.status, possible_mutation),
            warnings,
            blocking_errors,
        )

    def _result(
        self,
        backup: Path,
        manifest_valid: bool,
        backup_id: str | None,
        created_at: str | None,
        snapshot_scope: str | None,
        file_count: int,
        raw_file_count: int,
        lake_file_count: int,
        catalog_checksum_status: str | None,
        possible_mutation: bool,
        restore_check_status: str,
        recommended_action: str,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> BackupStatusResult:
        return BackupStatusResult(
            report_version=self.REPORT_VERSION,
            backup=str(backup),
            manifest_valid=manifest_valid,
            backup_id=backup_id,
            created_at=created_at,
            snapshot_scope=snapshot_scope,
            file_count=file_count,
            raw_file_count=raw_file_count,
            lake_file_count=lake_file_count,
            catalog_checksum_status=catalog_checksum_status,
            possible_mutation=possible_mutation,
            restore_check_status=restore_check_status,
            recommended_action=recommended_action,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _recommended_action(self, manifest_valid: bool, restore_status: str, possible_mutation: bool) -> str:
        if possible_mutation:
            return "replace backup before any controlled execution"
        if not manifest_valid:
            return "create a fresh backup with a valid manifest"
        if restore_status != "succeeded":
            return "investigate restore-check failures and rebuild backup"
        return "backup is ready for operator review"


class MirrorCoverageMatrixReporter:
    REPORT_VERSION = "mirror-coverage-matrix/v1"

    def report(
        self,
        *,
        root: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
    ) -> MirrorCoverageMatrixResult:
        ensure_mirror_scope(scope)
        mirror_root = Path(root)
        warnings = ["mirror-coverage-matrix is read-only and does not fetch, backfill, or validate"]
        blocking_errors: list[str] = []
        catalog = CatalogStore(mirror_root, read_only=True)
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
            return self._result(mirror_root, scope, start_date, end_date, [], warnings, blocking_errors)
        items = []
        for api_name in coverage_matrix_apis_for_scope(scope):
            items.append(self._api_row(mirror_root, catalog, api_name, start_date, end_date, warnings))
        return self._result(mirror_root, scope, start_date, end_date, items, warnings, blocking_errors)

    def _result(
        self,
        root: Path,
        scope: str,
        start_date: str,
        end_date: str,
        items: list[dict[str, Any]],
        warnings: list[str],
        blocking_errors: list[str],
    ) -> MirrorCoverageMatrixResult:
        return MirrorCoverageMatrixResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            items=items,
            warnings=_dedupe_messages(warnings),
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _api_row(self, root: Path, catalog: CatalogStore, api_name: str, start_date: str, end_date: str, warnings: list[str]) -> dict[str, Any]:
        daily_like = set(DAILY_LIKE_MIRROR_APIS) | set(A_SHARE_LOW_RISK_DAILY_LIKE_APIS)
        coverage_class = "daily_like" if api_name in daily_like else "weekly_monthly"
        try:
            if api_name in daily_like:
                report = CoverageReporter(root, catalog).report(
                    api_name,
                    start_date=start_date,
                    end_date=end_date,
                    trading_days_only=True,
                    calendar_exchange="SSE",
                )
            else:
                dates = self._weekly_monthly_dates(root, catalog, api_name, start_date, end_date)
                report = CoverageReporter(root, catalog).report(api_name, dates=dates)
        except Exception as exc:
            if api_name in daily_like:
                warnings.append(f"{api_name} coverage unavailable: {exc}")
                return self._empty_row(api_name, coverage_class, "blocked_missing_trade_cal", str(exc))
            warnings.append(f"{api_name} coverage unavailable: {exc}")
            return self._empty_row(api_name, coverage_class, "blocked", str(exc))
        missing_dates = [item.date for item in report.items if item.existing_status == "missing"]
        failed_dates = [item.date for item in report.items if item.existing_status in {"failed_exists", "quarantined_exists"}]
        status = "complete" if report.total_dates > 0 and report.missing_dates == 0 and report.failed_dates == 0 and report.quarantined_dates == 0 else "partial"
        if report.total_dates == 0:
            status = "empty"
        return {
            "api": api_name,
            "coverage_class": coverage_class,
            "total_dates": report.total_dates,
            "covered_dates": report.covered_dates,
            "missing_dates": report.missing_dates,
            "coverage_ratio": report.coverage_ratio,
            "missing_date_sample": missing_dates[:10],
            "status": status,
            "failed_date_sample": failed_dates[:10],
        }

    def _empty_row(self, api_name: str, coverage_class: str, status: str, reason: str) -> dict[str, Any]:
        return {
            "api": api_name,
            "coverage_class": coverage_class,
            "total_dates": 0,
            "covered_dates": 0,
            "missing_dates": 0,
            "coverage_ratio": 0.0,
            "missing_date_sample": [],
            "status": status,
            "reason": reason,
            "failed_date_sample": [],
        }

    def _weekly_dates(self, root: Path, catalog: CatalogStore, start_date: str, end_date: str) -> list[str]:
        return _pilot_compatible_period_dates(root, catalog, start_date, end_date, period="weekly")

    def _monthly_dates(self, root: Path, catalog: CatalogStore, start_date: str, end_date: str) -> list[str]:
        return _pilot_compatible_period_dates(root, catalog, start_date, end_date, period="monthly")

    def _weekly_monthly_dates(self, root: Path, catalog: CatalogStore, api_name: str, start_date: str, end_date: str) -> list[str]:
        if api_name in {"weekly", "index_weekly"}:
            fallback_dates = self._weekly_dates(root, catalog, start_date, end_date)
            pilot_dates = PILOT_JAN_2025_WEEKLY_DATES
        else:
            fallback_dates = self._monthly_dates(root, catalog, start_date, end_date)
            pilot_dates = PILOT_JAN_2025_MONTHLY_DATES
        expected = {date for date in fallback_dates if not ("20250101" <= date <= "20250131")}
        expected.update(date for date in pilot_dates if start_date <= date <= end_date)
        return sorted(expected)


class RequestEstimateReporter:
    REPORT_VERSION = "request-estimate/v1"

    def report(
        self,
        *,
        root: Path | str,
        scope: str,
        start_date: str,
        end_date: str,
    ) -> RequestEstimateResult:
        ensure_mirror_scope(scope)
        mirror_root = Path(root)
        warnings = ["request-estimate is read-only, does not call Tushare, and does not inspect token quota"]
        blocking_errors: list[str] = []
        catalog = CatalogStore(mirror_root, read_only=True)
        if not catalog.db_path.exists():
            blocking_errors.append(f"catalog not found: {catalog.db_path}; run init-catalog first")
            return self._result(mirror_root, scope, start_date, end_date, {}, 0, 0, 0, 0, 0, 0, 0, 0, "unknown", None, {}, "unknown", False, "unknown", warnings, blocking_errors)
        try:
            plan = MirrorBatchPlanner(mirror_root, catalog).plan(
                scope=scope,
                start_date=start_date,
                end_date=end_date,
                calendar_exchange="SSE",
                max_jobs_per_api=MirrorNextBatchReporter.RECOMMENDED_MAX_JOBS_PER_API,
            )
        except Exception as exc:
            blocking_errors.append(f"request estimate failed: {exc}")
            return self._result(mirror_root, scope, start_date, end_date, {}, 0, 0, 0, 0, 0, 0, 0, 0, "unknown", None, {}, "unknown", False, "unknown", warnings, blocking_errors)
        by_api = {item.endpoint: int(item.missing_jobs) for item in plan.endpoint_plans}
        daily_like = sum(by_api.get(api_name, 0) for api_name in daily_like_apis_for_scope(scope))
        periodic = ["weekly", "monthly"]
        if scope == "a-share-low-risk":
            periodic.extend(["index_weekly", "index_monthly"])
        weekly_monthly = sum(by_api.get(api_name, 0) for api_name in periodic)
        reference_refresh = sum(by_api.get(api_name, 0) for api_name in reference_refresh_apis_for_scope(scope))
        trade_cal_requests = by_api.get("trade_cal", 0)
        if plan.trade_cal_dependency_status != "covered":
            warnings.append(f"local trade_cal range is {plan.trade_cal_dependency_status}; daily-like estimates may be deferred until calendar is present")
            warnings.append("daily-like request counts are deferred until trade_cal is local; natural day fallback is disabled")
        total = sum(by_api.values())
        return self._result(
            mirror_root,
            scope,
            start_date,
            end_date,
            by_api,
            total,
            trade_cal_requests,
            daily_like,
            weekly_monthly,
            reference_refresh,
            plan.dependency_requests,
            plan.executable_after_dependency_requests,
            plan.currently_unblocked_requests,
            plan.dependency_status,
            plan.dependency_action,
            plan.trade_cal_params,
            plan.daily_like_status,
            plan.natural_day_fallback,
            self._risk_level(total),
            warnings,
            blocking_errors,
        )

    def _result(
        self,
        root: Path,
        scope: str,
        start_date: str,
        end_date: str,
        estimated_requests_by_api: dict[str, int],
        estimated_total_requests: int,
        planned_trade_cal_requests: int,
        daily_like_requests: int,
        weekly_monthly_requests: int,
        reference_refresh_requests: int,
        dependency_requests: int,
        executable_after_dependency_requests: int,
        currently_unblocked_requests: int,
        dependency_status: str,
        dependency_action: str | None,
        trade_cal_params: dict[str, str],
        daily_like_status: str,
        natural_day_fallback: bool,
        risk_level: str,
        warnings: list[str],
        blocking_errors: list[str],
    ) -> RequestEstimateResult:
        return RequestEstimateResult(
            report_version=self.REPORT_VERSION,
            root=str(root),
            scope=scope,
            start_date=start_date,
            end_date=end_date,
            estimated_requests_by_api=estimated_requests_by_api,
            estimated_total_requests=estimated_total_requests,
            planned_trade_cal_requests=planned_trade_cal_requests,
            daily_like_requests=daily_like_requests,
            weekly_monthly_requests=weekly_monthly_requests,
            reference_refresh_requests=reference_refresh_requests,
            dependency_requests=dependency_requests,
            executable_after_dependency_requests=executable_after_dependency_requests,
            currently_unblocked_requests=currently_unblocked_requests,
            dependency_status=dependency_status,
            dependency_action=dependency_action,
            trade_cal_params=trade_cal_params,
            daily_like_status=daily_like_status,
            natural_day_fallback=natural_day_fallback,
            risk_level=risk_level,
            assumptions=[
                "one planned missing job maps to one estimated request",
                "daily-like requests use local trade_cal when the requested range is covered",
                "daily-like requests are blocked until trade_cal is local when dependency_status is missing",
                "natural day fallback is disabled for daily-like endpoints",
                "weekly/monthly requests use bounded date lists only",
                "stock loop, financial, object/text, intraday, and compaction execution remain excluded",
            ],
            warnings=_dedupe_messages(warnings),
            not_a_quota_guarantee=True,
            blocking_errors=_dedupe_messages(blocking_errors),
        )

    def _risk_level(self, total: int) -> str:
        if total <= 25:
            return "low"
        if total <= 100:
            return "moderate"
        return "high"


class MirrorBatchPlanner:
    REFERENCE_APIS = ["stock_basic", "hs_const"]
    EVENT_APIS = ["namechange", "stk_managers", "stk_rewards"]

    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        *,
        scope: str,
        start_date: str,
        end_date: str,
        calendar_exchange: str = "SSE",
        max_jobs_per_api: int = 20,
    ) -> MirrorBatchPlan:
        ensure_mirror_scope(scope)
        if max_jobs_per_api <= 0:
            raise ValueError("--max-jobs-per-api must be positive")
        if max_jobs_per_api > MODE_MAX_JOBS["pilot"]:
            raise ValueError("mirror-batch-plan max-jobs-per-api cannot exceed 20")
        start = DatePlanner(self.root, self.catalog)._normalize_date(start_date)
        end = DatePlanner(self.root, self.catalog)._normalize_date(end_date)
        if start > end:
            raise ValueError("start_date must be <= end_date")
        warnings = ["mirror-batch-plan is read-only; execute requires separate user confirmation"]
        calendar = self._calendar_range(start, end, calendar_exchange)
        endpoint_plans: list[MirrorBatchEndpointPlan] = []
        endpoint_plans.append(self._trade_cal_plan(start, end, calendar_exchange, calendar))
        for endpoint in reference_refresh_apis_for_scope(scope):
            endpoint_plans.append(self._reference_plan(endpoint))
        for endpoint in daily_like_apis_for_scope(scope):
            endpoint_plans.append(self._daily_like_plan(endpoint, start, end, calendar_exchange, max_jobs_per_api, calendar))
        endpoint_plans.append(self._explicit_date_plan("weekly", self._weekly_dates(start, end), max_jobs_per_api, "weekly uses bounded explicit date planning only"))
        endpoint_plans.append(self._explicit_date_plan("monthly", self._monthly_dates(start, end), max_jobs_per_api, "monthly uses bounded explicit date planning only"))
        if scope == "a-share-low-risk":
            endpoint_plans.append(self._explicit_date_plan("index_weekly", self._weekly_dates(start, end), max_jobs_per_api, "index_weekly uses bounded explicit date planning only"))
            endpoint_plans.append(self._explicit_date_plan("index_monthly", self._monthly_dates(start, end), max_jobs_per_api, "index_monthly uses bounded explicit date planning only"))
            excluded = A_SHARE_LOW_RISK_PLAN_ONLY_APIS
        else:
            excluded = self.EVENT_APIS
        for endpoint in excluded:
            endpoint_plans.append(self._excluded_plan(endpoint))
        total_candidate = sum(item.total_candidate_jobs for item in endpoint_plans)
        total_planned = sum(item.planned_jobs for item in endpoint_plans)
        blocked = sum(1 for item in endpoint_plans if item.plan_status.startswith("blocked"))
        estimated = sum(item.missing_jobs for item in endpoint_plans if item.plan_status not in {"excluded_no_stock_loop"})
        dependency_missing = calendar["status"] != "covered"
        dependency_requests = sum(item.missing_jobs for item in endpoint_plans if item.endpoint == "trade_cal")
        currently_unblocked_requests = sum(
            item.missing_jobs
            for item in endpoint_plans
            if item.endpoint != "trade_cal"
            and item.category != "daily_like"
            and item.plan_status not in {"excluded_no_stock_loop"}
        )
        if not dependency_missing:
            currently_unblocked_requests += sum(item.missing_jobs for item in endpoint_plans if item.category == "daily_like")
        return MirrorBatchPlan(
            batch_id=f"batch_{start}_{end}",
            scope=scope,
            root=str(self.root),
            start_date=start,
            end_date=end,
            calendar_exchange=calendar_exchange.upper(),
            max_jobs_per_api=max_jobs_per_api,
            endpoint_plans=endpoint_plans,
            total_candidate_jobs=total_candidate,
            total_planned_jobs=total_planned,
            blocked_endpoints=blocked,
            warnings=warnings,
            estimated_request_count=estimated,
            requires_execute_confirmation=True,
            trade_cal_dependency_status=calendar["status"],
            dependency_status="missing" if dependency_missing else "covered",
            dependency_action="fetch_trade_cal_first" if dependency_missing else None,
            trade_cal_params={"exchange": calendar_exchange.upper(), "start_date": start, "end_date": end},
            daily_like_status="blocked_until_trade_cal" if dependency_missing else "ready",
            natural_day_fallback=False,
            dependency_requests=dependency_requests,
            executable_after_dependency_requests=0,
            currently_unblocked_requests=currently_unblocked_requests,
        )

    def _calendar_range(self, start: str, end: str, exchange: str) -> dict[str, Any]:
        natural = self._natural_dates(start, end)
        snapshot = self.catalog.latest_snapshot("trade_cal")
        if not snapshot:
            return {
                "status": "missing_snapshot",
                "natural_dates": natural,
                "open_dates": [],
                "missing_calendar_dates": natural,
                "filtered_non_trading_dates": [],
            }
        try:
            table = LakeReader(self.root, self.catalog).scan_api("trade_cal", columns=["exchange", "cal_date", "is_open"])
        except Exception as exc:
            return {
                "status": "unreadable",
                "error": str(exc),
                "natural_dates": natural,
                "open_dates": [],
                "missing_calendar_dates": natural,
                "filtered_non_trading_dates": [],
            }
        exchange_upper = exchange.upper()
        present: set[str] = set()
        open_dates: set[str] = set()
        if table.num_rows:
            exchanges = table["exchange"].to_pylist()
            cal_dates = table["cal_date"].to_pylist()
            flags = table["is_open"].to_pylist()
            planner = DatePlanner(self.root, self.catalog)
            for source_exchange, cal_date, flag in zip(exchanges, cal_dates, flags):
                if str(source_exchange).upper() != exchange_upper:
                    continue
                try:
                    date = planner._normalize_date(str(cal_date))
                except ValueError:
                    continue
                if start <= date <= end:
                    present.add(date)
                    if planner._is_open_flag(flag):
                        open_dates.add(date)
        missing = sorted(set(natural) - present)
        open_sorted = sorted(open_dates)
        return {
            "status": "covered" if not missing else "missing_range",
            "natural_dates": natural,
            "open_dates": open_sorted,
            "missing_calendar_dates": missing,
            "filtered_non_trading_dates": sorted(set(natural) - set(open_sorted) - set(missing)),
        }

    def _trade_cal_plan(self, start: str, end: str, exchange: str, calendar: dict[str, Any]) -> MirrorBatchEndpointPlan:
        missing = calendar["status"] != "covered"
        return MirrorBatchEndpointPlan(
            endpoint="trade_cal",
            category="calendar_dependency",
            requires_trade_cal=False,
            plan_status="planned" if missing else "current",
            planned_action="fetch_calendar_range" if missing else "skip_existing_range",
            total_candidate_jobs=1,
            planned_jobs=1 if missing else 0,
            missing_jobs=1 if missing else 0,
            skipped_jobs=0 if missing else 1,
            blocked_jobs=0,
            max_jobs=1,
            truncated=False,
            dates=[],
            refresh_strategy=f"ensure SSE trade_cal coverage for {start}-{end}",
            blocked_reason=None,
            warnings=[f"missing calendar dates: {calendar['missing_calendar_dates']}"] if missing else [],
        )

    def _reference_plan(self, endpoint: str) -> MirrorBatchEndpointPlan:
        snapshot = self.catalog.latest_snapshot(endpoint)
        missing = snapshot is None
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="reference",
            requires_trade_cal=False,
            plan_status="planned" if missing else "current",
            planned_action="fetch_reference_once" if missing else "skip_existing_reference",
            total_candidate_jobs=1,
            planned_jobs=1 if missing else 0,
            missing_jobs=1 if missing else 0,
            skipped_jobs=0 if missing else 1,
            blocked_jobs=0,
            max_jobs=1,
            truncated=False,
            dates=[],
            refresh_strategy="fetch once if missing; do not refetch blindly",
        )

    def _daily_like_plan(self, endpoint: str, start: str, end: str, exchange: str, max_jobs: int, calendar: dict[str, Any]) -> MirrorBatchEndpointPlan:
        if calendar["status"] != "covered":
            return MirrorBatchEndpointPlan(
                endpoint=endpoint,
                category="daily_like",
                requires_trade_cal=True,
                plan_status="blocked_until_trade_cal",
                planned_action="blocked",
                total_candidate_jobs=0,
                planned_jobs=0,
                missing_jobs=0,
                skipped_jobs=0,
                blocked_jobs=1,
                max_jobs=max_jobs,
                truncated=False,
                dates=[],
                blocked_reason="missing_trade_cal_range",
                warnings=["daily-like endpoints do not fall back to natural days"],
            )
        metadata = {
            "calendar_source": "local trade_cal latest snapshot",
            "exchange": exchange.upper(),
            "requested_start_date": start,
            "requested_end_date": end,
            "natural_days": len(calendar["natural_dates"]),
            "trading_days": len(calendar["open_dates"]),
            "filtered_non_trading_days": len(calendar["filtered_non_trading_dates"]),
            "filtered_non_trading_dates": calendar["filtered_non_trading_dates"],
        }
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, calendar["open_dates"], max_jobs=max_jobs, calendar_metadata=metadata)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        blocked = sum(1 for job in plan.planned_jobs if job.planned_action == "blocked_quarantined")
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="daily_like",
            requires_trade_cal=True,
            plan_status="planned" if missing else "current",
            planned_action="calendar_backfill_missing",
            total_candidate_jobs=plan.total_candidate_jobs,
            planned_jobs=len(plan.planned_jobs),
            missing_jobs=missing,
            skipped_jobs=skipped,
            blocked_jobs=blocked,
            max_jobs=max_jobs,
            truncated=plan.truncated_by_max_jobs,
            dates=[job.date for job in plan.planned_jobs],
            warnings=plan.warnings,
        )

    def _explicit_date_plan(self, endpoint: str, dates: list[str], max_jobs: int, notes: str) -> MirrorBatchEndpointPlan:
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        blocked = sum(1 for job in plan.planned_jobs if job.planned_action == "blocked_quarantined")
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="date_based",
            requires_trade_cal=False,
            plan_status="planned" if missing else "current",
            planned_action="bounded_explicit_date_plan",
            total_candidate_jobs=plan.total_candidate_jobs,
            planned_jobs=len(plan.planned_jobs),
            missing_jobs=missing,
            skipped_jobs=skipped,
            blocked_jobs=blocked,
            max_jobs=max_jobs,
            truncated=plan.truncated_by_max_jobs,
            dates=[job.date for job in plan.planned_jobs],
            refresh_strategy=notes,
            warnings=plan.warnings,
        )

    def _excluded_plan(self, endpoint: str) -> MirrorBatchEndpointPlan:
        return MirrorBatchEndpointPlan(
            endpoint=endpoint,
            category="event_company",
            requires_trade_cal=False,
            plan_status="excluded_no_stock_loop",
            planned_action="excluded",
            total_candidate_jobs=0,
            planned_jobs=0,
            missing_jobs=0,
            skipped_jobs=0,
            blocked_jobs=0,
            max_jobs=0,
            truncated=False,
            dates=[],
            refresh_strategy="excluded from controlled batch planning; no stock loop",
        )

    def _natural_dates(self, start: str, end: str) -> list[str]:
        return _natural_dates_between(start, end)

    def _weekly_dates(self, start: str, end: str) -> list[str]:
        return _pilot_compatible_period_dates(self.root, self.catalog, start, end, period="weekly")

    def _monthly_dates(self, start: str, end: str) -> list[str]:
        return _pilot_compatible_period_dates(self.root, self.catalog, start, end, period="monthly")


class MirrorPreflightChecker:
    PILOT_SIZE_ESTIMATE_BYTES = 100 * 1024 * 1024

    def __init__(self, *, token_available: bool | None = None):
        self._token_available_override = token_available

    def check(
        self,
        *,
        mirror_root: Path | str,
        backup_target: Path | str,
        scope: str,
        mode: str,
        start_date: str | None = None,
        end_date: str | None = None,
        max_jobs_per_api: int | None = None,
    ) -> MirrorPreflightResult:
        mirror = _resolve_path(Path(mirror_root))
        backup = _resolve_path(Path(backup_target))
        warnings: list[str] = []
        blocking_errors: list[str] = []

        self._check_scope_mode(scope, mode, start_date, end_date, max_jobs_per_api, blocking_errors)
        token_available = self._token_available_override if self._token_available_override is not None else _token_available_from_env()
        if not token_available:
            blocking_errors.append('TUSHARE_TOKEN is not available; mirror-run --execute would fail')

        mirror_status, catalog_info = self._inspect_mirror_root(mirror, warnings, blocking_errors)
        backup_status, backup_info = self._inspect_backup_target(backup, warnings, blocking_errors)
        relationship = self._path_relationship(mirror, backup)
        if relationship != 'ok':
            blocking_errors.append(f'unsafe path relationship: {relationship}')
        if _is_under_tmp(mirror):
            warnings.append('mirror_root is under /tmp; use a durable path for long-lived mirror data')
        if _is_under_tmp(backup):
            warnings.append('backup_target is under /tmp; use a durable path for long-lived backup artifacts')

        disk_space = self._disk_space_summary(mirror, backup, warnings)
        status = 'blocked' if blocking_errors else ('warning' if warnings else 'passed')
        return MirrorPreflightResult(
            status=status,
            ready_to_execute=not blocking_errors,
            mirror_root=str(mirror),
            backup_target=str(backup),
            scope=scope,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=int(max_jobs_per_api or MODE_MAX_JOBS.get(mode, 0)),
            token_available=bool(token_available),
            mirror_root_status=mirror_status,
            backup_target_status=backup_status,
            path_relationship_status=relationship,
            disk_space=disk_space,
            existing_catalog=catalog_info,
            existing_backup=backup_info,
            warnings=warnings,
            blocking_errors=blocking_errors,
        )

    def _check_scope_mode(self, scope: str, mode: str, start_date: str | None, end_date: str | None, max_jobs_per_api: int | None, blocking_errors: list[str]) -> None:
        try:
            ensure_mirror_scope(scope)
        except ValueError as exc:
            blocking_errors.append(str(exc))
        try:
            ensure_mirror_mode(mode)
        except ValueError as exc:
            blocking_errors.append(str(exc))
            return
        if mode == 'pilot' and (not start_date or not end_date):
            blocking_errors.append('pilot mode requires --start-date and --end-date')
        max_jobs = max_jobs_per_api if max_jobs_per_api is not None else MODE_MAX_JOBS[mode]
        if max_jobs <= 0:
            blocking_errors.append('--max-jobs-per-api must be positive')
        elif max_jobs > MODE_MAX_JOBS[mode]:
            blocking_errors.append(f'{mode} mode max-jobs-per-api cannot exceed {MODE_MAX_JOBS[mode]}')

    def _inspect_mirror_root(self, mirror: Path, warnings: list[str], blocking_errors: list[str]) -> tuple[str, dict[str, Any]]:
        default_catalog = {'present': False, 'schema_version': None, 'endpoint_count': 0, 'snapshot_count': 0, 'latest_snapshots_count': 0, 'active_file_count': 0, 'has_active_data': False}
        if not mirror.exists():
            self._parent_warning(mirror, 'mirror_root', warnings)
            return 'missing', default_catalog
        if not mirror.is_dir():
            blocking_errors.append('mirror_root exists but is not a directory')
            return 'non_empty_unknown', default_catalog
        catalog_path = mirror / '_catalog' / 'catalog.sqlite'
        if catalog_path.exists():
            try:
                return 'existing_catalog', _catalog_counts_readonly(catalog_path)
            except Exception as exc:
                blocking_errors.append(f'existing mirror catalog is unreadable: {exc}')
                return 'existing_catalog', default_catalog | {'present': True}
        if not any(mirror.iterdir()):
            return 'empty', default_catalog
        blocking_errors.append('mirror_root is non-empty but has no _catalog/catalog.sqlite')
        return 'non_empty_unknown', default_catalog

    def _inspect_backup_target(self, backup: Path, warnings: list[str], blocking_errors: list[str]) -> tuple[str, dict[str, Any]]:
        default_backup = {'present': False, 'backup_id': None, 'manifest_version': None, 'snapshot_scope': None, 'file_count': None, 'catalog_checksum_status': None, 'possible_mutation': False}
        if not backup.exists():
            self._parent_warning(backup, 'backup_target', warnings)
            return 'missing', default_backup
        if not backup.is_dir():
            blocking_errors.append('backup_target exists but is not a directory')
            return 'non_empty_unknown', default_backup
        manifest = backup / 'manifest.json'
        if manifest.exists():
            inspect = BackupInspector().inspect(backup)
            info = {
                'present': True,
                'backup_id': inspect.backup_id,
                'manifest_version': inspect.manifest_version,
                'snapshot_scope': inspect.snapshot_scope,
                'file_count': inspect.file_count,
                'catalog_checksum_status': inspect.catalog_checksum_status,
                'possible_mutation': inspect.possible_mutation,
                'manifest_validation_status': inspect.manifest_validation_status,
            }
            blocking_errors.append('backup_target already contains manifest.json; choose a new target or clear it explicitly')
            return 'existing_manifest', info
        if not any(backup.iterdir()):
            return 'empty', default_backup
        blocking_errors.append('backup_target is non-empty and is not a recognized backup artifact')
        return 'non_empty_unknown', default_backup

    def _path_relationship(self, mirror: Path, backup: Path) -> str:
        if mirror == backup:
            return 'same_path'
        if _is_relative_to(backup, mirror):
            return 'backup_inside_mirror'
        if _is_relative_to(mirror, backup):
            return 'mirror_inside_backup'
        return 'ok'

    def _disk_space_summary(self, mirror: Path, backup: Path, warnings: list[str]) -> dict[str, Any]:
        mirror_free, mirror_warning = _disk_free(mirror)
        backup_free, backup_warning = _disk_free(backup)
        disk_warning = None
        if mirror_warning or backup_warning:
            disk_warning = '; '.join(filter(None, [mirror_warning, backup_warning]))
            warnings.append(f'disk space could not be fully checked: {disk_warning}')
        enough = None
        if mirror_free is not None and backup_free is not None:
            enough = mirror_free >= self.PILOT_SIZE_ESTIMATE_BYTES and backup_free >= self.PILOT_SIZE_ESTIMATE_BYTES
            if not enough:
                warnings.append('available disk space is below the rough one-month pilot estimate')
        return {
            'mirror_parent_free_bytes': mirror_free,
            'backup_parent_free_bytes': backup_free,
            'pilot_estimate_bytes': self.PILOT_SIZE_ESTIMATE_BYTES,
            'expected_size_class': 'tens_of_mb',
            'enough_for_pilot': enough,
            'warning': disk_warning,
        }

    def _parent_warning(self, path: Path, label: str, warnings: list[str]) -> None:
        if path.parent.exists():
            return
        warnings.append(f'{label} parent directory does not exist and must be created before execution: {path.parent}')


class MirrorPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan(
        self,
        *,
        scope: str = "low-risk-a-share",
        mode: str = "smoke",
        start_date: str | None = None,
        end_date: str | None = None,
        max_jobs_per_api: int | None = None,
    ) -> MirrorPlan:
        ensure_mirror_scope(scope)
        ensure_mirror_mode(mode)
        max_jobs = max_jobs_per_api or MODE_MAX_JOBS[mode]
        if max_jobs <= 0:
            raise ValueError("--max-jobs-per-api must be positive")
        if max_jobs > MODE_MAX_JOBS[mode]:
            raise ValueError(f"{mode} mode max-jobs-per-api cannot exceed {MODE_MAX_JOBS[mode]}")
        if scope == "a-share-low-risk":
            items = self._a_share_smoke_items(max_jobs) if mode == "smoke" else self._a_share_pilot_items(start_date, end_date, max_jobs)
        else:
            items = self._smoke_items(max_jobs) if mode == "smoke" else self._pilot_items(start_date, end_date, max_jobs)
        planned_endpoint_count = sum(1 for item in items if item.plan_status in {"planned", "skip_existing"})
        blocked_endpoint_count = sum(1 for item in items if item.plan_status.startswith("blocked"))
        return MirrorPlan(
            scope=scope,
            mode=mode,
            root=str(self.root),
            start_date=start_date,
            end_date=end_date,
            max_jobs_per_api=max_jobs,
            endpoint_count=len(items),
            planned_endpoint_count=planned_endpoint_count,
            blocked_endpoint_count=blocked_endpoint_count,
            total_planned_jobs=sum(item.planned_jobs for item in items if item.plan_status not in {"blocked", "blocked_until_trade_cal", "excluded_from_pilot_execution"}),
            requires_real_requests=True,
            dry_run=True,
            items=items,
            warnings=[],
        )

    def _smoke_items(self, max_jobs: int) -> list[MirrorPlanItem]:
        items: list[MirrorPlanItem] = []
        for endpoint in ["stock_basic", "trade_cal", "hs_const"]:
            items.append(self._fetch_item(endpoint, "snapshot_reference", SMOKE_REFERENCE_FETCHES[endpoint], max_jobs=1))
        trade_cal_ready = bool(self.catalog.latest_snapshot("trade_cal"))
        for endpoint in SMOKE_CALENDAR_BACKFILL_APIS:
            if not trade_cal_ready:
                items.append(self._blocked_item(endpoint, "daily_like", True, max_jobs, "missing_trade_cal_snapshot"))
            else:
                items.append(self._calendar_backfill_item(endpoint, "20250101", "20250110", max_jobs))
        for endpoint, dates in SMOKE_EXPLICIT_DATE_APIS.items():
            items.append(self._date_backfill_item(endpoint, dates, min(max_jobs, len(dates)), requires_trade_cal=False))
        for endpoint in ["namechange", "stk_managers", "stk_rewards"]:
            items.append(self._fetch_item(endpoint, "event_snapshot", SMOKE_REFERENCE_FETCHES[endpoint], max_jobs=1))
        return items

    def _a_share_smoke_items(self, max_jobs: int) -> list[MirrorPlanItem]:
        items: list[MirrorPlanItem] = []
        for endpoint, params in A_SHARE_LOW_RISK_REFERENCE_FETCHES.items():
            items.append(self._fetch_item(endpoint, "snapshot_reference", params, max_jobs=1))
        trade_cal_ready = bool(self.catalog.latest_snapshot("trade_cal"))
        for endpoint in A_SHARE_LOW_RISK_CALENDAR_BACKFILL_APIS:
            if not trade_cal_ready:
                items.append(self._blocked_item(endpoint, "daily_like", True, max_jobs, "missing_trade_cal_snapshot"))
            else:
                items.append(self._calendar_backfill_item(endpoint, "20250101", "20250110", max_jobs))
        for endpoint, dates in A_SHARE_LOW_RISK_EXPLICIT_DATE_APIS.items():
            items.append(self._date_backfill_item(endpoint, dates, min(max_jobs, len(dates)), requires_trade_cal=False))
        for endpoint, params in A_SHARE_LOW_RISK_STOCK_CODE_SMOKE_FETCHES.items():
            items.append(self._fetch_item(endpoint, "bounded_stock_code_smoke", params, max_jobs=1))
        for endpoint in A_SHARE_LOW_RISK_PLAN_ONLY_APIS:
            if endpoint not in A_SHARE_LOW_RISK_STOCK_CODE_SMOKE_FETCHES:
                items.append(self._plan_only_item(endpoint, "plan_only", "requires bounded code-loop or endpoint-specific enablement before execution"))
        return items

    def _pilot_items(self, start_date: str | None, end_date: str | None, max_jobs: int) -> list[MirrorPlanItem]:
        if not start_date or not end_date:
            raise ValueError("pilot mode requires --start-date and --end-date")
        items = [
            self._fetch_item("stock_basic", "snapshot_reference", {"list_status": "L"}, max_jobs=1),
            self._fetch_item("trade_cal", "calendar_dependency", {"exchange": "SSE", "start_date": start_date, "end_date": end_date}, max_jobs=1, planned_action="fetch_calendar", required_by=["daily", "adj_factor", "daily_basic", "suspend_d"]),
            self._fetch_item("hs_const", "snapshot_reference", {"hs_type": "SH", "is_new": "1"}, max_jobs=1),
        ]
        trade_cal_ready = bool(self.catalog.latest_snapshot("trade_cal"))
        for endpoint in ["daily", "adj_factor", "daily_basic", "suspend_d"]:
            if not trade_cal_ready:
                items.append(self._blocked_item(endpoint, "daily_like", True, max_jobs, "missing_trade_cal_snapshot", plan_status="blocked_until_trade_cal", notes="calendar-aware backfill waits for local trade_cal latest snapshot"))
            else:
                items.append(self._calendar_backfill_item(endpoint, start_date, end_date, max_jobs))
        items.append(self._date_backfill_item("weekly", self._pilot_weekly_dates(start_date, end_date), max_jobs, requires_trade_cal=False, notes="weekly does not use trading-days-only in Phase 3.1"))
        items.append(self._date_backfill_item("monthly", self._pilot_monthly_dates(start_date, end_date), max_jobs, requires_trade_cal=False, notes="monthly does not use trading-days-only in Phase 3.1"))
        for endpoint in ["namechange", "stk_managers", "stk_rewards"]:
            items.append(self._excluded_item(endpoint, "event_snapshot", max_jobs, "excluded_from_pilot_execution", "pilot does not run stock loops; smoke mode only fetches 000001.SZ once"))
        return items

    def _a_share_pilot_items(self, start_date: str | None, end_date: str | None, max_jobs: int) -> list[MirrorPlanItem]:
        if not start_date or not end_date:
            raise ValueError("pilot mode requires --start-date and --end-date")
        items = [
            self._fetch_item("stock_basic", "snapshot_reference", {"list_status": "L"}, max_jobs=1),
            self._fetch_item("stock_company", "snapshot_reference", {"exchange": "SSE"}, max_jobs=1),
            self._fetch_item(
                "trade_cal",
                "calendar_dependency",
                {"exchange": "SSE", "start_date": start_date, "end_date": end_date},
                max_jobs=1,
                planned_action="fetch_calendar",
                required_by=A_SHARE_LOW_RISK_DAILY_LIKE_APIS,
            ),
            self._fetch_item("hs_const", "snapshot_reference", {"hs_type": "SH", "is_new": "1"}, max_jobs=1),
            self._fetch_item("concept", "snapshot_reference", {"src": "ts"}, max_jobs=1),
            self._fetch_item("index_basic", "snapshot_reference", {"market": "SSE"}, max_jobs=1),
            self._fetch_item("ths_index", "snapshot_reference", {"exchange": "A", "type": "N"}, max_jobs=1),
            self._fetch_item("index_classify", "snapshot_reference", {"src": "SW2021", "level": "L1"}, max_jobs=1),
        ]
        trade_cal_ready = bool(self.catalog.latest_snapshot("trade_cal"))
        for endpoint in A_SHARE_LOW_RISK_DAILY_LIKE_APIS:
            if not trade_cal_ready:
                items.append(self._blocked_item(endpoint, "daily_like", True, max_jobs, "missing_trade_cal_snapshot", plan_status="blocked_until_trade_cal", notes="calendar-aware backfill waits for local trade_cal latest snapshot"))
            else:
                items.append(self._calendar_backfill_item(endpoint, start_date, end_date, max_jobs))
        weekly_dates = self._pilot_weekly_dates(start_date, end_date)
        monthly_dates = self._pilot_monthly_dates(start_date, end_date)
        for endpoint in ["weekly", "index_weekly"]:
            items.append(self._date_backfill_item(endpoint, weekly_dates, max_jobs, requires_trade_cal=False, notes="weekly uses bounded explicit date planning only"))
        for endpoint in ["monthly", "index_monthly"]:
            items.append(self._date_backfill_item(endpoint, monthly_dates, max_jobs, requires_trade_cal=False, notes="monthly uses bounded explicit date planning only"))
        for endpoint in A_SHARE_LOW_RISK_PLAN_ONLY_APIS:
            items.append(self._plan_only_item(endpoint, "plan_only", "excluded from pilot execution; no stock, concept, or index-code loop execution is allowed"))
        return items

    def _fetch_item(self, endpoint: str, category: str, params: dict[str, Any], max_jobs: int, *, planned_action: str = "fetch", required_by: list[str] | None = None) -> MirrorPlanItem:
        plan = JobPlanner(self.root, self.catalog).plan_single_fetch(endpoint, params)
        status = "skip_existing" if plan.existing_active_data else "planned"
        return MirrorPlanItem(
            endpoint=endpoint,
            category=category,
            requires_trade_cal=False,
            plan_status=status,
            planned_jobs=0 if plan.existing_active_data else 1,
            max_jobs=max_jobs,
            existing_coverage="active_exists" if plan.existing_active_data else "missing",
            missing_jobs=0 if plan.existing_active_data else 1,
            blocked_reason=None,
            will_execute=not plan.existing_active_data,
            params=plan.params,
            permission_status=plan.permission_status,
            planned_action="skip_existing" if plan.existing_active_data else planned_action,
            required_by=required_by,
        )

    def _calendar_backfill_item(self, endpoint: str, start_date: str, end_date: str, max_jobs: int) -> MirrorPlanItem:
        dates, calendar = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(
            start_date=start_date,
            end_date=end_date,
            trading_days_only=True,
            calendar_exchange="SSE",
        )
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs, calendar_metadata=calendar)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        status = "skip_existing" if skipped == len(plan.planned_jobs) and plan.planned_jobs else "planned"
        return MirrorPlanItem(
            endpoint=endpoint,
            category="daily_like",
            requires_trade_cal=True,
            plan_status=status,
            planned_jobs=len(plan.planned_jobs),
            max_jobs=max_jobs,
            existing_coverage=f"covered={skipped},missing_or_retry={missing}",
            missing_jobs=missing,
            blocked_reason=None,
            will_execute=missing > 0,
            dates=[job.date for job in plan.planned_jobs],
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="calendar_backfill",
        )

    def _date_backfill_item(self, endpoint: str, dates: list[str], max_jobs: int, *, requires_trade_cal: bool, notes: str | None = None) -> MirrorPlanItem:
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs)
        missing = sum(1 for job in plan.planned_jobs if job.planned_action in {"fetch", "retry_failed"})
        skipped = sum(1 for job in plan.planned_jobs if job.planned_action == "skip_existing")
        status = "skip_existing" if skipped == len(plan.planned_jobs) and plan.planned_jobs else "planned"
        return MirrorPlanItem(
            endpoint=endpoint,
            category="date_based",
            requires_trade_cal=requires_trade_cal,
            plan_status=status,
            planned_jobs=len(plan.planned_jobs),
            max_jobs=max_jobs,
            existing_coverage=f"covered={skipped},missing_or_retry={missing}",
            missing_jobs=missing,
            blocked_reason=None,
            will_execute=missing > 0,
            dates=[job.date for job in plan.planned_jobs],
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="date_backfill",
            notes=notes,
        )

    def _blocked_item(self, endpoint: str, category: str, requires_trade_cal: bool, max_jobs: int, reason: str, *, plan_status: str = "blocked", notes: str | None = None) -> MirrorPlanItem:
        return MirrorPlanItem(
            endpoint=endpoint,
            category=category,
            requires_trade_cal=requires_trade_cal,
            plan_status=plan_status,
            planned_jobs=0,
            max_jobs=max_jobs,
            existing_coverage=None,
            missing_jobs=0,
            blocked_reason=reason,
            will_execute=False,
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="blocked",
            notes=notes,
        )

    def _excluded_item(self, endpoint: str, category: str, max_jobs: int, reason: str, notes: str) -> MirrorPlanItem:
        return MirrorPlanItem(
            endpoint=endpoint,
            category=category,
            requires_trade_cal=False,
            plan_status="excluded_from_pilot_execution",
            planned_jobs=0,
            max_jobs=max_jobs,
            existing_coverage=None,
            missing_jobs=0,
            blocked_reason=reason,
            will_execute=False,
            params={"ts_code": "000001.SZ"},
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="excluded",
            notes=notes,
        )

    def _plan_only_item(self, endpoint: str, category: str, reason: str) -> MirrorPlanItem:
        return MirrorPlanItem(
            endpoint=endpoint,
            category=category,
            requires_trade_cal=False,
            plan_status="plan_only_no_execution",
            planned_jobs=0,
            max_jobs=0,
            existing_coverage=None,
            missing_jobs=0,
            blocked_reason=reason,
            will_execute=False,
            permission_status=(self.catalog.latest_permission(endpoint) or {}).get("status"),
            planned_action="plan_only",
            notes="read-only planning only; direct execution remains blocked",
        )

    def _pilot_weekly_dates(self, start_date: str, end_date: str) -> list[str]:
        return _pilot_compatible_period_dates(self.root, self.catalog, start_date, end_date, period="weekly")

    def _pilot_monthly_dates(self, start_date: str, end_date: str) -> list[str]:
        return _pilot_compatible_period_dates(self.root, self.catalog, start_date, end_date, period="monthly")


class MirrorOrchestrator:
    def __init__(self, root: Path | str, catalog: CatalogStore, client, *, sleep=time.sleep):
        self.root = Path(root)
        self.catalog = catalog
        self.client = client
        self.sleep = sleep

    def run(
        self,
        *,
        scope: str,
        mode: str,
        max_jobs_per_api: int,
        start_date: str | None = None,
        end_date: str | None = None,
        backup_target: str | None = None,
        backup_overwrite: bool = False,
        allow_quarantined_retry: bool = False,
    ) -> MirrorRunResult:
        ensure_mirror_scope(scope)
        ensure_mirror_mode(mode)
        if max_jobs_per_api <= 0:
            raise ValueError("--max-jobs-per-api must be positive")
        if max_jobs_per_api > MODE_MAX_JOBS[mode]:
            raise ValueError(f"{mode} mode max-jobs-per-api cannot exceed {MODE_MAX_JOBS[mode]}")
        run_id = self.catalog.create_run("mirror")
        items: list[dict[str, Any]] = []
        probe_statuses = self._probe_all(scope, mode)
        trade_cal_status = self._execute_fetch("trade_cal", self._trade_cal_params(mode, start_date, end_date), run_id, probe_statuses)
        items.append(trade_cal_status)
        if scope == "a-share-low-risk":
            reference_fetches = {
                key: value
                for key, value in A_SHARE_LOW_RISK_REFERENCE_FETCHES.items()
                if key != "trade_cal"
            }
        else:
            reference_fetches = {endpoint: SMOKE_REFERENCE_FETCHES[endpoint] for endpoint in ["stock_basic", "hs_const"]}
        for endpoint, params in reference_fetches.items():
            items.append(self._execute_fetch(endpoint, params, run_id, probe_statuses))
        if self.catalog.latest_snapshot("trade_cal") and trade_cal_status["status"] not in {"failed", "blocked"}:
            calendar_apis = A_SHARE_LOW_RISK_CALENDAR_BACKFILL_APIS if scope == "a-share-low-risk" else SMOKE_CALENDAR_BACKFILL_APIS
            if mode != "smoke":
                calendar_apis = A_SHARE_LOW_RISK_DAILY_LIKE_APIS if scope == "a-share-low-risk" else ["daily", "adj_factor", "daily_basic", "suspend_d"]
            for endpoint in calendar_apis:
                items.append(self._execute_calendar_backfill(endpoint, mode, max_jobs_per_api, start_date, end_date, probe_statuses, allow_quarantined_retry=allow_quarantined_retry))
        else:
            calendar_apis = A_SHARE_LOW_RISK_CALENDAR_BACKFILL_APIS if scope == "a-share-low-risk" else SMOKE_CALENDAR_BACKFILL_APIS
            if mode != "smoke":
                calendar_apis = A_SHARE_LOW_RISK_DAILY_LIKE_APIS if scope == "a-share-low-risk" else ["daily", "adj_factor", "daily_basic", "suspend_d"]
            for endpoint in calendar_apis:
                items.append(self._blocked(endpoint, "daily_like", "missing_or_failed_trade_cal"))
        if mode == "smoke":
            date_groups = A_SHARE_LOW_RISK_EXPLICIT_DATE_APIS.items() if scope == "a-share-low-risk" else SMOKE_EXPLICIT_DATE_APIS.items()
        else:
            if not start_date or not end_date:
                raise ValueError("pilot mode requires --start-date and --end-date")
            if scope == "a-share-low-risk":
                date_groups = [
                    ("weekly", self._pilot_weekly_dates(start_date, end_date)),
                    ("monthly", self._pilot_monthly_dates(start_date, end_date)),
                    ("index_weekly", self._pilot_weekly_dates(start_date, end_date)),
                    ("index_monthly", self._pilot_monthly_dates(start_date, end_date)),
                ]
            else:
                date_groups = [("weekly", self._pilot_weekly_dates(start_date, end_date)), ("monthly", self._pilot_monthly_dates(start_date, end_date))]
        for endpoint, dates in date_groups:
            items.append(self._execute_date_backfill(endpoint, list(dates), max_jobs_per_api, probe_statuses, allow_quarantined_retry=allow_quarantined_retry))
        if mode == "smoke":
            stock_code_fetches = A_SHARE_LOW_RISK_STOCK_CODE_SMOKE_FETCHES if scope == "a-share-low-risk" else {endpoint: SMOKE_REFERENCE_FETCHES[endpoint] for endpoint in ["namechange", "stk_managers", "stk_rewards"]}
            for endpoint, params in stock_code_fetches.items():
                items.append(self._execute_fetch(endpoint, params, run_id, probe_statuses))
        if scope == "a-share-low-risk":
            for endpoint in A_SHARE_LOW_RISK_PLAN_ONLY_APIS:
                if mode == "smoke" and endpoint in A_SHARE_LOW_RISK_STOCK_CODE_SMOKE_FETCHES:
                    continue
                items.append(self._excluded_summary(endpoint, "plan_only", "no stock, concept, or index-code loop execution is allowed"))
        validation_ok, validation_results = Validator(self.root, self.catalog).validate_latest_snapshots(record=True)
        validation = {
            "status": "succeeded" if validation_ok else "failed",
            "results": validation_results,
        }
        backup = None
        restore_check = None
        if backup_target:
            plan = BackupPlanner(self.root, self.catalog).plan(backup_target)
            backup_result = BackupExecutor(self.root, self.catalog).backup(plan, overwrite=backup_overwrite)
            backup = backup_result.to_dict()
            restore = RestoreChecker().check(Path(backup_target))
            restore_check = restore.to_dict()
        summary = self._summary(scope, mode, max_jobs_per_api, backup_target, items, validation, backup, restore_check)
        status = "failed" if summary["failed_endpoints"] or summary.get("critical_dependency_failed") else "succeeded"
        self.catalog.finish_run(run_id, status, None if status == "succeeded" else "mirror had failed endpoints", None, summary)
        return MirrorRunResult(run_id, status, summary, validation, backup, restore_check)

    def _trade_cal_params(self, mode: str, start_date: str | None, end_date: str | None) -> dict[str, Any]:
        if mode == "pilot":
            if not start_date or not end_date:
                raise ValueError("pilot mode requires --start-date and --end-date")
            return {"exchange": "SSE", "start_date": start_date, "end_date": end_date}
        return SMOKE_REFERENCE_FETCHES["trade_cal"]

    def _pilot_weekly_dates(self, start_date: str, end_date: str) -> list[str]:
        return _pilot_compatible_period_dates(self.root, self.catalog, start_date, end_date, period="weekly")

    def _pilot_monthly_dates(self, start_date: str, end_date: str) -> list[str]:
        return _pilot_compatible_period_dates(self.root, self.catalog, start_date, end_date, period="monthly")

    def _probe_all(self, scope: str, mode: str) -> dict[str, str]:
        statuses: dict[str, str] = {}
        thash = token_hash(getattr(self.client, "token", "mirror-client"))
        planner = JobPlanner(self.root, self.catalog)
        endpoints = LOW_RISK_A_SHARE_ENDPOINTS
        if scope == "a-share-low-risk":
            endpoints = [
                *A_SHARE_LOW_RISK_REFERENCE_FETCHES,
                *A_SHARE_LOW_RISK_CALENDAR_BACKFILL_APIS,
                *A_SHARE_LOW_RISK_EXPLICIT_DATE_APIS,
            ]
            if mode == "smoke":
                endpoints.extend(A_SHARE_LOW_RISK_STOCK_CODE_SMOKE_FETCHES)
        for endpoint in dict.fromkeys(endpoints):
            probe = planner.plan_probe(endpoint)
            response, status, error = self._probe(endpoint, probe.params, probe.fields)
            row_count = len(((response.get("data") or {}).get("items")) or [])
            error_type = None if status in {"accessible", "empty_but_accessible"} else status
            self.catalog.record_probe(endpoint, thash, status, probe.params, probe.fields, _valid_until_for(status), error, response, row_count=row_count, error_type=error_type)
            statuses[endpoint] = status
        return statuses

    def _probe(self, api_name: str, params: dict[str, Any], fields: list[str], max_attempts: int = 3) -> tuple[dict[str, Any], str, str | None]:
        attempt = 1
        while True:
            try:
                response = self.client.request(api_name, params, fields)
                status, error = classify_probe_response(response)
            except Exception as exc:
                response = {"error": str(exc)}
                err = classify_exception(exc)
                status, error = err.value, str(exc)
            try:
                retryable = should_retry(status, attempt, max_attempts)
            except ValueError:
                retryable = False
            if status in {"accessible", "empty_but_accessible"} or not retryable:
                return response, status, error
            self.sleep(retry_delay_seconds(status, attempt))
            attempt += 1

    def _permission_blocks(self, endpoint: str, probe_statuses: Mapping[str, str]) -> str | None:
        status = probe_statuses.get(endpoint) or "unknown"
        if status in {"permission_denied", "invalid_params", "invalid_endpoint", "rate_limited", "network_error", "server_error", "unknown_error"}:
            return status
        return None

    def _execute_fetch(self, endpoint: str, params: dict[str, Any], run_id: str, probe_statuses: Mapping[str, str]) -> dict[str, Any]:
        blocked = self._permission_blocks(endpoint, probe_statuses)
        if blocked:
            return self._blocked(endpoint, "fetch", blocked)
        plan = JobPlanner(self.root, self.catalog).plan_single_fetch(endpoint, params)
        if plan.existing_active_data:
            job = self.catalog.get_job(plan.job_key) or {}
            snap = self.catalog.latest_snapshot(endpoint)
            return {
                "endpoint": endpoint,
                "category": "fetch",
                "status": "skipped",
                "planned_jobs": 1,
                "executed_jobs": 0,
                "skipped_jobs": 1,
                "record_count": job.get("record_count"),
                "raw_event_count": job.get("raw_event_count"),
                "snapshot_id": snap.get("snapshot_id") if snap else None,
                "notes": "active data already exists",
            }
        try:
            result = FileLakeStore(self.root, self.catalog).fetch(endpoint, params, self.client, run_id=run_id, finish_run=False)
            job = self.catalog.get_job(result.job_key) or {}
            return {
                "endpoint": endpoint,
                "category": "fetch",
                "status": "succeeded" if result.snapshot_id or result.skipped else "failed",
                "planned_jobs": 1,
                "executed_jobs": 0 if result.skipped else 1,
                "skipped_jobs": 1 if result.skipped else 0,
                "record_count": job.get("record_count") or result.record_count,
                "raw_event_count": job.get("raw_event_count"),
                "snapshot_id": result.snapshot_id,
                "job_key": result.job_key,
            }
        except Exception as exc:
            return {
                "endpoint": endpoint,
                "category": "fetch",
                "status": "failed",
                "planned_jobs": 1,
                "executed_jobs": 1,
                "skipped_jobs": 0,
                "record_count": None,
                "raw_event_count": None,
                "snapshot_id": None,
                "error_type": classify_exception(exc).value,
                "error": str(exc),
            }

    def _execute_calendar_backfill(self, endpoint: str, mode: str, max_jobs: int, start_date: str | None, end_date: str | None, probe_statuses: Mapping[str, str], *, allow_quarantined_retry: bool = False) -> dict[str, Any]:
        if self._permission_blocks(endpoint, probe_statuses):
            return self._blocked(endpoint, "daily_like", self._permission_blocks(endpoint, probe_statuses) or "blocked")
        start = start_date if mode == "pilot" else "20250101"
        end = end_date if mode == "pilot" else "20250110"
        dates, calendar = DatePlanner(self.root, self.catalog).plan_dates_with_metadata(start_date=start, end_date=end, trading_days_only=True, calendar_exchange="SSE")
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs, calendar_metadata=calendar, allow_quarantined_retry=allow_quarantined_retry)
        result = BackfillExecutor(self.root, self.catalog).execute(plan, self.client, validate_latest=True)
        return self._backfill_item(endpoint, "daily_like", result)

    def _execute_date_backfill(self, endpoint: str, dates: list[str], max_jobs: int, probe_statuses: Mapping[str, str], *, allow_quarantined_retry: bool = False) -> dict[str, Any]:
        if self._permission_blocks(endpoint, probe_statuses):
            return self._blocked(endpoint, "date_based", self._permission_blocks(endpoint, probe_statuses) or "blocked")
        if not dates:
            return {
                "endpoint": endpoint,
                "category": "date_based",
                "status": "skipped",
                "planned_jobs": 0,
                "executed_jobs": 0,
                "skipped_jobs": 0,
                "record_count": 0,
                "raw_event_count": 0,
                "snapshot_id": None,
                "notes": "no explicit period dates in bounded window",
            }
        plan = BackfillPlanner(self.root, self.catalog).plan_date_backfill(endpoint, dates, max_jobs=max_jobs, allow_quarantined_retry=allow_quarantined_retry)
        result = BackfillExecutor(self.root, self.catalog).execute(plan, self.client, validate_latest=True)
        return self._backfill_item(endpoint, "date_based", result)

    def _backfill_item(self, endpoint: str, category: str, result) -> dict[str, Any]:
        summary = result.summary
        return {
            "endpoint": endpoint,
            "category": category,
            "status": result.status,
            "planned_jobs": summary.get("planned_jobs"),
            "executed_jobs": summary.get("executed_jobs"),
            "skipped_jobs": summary.get("skipped_jobs"),
            "succeeded_jobs": summary.get("succeeded_jobs"),
            "failed_jobs": summary.get("failed_jobs"),
            "blocked_jobs": summary.get("blocked_jobs"),
            "record_count": sum(int(row.record_count or 0) for row in result.results),
            "raw_event_count": sum(int(row.raw_event_count or 0) for row in result.results),
            "snapshot_id": (self.catalog.latest_snapshot(endpoint) or {}).get("snapshot_id"),
            "backfill_run_id": result.run_id,
        }

    def _blocked(self, endpoint: str, category: str, reason: str) -> dict[str, Any]:
        return {
            "endpoint": endpoint,
            "category": category,
            "status": "blocked",
            "planned_jobs": 0,
            "executed_jobs": 0,
            "skipped_jobs": 0,
            "blocked_reason": reason,
        }

    def _excluded_summary(self, endpoint: str, category: str, reason: str) -> dict[str, Any]:
        return {
            "endpoint": endpoint,
            "category": category,
            "status": "excluded",
            "planned_jobs": 0,
            "executed_jobs": 0,
            "skipped_jobs": 0,
            "blocked_reason": reason,
        }

    def _summary(self, scope: str, mode: str, max_jobs: int, backup_target: str | None, items: list[dict[str, Any]], validation: dict[str, Any], backup: dict[str, Any] | None, restore_check: dict[str, Any] | None) -> dict[str, Any]:
        failed = [item for item in items if item.get("status") == "failed"]
        blocked = [item for item in items if item.get("status") == "blocked"]
        critical_dependency_failed = any(item.get("endpoint") == "trade_cal" and item.get("status") in {"failed", "blocked"} for item in items)
        succeeded = [item for item in items if item.get("status") == "succeeded"]
        skipped = [item for item in items if item.get("status") == "skipped"]
        return {
            "scope": scope,
            "mode": mode,
            "execute": True,
            "endpoint_count": len(items),
            "succeeded_endpoints": len(succeeded),
            "skipped_endpoints": len(skipped),
            "blocked_endpoints": len(blocked),
            "failed_endpoints": len(failed),
            "critical_dependency_failed": critical_dependency_failed,
            "total_jobs_executed": sum(int(item.get("executed_jobs") or 0) for item in items),
            "max_jobs_per_api": max_jobs,
            "backup_target": backup_target,
            "backup_status": (backup or {}).get("status") if backup else "not_requested",
            "restore_check_status": (restore_check or {}).get("status") if restore_check else "not_requested",
            "validation_status": validation.get("status"),
            "created_at": now_utc(),
            "items": items,
        }


def init_catalog_if_requested(root: Path, init_if_missing: bool) -> CatalogStore:
    catalog = CatalogStore(root)
    if catalog.db_path.exists():
        return catalog
    if not init_if_missing:
        raise FileNotFoundError(f"catalog not found: {catalog.db_path}; run init-catalog first or pass --init-if-missing")
    catalog.init()
    load_into_catalog(root, catalog)
    return catalog
