from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .api_infra import ApiInfrastructureReadinessReporter
from .backup import BackupExecutor, BackupInspector, BackupPlanner, RestoreChecker
from .backfill import (
    BackfillExecutor,
    BackfillPlanner,
    DatePlanner,
    PHASE21_EXECUTE_MAX_JOBS,
    TRADING_DAY_BACKFILL_APIS,
    execution_to_rows,
    plan_to_rows,
)
from .catalog import CatalogStore
from .client import TushareClient, classify_probe_response
from .code_period_planner import CodePeriodPlanner
from .code_date_matrix_planner import CodeDateMatrixPlanner
from .code_list_planner import CodeListPlanner
from .code_universe import CodeUniverseProvider
from .compaction import CompactionPlanner
from .coverage import CoverageReporter
from .enablement import EndpointEnablementChecklistReporter
from .endpoints import load_into_catalog
from .errors import ErrorType, classify_exception, retry_delay_seconds, should_retry
from .financial_command import FinancialPullCommandReporter
from .financial_reports import FinancialCoverageMatrixReporter, FinancialReadinessReporter, FinancialRequestEstimateReporter
from .financial_probe import HKUSFinancialProbeReporter
from .hashing import token_hash
from .intraday_plan import IntradayPlanner
from .missing_backfill import MissingBackfillPlanner
from .mirror import BackupStatusReporter, CommandSafetyAnalyzer, ExecuteReadinessReporter, ExecuteScriptReporter, FinalGateReporter, MirrorAuditReporter, MirrorAutoSyncCommandReporter, MirrorAutoSyncRecoveryPlanReporter, MirrorAutoSyncReporter, MirrorAutoSyncStatusReporter, MirrorBatchBundleReporter, MirrorBatchBundleVerifier, MirrorBatchCertificateReporter, MirrorBatchLedgerReporter, MirrorBatchPlanner, MirrorBatchRehearsalReporter, MirrorCoverageMatrixReporter, MirrorFailureDrillReporter, MirrorNextBatchReporter, MirrorOperatorChecklistReporter, MirrorOpsReportReporter, MirrorOrchestrator, MirrorPlanner, MirrorPreflightChecker, MirrorPullCommandReporter, MirrorReadinessReporter, MirrorReviewer, MirrorScopeReporter, MirrorStatusReporter, MonthlyPromotionChecklistReporter, PathDiagnosticsReporter, RequestEstimateReporter, SchemaStatusReporter, StopPolicyReporter, TokenHygieneScanner, init_catalog_if_requested
from .object_plan import ObjectPlanner
from .period_planner import PeriodPlanner
from .pit import PITReadinessReporter
from .planner import JobPlanner
from .rate_policy import RatePolicyReporter
from .reader import LakeReader
from .store import FileLakeStore
from .storage_estimate import StorageEstimator
from .validation import Validator


def load_dotenv(path: Path = Path('.env')) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        os.environ.setdefault(key.strip(), value.strip())


def valid_until_for(status: str) -> str:
    now = datetime.now(timezone.utc)
    if status in {'accessible', 'empty_but_accessible'}:
        delta = timedelta(days=7)
    elif status in {'rate_limited', 'network_error', 'server_error', 'unknown_error'}:
        delta = timedelta(days=1)
    else:
        delta = timedelta(days=30)
    return (now + delta).isoformat().replace('+00:00', 'Z')


def require_token() -> str:
    load_dotenv()
    token = os.environ.get('TUSHARE_TOKEN')
    if not token:
        raise SystemExit('TUSHARE_TOKEN is required')
    return token


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _stringify(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _print_table(rows: Iterable[dict[str, Any]], columns: list[str]) -> None:
    materialized = list(rows)
    widths = {col: len(col) for col in columns}
    for row in materialized:
        for col in columns:
            widths[col] = max(widths[col], min(len(_stringify(row.get(col))), 80))
    print('  '.join(col.ljust(widths[col]) for col in columns))
    print('  '.join('-' * widths[col] for col in columns))
    for row in materialized:
        cells = []
        for col in columns:
            value = _stringify(row.get(col))
            if len(value) > 80:
                value = value[:77] + '...'
            cells.append(value.ljust(widths[col]))
        print('  '.join(cells))
    print(f'total={len(materialized)}')


def _print_key_values(row: dict[str, Any]) -> None:
    _print_table([{'key': k, 'value': v} for k, v in row.items()], ['key', 'value'])


def cmd_init_catalog(args) -> int:
    root = Path(args.root)
    catalog = CatalogStore(root)
    catalog.init()
    configs = load_into_catalog(root, catalog)
    print(f'initialized catalog: {catalog.db_path}')
    print(f'loaded endpoints: {len(configs)}')
    return 0


def _ensure_catalog(root: Path) -> CatalogStore:
    catalog = CatalogStore(root)
    catalog.init()
    load_into_catalog(root, catalog)
    return catalog


def _open_existing_catalog(root: Path) -> CatalogStore:
    catalog = CatalogStore(root)
    if not catalog.db_path.exists():
        raise SystemExit(f"catalog not found: {catalog.db_path}; run init-catalog first")
    return catalog


def _probe_request_with_retry(client: TushareClient, api_name: str, params: dict[str, Any], fields: list[str], max_attempts: int = 3) -> tuple[dict[str, Any], str, str | None]:
    attempt = 1
    while True:
        try:
            response = client.request(api_name, params, fields)
            status, error = classify_probe_response(response)
        except Exception as e:
            response = {'error': str(e)}
            err = classify_exception(e)
            status, error = err.value, str(e)
        if status in {'accessible', 'empty_but_accessible'}:
            return response, status, error
        try:
            retryable = should_retry(status, attempt, max_attempts)
        except ValueError:
            retryable = False
        if retryable:
            time.sleep(retry_delay_seconds(status, attempt))
            attempt += 1
            continue
        return response, status, error


def cmd_probe(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    token = require_token()
    client = TushareClient(token)
    endpoints = []
    if args.all:
        endpoints = [e['api_name'] for e in catalog.list_endpoints(args.family)]
    elif args.family:
        endpoints = [e['api_name'] for e in catalog.list_endpoints(args.family)]
    elif args.api:
        endpoints = [args.api]
    else:
        raise SystemExit('probe requires --api, --family, or --all')
    thash = token_hash(token)
    exit_code = 0
    outputs: list[dict[str, Any]] = []
    planner = JobPlanner(root, catalog)
    for api_name in endpoints:
        cfg = catalog.get_endpoint_config(api_name)
        probe_cfg = cfg.get('probe') or {}
        probe_plan = planner.plan_probe(api_name)
        params = probe_plan.params
        fields = probe_plan.fields
        response, status, error = _probe_request_with_retry(client, api_name, params, fields)
        row_count = len(((response.get('data') or {}).get('items')) or [])
        error_type = None if status in {'accessible', 'empty_but_accessible'} else status
        if status == 'empty_but_accessible' and not probe_cfg.get('allow_empty_probe'):
            exit_code = 1
        if status not in {'accessible', 'empty_but_accessible'}:
            exit_code = 1
        catalog.record_probe(api_name, thash, status, params, fields, valid_until_for(status), error, response, row_count=row_count, error_type=error_type)
        outputs.append({'api_name': api_name, 'status': status, 'row_count': row_count, 'error_type': error_type, 'error_message': error})
    if args.json:
        _print_json(outputs)
    else:
        _print_table(outputs, ['api_name', 'status', 'row_count', 'error_type', 'error_message'])
    return exit_code


def cmd_fetch(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    params = json.loads(args.params)
    store = FileLakeStore(root, catalog)
    if args.dry_run:
        plan = store.plan_fetch(args.api, params)
        if args.json:
            _print_json(plan)
        else:
            _print_key_values(plan)
        return 0
    token = require_token()
    result = store.fetch(args.api, params, TushareClient(token))
    output = {
        'run_id': result.run_id,
        'job_key': result.job_key,
        'snapshot_id': result.snapshot_id,
        'record_count': result.record_count,
        'skipped': result.skipped,
    }
    if args.json:
        _print_json(output)
    elif result.skipped:
        print(f'skipped existing job: {result.job_key}')
    else:
        for key, value in output.items():
            print(f'{key}={value}')
    return 0 if result.snapshot_id or result.skipped else 1


def _backfill_max_jobs(args, execute: bool) -> int:
    if execute and args.max_jobs is None:
        raise SystemExit('backfill --execute requires --max-jobs')
    max_jobs = args.max_jobs if args.max_jobs is not None else 20
    if max_jobs <= 0:
        raise SystemExit('--max-jobs must be positive')
    if execute and max_jobs > PHASE21_EXECUTE_MAX_JOBS:
        raise SystemExit(f'Refusing to execute {max_jobs} jobs in Phase 2.1. max allowed: {PHASE21_EXECUTE_MAX_JOBS}.')
    return max_jobs


def _make_backfill_plan(args, execute: bool):
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    try:
        if args.trading_days_only and args.api not in TRADING_DAY_BACKFILL_APIS:
            raise ValueError("trading-days-only is only supported for daily-like endpoints in Phase 2.4")
        dates, calendar_metadata = DatePlanner(root, catalog).plan_dates_with_metadata(
            dates=args.dates,
            start_date=args.start_date,
            end_date=args.end_date,
            trading_days_only=args.trading_days_only,
            calendar_exchange=args.calendar_exchange,
        )
        max_jobs = _backfill_max_jobs(args, execute)
        plan = BackfillPlanner(root, catalog).plan_date_backfill(
            args.api,
            dates,
            max_jobs=max_jobs,
            dry_run=not execute,
            calendar_metadata=calendar_metadata,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    return root, catalog, plan


def _print_backfill_plan(plan, as_json: bool) -> None:
    if as_json:
        _print_json(plan.to_dict())
        return
    _print_table(plan_to_rows(plan), ['api_name', 'date', 'job_key', 'existing_status', 'planned_action', 'partition', 'raw_path', 'lake_path_prefix'])
    summary = {}
    if plan.calendar_source:
        summary.update({
            'calendar_source': plan.calendar_source,
            'exchange': plan.exchange,
            'requested_start_date': plan.requested_start_date,
            'requested_end_date': plan.requested_end_date,
            'natural_days': plan.natural_days,
            'trading_days': plan.trading_days,
            'filtered_non_trading_days': plan.filtered_non_trading_days,
            'filtered_non_trading_dates': plan.filtered_non_trading_dates,
            'truncated_by_max_jobs': plan.truncated_by_max_jobs,
        })
    summary.update({
        'total_candidate_jobs': plan.total_candidate_jobs,
        'planned_jobs': len(plan.planned_jobs),
        'skipped_jobs': plan.skipped_jobs,
        'blocked_jobs': plan.blocked_jobs,
        'max_jobs': plan.max_jobs,
        'dry_run': plan.dry_run,
        'warnings': plan.warnings,
    })
    if not plan.calendar_source:
        summary['truncated_by_max_jobs'] = plan.truncated_by_max_jobs
    _print_key_values(summary)


def cmd_backfill_plan(args) -> int:
    _, _, plan = _make_backfill_plan(args, execute=False)
    _print_backfill_plan(plan, args.json)
    return 0


def cmd_backfill(args) -> int:
    execute = bool(args.execute)
    root, catalog, plan = _make_backfill_plan(args, execute=execute)
    if not execute:
        _print_backfill_plan(plan, args.json)
        return 0
    token = require_token()
    store = FileLakeStore(root, catalog)
    result = BackfillExecutor(root, catalog, store).execute(
        plan,
        TushareClient(token),
        validate_latest=args.validate_latest,
        stop_on_error=args.stop_on_error,
    )
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_table(execution_to_rows(result), ['date', 'job_key', 'action', 'status', 'record_count', 'raw_event_count', 'snapshot_id', 'error_type'])
        _print_key_values(result.summary)
        if result.validation:
            _print_table([result.validation], ['validation_id', 'scope', 'api_name', 'snapshot_id', 'status', 'checked_file_count', 'failure_count', 'record_count', 'raw_event_count'])
    nonfatal = {'permission_denied', 'empty_result'}
    fatal_failures = [row for row in result.results if row.status in {'failed', 'blocked'} and row.error_type not in nonfatal]
    return 1 if fatal_failures else 0


def _print_coverage_report(report, as_json: bool) -> None:
    if as_json:
        _print_json(report.to_dict())
        return
    _print_table(
        [item.to_dict() for item in report.items],
        [
            'date',
            'existing_status',
            'planned_action',
            'job_key',
            'snapshot_id',
            'record_count',
            'raw_event_count',
            'file_count',
            'last_job_status',
            'last_error_type',
            'notes',
        ],
    )
    _print_key_values({
        'api_name': report.api_name,
        'requested_start_date': report.requested_start_date,
        'requested_end_date': report.requested_end_date,
        'calendar_source': report.calendar_source,
        'calendar_exchange': report.calendar_exchange,
        'natural_days': report.natural_days,
        'trading_days': report.trading_days,
        'filtered_non_trading_days': report.filtered_non_trading_days,
        'filtered_non_trading_dates': report.filtered_non_trading_dates,
        'total_dates': report.total_dates,
        'covered_dates': report.covered_dates,
        'missing_dates': report.missing_dates,
        'failed_dates': report.failed_dates,
        'quarantined_dates': report.quarantined_dates,
        'coverage_ratio': report.coverage_ratio,
    })


def cmd_coverage(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    try:
        report = CoverageReporter(root, catalog).report(
            args.api,
            dates=args.dates,
            start_date=args.start_date,
            end_date=args.end_date,
            trading_days_only=args.trading_days_only,
            calendar_exchange=args.calendar_exchange,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    _print_coverage_report(report, args.json)
    return 0


def cmd_backup_inspect(args) -> int:
    result = BackupInspector().inspect(Path(args.backup))
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        if result.catalog_counts:
            _print_key_values({f'catalog_{key}': value for key, value in result.catalog_counts.items()})
        if result.errors:
            _print_table(result.errors, ['reason', 'field', 'expected', 'actual', 'details'])
        if result.warnings:
            _print_table(result.warnings, ['reason', 'field', 'details'])
    return 0 if result.status == 'succeeded' else 1


def _print_backup_plan(plan, as_json: bool) -> None:
    if as_json:
        _print_json(plan.to_dict())
        return
    _print_key_values(plan.summary())
    if plan.rejected_reason:
        print('No active snapshots to backup.')


def cmd_backup_plan(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    plan = BackupPlanner(root, catalog).plan(args.target, args.api)
    _print_backup_plan(plan, args.json)
    return 0


def cmd_backup(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    plan = BackupPlanner(root, catalog).plan(args.target, args.api)
    try:
        result = BackupExecutor(root, catalog).backup(plan, overwrite=args.overwrite)
    except (ValueError, FileExistsError, FileNotFoundError) as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 0


def cmd_restore_check(args) -> int:
    result = RestoreChecker().check(Path(args.backup))
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        _print_key_values({
            'restore_check_scope': 'validates backup artifact only',
            'restore_check_writes': 'none',
            'read_as_root': 'use --root <backup> with catalog-inspect/list-files/coverage',
            'validate_note': 'validate on a backup root writes validation_runs unless --no-record is used',
        })
        if result.failures:
            _print_table(result.failures, ['reason', 'file_id', 'path', 'expected', 'actual', 'details'])
    return 0 if result.status == 'succeeded' else 1


def _missing_backfill_max_jobs(args, execute: bool) -> int:
    if args.max_jobs is None:
        raise SystemExit('backfill-missing requires --max-jobs')
    if args.max_jobs <= 0:
        raise SystemExit('--max-jobs must be positive')
    if execute and args.max_jobs > PHASE21_EXECUTE_MAX_JOBS:
        raise SystemExit(f'Refusing to execute {args.max_jobs} jobs in Phase 2.7. max allowed: {PHASE21_EXECUTE_MAX_JOBS}.')
    return args.max_jobs


def _make_missing_backfill_plan(args, execute: bool):
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    try:
        plan = MissingBackfillPlanner(root, catalog).plan(
            args.api,
            dates=args.dates,
            start_date=args.start_date,
            end_date=args.end_date,
            trading_days_only=args.trading_days_only,
            calendar_exchange=args.calendar_exchange,
            max_jobs=_missing_backfill_max_jobs(args, execute),
            retry_failed=args.retry_failed,
            execute=execute,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    return root, catalog, plan


def _print_missing_backfill_plan(plan, as_json: bool) -> None:
    if as_json:
        _print_json(plan.to_dict())
        return
    _print_table(
        [item.to_dict() for item in plan.items],
        ['date', 'existing_status', 'planned_action', 'will_execute', 'job_key', 'snapshot_id', 'record_count', 'raw_event_count', 'notes'],
    )
    _print_key_values(plan.summary())
    if not plan.planned_jobs:
        print('No missing jobs to backfill.')


def _missing_execution_summary(plan, result) -> dict[str, Any]:
    validation_status = (result.validation or {}).get('status') if result.validation else None
    return {
        'api_name': plan.api_name,
        'candidate_jobs': plan.candidate_jobs,
        'planned_jobs': plan.planned_jobs,
        'executed_jobs': result.summary.get('executed_jobs'),
        'skipped_jobs': result.summary.get('skipped_jobs'),
        'succeeded_jobs': result.summary.get('succeeded_jobs'),
        'failed_jobs': result.summary.get('failed_jobs'),
        'blocked_jobs': int(result.summary.get('blocked_jobs') or 0) + plan.blocked_jobs,
        'quarantined_jobs': int(result.summary.get('quarantined_jobs') or 0) + plan.coverage.get('quarantined_dates', 0),
        'validate_latest_status': validation_status,
        'dry_run': False,
        'execute': True,
        'retry_failed': plan.retry_failed,
        'truncated_by_max_jobs': plan.truncated_by_max_jobs,
        'warnings': plan.warnings,
    }


def cmd_backfill_missing(args) -> int:
    execute = bool(args.execute)
    root, catalog, plan = _make_missing_backfill_plan(args, execute=execute)
    if not execute:
        _print_missing_backfill_plan(plan, args.json)
        return 0
    if not plan.planned_jobs:
        if args.json:
            _print_json({'missing_plan': plan.to_dict(), 'execution': None, 'message': 'No missing jobs to backfill.'})
        else:
            _print_missing_backfill_plan(plan, False)
        return 0
    token = require_token()
    result = BackfillExecutor(root, catalog, FileLakeStore(root, catalog)).execute(
        plan.backfill_plan,
        TushareClient(token),
        validate_latest=args.validate_latest,
        stop_on_error=args.stop_on_error,
    )
    if args.json:
        _print_json({'missing_plan': plan.to_dict(), 'execution': result.to_dict(), 'summary': _missing_execution_summary(plan, result)})
    else:
        _print_table(execution_to_rows(result), ['date', 'job_key', 'action', 'status', 'record_count', 'raw_event_count', 'snapshot_id', 'error_type'])
        _print_key_values(_missing_execution_summary(plan, result))
        if result.validation:
            _print_table([result.validation], ['validation_id', 'scope', 'api_name', 'snapshot_id', 'status', 'checked_file_count', 'failure_count', 'record_count', 'raw_event_count'])
    nonfatal = {'permission_denied', 'empty_result'}
    fatal_failures = [row for row in result.results if row.status in {'failed', 'blocked'} and row.error_type not in nonfatal]
    return 1 if fatal_failures else 0


def _print_validation_reports(reports: list[dict[str, Any]], overall_ok: bool, as_json: bool) -> None:
    if as_json:
        _print_json({'status': 'succeeded' if overall_ok else 'failed', 'results': reports})
    else:
        _print_table(reports, ['validation_id', 'scope', 'api_name', 'snapshot_id', 'status', 'checked_file_count', 'failure_count', 'record_count', 'raw_event_count'])
        print('overall_status=' + ('succeeded' if overall_ok else 'failed'))


def cmd_mirror_preflight(args) -> int:
    result = MirrorPreflightChecker().check(
        mirror_root=args.mirror_root,
        backup_target=args.backup_target,
        scope=args.scope,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        max_jobs_per_api=args.max_jobs_per_api,
    )
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == 'blocked' else 0


def cmd_mirror_scope(args) -> int:
    try:
        result = MirrorScopeReporter().report(scope=args.scope)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        _print_table(
            [
                {"status": "executable_now", "endpoint": endpoint}
                for endpoint in result.executable_now
            ]
            + [
                {"status": "plan_only", "endpoint": endpoint}
                for endpoint in result.plan_only
            ]
            + [
                {"status": "disabled", "endpoint": endpoint}
                for endpoint in result.disabled
            ],
            ["status", "endpoint"],
        )
    return 0


def cmd_hk_us_financial_probe_report(args) -> int:
    report = HKUSFinancialProbeReporter().report(input_path=args.input)
    payload = report.to_dict()
    if args.json:
        _print_json(payload)
    else:
        summary = dict(payload)
        endpoints = summary.pop("endpoints", [])
        _print_key_values(summary)
        if endpoints:
            _print_table(
                endpoints,
                [
                    "api_name",
                    "probe_status",
                    "raw_executable_candidate",
                    "pit_safe_candidate",
                    "pit_usable_after_status",
                    "recommended_execution_status",
                ],
            )
    return 1 if report.blocking_errors else 0


def cmd_financial_readiness(args) -> int:
    report = FinancialReadinessReporter().report(scope=args.scope, root=args.root_arg)
    if args.json:
        _print_json(report.to_dict())
    else:
        _print_key_values(report.summary())
        if report.items:
            _print_table(
                report.items,
                ["api_name", "probe_status", "raw_ready", "pit_safe_ready", "pit_usable_after_status", "recommended_execution_status"],
            )
    return 1 if report.blocking_errors else 0


def cmd_financial_request_estimate(args) -> int:
    report = FinancialRequestEstimateReporter().report(
        scope=args.scope,
        from_period=args.from_period,
        to_period=args.to_period,
        limit_codes=args.limit_codes,
        max_periods=args.max_periods,
    )
    if args.json:
        _print_json(report.to_dict())
    else:
        _print_key_values(report.summary())
        if report.items:
            _print_table(report.items, ["api_name", "raw_ready", "pit_safe_ready", "estimated_requests", "not_a_quota_guarantee"])
    return 1 if report.blocking_errors else 0


def cmd_financial_coverage_matrix(args) -> int:
    report = FinancialCoverageMatrixReporter().report(
        root=args.root_arg,
        scope=args.scope,
        periods=args.periods,
        limit_codes=args.limit_codes,
        universe=args.universe,
    )
    if args.json:
        _print_json(report.to_dict())
    else:
        _print_key_values(report.summary())
        if report.items:
            _print_table(report.items, ["api_name", "coverage_class", "total_code_periods", "covered_code_periods", "missing_code_periods", "coverage_ratio", "status"])
    return 1 if report.blocking_errors else 0


def cmd_financial_pull_command(args) -> int:
    report = FinancialPullCommandReporter().create(
        scope=args.scope,
        root=args.root_arg,
        backup=args.backup,
        from_period=args.from_period,
        to_period=args.to_period,
        limit_codes=args.limit_codes,
        max_periods=args.max_periods,
        output=args.output,
        overwrite=args.overwrite,
    )
    if args.json:
        _print_json(report.to_dict())
    else:
        _print_key_values(report.summary())
    return 1 if report.blocking_errors else 0


def cmd_mirror_review(args) -> int:
    try:
        result = MirrorReviewer().review(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
            mode=args.mode,
            start_date=args.start_date,
            end_date=args.end_date,
            calendar_exchange=args.calendar_exchange,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        if result.endpoint_summary:
            _print_table(result.endpoint_summary, ['endpoint', 'status', 'snapshot_id', 'record_count', 'raw_event_count', 'raw_files', 'lake_files'])
        if result.coverage_summary:
            _print_table(result.coverage_summary, ['api_name', 'total_dates', 'covered_dates', 'missing_dates', 'failed_dates', 'quarantined_dates', 'coverage_ratio'])
    return 1 if result.blocking_errors else 0


def cmd_mirror_readiness(args) -> int:
    try:
        result = MirrorReadinessReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        _print_table(
            [
                {
                    'check': name,
                    'required': check.get('required'),
                    'passed': check.get('passed'),
                    'message': check.get('message'),
                }
                for name, check in result.checks.items()
            ],
            ['check', 'required', 'passed', 'message'],
        )
    return 1 if result.readiness_status == 'blocked' else 0


def cmd_mirror_status(args) -> int:
    try:
        result = MirrorStatusReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        if result.daily_like_coverage_summary:
            _print_table(
                result.daily_like_coverage_summary,
                ['api_name', 'total_dates', 'covered_dates', 'missing_dates', 'failed_dates', 'quarantined_dates', 'coverage_ratio'],
            )
    return 1 if result.blocking_errors else 0


def cmd_mirror_audit(args) -> int:
    try:
        result = MirrorAuditReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
            since=args.since,
            limit=args.limit,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        if result.snapshot_count_by_api:
            _print_table(
                [
                    {"api_name": api_name, "snapshot_count": count}
                    for api_name, count in result.snapshot_count_by_api.items()
                ],
                ['api_name', 'snapshot_count'],
            )
        if result.failed_jobs:
            _print_table(
                result.failed_jobs,
                ['job_key', 'run_id', 'api_name', 'status', 'last_error_type', 'last_error'],
            )
    return 1 if result.blocking_errors else 0


def cmd_mirror_next_batch(args) -> int:
    try:
        result = MirrorNextBatchReporter().report(
            root=args.mirror_root_arg,
            scope=args.scope,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_mirror_batch_bundle(args) -> int:
    try:
        result = MirrorBatchBundleReporter().create(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
            max_jobs_per_api=args.max_jobs_per_api,
            output=args.output,
            overwrite=args.overwrite,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_mirror_batch_bundle_verify(args) -> int:
    result = MirrorBatchBundleVerifier().verify(bundle=args.bundle)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_command_safety_check(args) -> int:
    result = CommandSafetyAnalyzer().analyze(file=args.file)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_mirror_batch_rehearse(args) -> int:
    result = MirrorBatchRehearsalReporter().rehearse(root=args.mirror_root_arg, backup=args.backup, bundle=args.bundle)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.rehearsal_status == "blocked" else 0


def cmd_mirror_batch_ledger(args) -> int:
    result = MirrorBatchLedgerReporter().report(root=args.mirror_root_arg, scope=args.scope, bundle=args.bundle)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.ledger_status == "blocked" else 0


def cmd_mirror_batch_certificate(args) -> int:
    result = MirrorBatchCertificateReporter().create(
        root=args.mirror_root_arg,
        backup=args.backup,
        scope=args.scope,
        start_date=args.start_date,
        end_date=args.end_date,
        output=args.output,
        overwrite=args.overwrite,
    )
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_mirror_operator_checklist(args) -> int:
    try:
        result = MirrorOperatorChecklistReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_stop_policy(args) -> int:
    try:
        result = StopPolicyReporter().report(scope=args.scope, category=args.category)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 0


def cmd_mirror_failure_drill(args) -> int:
    try:
        result = MirrorFailureDrillReporter().report(scenario=args.scenario, scope=args.scope)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 0


def cmd_path_diagnostics(args) -> int:
    result = PathDiagnosticsReporter().report(root=args.mirror_root_arg, backup=args.backup)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_token_hygiene(args) -> int:
    result = TokenHygieneScanner().scan(path=args.path)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_monthly_promotion_checklist(args) -> int:
    try:
        result = MonthlyPromotionChecklistReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
            from_month=args.from_month,
            to_month=args.to_month,
            bundle=args.bundle,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_mirror_ops_report(args) -> int:
    try:
        result = MirrorOpsReportReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
            next_start_date=args.next_start_date,
            next_end_date=args.next_end_date,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_mirror_final_gate(args) -> int:
    try:
        result = FinalGateReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            bundle=args.bundle,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
            max_jobs_per_api=args.max_jobs_per_api,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        if result.checks:
            _print_table(result.checks, ["name", "status", "passed", "details"])
    return 1 if result.gate_status == "blocked" else 0


def cmd_mirror_execute_script(args) -> int:
    try:
        result = ExecuteScriptReporter().create(
            root=args.mirror_root_arg,
            backup=args.backup,
            bundle=args.bundle,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
            max_jobs_per_api=args.max_jobs_per_api,
            output=args.output,
            overwrite=args.overwrite,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_mirror_execute_readiness(args) -> int:
    try:
        result = ExecuteReadinessReporter().report(
            root=args.mirror_root_arg,
            backup=args.backup,
            bundle=args.bundle,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
            max_jobs_per_api=args.max_jobs_per_api,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.execute_readiness_status == "blocked" else 0


def cmd_mirror_pull_command(args) -> int:
    try:
        result = MirrorPullCommandReporter().create(
            scope=args.scope,
            root=args.mirror_root_arg,
            backup=args.backup,
            start_date=args.start_date,
            end_date=args.end_date,
            max_jobs_per_api=args.max_jobs_per_api,
            output=args.output,
            overwrite=args.overwrite,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_mirror_auto_sync(args) -> int:
    client = TushareClient(require_token()) if args.execute else None
    try:
        result = MirrorAutoSyncReporter().create(
            root=args.mirror_root_arg,
            backup=args.backup,
            scope=args.scope,
            from_date=args.from_date,
            to_date=args.to_date,
            window_days=args.window_days,
            max_jobs_per_api=args.max_jobs_per_api,
            state=args.state,
            execute=args.execute,
            confirm_auto_sync=args.confirm_auto_sync,
            confirm_hk_us_auto_sync=args.confirm_hk_us_auto_sync,
            max_attempts=args.max_attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
            client=client,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        if result.windows:
            _print_table(
                result.windows,
                ["start_date", "end_date", "status", "attempts", "mirror_run_status", "run_id"],
            )
    return 1 if result.status == "blocked" else 0


def cmd_mirror_auto_sync_command(args) -> int:
    result = MirrorAutoSyncCommandReporter().create(
        root=args.mirror_root_arg,
        backup=args.backup,
        scope=args.scope,
        from_date=args.from_date,
        to_date=args.to_date,
        window_days=args.window_days,
        max_jobs_per_api=args.max_jobs_per_api,
        state=args.state,
        output=args.output,
        overwrite=args.overwrite,
        max_attempts=args.max_attempts,
        retry_backoff_seconds=args.retry_backoff_seconds,
    )
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_mirror_auto_sync_status(args) -> int:
    result = MirrorAutoSyncStatusReporter().report(state=args.state)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_mirror_auto_sync_recovery_plan(args) -> int:
    result = MirrorAutoSyncRecoveryPlanReporter().report(
        root=args.mirror_root_arg,
        backup=args.backup,
        scope=args.scope,
        state=args.state,
    )
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.status == "blocked" else 0


def cmd_schema_status(args) -> int:
    result = SchemaStatusReporter().report(root=args.mirror_root_arg)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_backup_status(args) -> int:
    result = BackupStatusReporter().report(backup=args.backup)
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_mirror_coverage_matrix(args) -> int:
    try:
        result = MirrorCoverageMatrixReporter().report(
            root=args.mirror_root_arg,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
        if result.items:
            _print_table(result.items, ['api', 'total_dates', 'covered_dates', 'missing_dates', 'coverage_ratio', 'status', 'missing_date_sample'])
    return 1 if result.blocking_errors else 0


def cmd_request_estimate(args) -> int:
    try:
        result = RequestEstimateReporter().report(
            root=args.root_arg,
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary())
    return 1 if result.blocking_errors else 0


def cmd_mirror_batch_plan(args) -> int:
    root = Path(args.mirror_root_arg)
    if not (root / '_catalog' / 'catalog.sqlite').exists():
        raise SystemExit(f"catalog not found: {root / '_catalog' / 'catalog.sqlite'}; run init-catalog first")
    catalog = CatalogStore(root, read_only=True)
    try:
        plan = MirrorBatchPlanner(root, catalog).plan(
            scope=args.scope,
            start_date=args.start_date,
            end_date=args.end_date,
            calendar_exchange=args.calendar_exchange,
            max_jobs_per_api=args.max_jobs_per_api,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.json:
        _print_json(plan.to_dict())
    else:
        _print_table(
            [item.to_dict() for item in plan.endpoint_plans],
            ['endpoint', 'category', 'requires_trade_cal', 'plan_status', 'planned_action', 'total_candidate_jobs', 'planned_jobs', 'missing_jobs', 'skipped_jobs', 'blocked_jobs', 'max_jobs', 'truncated', 'blocked_reason', 'refresh_strategy'],
        )
        _print_key_values(plan.summary())
    return 0


def cmd_api_infra_readiness(args) -> int:
    try:
        report = ApiInfrastructureReadinessReporter().report(scope=args.scope)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    payload = report.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(
            {
                "scope": payload["scope"],
                "enabled_executable_endpoint_count": payload["enabled_executable_endpoint_count"],
                "disabled_inventory_endpoint_count": payload["disabled_inventory_endpoint_count"],
                "supported_endpoint_kinds": payload["supported_endpoint_kinds"],
                "supported_planner_kinds": payload["supported_planner_kinds"],
                "blocked_planner_kinds": payload["blocked_planner_kinds"],
                "code_universe_provider": payload["code_universe_provider"],
                "code_list_planner": payload["code_list_planner"],
                "code_date_matrix_planner": payload["code_date_matrix_planner"],
                "code_date_matrix_existing_status": payload["code_date_matrix_existing_status"],
                "period_planner": payload["period_planner"],
                "code_period_matrix_planner": payload["code_period_matrix_planner"],
                "pit_safety_metadata": payload["pit_safety_metadata"],
                "pit_readiness_report": payload["pit_readiness_report"],
                "object_text_planner": payload["object_text_planner"],
                "object_download_execution": payload["object_download_execution"],
                "intraday_bucket_planner": payload["intraday_bucket_planner"],
                "intraday_execution": payload["intraday_execution"],
                "compaction_planner": payload["compaction_planner"],
                "compaction_execution": payload["compaction_execution"],
                "storage_estimate": payload["storage_estimate"],
                "rate_policy_report": payload["rate_policy_report"],
                "endpoint_enable_checklist": payload["endpoint_enable_checklist"],
                "executable_code_loop": payload["executable_code_loop"],
                "executable_code_date_matrix": payload["executable_code_date_matrix"],
                "executable_period_loop": payload["executable_period_loop"],
                "executable_code_period_loop": payload["executable_code_period_loop"],
                "financial_execution": payload["financial_execution"],
                "max_safe_code_plan_limit": payload["max_safe_code_plan_limit"],
                "max_safe_code_limit": payload["max_safe_code_limit"],
                "max_safe_date_limit": payload["max_safe_date_limit"],
                "max_safe_period_limit": payload["max_safe_period_limit"],
                "max_safe_candidate_jobs": payload["max_safe_candidate_jobs"],
                "missing_for_execution": payload["missing_for_execution"],
                "warnings": payload["warnings"],
            }
        )
        rows = [
            {"category": category, "api_names": api_names, "count": len(api_names)}
            for category, api_names in payload["missing_infrastructure_by_category"].items()
        ]
        _print_table(rows, ["category", "count", "api_names"])
    return 0


def cmd_pit_readiness(args) -> int:
    report = PITReadinessReporter().report()
    payload = report.to_dict()
    if args.json:
        _print_json(payload)
    else:
        summary = dict(payload)
        items = summary.pop("items", [])
        _print_key_values(summary)
        if items:
            _print_table(
                items,
                ["api_name", "endpoint_kind", "planner_kind", "execution_status", "pit_required", "pit_safety_status", "period_field", "usable_after_field", "strategy_safe_default"],
            )
    return 0


def cmd_object_plan(args) -> int:
    plan = ObjectPlanner().plan(api_name=args.api, start_date=args.start_date, end_date=args.end_date)
    payload = plan.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(payload)
    return 1 if plan.blocking_errors else 0


def cmd_intraday_plan(args) -> int:
    plan = IntradayPlanner().plan(
        api_name=args.api,
        freq=args.freq,
        start_date=args.start_date,
        end_date=args.end_date,
        bucket_count=args.bucket_count,
    )
    payload = plan.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(payload)
    return 1 if plan.blocking_errors else 0


def cmd_storage_estimate(args) -> int:
    estimate = StorageEstimator().estimate(
        scope=args.scope,
        category=args.category,
        api_name=args.api,
        freq=args.freq,
        start_date=args.start_date,
        end_date=args.end_date,
        bucket_count=args.bucket_count,
    )
    payload = estimate.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(payload)
    return 1 if estimate.blocking_errors else 0


def cmd_compaction_plan(args) -> int:
    root = Path(args.compaction_root or args.root)
    plan = CompactionPlanner(root).plan(args.api)
    payload = plan.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_table([item.to_dict() for item in plan.candidate_partitions], ["partition_key", "file_count", "small_file_count", "oversized_file_count", "total_size_bytes", "estimated_action"])
        summary = dict(payload)
        summary.pop("candidate_partitions", None)
        _print_key_values(summary)
    return 1 if plan.blocking_errors else 0


def cmd_rate_policy(args) -> int:
    policy = RatePolicyReporter().report(scope=args.scope, category=args.category)
    payload = policy.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(payload)
    return 1 if policy.blocking_errors else 0


def cmd_endpoint_enable_checklist(args) -> int:
    checklist = EndpointEnablementChecklistReporter().report(args.api)
    payload = checklist.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(payload)
    return 1 if checklist.blocking_errors and checklist.current_execution_status is None else 0


def cmd_code_universe(args) -> int:
    root = Path(args.root)
    catalog = CatalogStore(root, read_only=True)
    if not catalog.db_path.exists():
        raise SystemExit(f"catalog not found: {catalog.db_path}; run init-catalog first")
    result = CodeUniverseProvider(root, catalog).get(args.universe, limit=args.limit)
    payload = result.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(payload)
    return 1 if result.blocked else 0


def cmd_code_list_plan(args) -> int:
    root = Path(args.root)
    catalog = CatalogStore(root, read_only=True)
    if not catalog.db_path.exists():
        raise SystemExit(f"catalog not found: {catalog.db_path}; run init-catalog first")
    plan = CodeListPlanner(root, catalog).plan(
        api_name=args.api,
        universe=args.universe,
        limit_codes=args.limit_codes,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    payload = plan.to_dict()
    if args.json:
        _print_json(payload)
    else:
        summary = dict(payload)
        summary.pop("items", None)
        _print_key_values(summary)
        if plan.items:
            _print_table(
                [item.to_dict() for item in plan.items],
                ["api_name", "ts_code", "existing_status", "planned_action", "job_key", "would_require_real_request", "blocked_reason"],
            )
    return 1 if plan.blocked else 0


def cmd_code_date_matrix_plan(args) -> int:
    root = Path(args.root)
    catalog = CatalogStore(root, read_only=True)
    if not catalog.db_path.exists():
        raise SystemExit(f"catalog not found: {catalog.db_path}; run init-catalog first")
    plan = CodeDateMatrixPlanner(root, catalog).plan(
        api_name=args.api,
        universe=args.universe,
        limit_codes=args.limit_codes,
        dates=args.dates,
        start_date=args.start_date,
        end_date=args.end_date,
        max_dates=args.max_dates,
        trading_days_only=args.trading_days_only,
        calendar_exchange=args.calendar_exchange,
    )
    payload = plan.to_dict()
    if args.json:
        _print_json(payload)
    else:
        summary = dict(payload)
        summary.pop("items", None)
        _print_key_values(summary)
        if plan.items:
            _print_table(
                [item.to_dict() for item in plan.items],
                ["api_name", "ts_code", "date", "existing_status", "planned_action", "job_key", "would_require_real_request", "execution_allowed", "blocked_reason"],
            )
    return 1 if plan.blocked else 0


def cmd_period_plan(args) -> int:
    root = Path(args.root)
    catalog = CatalogStore(root, read_only=True)
    plan = PeriodPlanner(root, catalog).plan(
        api_name=args.api,
        periods=args.periods,
        start_period=args.start_period,
        end_period=args.end_period,
        period_frequency=args.period_frequency,
        max_periods=args.max_periods,
    )
    payload = plan.to_dict()
    if args.json:
        _print_json(payload)
    else:
        _print_key_values(payload)
    return 1 if plan.blocked else 0


def cmd_code_period_plan(args) -> int:
    root = Path(args.root)
    catalog = CatalogStore(root, read_only=True)
    if not catalog.db_path.exists():
        raise SystemExit(f"catalog not found: {catalog.db_path}; run init-catalog first")
    plan = CodePeriodPlanner(root, catalog).plan(
        api_name=args.api,
        universe=args.universe,
        limit_codes=args.limit_codes,
        periods=args.periods,
        start_period=args.start_period,
        end_period=args.end_period,
        period_frequency=args.period_frequency,
        max_periods=args.max_periods,
        max_candidate_jobs=args.max_candidate_jobs,
        scope=args.scope,
    )
    payload = plan.to_dict()
    if args.json:
        _print_json(payload)
    else:
        summary = dict(payload)
        summary.pop("items", None)
        _print_key_values(summary)
        if plan.items:
            _print_table(
                [item.to_dict() for item in plan.items],
                ["api_name", "ts_code", "period", "existing_status", "planned_action", "job_key", "pit_required", "pit_safety_status", "would_require_real_request", "execution_allowed", "blocked_reason"],
            )
    return 1 if plan.blocked else 0


def _print_mirror_plan(plan, as_json: bool) -> None:
    if as_json:
        _print_json(plan.to_dict())
        return
    _print_table(
        [item.to_dict() for item in plan.items],
        [
            'endpoint',
            'category',
            'requires_trade_cal',
            'plan_status',
            'planned_jobs',
            'max_jobs',
            'existing_coverage',
            'missing_jobs',
            'blocked_reason',
            'will_execute',
            'planned_action',
            'required_by',
            'notes',
            'permission_status',
        ],
    )
    _print_key_values(plan.summary())


def cmd_mirror_plan(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    try:
        plan = MirrorPlanner(root, catalog).plan(
            scope=args.scope,
            mode=args.mode,
            start_date=args.start_date,
            end_date=args.end_date,
            max_jobs_per_api=args.max_jobs_per_api,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    _print_mirror_plan(plan, args.json)
    return 0


def cmd_mirror_run(args) -> int:
    root = Path(args.root)
    execute = bool(args.execute)
    if not execute:
        print('mirror-run without --execute is dry-run only')
        return cmd_mirror_plan(args)
    if args.max_jobs_per_api is None:
        raise SystemExit('mirror-run --execute requires --max-jobs-per-api')
    try:
        catalog = init_catalog_if_requested(root, args.init_if_missing)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc
    if catalog.db_path.exists():
        load_into_catalog(root, catalog)
    token = require_token()
    result = MirrorOrchestrator(root, catalog, TushareClient(token)).run(
        scope=args.scope,
        mode=args.mode,
        max_jobs_per_api=args.max_jobs_per_api,
        start_date=args.start_date,
        end_date=args.end_date,
        backup_target=args.backup_target,
    )
    if args.json:
        _print_json(result.to_dict())
    else:
        _print_key_values(result.summary)
        if result.summary.get('items'):
            _print_table(
                result.summary['items'],
                ['endpoint', 'category', 'status', 'planned_jobs', 'executed_jobs', 'skipped_jobs', 'record_count', 'snapshot_id', 'blocked_reason'],
            )
    return 0 if result.status == 'succeeded' else 1


def cmd_validate(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root) if args.no_record else _ensure_catalog(root)
    validator = Validator(root, catalog)
    latest_all = args.latest_all or args.all_active or (args.snapshot in (None, 'latest') and not args.api)
    if latest_all:
        ok, reports = validator.validate_latest_snapshots(args.api, record=not args.no_record)
        _print_validation_reports(reports, ok, args.json)
        return 0 if ok else 1
    report = validator.validate_snapshot_report(args.snapshot, args.api, record=not args.no_record)
    ok = report['status'] == 'succeeded'
    _print_validation_reports([report], ok, args.json)
    return 0 if ok else 1


def _visible_lake_files_for_snapshot(catalog: CatalogStore, snapshot_id: str) -> list[dict[str, Any]]:
    blocked = {'quarantined', 'missing', 'deleted', 'deleted_pending', 'superseded', 'compacted'}
    rows = catalog.files_for_snapshot(snapshot_id, content_type='lake')
    out = []
    for row in rows:
        if row.get('status') not in blocked:
            item = dict(row)
            item['snapshot_id'] = snapshot_id
            out.append(item)
    return out


def cmd_list_files(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    if args.api:
        files = LakeReader(root, catalog).list_active_files(args.api, args.snapshot)
    else:
        if args.snapshot not in (None, 'latest'):
            raise SystemExit('list-files requires --api when --snapshot is not latest')
        files = []
        for snap in catalog.latest_snapshots():
            files.extend(_visible_lake_files_for_snapshot(catalog, snap['snapshot_id']))
    if args.json:
        _print_json(files)
    else:
        _print_table(files, ['snapshot_id', 'api_name', 'file_id', 'content_type', 'record_count', 'status', 'relative_path'])
    return 0


def cmd_catalog_inspect(args) -> int:
    catalog = _open_existing_catalog(Path(args.root))
    summary = catalog.inspect_summary()
    if args.json:
        _print_json(summary)
    else:
        _print_key_values(summary)
    return 0


def cmd_show_runs(args) -> int:
    rows = _open_existing_catalog(Path(args.root)).list_runs(args.api, args.limit)
    if args.json:
        _print_json(rows)
    else:
        _print_table(rows, [
            'run_id',
            'run_type',
            'status',
            'api_name',
            'planned_jobs',
            'executed_jobs',
            'skipped_jobs',
            'succeeded_jobs',
            'failed_jobs',
            'blocked_jobs',
            'quarantined_jobs',
            'started_at',
            'finished_at',
            'job_count',
        ])
    return 0


def _backfill_run_items(root: Path, catalog: CatalogStore, run: dict[str, Any]) -> list[dict[str, Any]]:
    summary = run.get('summary') or {}
    if run.get('run_type') == 'backfill' and summary.get('items'):
        return list(summary['items'])
    if run.get('run_type') != 'backfill':
        return []
    api_name = summary.get('api_name')
    requested_dates = list(summary.get('requested_dates') or [])
    jobs = catalog.jobs_for_run(str(run.get('run_id')))
    if jobs:
        items = []
        for job in jobs:
            params = job.get('params') or {}
            date = params.get('trade_date') or params.get('date') or ''
            status = 'succeeded' if job.get('status') == 'done' else job.get('status')
            items.append({
                'date': date,
                'job_key': job.get('job_key'),
                'existing_status': 'missing',
                'planned_action': 'fetch',
                'result_status': status,
                'snapshot_id': catalog.snapshot_id_for_job(str(job.get('job_key')), api_name),
                'record_count': job.get('record_count'),
                'raw_event_count': job.get('raw_event_count'),
                'error_type': job.get('last_error_type'),
            })
        return items
    if api_name and requested_dates:
        try:
            plan = BackfillPlanner(root, catalog).plan_date_backfill(api_name, requested_dates, max_jobs=len(requested_dates), dry_run=False)
        except ValueError:
            return []
        items = []
        snapshot = catalog.latest_snapshot(api_name)
        for job in plan.planned_jobs:
            existing = catalog.get_job(job.job_key) or {}
            items.append({
                'date': job.date,
                'job_key': job.job_key,
                'existing_status': job.existing_status,
                'planned_action': job.planned_action,
                'result_status': 'skipped' if job.planned_action == 'skip_existing' else job.planned_action,
                'snapshot_id': snapshot.get('snapshot_id') if snapshot else None,
                'record_count': existing.get('record_count'),
                'raw_event_count': existing.get('raw_event_count'),
                'error_type': existing.get('last_error_type'),
            })
        return items
    return []


def cmd_show_run(args) -> int:
    root = Path(args.root)
    catalog = _open_existing_catalog(root)
    run = catalog.get_run(args.run_id)
    if not run:
        raise SystemExit(f'run not found: {args.run_id}')
    summary = dict(run.get('summary') or {})
    if run.get('run_type') == 'backfill':
        items = _backfill_run_items(root, catalog, run)
    else:
        items = list(summary.get('items') or []) if isinstance(summary.get('items'), list) else []
    if items and not summary.get('items'):
        summary['items'] = items
        run = dict(run)
        run['summary'] = summary
    if args.json:
        _print_json(run)
        return 0
    _print_key_values({
        'run_id': run.get('run_id'),
        'run_type': run.get('run_type'),
        'status': run.get('status'),
        'api_name': run.get('api_name'),
        'planned_jobs': run.get('planned_jobs'),
        'executed_jobs': run.get('executed_jobs'),
        'skipped_jobs': run.get('skipped_jobs'),
        'succeeded_jobs': run.get('succeeded_jobs'),
        'failed_jobs': run.get('failed_jobs'),
        'blocked_jobs': run.get('blocked_jobs'),
        'quarantined_jobs': run.get('quarantined_jobs'),
        'job_count': run.get('job_count'),
        'started_at': run.get('started_at'),
        'finished_at': run.get('finished_at'),
        'last_error_type': run.get('last_error_type'),
        'error_message': run.get('error_message'),
    })
    if items:
        if run.get('run_type') == 'mirror':
            _print_table(items, ['endpoint', 'category', 'status', 'planned_jobs', 'executed_jobs', 'skipped_jobs', 'record_count', 'snapshot_id', 'blocked_reason'])
        else:
            _print_table(items, ['date', 'job_key', 'existing_status', 'planned_action', 'result_status', 'snapshot_id', 'record_count', 'raw_event_count', 'error_type'])
    else:
        _print_key_values({'summary': summary})
    return 0


def cmd_show_jobs(args) -> int:
    rows = _open_existing_catalog(Path(args.root)).list_jobs(args.api, args.limit)
    if args.json:
        _print_json(rows)
    else:
        _print_table(rows, ['job_key', 'run_id', 'api_name', 'status', 'params_hash', 'record_count', 'raw_event_count', 'started_at', 'finished_at', 'last_error_type', 'last_error'])
    return 0


def cmd_show_snapshots(args) -> int:
    rows = _open_existing_catalog(Path(args.root)).list_snapshots(args.api, args.limit, latest=args.latest)
    if args.json:
        _print_json(rows)
    else:
        _print_table(rows, ['snapshot_id', 'table_id', 'api_name', 'status', 'created_at', 'parent_snapshot_id', 'file_count', 'record_count', 'raw_event_count'])
    return 0


def cmd_show_validations(args) -> int:
    rows = _open_existing_catalog(Path(args.root)).list_validations(args.api, args.limit)
    if args.json:
        _print_json(rows)
    else:
        _print_table(rows, ['validation_id', 'api_name', 'snapshot_id', 'status', 'started_at', 'finished_at', 'checked_file_count', 'failure_count', 'record_count', 'raw_event_count'])
    return 0


def cmd_show_permissions(args) -> int:
    rows = _open_existing_catalog(Path(args.root)).list_permissions(args.api, args.limit)
    if args.json:
        _print_json(rows)
    else:
        _print_table(rows, ['api_name', 'status', 'probed_at', 'valid_until', 'row_count', 'error_type', 'error_message'])
    return 0


def cmd_catalog_backup(args) -> int:
    catalog = _open_existing_catalog(Path(args.root))
    out = catalog.backup(args.output)
    print(f'backup={out}')
    return 0


def cmd_catalog_version(args) -> int:
    catalog = _open_existing_catalog(Path(args.root))
    print(catalog.schema_version())
    return 0


def _add_observe_args(p: argparse.ArgumentParser) -> None:
    p.add_argument('--api')
    p.add_argument('--limit', type=int, default=20)
    p.add_argument('--json', action='store_true')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='python -m tushare_mirror')
    parser.add_argument('--root', default='data/tushare')
    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('init-catalog')
    p.set_defaults(func=cmd_init_catalog)

    p = sub.add_parser('probe')
    p.add_argument('--api')
    p.add_argument('--family')
    p.add_argument('--all', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_probe)

    p = sub.add_parser('fetch')
    p.add_argument('--api', required=True)
    p.add_argument('--params', required=True)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser('backfill-plan')
    p.add_argument('--api', required=True)
    p.add_argument('--dates')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--trading-days-only', action='store_true')
    p.add_argument('--calendar-exchange', default='SSE')
    p.add_argument('--max-jobs', type=int)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_backfill_plan)

    p = sub.add_parser('backfill')
    p.add_argument('--api', required=True)
    p.add_argument('--dates')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--trading-days-only', action='store_true')
    p.add_argument('--calendar-exchange', default='SSE')
    p.add_argument('--max-jobs', type=int)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--stop-on-error', action='store_true')
    p.add_argument('--validate-latest', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_backfill)

    p = sub.add_parser('coverage')
    p.add_argument('--api', required=True)
    p.add_argument('--dates')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--trading-days-only', action='store_true')
    p.add_argument('--calendar-exchange', default='SSE')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_coverage)

    p = sub.add_parser('backup-inspect')
    p.add_argument('--backup', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_backup_inspect)

    p = sub.add_parser('backup-plan')
    p.add_argument('--target', required=True)
    p.add_argument('--api')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_backup_plan)

    p = sub.add_parser('backup')
    p.add_argument('--target', required=True)
    p.add_argument('--api')
    p.add_argument('--overwrite', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_backup)

    p = sub.add_parser('restore-check')
    p.add_argument('--backup', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_restore_check)

    p = sub.add_parser('backfill-missing')
    p.add_argument('--api', required=True)
    p.add_argument('--dates')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--trading-days-only', action='store_true')
    p.add_argument('--calendar-exchange', default='SSE')
    p.add_argument('--max-jobs', type=int)
    p.add_argument('--retry-failed', action='store_true')
    p.add_argument('--execute', action='store_true')
    p.add_argument('--stop-on-error', action='store_true')
    p.add_argument('--validate-latest', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_backfill_missing)


    p = sub.add_parser('mirror-preflight')
    p.add_argument('--mirror-root', required=True)
    p.add_argument('--backup-target', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--mode', default='smoke')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--max-jobs-per-api', type=int)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_preflight)

    p = sub.add_parser('mirror-scope', description='Read-only mirror scope report; classifies executable, plan-only, and disabled endpoints without fetching or writing catalog state.')
    p.add_argument('--scope', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_scope)

    p = sub.add_parser('hk-us-financial-probe-report', description='Read-only HK/US financial probe contract report; compares source-map fields, observed probe fields, and PIT assumptions without fetching or writing catalog state.')
    p.add_argument('--input', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_hk_us_financial_probe_report)

    p = sub.add_parser('financial-readiness', description='Read-only HK/US financial raw readiness report; does not fetch or write catalog state.')
    p.add_argument('--scope', required=True)
    p.add_argument('--root', dest='root_arg')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_financial_readiness)

    p = sub.add_parser('financial-request-estimate', description='Read-only HK/US financial code-period request estimate; does not call Tushare or inspect quota.')
    p.add_argument('--scope', required=True)
    p.add_argument('--from-period', required=True)
    p.add_argument('--to-period', required=True)
    p.add_argument('--limit-codes', type=int, required=True)
    p.add_argument('--max-periods', type=int, default=20)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_financial_request_estimate)

    p = sub.add_parser('financial-coverage-matrix', description='Read-only HK/US financial code-period coverage matrix; does not fetch or write catalog state.')
    p.add_argument('--root', dest='root_arg', required=True)
    p.add_argument('--scope', required=True)
    p.add_argument('--periods', required=True)
    p.add_argument('--limit-codes', type=int, required=True)
    p.add_argument('--universe')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_financial_coverage_matrix)

    p = sub.add_parser('financial-pull-command', description='Read-only/file-output guarded HK/US financial pull command bundle generator; does not execute or fetch.')
    p.add_argument('--scope', required=True)
    p.add_argument('--root', dest='root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--from-period', required=True)
    p.add_argument('--to-period', required=True)
    p.add_argument('--limit-codes', type=int, required=True)
    p.add_argument('--max-periods', type=int, default=20)
    p.add_argument('--output')
    p.add_argument('--overwrite', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_financial_pull_command)

    p = sub.add_parser('mirror-review')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--mode', default='pilot')
    p.add_argument('--start-date', default='20250101')
    p.add_argument('--end-date', default='20250131')
    p.add_argument('--calendar-exchange', default='SSE')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_review)

    p = sub.add_parser('mirror-readiness')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_readiness)

    p = sub.add_parser('mirror-status', description='Read-only mirror status dashboard; does not fetch, backfill, or write catalog state.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_status)

    p = sub.add_parser('mirror-audit', description='Read-only mirror audit report; queries local catalog and optional backup only, does not make real requests or write catalog state.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup')
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--since')
    p.add_argument('--limit', type=int, default=20)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_audit)

    p = sub.add_parser('mirror-next-batch', description='Read-only next batch recommender; inspects local trade_cal and coverage only, does not fetch or write catalog state.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_next_batch)

    p = sub.add_parser('mirror-batch-bundle', description='Read-only/file-output dry-run bundle generator; writes only --output outside mirror and backup roots, does not fetch or execute generated commands.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--max-jobs-per-api', type=int, required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--overwrite', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_batch_bundle)

    p = sub.add_parser('mirror-batch-bundle-verify', description='Read-only batch bundle verification; checks manifest, hashes, command guards, and token hygiene without executing commands.')
    p.add_argument('--bundle', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_batch_bundle_verify)

    p = sub.add_parser('command-safety-check', description='Read-only shell command safety analyzer; inspects command files without executing them.')
    p.add_argument('--file', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_command_safety_check)

    p = sub.add_parser('mirror-batch-rehearse', description='Read-only batch rehearsal simulator; does not execute mirror-run, fetch, backfill, write catalog, or write backup.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--bundle', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_batch_rehearse)

    p = sub.add_parser('mirror-batch-ledger', description='Read-only mirror batch ledger report inferred from local catalog runs and coverage.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--bundle')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_batch_ledger)

    p = sub.add_parser('mirror-batch-certificate', description='Read-only/file-output batch completion certificate; writes only --output outside mirror and backup roots.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--overwrite', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_batch_certificate)

    p = sub.add_parser('mirror-operator-checklist', description='Read-only operator checklist before any user-confirmed controlled batch execution; does not make real requests or write catalog state.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_operator_checklist)

    p = sub.add_parser('stop-policy', description='Read-only stop-condition policy report; does not inspect local data or make real requests.')
    p.add_argument('--scope')
    p.add_argument('--category')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_stop_policy)

    p = sub.add_parser('mirror-failure-drill', description='Read-only failure drill simulator; describes operator response without injecting failures, fetching, or writing catalog state.')
    p.add_argument('--scenario', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_failure_drill)

    p = sub.add_parser('path-diagnostics', description='Read-only mirror path and disk diagnostics; counts local files and checks path relationships without writing.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_path_diagnostics)

    p = sub.add_parser('token-hygiene', description='Read-only token hygiene scanner; reports suspicious counts and paths only, never matched token values.')
    p.add_argument('--path', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_token_hygiene)

    p = sub.add_parser('monthly-promotion-checklist', description='Read-only monthly promotion checklist; does not execute mirror-run, fetch, backfill, or generated commands.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--from-month', required=True)
    p.add_argument('--to-month', required=True)
    p.add_argument('--bundle')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_monthly_promotion_checklist)

    p = sub.add_parser('mirror-ops-report', description='Read-only aggregate operations report; composes local reports without executing mirror-run, fetch, backfill, or generated commands.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--next-start-date', required=True)
    p.add_argument('--next-end-date', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_ops_report)

    p = sub.add_parser('mirror-final-gate', description='Read-only final user-confirmed execution gate; aggregates local safety reports without executing mirror-run, commands.sh, fetch, or backfill.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--bundle', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--max-jobs-per-api', type=int, required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_final_gate)

    p = sub.add_parser('mirror-execute-script', description='Read-only/file-output guarded execute script generator; writes only --output outside mirror and backup roots and does not execute the script.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--bundle', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--max-jobs-per-api', type=int, required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--overwrite', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_execute_script)

    p = sub.add_parser('mirror-execute-readiness', description='Read-only final execute readiness report over mirror-final-gate; does not execute mirror-run, commands.sh, fetch, or backfill.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--bundle', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--max-jobs-per-api', type=int, required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_execute_readiness)

    p = sub.add_parser('mirror-pull-command', description='Read-only/file-output guarded pull command generator; writes only --output outside mirror and backup roots and does not execute commands.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', required=True)
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--max-jobs-per-api', type=int, required=True)
    p.add_argument('--output')
    p.add_argument('--overwrite', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_pull_command)

    p = sub.add_parser('mirror-auto-sync', description='Controlled foreground auto-sync runner with bounded windows, checkpoint state, and retries. Dry-run by default; execute requires --execute --confirm-auto-sync and TUSHARE_TOKEN.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', default='a-share-low-risk')
    p.add_argument('--from-date', required=True)
    p.add_argument('--to-date', default='latest-trade-date')
    p.add_argument('--window-days', type=int, default=20)
    p.add_argument('--max-jobs-per-api', type=int, default=20)
    p.add_argument('--state')
    p.add_argument('--max-attempts', type=int, default=3)
    p.add_argument('--retry-backoff-seconds', type=int, default=60)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--confirm-auto-sync', action='store_true')
    p.add_argument('--confirm-hk-us-auto-sync', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_auto_sync)

    p = sub.add_parser('mirror-auto-sync-command', description='Read-only/file-output guarded HK/US auto-sync command bundle generator; writes only --output outside mirror and backup roots and does not execute commands.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', required=True)
    p.add_argument('--from-date', required=True)
    p.add_argument('--to-date', default='latest-trade-date')
    p.add_argument('--window-days', type=int, default=20)
    p.add_argument('--max-jobs-per-api', type=int, default=20)
    p.add_argument('--state', required=True)
    p.add_argument('--output')
    p.add_argument('--overwrite', action='store_true')
    p.add_argument('--max-attempts', type=int, default=3)
    p.add_argument('--retry-backoff-seconds', type=int, default=60)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_auto_sync_command)

    p = sub.add_parser('mirror-auto-sync-status', description='Read-only auto-sync checkpoint status report; does not acquire locks, fetch, or write state.')
    p.add_argument('--state', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_auto_sync_status)

    p = sub.add_parser('mirror-auto-sync-recovery-plan', description='Read-only auto-sync checkpoint recovery report; does not modify state or execute commands.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--backup', required=True)
    p.add_argument('--scope', required=True)
    p.add_argument('--state', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_auto_sync_recovery_plan)

    p = sub.add_parser('schema-status', description='Read-only schema drift and quarantine status report; does not make real requests or write catalog state.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_schema_status)

    p = sub.add_parser('backup-status', description='Read-only backup history and mutation diagnostics; does not make real requests or write catalog state.')
    p.add_argument('--backup', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_backup_status)

    p = sub.add_parser('mirror-coverage-matrix', description='Read-only coverage matrix for low-risk daily, weekly, and monthly endpoints; does not make real requests or write catalog state.')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_coverage_matrix)

    p = sub.add_parser('request-estimate', description='Read-only request estimate; does not call Tushare, inspect quota, or write catalog state.')
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--root', dest='root_arg', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_request_estimate)

    p = sub.add_parser('mirror-batch-plan')
    p.add_argument('--root', dest='mirror_root_arg', required=True)
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--calendar-exchange', default='SSE')
    p.add_argument('--max-jobs-per-api', type=int, required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_batch_plan)

    p = sub.add_parser('api-infra-readiness')
    p.add_argument('--scope', default='all')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_api_infra_readiness)

    p = sub.add_parser('pit-readiness')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_pit_readiness)

    p = sub.add_parser('object-plan')
    p.add_argument('--api', required=True)
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_object_plan)

    p = sub.add_parser('intraday-plan')
    p.add_argument('--api', required=True)
    p.add_argument('--freq')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--bucket-count', type=int, required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_intraday_plan)

    p = sub.add_parser('storage-estimate')
    p.add_argument('--scope')
    p.add_argument('--category')
    p.add_argument('--api')
    p.add_argument('--freq')
    p.add_argument('--start-date', required=True)
    p.add_argument('--end-date', required=True)
    p.add_argument('--bucket-count', type=int)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_storage_estimate)

    p = sub.add_parser('compaction-plan')
    p.add_argument('--root', dest='compaction_root')
    p.add_argument('--api', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_compaction_plan)

    p = sub.add_parser('rate-policy')
    p.add_argument('--scope')
    p.add_argument('--category')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_rate_policy)

    p = sub.add_parser('endpoint-enable-checklist')
    p.add_argument('--api', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_endpoint_enable_checklist)

    p = sub.add_parser('code-universe')
    p.add_argument('--universe', required=True)
    p.add_argument('--limit', type=int, default=20)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_code_universe)

    p = sub.add_parser('code-list-plan')
    p.add_argument('--api', required=True)
    p.add_argument('--universe', required=True)
    p.add_argument('--limit-codes', type=int, required=True)
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_code_list_plan)

    p = sub.add_parser('code-date-matrix-plan')
    p.add_argument('--api', required=True)
    p.add_argument('--universe', required=True)
    p.add_argument('--limit-codes', type=int, required=True)
    p.add_argument('--dates')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--max-dates', type=int)
    p.add_argument('--trading-days-only', action='store_true')
    p.add_argument('--calendar-exchange', default='SSE')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_code_date_matrix_plan)

    p = sub.add_parser('period-plan')
    p.add_argument('--api', required=True)
    p.add_argument('--periods')
    p.add_argument('--start-period')
    p.add_argument('--end-period')
    p.add_argument('--period-frequency', choices=['quarterly', 'annual'], default='quarterly')
    p.add_argument('--max-periods', type=int, default=20)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_period_plan)

    p = sub.add_parser('code-period-plan')
    p.add_argument('--scope')
    p.add_argument('--api', required=True)
    p.add_argument('--universe', required=True)
    p.add_argument('--limit-codes', type=int, required=True)
    p.add_argument('--periods')
    p.add_argument('--start-period')
    p.add_argument('--end-period')
    p.add_argument('--period-frequency', choices=['quarterly', 'annual'], default='quarterly')
    p.add_argument('--max-periods', type=int, default=20)
    p.add_argument('--max-candidate-jobs', type=int, default=100)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_code_period_plan)

    p = sub.add_parser('mirror-plan')
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--mode', default='smoke')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--max-jobs-per-api', type=int)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_plan)

    p = sub.add_parser('mirror-run')
    p.add_argument('--scope', default='low-risk-a-share')
    p.add_argument('--mode', default='smoke')
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    p.add_argument('--max-jobs-per-api', type=int)
    p.add_argument('--backup-target')
    p.add_argument('--execute', action='store_true')
    p.add_argument('--init-if-missing', action='store_true')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_mirror_run)

    p = sub.add_parser('validate')
    p.add_argument('--snapshot', default='latest')
    p.add_argument('--api')
    p.add_argument('--all-active', action='store_true')
    p.add_argument('--latest-all', action='store_true')
    p.add_argument('--json', action='store_true')
    p.add_argument('--no-record', action='store_true')
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser('list-files')
    p.add_argument('--api')
    p.add_argument('--snapshot', default='latest')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_list_files)

    p = sub.add_parser('catalog-inspect')
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_catalog_inspect)

    p = sub.add_parser('show-runs')
    _add_observe_args(p)
    p.set_defaults(func=cmd_show_runs)

    p = sub.add_parser('show-run')
    p.add_argument('--run-id', required=True)
    p.add_argument('--json', action='store_true')
    p.set_defaults(func=cmd_show_run)

    p = sub.add_parser('show-jobs')
    _add_observe_args(p)
    p.set_defaults(func=cmd_show_jobs)

    p = sub.add_parser('show-snapshots')
    _add_observe_args(p)
    p.add_argument('--latest', action='store_true')
    p.set_defaults(func=cmd_show_snapshots)

    p = sub.add_parser('show-validations')
    _add_observe_args(p)
    p.set_defaults(func=cmd_show_validations)

    p = sub.add_parser('show-permissions')
    _add_observe_args(p)
    p.set_defaults(func=cmd_show_permissions)

    p = sub.add_parser('catalog-backup')
    p.add_argument('--output', required=True)
    p.set_defaults(func=cmd_catalog_backup)

    p = sub.add_parser('catalog-version')
    p.set_defaults(func=cmd_catalog_version)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
