#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import TushareClient, classify_probe_response
from tushare_mirror.cli import load_dotenv
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.source_metadata import hk_us_low_risk_source_endpoints

PHASE1_ENDPOINTS: dict[str, dict[str, Any]] = {
    "daily": {"trade_date": "20250102"},
    "stock_basic": {"list_status": "L"},
    "trade_cal": {"exchange": "SSE", "start_date": "20250101", "end_date": "20250131"},
    "adj_factor": {"trade_date": "20250102"},
    "daily_basic": {"trade_date": "20250102"},
}

PHASE2_LOW_VOLUME_ENDPOINTS: dict[str, dict[str, Any]] = {
    "weekly": {"trade_date": "20250103"},
    "monthly": {"trade_date": "20250127"},
    "suspend_d": {"trade_date": "20250102"},
    "namechange": {"ts_code": "000001.SZ"},
    "hs_const": {"hs_type": "SH", "is_new": "1"},
    "stk_managers": {"ts_code": "000001.SZ"},
    "stk_rewards": {"ts_code": "000001.SZ"},
}

A_SHARE_LOW_RISK_ENDPOINTS: dict[str, dict[str, Any]] = {
    **PHASE1_ENDPOINTS,
    **PHASE2_LOW_VOLUME_ENDPOINTS,
    "stock_company": {"exchange": "SSE"},
    "index_basic": {"market": "SSE"},
    "index_weekly": {"trade_date": "20250103"},
    "index_monthly": {"trade_date": "20250127"},
    "ths_index": {"exchange": "A", "type": "N"},
    "index_classify": {"src": "SW2021", "level": "L1"},
}

ENDPOINTS: dict[str, dict[str, Any]] = {**A_SHARE_LOW_RISK_ENDPOINTS}

ACCESSIBLE = {"accessible", "empty_but_accessible"}
PERMISSION_STATUSES = {"permission_denied", "rate_limited"}
HK_US_LOW_RISK_PROBE_FIELDS: dict[str, list[str]] = {
    "hk_basic": ["ts_code", "name", "list_status", "list_date"],
    "hk_tradecal": ["cal_date", "is_open", "pretrade_date"],
    "hk_daily": ["ts_code", "trade_date", "open", "close", "vol", "amount"],
    "hk_daily_adj": ["ts_code", "trade_date", "close", "adj_factor", "total_mv"],
    "hk_adjfactor": ["ts_code", "trade_date", "cum_adjfactor", "close_price"],
    "us_basic": ["ts_code", "name", "enname", "classify", "list_date", "delist_date"],
    "us_tradecal": ["cal_date", "is_open", "pretrade_date"],
    "us_daily": ["ts_code", "trade_date", "close", "open", "vol", "amount", "vwap"],
    "us_daily_adj": ["ts_code", "trade_date", "close", "adj_factor", "exchange"],
    "us_adjfactor": ["ts_code", "trade_date", "exchange", "cum_adjfactor", "close_price"],
}
HK_US_LOW_RISK_PROBE_REQUESTS: dict[str, list[dict[str, Any]]] = {
    "hk_basic": [{"list_status": "L"}],
    "hk_tradecal": [{"start_date": "20250101", "end_date": "20250110", "is_open": "1"}],
    "hk_daily": [{"ts_code": "00001.HK", "start_date": "20250102", "end_date": "20250102"}],
    "hk_daily_adj": [
        {"trade_date": "20250102", "limit": 2, "offset": 0},
        {"trade_date": "20250102", "limit": 2, "offset": 2},
    ],
    "hk_adjfactor": [{"ts_code": "00001.HK", "start_date": "20250102", "end_date": "20250102"}],
    "us_basic": [
        {"classify": "EQ", "limit": 2, "offset": 0},
        {"classify": "EQ", "limit": 2, "offset": 2},
    ],
    "us_tradecal": [{"start_date": "20250101", "end_date": "20250110", "is_open": "1"}],
    "us_daily": [
        {"ts_code": "AAPL", "start_date": "20250102", "end_date": "20250102"},
        {"trade_date": "20250102", "limit": 2, "offset": 0},
    ],
    "us_daily_adj": [
        {"trade_date": "20250102", "exchange": "NAS", "limit": 2, "offset": 0},
        {"trade_date": "20250102", "exchange": "NAS", "limit": 2, "offset": 2},
    ],
    "us_adjfactor": [{"ts_code": "AAPL", "start_date": "20250102", "end_date": "20250102"}],
}


def run_cli(root: Path, args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, "-m", "tushare_mirror", "--root", str(root), *args]
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


def parse_json_stdout(proc: subprocess.CompletedProcess[str]) -> Any:
    return json.loads(proc.stdout) if proc.stdout.strip() else None


def counts(catalog: CatalogStore, api_name: str) -> dict[str, Any]:
    with catalog.connect() as conn:
        job = conn.execute("select status, record_count, raw_event_count from jobs where api_name=? order by updated_at desc limit 1", (api_name,)).fetchone()
        snap = conn.execute("select snapshot_id from snapshots where api_name=? and status='current' order by created_at desc limit 1", (api_name,)).fetchone()
        raw_count = conn.execute("select count(*) from files where api_name=? and content_type='raw'", (api_name,)).fetchone()[0]
        lake_count = conn.execute("select count(*) from files where api_name=? and content_type='lake'", (api_name,)).fetchone()[0]
    return {
        "fetch_status": job["status"] if job else "not_run",
        "record_count": job["record_count"] if job else None,
        "raw_event_count": job["raw_event_count"] if job else None,
        "snapshot_id": snap["snapshot_id"] if snap else None,
        "raw_file_count": raw_count,
        "lake_file_count": lake_count,
    }


def print_table(rows: list[dict[str, Any]]) -> None:
    columns = ["endpoint", "probe_status", "fetch_status", "raw_file_count", "lake_file_count", "snapshot_id", "validation_status", "record_count", "raw_event_count"]
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], min(len(str(row.get(col, "") or "")), 48))
    print("  ".join(col.ljust(widths[col]) for col in columns))
    print("  ".join("-" * widths[col] for col in columns))
    for row in rows:
        print("  ".join(str(row.get(col, "") or "")[:48].ljust(widths[col]) for col in columns))


def smoke_command_preview(root: Path, endpoints: list[str]) -> list[list[str]]:
    commands: list[list[str]] = [[sys.executable, "-m", "tushare_mirror", "--root", str(root), "init-catalog"]]
    for endpoint in endpoints:
        commands.append([sys.executable, "-m", "tushare_mirror", "--root", str(root), "probe", "--api", endpoint, "--json"])
        commands.append([
            sys.executable,
            "-m",
            "tushare_mirror",
            "--root",
            str(root),
            "fetch",
            "--api",
            endpoint,
            "--params",
            json.dumps(ENDPOINTS[endpoint], separators=(",", ":")),
            "--json",
        ])
        commands.append([sys.executable, "-m", "tushare_mirror", "--root", str(root), "validate", "--api", endpoint, "--snapshot", "latest", "--json"])
    return commands


def print_command_preview(root: Path, endpoints: list[str]) -> None:
    payload = {
        "root": str(root),
        "endpoint_count": len(endpoints),
        "endpoints": endpoints,
        "commands": [" ".join(command) for command in smoke_command_preview(root, endpoints)],
        "safety_limits": {
            "snapshot_endpoint_requests": "max 1 request per selected endpoint",
            "date_endpoint_requests": "max 1 selected date per selected endpoint",
            "stock_loop": False,
            "full_backfill": False,
        },
        "real_requests_sent": False,
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _ensure_tmp_output_path(output: Path) -> Path:
    resolved = output.expanduser().resolve()
    tmp_root = Path("/tmp").resolve()
    if not _is_relative_to(resolved, tmp_root):
        raise ValueError("--output for real probes must be under /tmp")
    if resolved.exists() and resolved.is_dir():
        raise ValueError("--output must be a file path, not a directory")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def _redact_for_probe(value: Any, token: str | None) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, child in value.items():
            if "token" in str(key).lower():
                redacted[key] = "<redacted>"
            else:
                redacted[key] = _redact_for_probe(child, token)
        return redacted
    if isinstance(value, list):
        return [_redact_for_probe(item, token) for item in value]
    if isinstance(value, str) and token:
        return value.replace(token, "<redacted>")
    return value


def _write_probe_report(output: Path, payload: dict[str, Any]) -> None:
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def _probe_report_header(output: Path, max_requests_per_endpoint: int) -> dict[str, Any]:
    return {
        "report_version": "hk-us-low-risk-probe/v1",
        "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "output": str(output),
        "max_requests_per_endpoint": max_requests_per_endpoint,
        "real_requests_sent": False,
        "token_plaintext_found": False,
        "overall_status": "blocked",
        "blocking_errors": [],
        "warnings": [],
        "endpoints": [],
    }


def _executable_hk_us_source_endpoints() -> list[dict[str, Any]]:
    return [item for item in hk_us_low_risk_source_endpoints() if item.get("recommendation") == "executable_candidate"]


def _probe_endpoint(client: Any, endpoint: dict[str, Any], max_requests_per_endpoint: int, token: str | None) -> dict[str, Any]:
    api_name = str(endpoint["api_name"])
    requests = HK_US_LOW_RISK_PROBE_REQUESTS.get(api_name, [])[:max_requests_per_endpoint]
    fields = HK_US_LOW_RISK_PROBE_FIELDS.get(api_name, list(endpoint.get("documented_fields") or [])[:8])
    statuses: list[str] = []
    errors: list[str] = []
    response_fields: list[str] = []
    row_count = 0
    params_used: list[dict[str, Any]] = []
    successful_offset_request = False
    offset_request_seen = False
    for params in requests:
        params_used.append(_redact_for_probe(dict(params), token))
        if "offset" in params or "limit" in params:
            offset_request_seen = True
        try:
            response = client.request(api_name, params, fields)
        except Exception as exc:
            statuses.append("network_error")
            errors.append(_redact_for_probe(str(exc), token))
            continue
        response = _redact_for_probe(response, token)
        status, message = classify_probe_response(response)
        statuses.append(status)
        if message:
            errors.append(message)
        data = response.get("data") or {}
        items = list(data.get("items") or [])
        if not response_fields:
            response_fields = [str(item) for item in (data.get("fields") or [])]
        row_count += len(items)
        if status in ACCESSIBLE and ("offset" in params or "limit" in params):
            successful_offset_request = True
    status = "not_run"
    if any(item in ACCESSIBLE for item in statuses):
        status = "accessible" if row_count else "empty_but_accessible"
    elif statuses:
        status = statuses[-1]
    return {
        "endpoint": api_name,
        "market": endpoint.get("market"),
        "status": status,
        "request_count": len(requests),
        "params_used": params_used,
        "fields": response_fields,
        "row_count": row_count,
        "page_count_tested": len(requests),
        "pagination_supported": bool(successful_offset_request),
        "pagination_probe_attempted": bool(offset_request_seen),
        "recommended_planner_kind": endpoint.get("recommended_planner_kind"),
        "recommended_partition_template": endpoint.get("recommended_partition_template"),
        "recommended_pagination_strategy": endpoint.get("recommended_pagination_strategy"),
        "safety_notes": endpoint.get("safety_notes") or [],
        "errors": errors,
    }


def run_hk_us_low_risk_probe(
    output: Path,
    max_requests_per_endpoint: int,
    *,
    token: str | None = None,
    client: Any | None = None,
) -> int:
    if max_requests_per_endpoint < 1:
        raise ValueError("--max-requests-per-endpoint must be positive")
    if max_requests_per_endpoint > 2:
        raise ValueError("--max-requests-per-endpoint must be <= 2 for HK/US low-risk probes")
    output = _ensure_tmp_output_path(output)
    load_dotenv()
    token = token if token is not None else os.environ.get("TUSHARE_TOKEN")
    report = _probe_report_header(output, max_requests_per_endpoint)
    if not token:
        report["blocking_errors"].append("TUSHARE_TOKEN is required; no real requests were sent")
        _write_probe_report(output, report)
        print(json.dumps({"output": str(output), "overall_status": "blocked", "real_requests_sent": False}, sort_keys=True))
        return 2

    client = client if client is not None else TushareClient(token)
    endpoints = _executable_hk_us_source_endpoints()
    rows = [_probe_endpoint(client, endpoint, max_requests_per_endpoint, token) for endpoint in endpoints]
    report["endpoints"] = rows
    report["real_requests_sent"] = True
    blocked_statuses = {"invalid_params", "invalid_endpoint", "network_error", "server_error", "unknown_error"}
    permission_statuses = {"permission_denied", "rate_limited"}
    if any(row["status"] in blocked_statuses for row in rows):
        report["overall_status"] = "blocked"
        report["blocking_errors"].append("one or more probes returned blocking errors")
    elif any(row["status"] in permission_statuses for row in rows):
        report["overall_status"] = "warning"
        report["warnings"].append("one or more probes could not confirm data access due to permission or rate status")
    else:
        report["overall_status"] = "passed"
    encoded = json.dumps(report, ensure_ascii=False)
    report["token_plaintext_found"] = bool(token and token in encoded)
    if report["token_plaintext_found"]:
        report["overall_status"] = "blocked"
        report["blocking_errors"].append("probe report would contain token plaintext")
        report = _redact_for_probe(report, token)
    _write_probe_report(output, report)
    print(json.dumps({"output": str(output), "overall_status": report["overall_status"], "real_requests_sent": True}, sort_keys=True))
    return 0 if report["overall_status"] in {"passed", "warning"} else 1


def run_smoke(root: Path, endpoints: list[str], reset_root: bool) -> int:
    load_dotenv()
    if not os.environ.get("TUSHARE_TOKEN"):
        print("TUSHARE_TOKEN is required; no real requests were sent.", file=sys.stderr)
        return 2
    if reset_root and root.exists():
        shutil.rmtree(root)
    run_cli(root, ["init-catalog"])
    catalog = CatalogStore(root)
    load_into_catalog(root, catalog)
    rows: list[dict[str, Any]] = []
    failed = False
    for endpoint in endpoints:
        row: dict[str, Any] = {"endpoint": endpoint, "probe_status": "not_run", "fetch_status": "not_run", "validation_status": "not_run"}
        probe = run_cli(root, ["probe", "--api", endpoint, "--json"], check=False)
        probe_payload = parse_json_stdout(probe) or []
        probe_status = probe_payload[0].get("status") if probe_payload else "probe_failed"
        row["probe_status"] = probe_status
        if probe_status not in ACCESSIBLE:
            row["fetch_status"] = f"skipped_{probe_status}"
            rows.append(row)
            if probe_status not in PERMISSION_STATUSES:
                failed = True
            continue
        try:
            run_cli(root, ["fetch", "--api", endpoint, "--params", json.dumps(ENDPOINTS[endpoint], separators=(",", ":")), "--json"])
            validation = run_cli(root, ["validate", "--api", endpoint, "--snapshot", "latest", "--json"])
            validation_payload = parse_json_stdout(validation) or {}
            row["validation_status"] = validation_payload.get("status", "unknown")
            row.update(counts(catalog, endpoint))
            if row["validation_status"] != "succeeded":
                failed = True
        except Exception as exc:
            row["fetch_status"] = "failed"
            row["validation_status"] = "not_run"
            row["error"] = str(exc)
            failed = True
        rows.append(row)
    print_table(rows)
    summary = CatalogStore(root).inspect_summary()
    print("\ncatalog_inspect=" + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 1 if failed else 0



def run_calendar_backfill_smoke(root: Path, reset_root: bool) -> int:
    load_dotenv()
    if not os.environ.get("TUSHARE_TOKEN"):
        print("TUSHARE_TOKEN is required; no real requests were sent.", file=sys.stderr)
        return 2
    if reset_root and root.exists():
        shutil.rmtree(root)
    run_cli(root, ["init-catalog"])
    trade_cal_params = {"exchange": "SSE", "start_date": "20250101", "end_date": "20250110"}
    run_cli(root, ["fetch", "--api", "trade_cal", "--params", json.dumps(trade_cal_params, separators=(",", ":")), "--json"])
    trade_cal_validation = parse_json_stdout(run_cli(root, ["validate", "--api", "trade_cal", "--snapshot", "latest", "--json"]))
    plan = parse_json_stdout(run_cli(root, [
        "backfill-plan",
        "--api",
        "daily",
        "--start-date",
        "20250101",
        "--end-date",
        "20250110",
        "--trading-days-only",
        "--calendar-exchange",
        "SSE",
        "--max-jobs",
        "3",
        "--json",
    ]))
    result = parse_json_stdout(run_cli(root, [
        "backfill",
        "--api",
        "daily",
        "--start-date",
        "20250101",
        "--end-date",
        "20250110",
        "--trading-days-only",
        "--calendar-exchange",
        "SSE",
        "--max-jobs",
        "3",
        "--execute",
        "--validate-latest",
        "--json",
    ]))
    inspect = parse_json_stdout(run_cli(root, ["catalog-inspect", "--json"]))
    summary = {
        "root": str(root),
        "trade_cal_validation_status": (trade_cal_validation or {}).get("status"),
        "calendar_source": (plan or {}).get("calendar_source"),
        "exchange": (plan or {}).get("exchange"),
        "natural_days": (plan or {}).get("natural_days"),
        "trading_days": (plan or {}).get("trading_days"),
        "filtered_non_trading_days": (plan or {}).get("filtered_non_trading_days"),
        "planned_jobs": len((plan or {}).get("planned_jobs") or []),
        "backfill_status": (result or {}).get("status"),
        "executed_jobs": (result or {}).get("summary", {}).get("executed_jobs"),
        "succeeded_jobs": (result or {}).get("summary", {}).get("succeeded_jobs"),
        "daily_validation_status": ((result or {}).get("validation") or {}).get("status"),
        "catalog_inspect": inspect,
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    ok = (summary["trade_cal_validation_status"] == "succeeded" and summary["backfill_status"] == "succeeded" and summary["daily_validation_status"] == "succeeded")
    return 0 if ok else 1

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Opt-in real Tushare smoke test. Sends real requests only when executed.")
    parser.add_argument("--root", default="/tmp/tushare-mirror-real-smoke")
    parser.add_argument("--endpoint", choices=sorted(ENDPOINTS), action="append")
    parser.add_argument("--all-phase-1", action="store_true", help="Run daily plus Phase 1.2 low-volume endpoints.")
    parser.add_argument("--phase-2-low-volume", action="store_true", help="Run Phase 2 low-risk endpoints only.")
    parser.add_argument("--a-share-low-risk-smoke", action="store_true", help="Run the bounded A-share low-risk endpoint smoke set.")
    parser.add_argument("--hk-us-low-risk-probe", action="store_true", help="Run bounded HK/US low-risk interface probes and write redacted diagnostics under /tmp.")
    parser.add_argument("--output", help="Probe output JSON path. Required for --hk-us-low-risk-probe and must be under /tmp.")
    parser.add_argument("--max-requests-per-endpoint", type=int, default=2, help="HK/US probe request cap per endpoint; maximum 2.")
    parser.add_argument("--print-commands", action="store_true", help="Print selected smoke commands without sending real requests.")
    parser.add_argument("--calendar-backfill", action="store_true", help="Run the Phase 2.4 calendar-aware daily backfill smoke.")
    parser.add_argument("--reset-root", action="store_true", help="Remove the root before running.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.hk_us_low_risk_probe:
        if not args.output:
            print("--output is required for --hk-us-low-risk-probe", file=sys.stderr)
            return 2
        try:
            return run_hk_us_low_risk_probe(Path(args.output), args.max_requests_per_endpoint)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    endpoints: list[str] = []
    if args.all_phase_1:
        endpoints.extend(PHASE1_ENDPOINTS)
    if args.phase_2_low_volume:
        endpoints.extend(PHASE2_LOW_VOLUME_ENDPOINTS)
    if args.a_share_low_risk_smoke:
        endpoints.extend(A_SHARE_LOW_RISK_ENDPOINTS)
    if args.endpoint:
        endpoints.extend(args.endpoint)
    if args.calendar_backfill:
        return run_calendar_backfill_smoke(Path(args.root), args.reset_root)
    seen: set[str] = set()
    endpoints = [api for api in endpoints if not (api in seen or seen.add(api))]
    if args.print_commands:
        if not endpoints:
            print("No endpoints selected. Use --all-phase-1, --phase-2-low-volume, --a-share-low-risk-smoke, --hk-us-low-risk-probe, or --endpoint.", file=sys.stderr)
            return 2
        print_command_preview(Path(args.root), endpoints)
        return 0
    if not endpoints:
        print("No endpoints selected. Use --all-phase-1, --phase-2-low-volume, --a-share-low-risk-smoke, --hk-us-low-risk-probe, --calendar-backfill, or --endpoint.", file=sys.stderr)
        return 2
    return run_smoke(Path(args.root), endpoints, args.reset_root)


if __name__ == "__main__":
    raise SystemExit(main())
