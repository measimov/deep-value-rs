#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from urllib.request import Request, urlopen
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import TushareClient, classify_probe_response
from tushare_mirror.cli import load_dotenv
from tushare_mirror.disclosure import DisclosureEvent, hkex_disclosure_automation_gate
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
HK_US_FINANCIAL_DISCLOSURE_FIELDS = {"ann_date", "f_ann_date", "notice_date", "disclosure_date", "publish_date"}
HK_US_FINANCIAL_PROBE_FIELDS: dict[str, list[str]] = {
    "hk_income": ["ts_code", "end_date", "name", "ind_name", "ind_value"],
    "hk_balancesheet": ["ts_code", "name", "end_date", "ind_name", "ind_value"],
    "hk_cashflow": ["ts_code", "end_date", "name", "ind_name", "ind_value"],
    "hk_fina_indicator": [
        "ts_code",
        "end_date",
        "ind_type",
        "security_name_abbr",
        "notice_date",
        "start_date",
        "std_report_date",
        "currency",
        "report_type",
    ],
    "us_income": ["ts_code", "end_date", "ind_type", "name", "ind_name", "ind_value", "report_type"],
    "us_balancesheet": ["ts_code", "end_date", "ind_type", "name", "ind_name", "ind_value", "report_type"],
    "us_cashflow": ["ts_code", "end_date", "ind_type", "name", "ind_name", "ind_value", "report_type"],
    "us_fina_indicator": [
        "ts_code",
        "end_date",
        "ind_type",
        "security_name_abbr",
        "accounting_standards",
        "notice_date",
        "start_date",
        "std_report_date",
        "financial_date",
        "currency",
        "report_type",
    ],
}
HK_US_FINANCIAL_PROBE_REQUESTS: dict[str, list[dict[str, Any]]] = {
    "hk_income": [{"ts_code": "00700.HK", "period": "20241231"}],
    "hk_balancesheet": [{"ts_code": "00700.HK", "period": "20241231"}],
    "hk_cashflow": [{"ts_code": "00700.HK", "period": "20241231"}],
    "hk_fina_indicator": [{"ts_code": "00700.HK", "period": "20241231"}],
    "us_income": [{"ts_code": "NVDA", "period": "20241231"}],
    "us_balancesheet": [{"ts_code": "NVDA", "period": "20241231"}],
    "us_cashflow": [{"ts_code": "NVDA", "period": "20241231"}],
    "us_fina_indicator": [{"ts_code": "NVDA", "period": "20241231"}],
}
SEC_DISCLOSURE_PROBE_VERSION = "sec-disclosure-probe/v1"
SEC_SUBMISSIONS_BASE_URL = "https://data.sec.gov/submissions"
DEFAULT_SEC_USER_AGENT = "deep-value-rs-tushare-mirror/0.1 contact=research@example.invalid"


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


def _sec_user_agent() -> str:
    return os.environ.get("SEC_USER_AGENT") or DEFAULT_SEC_USER_AGENT


def _normalize_cik(cik: str) -> str:
    digits = "".join(ch for ch in str(cik) if ch.isdigit())
    if not digits:
        raise ValueError("--cik must contain digits")
    return digits.zfill(10)


def _normalize_period(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) != 8:
        raise ValueError("--period must be YYYYMMDD")
    return digits


def _sec_period_to_report_date(period: str) -> str:
    normalized = _normalize_period(period)
    return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"


def _sec_json_get(url: str, headers: dict[str, str]) -> dict[str, Any]:
    request = Request(url, headers=headers)
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _recent_sec_filings(payload: dict[str, Any]) -> list[dict[str, Any]]:
    recent = ((payload.get("filings") or {}).get("recent") or {})
    forms = list(recent.get("form") or [])
    filings: list[dict[str, Any]] = []
    for index, form in enumerate(forms):
        filings.append(
            {
                "form": str(form),
                "filing_date": _recent_value(recent, "filingDate", index),
                "report_date": _recent_value(recent, "reportDate", index),
                "accession_number": _recent_value(recent, "accessionNumber", index),
                "accepted_at": _recent_value(recent, "acceptanceDateTime", index),
                "primary_document": _recent_value(recent, "primaryDocument", index),
            }
        )
    return filings


def _recent_value(recent: dict[str, Any], key: str, index: int) -> str | None:
    values = list(recent.get(key) or [])
    if index >= len(values):
        return None
    value = values[index]
    return str(value) if value is not None else None


def _accession_url(cik: str, accession_number: str | None) -> str | None:
    if not accession_number:
        return None
    compact = accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{compact}/"


def _sec_disclosure_event(ticker: str, cik: str, period: str, filing: dict[str, Any]) -> dict[str, Any]:
    accession = filing.get("accession_number")
    filing_date = filing.get("filing_date")
    form = filing.get("form")
    event = DisclosureEvent(
        event_id=f"sec_edgar_submissions:{ticker}:{period}:{form or 'unknown'}:{accession or 'unknown'}",
        market="us",
        source="sec_edgar_submissions",
        source_status="stable_public_json",
        source_doc_id=accession,
        source_url=_accession_url(cik, accession),
        ticker=ticker,
        ts_code=f"{ticker}.US",
        external_id=ticker,
        cik=cik,
        period=period,
        end_date=period,
        report_type=_report_type_from_form(form),
        form_type=form,
        filing_date=filing_date.replace("-", "") if filing_date else None,
        accepted_at=filing.get("accepted_at"),
        disclosure_date=filing_date.replace("-", "") if filing_date else None,
        announcement_title=f"Form {form}" if form else None,
        language="en",
        match_status="exact",
        match_confidence=1.0,
        pit_strength="availability_only",
        as_filed_value_verified=False,
        limitations=["SEC filing date is matched; Tushare values are not reconciled to filing facts"],
    )
    return event.to_dict()


def _fetch_sec_disclosure_match(
    *,
    ticker: str,
    cik: str,
    period: str,
    max_requests: int,
    http_get: Any | None = None,
) -> dict[str, Any]:
    if max_requests < 1:
        raise ValueError("--max-sec-requests must be positive")
    if max_requests > 3:
        raise ValueError("--max-sec-requests must be <= 3")
    normalized_cik = _normalize_cik(cik)
    normalized_period = _normalize_period(period)
    report_date = _sec_period_to_report_date(normalized_period)
    url = f"{SEC_SUBMISSIONS_BASE_URL}/CIK{normalized_cik}.json"
    headers = {"User-Agent": _sec_user_agent(), "Accept-Encoding": "gzip, deflate", "Host": "data.sec.gov"}
    getter = http_get or _sec_json_get
    try:
        payload = getter(url, headers)
    except Exception as exc:
        return {
            "sec_status": "blocked",
            "sec_request_count": 0,
            "sec_errors": [f"sec_request_failed:{exc}"],
            "matched_filings": [],
            "disclosure_events": [],
            "sec_disclosure_date": None,
        }
    filings = _recent_sec_filings(payload)
    matched = [item for item in filings if item.get("report_date") == report_date and item.get("form") in {"10-K", "10-Q", "20-F"}]
    events = [_sec_disclosure_event(ticker, normalized_cik, normalized_period, item) for item in matched]
    disclosure_date = events[0]["disclosure_date"] if events else None
    return {
        "sec_status": "passed" if matched else "warning",
        "sec_request_count": 1,
        "sec_errors": [] if matched else ["no matching 10-K/10-Q/20-F filing found for requested period"],
        "matched_filings": matched,
        "disclosure_events": events,
        "sec_disclosure_date": disclosure_date,
    }


def _report_type_from_form(form: str | None) -> str | None:
    if form == "10-K":
        return "annual"
    if form == "10-Q":
        return "quarterly"
    if form == "20-F":
        return "annual"
    return None


def run_sec_disclosure_probe(
    output: Path,
    *,
    ticker: str,
    cik: str,
    period: str,
    max_requests: int,
    http_get: Any | None = None,
) -> int:
    if max_requests < 1:
        raise ValueError("--max-requests must be positive")
    if max_requests > 3:
        raise ValueError("--max-requests must be <= 3 for SEC disclosure probes")
    output = _ensure_tmp_output_path(output)
    normalized_cik = _normalize_cik(cik)
    normalized_period = _normalize_period(period)
    report_date = _sec_period_to_report_date(normalized_period)
    url = f"{SEC_SUBMISSIONS_BASE_URL}/CIK{normalized_cik}.json"
    headers = {"User-Agent": _sec_user_agent(), "Accept-Encoding": "gzip, deflate", "Host": "data.sec.gov"}
    getter = http_get or _sec_json_get
    report: dict[str, Any] = {
        "report_version": SEC_DISCLOSURE_PROBE_VERSION,
        "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "output": str(output),
        "ticker": ticker,
        "cik": normalized_cik,
        "period": normalized_period,
        "sec_report_date": report_date,
        "request_count": 0,
        "max_requests": max_requests,
        "real_requests_sent": False,
        "source": "sec_edgar_submissions",
        "source_status": "stable_public_json",
        "user_agent_present": bool(headers["User-Agent"]),
        "matched_filings": [],
        "filing_sample": [],
        "disclosure_events": [],
        "overall_status": "blocked",
        "warnings": [],
        "blocking_errors": [],
        "token_plaintext_found": False,
    }
    try:
        payload = getter(url, headers)
        report["request_count"] = 1
        report["real_requests_sent"] = True
    except Exception as exc:
        report["blocking_errors"].append(f"sec_request_failed:{exc}")
        _write_probe_report(output, report)
        print(json.dumps({"output": str(output), "overall_status": "blocked", "real_requests_sent": report["real_requests_sent"]}, sort_keys=True))
        return 1

    filings = _recent_sec_filings(payload)
    report["filing_sample"] = filings[:5]
    matched = [item for item in filings if item.get("report_date") == report_date and item.get("form") in {"10-K", "10-Q", "20-F"}]
    report["matched_filings"] = matched
    report["disclosure_events"] = [_sec_disclosure_event(ticker, normalized_cik, normalized_period, item) for item in matched]
    if matched:
        report["overall_status"] = "passed"
    else:
        report["overall_status"] = "warning"
        report["warnings"].append("no matching 10-K/10-Q/20-F filing found for requested period")
    _write_probe_report(output, report)
    print(json.dumps({"output": str(output), "overall_status": report["overall_status"], "real_requests_sent": True}, sort_keys=True))
    return 0


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


def _financial_hk_us_source_endpoints() -> list[dict[str, Any]]:
    return [
        item
        for item in hk_us_low_risk_source_endpoints()
        if item.get("api_name") in HK_US_FINANCIAL_PROBE_REQUESTS
    ]


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


def _financial_probe_report_header(output: Path, max_requests_per_endpoint: int) -> dict[str, Any]:
    return {
        "report_version": "hk-us-financial-pit-probe/v1",
        "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "output": str(output),
        "max_requests_per_endpoint": max_requests_per_endpoint,
        "global_max_requests": len(HK_US_FINANCIAL_PROBE_REQUESTS) * max_requests_per_endpoint,
        "real_requests_sent": False,
        "token_plaintext_found": False,
        "overall_status": "blocked",
        "blocking_errors": [],
        "warnings": [],
        "endpoints": [],
    }


def _financial_probe_status(status: str, row_count: int) -> str:
    if status in ACCESSIBLE:
        return "passed" if row_count else "empty_but_authorized"
    if status == "permission_denied":
        return "permission_denied"
    if status in {"invalid_params", "invalid_endpoint"}:
        return "contract_changed"
    return "failed"


def _params_shape(params: dict[str, Any]) -> dict[str, str]:
    return {str(key): type(value).__name__ for key, value in sorted(params.items())}


def _probe_financial_endpoint(client: Any, endpoint: dict[str, Any], max_requests_per_endpoint: int, token: str | None) -> dict[str, Any]:
    api_name = str(endpoint["api_name"])
    requests = HK_US_FINANCIAL_PROBE_REQUESTS.get(api_name, [])[:max_requests_per_endpoint]
    fields = HK_US_FINANCIAL_PROBE_FIELDS.get(api_name, list(endpoint.get("documented_output_fields") or endpoint.get("documented_fields") or [])[:12])
    statuses: list[str] = []
    errors: list[str] = []
    observed_fields: list[str] = []
    row_count = 0
    params_shapes: list[dict[str, str]] = []
    request_count = 0
    for params in requests:
        request_count += 1
        params_shapes.append(_params_shape(params))
        try:
            response = client.request(api_name, params, fields)
        except Exception as exc:
            statuses.append("unknown_error")
            errors.append(_redact_for_probe(str(exc), token))
            continue
        response = _redact_for_probe(response, token)
        status, message = classify_probe_response(response)
        statuses.append(status)
        if message:
            errors.append(message)
        data = response.get("data") or {}
        current_fields = [str(item) for item in (data.get("fields") or [])]
        if current_fields:
            for field in current_fields:
                if field not in observed_fields:
                    observed_fields.append(field)
        row_count += len(list(data.get("items") or []))
    classified = "not_run"
    if statuses:
        if any(item in ACCESSIBLE for item in statuses):
            classified = "accessible" if row_count else "empty_but_accessible"
        else:
            classified = statuses[-1]
    observed_disclosure = [field for field in observed_fields if field in HK_US_FINANCIAL_DISCLOSURE_FIELDS]
    return {
        "api_name": api_name,
        "market": endpoint.get("market"),
        "category": endpoint.get("category"),
        "probe_status": _financial_probe_status(classified, row_count),
        "request_count": request_count,
        "params_shape": params_shapes,
        "observed_fields": observed_fields,
        "observed_row_count": row_count,
        "observed_disclosure_fields": observed_disclosure,
        "observed_pagination_behavior": "not_tested" if request_count <= 1 else "bounded_multi_request",
        "recommended_planner_kind": endpoint.get("recommended_planner_kind"),
        "recommended_pagination_strategy": endpoint.get("recommended_pagination_strategy"),
        "error_type": None if classified in ACCESSIBLE else classified,
        "errors": errors,
        "redaction_status": "redacted",
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


def run_hk_us_financial_pit_probe(
    output: Path,
    max_requests_per_endpoint: int,
    *,
    token: str | None = None,
    client: Any | None = None,
) -> int:
    if max_requests_per_endpoint < 1:
        raise ValueError("--max-requests-per-endpoint must be positive")
    if max_requests_per_endpoint > 2:
        raise ValueError("--max-requests-per-endpoint must be <= 2 for HK/US financial PIT probes")
    output = _ensure_tmp_output_path(output)
    load_dotenv()
    token = token if token is not None else os.environ.get("TUSHARE_TOKEN")
    report = _financial_probe_report_header(output, max_requests_per_endpoint)
    if not token:
        report["blocking_errors"].append("TUSHARE_TOKEN is required; no real requests were sent")
        _write_probe_report(output, report)
        print(json.dumps({"output": str(output), "overall_status": "blocked", "real_requests_sent": False}, sort_keys=True))
        return 2

    client = client if client is not None else TushareClient(token)
    endpoints = _financial_hk_us_source_endpoints()
    rows = [_probe_financial_endpoint(client, endpoint, max_requests_per_endpoint, token) for endpoint in endpoints]
    report["endpoints"] = rows
    report["real_requests_sent"] = True
    blocked_statuses = {"contract_changed", "failed"}
    if any(row["probe_status"] in blocked_statuses for row in rows):
        report["overall_status"] = "blocked"
        report["blocking_errors"].append("one or more financial probes returned blocking errors")
    elif any(row["probe_status"] == "permission_denied" for row in rows):
        report["overall_status"] = "warning"
        report["warnings"].append("one or more financial probes could not confirm access due to permission status")
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


def _normalize_us_tushare_code(value: str) -> str:
    text = str(value)
    return text[:-3] if text.upper().endswith(".US") else text


def _parse_yyyymmdd(value: str | None) -> dt.date | None:
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) != 8:
        return None
    try:
        return dt.date(int(digits[:4]), int(digits[4:6]), int(digits[6:]))
    except ValueError:
        return None


def _classify_disclosure_cross_check(sec_date: str | None, tushare_date: str | None) -> dict[str, Any]:
    sec_parsed = _parse_yyyymmdd(sec_date)
    tushare_parsed = _parse_yyyymmdd(tushare_date)
    if not sec_parsed or not tushare_parsed:
        return {
            "match_status": "unmatched",
            "match_confidence": 0.0,
            "date_delta_days": None,
            "pit_strength_candidate": "raw_only",
        }
    delta = abs((tushare_parsed - sec_parsed).days)
    if delta == 0:
        return {
            "match_status": "exact",
            "match_confidence": 1.0,
            "date_delta_days": delta,
            "pit_strength_candidate": "availability_only",
        }
    if delta <= 7:
        return {
            "match_status": "near",
            "match_confidence": 0.8,
            "date_delta_days": delta,
            "pit_strength_candidate": "availability_only",
        }
    return {
        "match_status": "period_only",
        "match_confidence": 0.5,
        "date_delta_days": delta,
        "pit_strength_candidate": "raw_only",
    }


def _first_field_value(response: dict[str, Any], field_name: str) -> Any | None:
    data = response.get("data") or {}
    fields = [str(item) for item in (data.get("fields") or [])]
    items = list(data.get("items") or [])
    if field_name not in fields or not items:
        return None
    index = fields.index(field_name)
    first = list(items[0])
    if index >= len(first):
        return None
    return first[index]


def run_sec_tushare_disclosure_cross_check(
    output: Path,
    *,
    api_name: str,
    ts_code: str,
    ticker: str,
    cik: str,
    period: str,
    max_sec_requests: int,
    max_tushare_requests: int,
    token: str | None = None,
    sec_http_get: Any | None = None,
    tushare_client: Any | None = None,
) -> int:
    if api_name != "us_fina_indicator":
        raise ValueError("--api-name currently supports only us_fina_indicator")
    if max_tushare_requests < 1:
        raise ValueError("--max-tushare-requests must be positive")
    if max_tushare_requests > 1:
        raise ValueError("--max-tushare-requests must be <= 1")
    output = _ensure_tmp_output_path(output)
    normalized_period = _normalize_period(period)
    sec = _fetch_sec_disclosure_match(ticker=ticker, cik=cik, period=normalized_period, max_requests=max_sec_requests, http_get=sec_http_get)
    load_dotenv()
    token = token if token is not None else os.environ.get("TUSHARE_TOKEN")
    tushare_status = "blocked_token_missing"
    tushare_notice_date: str | None = None
    tushare_errors: list[str] = []
    tushare_request_count = 0
    if token:
        client = tushare_client if tushare_client is not None else TushareClient(token)
        fields = HK_US_FINANCIAL_PROBE_FIELDS["us_fina_indicator"]
        params = {"ts_code": _normalize_us_tushare_code(ts_code), "period": normalized_period}
        try:
            response = client.request(api_name, params, fields)
            tushare_request_count = 1
            response = _redact_for_probe(response, token)
            status, message = classify_probe_response(response)
            if status in ACCESSIBLE:
                value = _first_field_value(response, "notice_date")
                tushare_notice_date = str(value) if value is not None else None
                tushare_status = "passed" if tushare_notice_date else "notice_date_missing"
            else:
                tushare_status = status
            if message:
                tushare_errors.append(message)
        except Exception as exc:
            tushare_status = "blocked"
            tushare_errors.append(_redact_for_probe(str(exc), token))

    match = _classify_disclosure_cross_check(sec.get("sec_disclosure_date"), tushare_notice_date)
    limitations = [
        "SEC filing date and Tushare notice_date are compared; values are not reconciled.",
        "This probe does not authorize financial full pull or feature usage.",
    ]
    warnings: list[str] = []
    blocking_errors: list[str] = []
    if sec["sec_status"] == "blocked":
        blocking_errors.extend(sec["sec_errors"])
    elif sec["sec_status"] == "warning":
        warnings.extend(sec["sec_errors"])
    if tushare_status == "blocked_token_missing":
        warnings.append("TUSHARE_TOKEN is missing; Tushare side was skipped")
    elif tushare_status != "passed":
        warnings.extend(tushare_errors or [f"tushare_status:{tushare_status}"])
    overall_status = "blocked" if blocking_errors else ("passed" if match["match_status"] in {"exact", "near"} and tushare_status == "passed" else "warning")
    report = {
        "report_version": "sec-tushare-disclosure-cross-check/v1",
        "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "output": str(output),
        "api_name": api_name,
        "ts_code": ts_code,
        "tushare_request_ts_code": _normalize_us_tushare_code(ts_code),
        "ticker": ticker,
        "cik": _normalize_cik(cik),
        "period": normalized_period,
        "sec_status": sec["sec_status"],
        "tushare_status": tushare_status,
        "sec_request_count": sec["sec_request_count"],
        "tushare_request_count": tushare_request_count,
        "max_sec_requests": max_sec_requests,
        "max_tushare_requests": max_tushare_requests,
        "sec_disclosure_date": sec.get("sec_disclosure_date"),
        "tushare_notice_date": tushare_notice_date,
        "date_delta_days": match["date_delta_days"],
        "match_status": match["match_status"],
        "match_confidence": match["match_confidence"],
        "pit_strength_candidate": match["pit_strength_candidate"],
        "matched_filings": sec["matched_filings"],
        "disclosure_events": sec["disclosure_events"],
        "limitations": limitations,
        "warnings": warnings,
        "blocking_errors": blocking_errors,
        "overall_status": overall_status,
        "real_requests_sent": bool(sec["sec_request_count"] or tushare_request_count),
        "token_plaintext_found": False,
    }
    encoded = json.dumps(report, ensure_ascii=False)
    report["token_plaintext_found"] = bool(token and token in encoded)
    if report["token_plaintext_found"]:
        report["overall_status"] = "blocked"
        report["blocking_errors"].append("cross-check report would contain token plaintext")
        report = _redact_for_probe(report, token)
    _write_probe_report(output, report)
    print(json.dumps({"output": str(output), "overall_status": report["overall_status"], "real_requests_sent": report["real_requests_sent"]}, sort_keys=True))
    return 0 if report["overall_status"] in {"passed", "warning"} else 1


def run_hkex_disclosure_metadata_probe(
    output: Path,
    *,
    stock_code: str,
    period: str,
    max_requests: int,
    announcement_title: str | None = None,
) -> int:
    output = _ensure_tmp_output_path(output)
    gate = hkex_disclosure_automation_gate(
        stock_code=stock_code,
        period=period,
        max_requests=max_requests,
        announcement_title=announcement_title,
    )
    payload = gate.to_dict()
    payload["output"] = str(output)
    _write_probe_report(output, payload)
    print(json.dumps({"output": str(output), "overall_status": "blocked" if gate.blocking_errors else "warning", "real_requests_sent": False}, sort_keys=True))
    return 1 if gate.blocking_errors else 0


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
    parser.add_argument("--hk-us-financial-pit-probe", action="store_true", help="Run bounded HK/US financial PIT contract probes and write redacted diagnostics under /tmp.")
    parser.add_argument("--sec-disclosure-probe", action="store_true", help="Run a bounded SEC EDGAR disclosure metadata probe and write redacted diagnostics under /tmp.")
    parser.add_argument("--sec-tushare-disclosure-cross-check", action="store_true", help="Run bounded SEC-to-Tushare disclosure date cross-check diagnostics under /tmp.")
    parser.add_argument("--hkex-disclosure-metadata-probe", action="store_true", help="Write a conservative HKEX disclosure metadata automation gate report under /tmp without crawling or downloading documents.")
    parser.add_argument("--api-name", help="Tushare API name for SEC-to-Tushare disclosure cross-check.")
    parser.add_argument("--ticker", help="Ticker for SEC disclosure probe.")
    parser.add_argument("--cik", help="CIK for SEC disclosure probe.")
    parser.add_argument("--period", help="Financial period for disclosure probe, formatted YYYYMMDD.")
    parser.add_argument("--ts-code", help="Tushare ts_code for SEC-to-Tushare disclosure cross-check.")
    parser.add_argument("--stock-code", help="HK stock code for HKEX disclosure metadata gate.")
    parser.add_argument("--announcement-title", help="Optional HKEX title fixture for local metadata gate diagnostics.")
    parser.add_argument("--output", help="Probe output JSON path. Required for HK/US probes and must be under /tmp.")
    parser.add_argument("--max-requests", type=int, default=3, help="SEC disclosure probe request cap; maximum 3.")
    parser.add_argument("--max-sec-requests", type=int, default=3, help="SEC side request cap for cross-check probes; maximum 3.")
    parser.add_argument("--max-tushare-requests", type=int, default=1, help="Tushare side request cap for cross-check probes; maximum 1.")
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
    if args.sec_tushare_disclosure_cross_check:
        missing = [name for name in ["output", "api_name", "ts_code", "ticker", "cik", "period"] if not getattr(args, name)]
        if missing:
            print(f"--{missing[0].replace('_', '-')} is required for --sec-tushare-disclosure-cross-check", file=sys.stderr)
            return 2
        try:
            return run_sec_tushare_disclosure_cross_check(
                Path(args.output),
                api_name=args.api_name,
                ts_code=args.ts_code,
                ticker=args.ticker,
                cik=args.cik,
                period=args.period,
                max_sec_requests=args.max_sec_requests,
                max_tushare_requests=args.max_tushare_requests,
            )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    if args.hkex_disclosure_metadata_probe:
        missing = [name for name in ["output", "stock_code", "period"] if not getattr(args, name)]
        if missing:
            print(f"--{missing[0].replace('_', '-')} is required for --hkex-disclosure-metadata-probe", file=sys.stderr)
            return 2
        try:
            return run_hkex_disclosure_metadata_probe(
                Path(args.output),
                stock_code=args.stock_code,
                period=args.period,
                max_requests=args.max_requests,
                announcement_title=args.announcement_title,
            )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    if args.hk_us_financial_pit_probe:
        if not args.output:
            print("--output is required for --hk-us-financial-pit-probe", file=sys.stderr)
            return 2
        try:
            return run_hk_us_financial_pit_probe(Path(args.output), args.max_requests_per_endpoint)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    if args.sec_disclosure_probe:
        missing = [name for name in ["output", "ticker", "cik", "period"] if not getattr(args, name)]
        if missing:
            print(f"--{missing[0].replace('_', '-')} is required for --sec-disclosure-probe", file=sys.stderr)
            return 2
        try:
            return run_sec_disclosure_probe(
                Path(args.output),
                ticker=args.ticker,
                cik=args.cik,
                period=args.period,
                max_requests=args.max_requests,
            )
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
            print("No endpoints selected. Use --all-phase-1, --phase-2-low-volume, --a-share-low-risk-smoke, --hk-us-low-risk-probe, --hk-us-financial-pit-probe, --sec-disclosure-probe, --sec-tushare-disclosure-cross-check, --hkex-disclosure-metadata-probe, or --endpoint.", file=sys.stderr)
            return 2
        print_command_preview(Path(args.root), endpoints)
        return 0
    if not endpoints:
        print("No endpoints selected. Use --all-phase-1, --phase-2-low-volume, --a-share-low-risk-smoke, --hk-us-low-risk-probe, --hk-us-financial-pit-probe, --sec-disclosure-probe, --sec-tushare-disclosure-cross-check, --hkex-disclosure-metadata-probe, --calendar-backfill, or --endpoint.", file=sys.stderr)
        return 2
    return run_smoke(Path(args.root), endpoints, args.reset_root)


if __name__ == "__main__":
    raise SystemExit(main())
