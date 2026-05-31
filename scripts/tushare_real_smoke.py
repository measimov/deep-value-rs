#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
from tushare_mirror.cli import load_dotenv
from tushare_mirror.endpoints import load_into_catalog

ENDPOINTS: dict[str, dict[str, Any]] = {
    "daily": {"trade_date": "20250102"},
    "stock_basic": {"list_status": "L"},
    "trade_cal": {"exchange": "SSE", "start_date": "20250101", "end_date": "20250131"},
    "adj_factor": {"trade_date": "20250102"},
    "daily_basic": {"trade_date": "20250102"},
}

ACCESSIBLE = {"accessible", "empty_but_accessible"}
PERMISSION_STATUSES = {"permission_denied", "rate_limited"}


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Opt-in real Tushare smoke test. Sends real requests only when executed.")
    parser.add_argument("--root", default="/tmp/tushare-mirror-real-smoke")
    parser.add_argument("--endpoint", choices=sorted(ENDPOINTS), action="append")
    parser.add_argument("--all-phase-1", action="store_true", help="Run daily plus Phase 1.2 low-volume endpoints.")
    parser.add_argument("--reset-root", action="store_true", help="Remove the root before running.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    endpoints = args.endpoint or list(ENDPOINTS)
    if args.all_phase_1:
        endpoints = list(ENDPOINTS)
    return run_smoke(Path(args.root), endpoints, args.reset_root)


if __name__ == "__main__":
    raise SystemExit(main())
