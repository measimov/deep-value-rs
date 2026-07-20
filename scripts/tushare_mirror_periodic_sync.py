#!/usr/bin/env python3
"""Sequential scheduled runner for the existing low-risk market checkpoints."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SUPPORTED_SCOPES = (
    "a-share-low-risk",
    "hk-low-risk",
    "us-low-risk",
)
STATE_FILENAMES = {
    "a-share-low-risk": "TuShare-auto-sync-state.json",
    "hk-low-risk": "TuShare-hk-low-risk-auto-sync-state.json",
    "us-low-risk": "TuShare-us-low-risk-auto-sync-state.json",
}
SUPPORTED_STATE_VERSIONS = {
    "mirror-auto-sync-state/v1",
    "mirror-auto-sync-state/v2",
}


@dataclass(frozen=True)
class ScopeRun:
    scope: str
    state_path: Path
    command: list[str]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sequential A-share/HK/US low-risk incremental-sync coordinator. "
            "Planning-only by default; real requests require --execute and "
            "--confirm-periodic-sync."
        )
    )
    parser.add_argument(
        "--root",
        default=os.environ.get("TUSHARE_MIRROR_ROOT", "/mnt/gw/TuShare"),
    )
    parser.add_argument(
        "--backup",
        default=os.environ.get("TUSHARE_MIRROR_BACKUP", "/mnt/gw/TuShare-backup"),
    )
    parser.add_argument(
        "--state-dir",
        default=os.environ.get("TUSHARE_MIRROR_STATE_DIR", "/mnt/gw"),
    )
    parser.add_argument(
        "--scope",
        action="append",
        choices=SUPPORTED_SCOPES,
        dest="scopes",
        help="Run only this market scope; repeat to select more than one.",
    )
    parser.add_argument("--from-date", default="19900101")
    parser.add_argument("--to-date", default="latest-trade-date")
    parser.add_argument("--window-days", type=int, default=20)
    parser.add_argument("--max-jobs-per-api", type=int, default=20)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", type=int, default=60)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-periodic-sync", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def resolve_path(value: str) -> Path:
    return Path(value).expanduser().resolve()


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def validate_args(args: argparse.Namespace) -> tuple[Path, Path, Path, list[str]]:
    errors: list[str] = []
    root = resolve_path(args.root)
    backup = resolve_path(args.backup)
    state_dir = resolve_path(args.state_dir)

    if not root.is_dir():
        errors.append(f"mirror root does not exist: {root}")
    if not backup.is_dir():
        errors.append(f"backup root does not exist: {backup}")
    if not state_dir.is_dir():
        errors.append(f"state directory does not exist: {state_dir}")
    if root == backup:
        errors.append("mirror root and backup root must be different")
    if is_relative_to(backup, root) or is_relative_to(root, backup):
        errors.append("mirror root and backup root must not be nested")
    if args.window_days <= 0:
        errors.append("--window-days must be positive")
    if args.max_jobs_per_api <= 0 or args.max_jobs_per_api > 20:
        errors.append("--max-jobs-per-api must be between 1 and 20")
    if args.window_days > args.max_jobs_per_api:
        errors.append("--window-days must be <= --max-jobs-per-api")
    if args.max_attempts <= 0:
        errors.append("--max-attempts must be positive")
    if args.retry_backoff_seconds < 0:
        errors.append("--retry-backoff-seconds must be >= 0")
    if args.execute and not args.confirm_periodic_sync:
        errors.append("--execute requires --confirm-periodic-sync")
    if args.execute and not os.environ.get("TUSHARE_TOKEN"):
        errors.append("TUSHARE_TOKEN is required for execute mode")

    return root, backup, state_dir, errors


def load_state(path: Path, *, scope: str, root: Path, backup: Path) -> list[str]:
    errors: list[str] = []
    if not path.is_file():
        return [
            f"existing checkpoint state is required for scheduled incremental sync: {path}"
        ]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [f"invalid checkpoint state {path}: {exc}"]
    if not isinstance(payload, dict):
        return [f"invalid checkpoint state {path}: expected JSON object"]
    if payload.get("state_version") not in SUPPORTED_STATE_VERSIONS:
        errors.append(f"unsupported checkpoint state version: {payload.get('state_version')}")
    if payload.get("scope") != scope:
        errors.append(f"checkpoint scope mismatch for {scope}: {payload.get('scope')}")
    if resolve_path(str(payload.get("root", "__missing__"))) != root:
        errors.append(f"checkpoint root mismatch for {scope}")
    if resolve_path(str(payload.get("backup", "__missing__"))) != backup:
        errors.append(f"checkpoint backup mismatch for {scope}")
    return errors


def build_scope_runs(
    args: argparse.Namespace,
    root: Path,
    backup: Path,
    state_dir: Path,
) -> tuple[list[ScopeRun], list[str]]:
    scopes = args.scopes or list(SUPPORTED_SCOPES)
    runs: list[ScopeRun] = []
    errors: list[str] = []
    for scope in scopes:
        state_path = (state_dir / STATE_FILENAMES[scope]).resolve()
        if is_relative_to(state_path, root) or is_relative_to(state_path, backup):
            errors.append(f"checkpoint state must be outside mirror and backup roots: {state_path}")
            continue
        errors.extend(load_state(state_path, scope=scope, root=root, backup=backup))
        command = [
            sys.executable,
            "-m",
            "tushare_mirror",
            "mirror-auto-sync",
            "--root",
            str(root),
            "--backup",
            str(backup),
            "--scope",
            scope,
            "--from-date",
            args.from_date,
            "--to-date",
            args.to_date,
            "--window-days",
            str(args.window_days),
            "--max-jobs-per-api",
            str(args.max_jobs_per_api),
            "--state",
            str(state_path),
            "--max-attempts",
            str(args.max_attempts),
            "--retry-backoff-seconds",
            str(args.retry_backoff_seconds),
            "--execute",
            "--confirm-auto-sync",
            "--json",
        ]
        if scope in {"hk-low-risk", "us-low-risk"}:
            command.insert(-1, "--confirm-hk-us-auto-sync")
        runs.append(ScopeRun(scope=scope, state_path=state_path, command=command))
    return runs, errors


def command_preview(command: list[str]) -> str:
    import shlex

    return shlex.join(command)


def run_scope(item: ScopeRun) -> dict[str, Any]:
    started_at = now_utc()
    completed = subprocess.run(
        item.command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    payload: dict[str, Any] = {}
    parse_error: str | None = None
    if completed.stdout.strip():
        try:
            parsed = json.loads(completed.stdout)
            if isinstance(parsed, dict):
                payload = parsed
            else:
                parse_error = "mirror-auto-sync JSON output was not an object"
        except json.JSONDecodeError as exc:
            parse_error = f"could not parse mirror-auto-sync JSON output: {exc}"
    else:
        parse_error = "mirror-auto-sync produced no JSON output"

    blocking_errors = list(payload.get("blocking_errors") or [])
    if completed.returncode != 0 and not blocking_errors:
        blocking_errors.append(
            completed.stderr.strip() or f"mirror-auto-sync exited with {completed.returncode}"
        )
    if parse_error:
        blocking_errors.append(parse_error)
    status = payload.get("status") or ("succeeded" if completed.returncode == 0 else "blocked")
    if completed.returncode != 0 or parse_error:
        status = "blocked"
    return {
        "scope": item.scope,
        "status": status,
        "exit_code": completed.returncode,
        "started_at": started_at,
        "finished_at": now_utc(),
        "resolved_to_date": payload.get("resolved_to_date"),
        "executed_window_count": payload.get("executed_window_count", 0),
        "succeeded_window_count": payload.get("succeeded_window_count", 0),
        "failed_window_count": payload.get("failed_window_count", 0),
        "next_start_date": payload.get("next_start_date"),
        "state_path": str(item.state_path),
        "warnings": list(payload.get("warnings") or []),
        "blocking_errors": blocking_errors,
    }


def emit(report: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2), flush=True)
        return
    print(f"periodic_sync_status={report['status']}")
    for item in report.get("runs", []):
        print(
            f"scope={item['scope']} status={item['status']} "
            f"executed_windows={item.get('executed_window_count', 0)} "
            f"next_start_date={item.get('next_start_date')}"
        )
    for error in report.get("blocking_errors", []):
        print(f"error={error}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root, backup, state_dir, errors = validate_args(args)
    runs, state_errors = build_scope_runs(args, root, backup, state_dir)
    errors.extend(state_errors)
    report: dict[str, Any] = {
        "report_version": "tushare-mirror-periodic-sync/v1",
        "status": "blocked" if errors else ("running" if args.execute else "planned"),
        "execute": bool(args.execute),
        "root": str(root),
        "backup": str(backup),
        "state_dir": str(state_dir),
        "scope_order": [item.scope for item in runs],
        "runs": [],
        "commands": [
            {
                "scope": item.scope,
                "command": command_preview(item.command),
                "user_confirmation_required": True,
            }
            for item in runs
        ],
        "started_at": now_utc(),
        "finished_at": None,
        "warnings": [
            "market scopes run sequentially against the shared catalog and backup",
            "scheduled sync requires existing checkpoint states and never bootstraps a full pull",
            "financial/PIT, intraday, realtime, object, and plan-only endpoints are excluded",
        ],
        "blocking_errors": errors,
    }
    if errors:
        report["finished_at"] = now_utc()
        emit(report, as_json=args.json)
        return 2
    if not args.execute:
        report["finished_at"] = now_utc()
        emit(report, as_json=args.json)
        return 0

    runtime_dir = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"))
    lock_path = runtime_dir / "tushare-mirror-periodic-sync.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            report["status"] = "blocked"
            report["blocking_errors"] = ["another periodic sync coordinator is already running"]
            report["finished_at"] = now_utc()
            emit(report, as_json=args.json)
            return 75

        for item in runs:
            scope_result = run_scope(item)
            report["runs"].append(scope_result)

    failed_runs = [item for item in report["runs"] if item["status"] != "succeeded"]
    report["status"] = "blocked" if failed_runs else "succeeded"
    report["blocking_errors"] = [
        f"{item['scope']}: {error}"
        for item in failed_runs
        for error in item.get("blocking_errors", [])
    ]
    report["finished_at"] = now_utc()
    emit(report, as_json=args.json)
    return 1 if failed_runs else 0


if __name__ == "__main__":
    raise SystemExit(main())
