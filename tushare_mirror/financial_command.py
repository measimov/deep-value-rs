from __future__ import annotations

import json
import shutil
import shlex
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .financial_reports import FinancialReadinessReporter, FinancialRequestEstimateReporter
from .source_metadata import hk_us_low_risk_source_endpoints


FINANCIAL_PULL_COMMAND_VERSION = "financial-pull-command/v1"


@dataclass(frozen=True)
class FinancialPullCommandResult:
    report_version: str
    scope: str
    root: str
    backup: str
    from_period: str
    to_period: str
    limit_codes: int
    max_periods: int
    output: str | None
    files: list[str]
    commands: list[dict[str, Any]]
    user_confirmation_required: bool
    warnings: list[str]
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


class FinancialPullCommandReporter:
    FILES = ["README.md", "commands.sh", "plan.json", "readiness.json", "probe_contract.json"]

    def create(
        self,
        *,
        scope: str,
        root: str | Path,
        backup: str | Path,
        from_period: str,
        to_period: str,
        limit_codes: int,
        max_periods: int,
        output: str | Path | None = None,
        overwrite: bool = False,
    ) -> FinancialPullCommandResult:
        mirror_root = _resolve_path(Path(root))
        backup_root = _resolve_path(Path(backup))
        output_path = _resolve_path(Path(output)) if output is not None else None
        warnings = [
            "financial-pull-command generates a guarded plan only and does not fetch or execute financial data",
            "commands.sh is commented and must not be run automatically",
        ]
        blocking_errors: list[str] = []
        if limit_codes <= 0:
            blocking_errors.append("limit_codes_must_be_positive")
        if limit_codes > 20:
            blocking_errors.append("limit_codes_exceeds_guarded_limit:20")
        if max_periods <= 0:
            blocking_errors.append("max_periods_must_be_positive")
        if max_periods > 20:
            blocking_errors.append("max_periods_exceeds_guarded_limit:20")
        if output_path is not None:
            blocking_errors.extend(_output_errors(output_path, mirror_root, backup_root, overwrite))

        readiness = FinancialReadinessReporter().report(scope=scope, root=mirror_root).to_dict()
        estimate = FinancialRequestEstimateReporter().report(
            scope=scope,
            from_period=from_period,
            to_period=to_period,
            limit_codes=limit_codes,
            max_periods=max_periods,
        ).to_dict()
        blocking_errors.extend(str(error) for error in readiness.get("blocking_errors", []))
        blocking_errors.extend(str(error) for error in estimate.get("blocking_errors", []))
        resolved_to_period = str(estimate.get("resolved_to_period") or to_period)
        commands = self._commands(scope, mirror_root, backup_root, from_period, to_period, resolved_to_period, limit_codes, max_periods, readiness)
        if blocking_errors:
            return self._result(scope, mirror_root, backup_root, from_period, to_period, limit_codes, max_periods, output_path, [], commands, warnings, blocking_errors)

        if output_path is not None:
            if output_path.exists() and overwrite:
                if output_path.is_dir():
                    shutil.rmtree(output_path)
                else:
                    output_path.unlink()
            output_path.mkdir(parents=True, exist_ok=False)
            probe_contract = self._probe_contract(scope)
            plan = {
                "report_version": FINANCIAL_PULL_COMMAND_VERSION,
                "scope": scope,
                "root": str(mirror_root),
                "backup": str(backup_root),
                "from_period": from_period,
                "to_period": to_period,
                "limit_codes": limit_codes,
                "max_periods": max_periods,
                "commands": commands,
                "user_confirmation_required": True,
                "not_a_full_pull": True,
            }
            (output_path / "README.md").write_text(self._readme(scope, from_period, to_period), encoding="utf-8")
            (output_path / "commands.sh").write_text(self._commands_sh(commands), encoding="utf-8")
            (output_path / "commands.sh").chmod(0o644)
            (output_path / "plan.json").write_text(_json(plan), encoding="utf-8")
            (output_path / "readiness.json").write_text(_json(readiness), encoding="utf-8")
            (output_path / "probe_contract.json").write_text(_json(probe_contract), encoding="utf-8")
            files = list(self.FILES)
        else:
            files = []
        return self._result(scope, mirror_root, backup_root, from_period, to_period, limit_codes, max_periods, output_path, files, commands, warnings, [])

    def _result(
        self,
        scope: str,
        root: Path,
        backup: Path,
        from_period: str,
        to_period: str,
        limit_codes: int,
        max_periods: int,
        output: Path | None,
        files: list[str],
        commands: list[dict[str, Any]],
        warnings: list[str],
        blocking_errors: list[str],
    ) -> FinancialPullCommandResult:
        return FinancialPullCommandResult(
            report_version=FINANCIAL_PULL_COMMAND_VERSION,
            scope=scope,
            root=str(root),
            backup=str(backup),
            from_period=from_period,
            to_period=to_period,
            limit_codes=limit_codes,
            max_periods=max_periods,
            output=str(output) if output is not None else None,
            files=files,
            commands=commands,
            user_confirmation_required=True,
            warnings=_dedupe(warnings),
            blocking_errors=_dedupe(blocking_errors),
        )

    def _commands(
        self,
        scope: str,
        root: Path,
        backup: Path,
        from_period: str,
        to_period: str,
        resolved_to_period: str,
        limit_codes: int,
        max_periods: int,
        readiness: dict[str, Any],
    ) -> list[dict[str, Any]]:
        raw_ready = list(readiness.get("raw_ready") or [])
        universe = "hk_listed" if scope == "hk-financial-raw" else "us_equity"
        commands = [
            self._command(
                "financial-readiness",
                f"python3 -m tushare_mirror financial-readiness --scope {shlex.quote(scope)} --root {shlex.quote(str(root))} --json",
            ),
            self._command(
                "financial-request-estimate",
                " ".join(
                    [
                        "python3 -m tushare_mirror financial-request-estimate",
                        f"--scope {shlex.quote(scope)}",
                        f"--from-period {shlex.quote(from_period)}",
                        f"--to-period {shlex.quote(to_period)}",
                        f"--limit-codes {limit_codes}",
                        f"--max-periods {max_periods}",
                        "--json",
                    ]
                ),
            ),
        ]
        for api_name in raw_ready:
            commands.append(
                self._command(
                    f"plan-{api_name}",
                    " ".join(
                        [
                            f"python3 -m tushare_mirror --root {shlex.quote(str(root))} code-period-plan",
                            f"--scope {shlex.quote(scope)}",
                            f"--api {shlex.quote(str(api_name))}",
                            f"--universe {universe}",
                            f"--limit-codes {limit_codes}",
                            f"--start-period {shlex.quote(from_period)}",
                            f"--end-period {shlex.quote(resolved_to_period)}",
                            f"--max-periods {max_periods}",
                            "--json",
                        ]
                    ),
                )
            )
        commands.append(
            {
                "command_name": "future-guarded-financial-execution",
                "command_text": (
                    "FINANCIAL_RAW_EXECUTOR_NOT_IMPLEMENTED; "
                    "USER_CONFIRMATION_REQUIRED before any future bounded financial raw execution"
                ),
                "would_execute_real_requests": False,
                "requires_user_confirmation": True,
                "guarded": True,
            }
        )
        commands.append(self._command("backup-status-after-manual-execution", f"python3 -m tushare_mirror backup-status --backup {shlex.quote(str(backup))} --json"))
        return commands

    def _command(self, name: str, text: str) -> dict[str, Any]:
        return {
            "command_name": name,
            "command_text": text,
            "would_execute_real_requests": False,
            "requires_user_confirmation": False,
            "guarded": True,
        }

    def _commands_sh(self, commands: list[dict[str, Any]]) -> str:
        lines = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "",
            "# Generated by financial-pull-command.",
            "# USER_CONFIRMATION_REQUIRED: this is a guarded plan artifact only.",
            "# Every command is commented. Do not run this script automatically.",
            "# No HK/US financial full pull has been authorized or executed.",
            "",
        ]
        for command in commands:
            lines.append(f"# {command['command_name']}")
            if command["requires_user_confirmation"]:
                lines.append("# USER_CONFIRMATION_REQUIRED: review and implement the future executor separately before use.")
            lines.append(f"# {command['command_text']}")
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def _readme(self, scope: str, from_period: str, to_period: str) -> str:
        return "\n".join(
            [
                "# HK/US Financial Pull Command Bundle",
                "",
                f"Scope: {scope}",
                f"Period range: {from_period}-{to_period}",
                "",
                "This bundle is a guarded command preview only.",
                "It does not fetch real Tushare data, execute stock loops, backfill financial data, or write durable mirror roots.",
                "Raw financial readiness is separate from PIT-safe readiness.",
                "commands.sh is commented and marked USER_CONFIRMATION_REQUIRED.",
            ]
        ) + "\n"

    def _probe_contract(self, scope: str) -> dict[str, Any]:
        market = "hk" if scope == "hk-financial-raw" else "us"
        endpoints = [
            item
            for item in hk_us_low_risk_source_endpoints()
            if item.get("market") == market and item.get("category") in {"financial_statement", "financial_indicator"}
        ]
        return {
            "report_version": "financial-probe-contract-source-map/v1",
            "scope": scope,
            "endpoints": endpoints,
        }


def _output_errors(output: Path, root: Path, backup: Path, overwrite: bool) -> list[str]:
    errors: list[str] = []
    if output == root or _is_relative_to(output, root):
        errors.append("output path is inside mirror root")
    if output == backup or _is_relative_to(output, backup):
        errors.append("output path is inside backup root")
    if output.exists() and not overwrite:
        errors.append("output path already exists; pass --overwrite to replace it")
    return errors


def _resolve_path(path: Path) -> Path:
    return path.expanduser().resolve()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out
