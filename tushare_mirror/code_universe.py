from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .reader import LakeReader


SUPPORTED_CODE_UNIVERSES = {
    "a_share_listed",
    "a_share_active",
    "a_share_mainboard",
    "a_share_sme",
    "a_share_chinext",
    "a_share_star",
    "hs_const_sh",
    "hs_const_sz",
}


@dataclass(frozen=True)
class CodeUniverseResult:
    universe_name: str
    source_api: str
    source_snapshot_id: str | None
    source_record_count: int
    code_count: int
    codes_sample: list[str]
    blocked_reason: str | None
    warnings: list[str]
    codes: list[str]

    def to_dict(self, include_codes: bool = False) -> dict[str, Any]:
        data = asdict(self)
        if not include_codes:
            data.pop("codes", None)
        return data

    @property
    def blocked(self) -> bool:
        return self.blocked_reason is not None


class CodeUniverseProvider:
    def __init__(self, root: Path | str, catalog: CatalogStore | None = None):
        self.root = Path(root)
        self.catalog = catalog or CatalogStore(self.root)

    def get(self, universe_name: str, limit: int = 20) -> CodeUniverseResult:
        if universe_name not in SUPPORTED_CODE_UNIVERSES:
            return CodeUniverseResult(
                universe_name=universe_name,
                source_api="",
                source_snapshot_id=None,
                source_record_count=0,
                code_count=0,
                codes_sample=[],
                blocked_reason="unknown_universe",
                warnings=[f"supported universes: {', '.join(sorted(SUPPORTED_CODE_UNIVERSES))}"],
                codes=[],
            )
        if universe_name.startswith("hs_const_"):
            return self._hs_const_universe(universe_name, limit)
        return self._stock_basic_universe(universe_name, limit)

    def _stock_basic_universe(self, universe_name: str, limit: int) -> CodeUniverseResult:
        snapshot = self.catalog.latest_snapshot("stock_basic")
        if not snapshot:
            return self._blocked(universe_name, "stock_basic", "missing_stock_basic_latest_snapshot")
        table = LakeReader(self.root, self.catalog).scan_api("stock_basic", snapshot_id=snapshot["snapshot_id"])
        rows = table.to_pylist()
        warnings: list[str] = []
        if "ts_code" not in table.column_names:
            return self._blocked(universe_name, "stock_basic", "stock_basic_missing_ts_code_column", snapshot, table.num_rows)
        filtered = self._filter_stock_basic_rows(universe_name, rows, warnings)
        codes = sorted({str(row.get("ts_code")) for row in filtered if row.get("ts_code")})
        return CodeUniverseResult(
            universe_name=universe_name,
            source_api="stock_basic",
            source_snapshot_id=str(snapshot["snapshot_id"]),
            source_record_count=table.num_rows,
            code_count=len(codes),
            codes_sample=codes[: max(limit, 0)],
            blocked_reason=None,
            warnings=warnings,
            codes=codes,
        )

    def _filter_stock_basic_rows(self, universe_name: str, rows: list[dict[str, Any]], warnings: list[str]) -> list[dict[str, Any]]:
        if universe_name in {"a_share_listed", "a_share_active"}:
            return rows
        markets = {
            "a_share_mainboard": {"主板", "Main Board", "主板A股", "A股主板"},
            "a_share_sme": {"中小板", "SME", "中小企业板"},
            "a_share_chinext": {"创业板", "ChiNext", "创业板A股"},
            "a_share_star": {"科创板", "STAR", "科创板A股"},
        }
        allowed = markets.get(universe_name)
        if not allowed:
            return rows
        if rows and "market" not in rows[0]:
            warnings.append("stock_basic market column is unavailable; market-specific universe is empty")
            return []
        return [row for row in rows if row.get("market") in allowed]

    def _hs_const_universe(self, universe_name: str, limit: int) -> CodeUniverseResult:
        snapshot = self.catalog.latest_snapshot("hs_const")
        if not snapshot:
            return self._blocked(universe_name, "hs_const", "missing_hs_const_latest_snapshot")
        table = LakeReader(self.root, self.catalog).scan_api("hs_const", snapshot_id=snapshot["snapshot_id"])
        rows = table.to_pylist()
        if "ts_code" not in table.column_names:
            return self._blocked(universe_name, "hs_const", "hs_const_missing_ts_code_column", snapshot, table.num_rows)
        hs_type = "SH" if universe_name == "hs_const_sh" else "SZ"
        filtered = [row for row in rows if str(row.get("hs_type") or "").upper() == hs_type]
        codes = sorted({str(row.get("ts_code")) for row in filtered if row.get("ts_code")})
        return CodeUniverseResult(
            universe_name=universe_name,
            source_api="hs_const",
            source_snapshot_id=str(snapshot["snapshot_id"]),
            source_record_count=table.num_rows,
            code_count=len(codes),
            codes_sample=codes[: max(limit, 0)],
            blocked_reason=None,
            warnings=[],
            codes=codes,
        )

    def _blocked(
        self,
        universe_name: str,
        source_api: str,
        reason: str,
        snapshot: dict[str, Any] | None = None,
        source_record_count: int = 0,
    ) -> CodeUniverseResult:
        return CodeUniverseResult(
            universe_name=universe_name,
            source_api=source_api,
            source_snapshot_id=str(snapshot["snapshot_id"]) if snapshot else None,
            source_record_count=source_record_count,
            code_count=0,
            codes_sample=[],
            blocked_reason=reason,
            warnings=[],
            codes=[],
        )
