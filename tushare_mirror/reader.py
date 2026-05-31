from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .catalog import CatalogStore, loads


class LakeReader:
    def __init__(self, root: Path | str, catalog: CatalogStore | None = None):
        self.root = Path(root)
        self.catalog = catalog or CatalogStore(self.root)

    def list_active_files(self, api_name: str, snapshot_id: str | None = None) -> list[dict[str, Any]]:
        if snapshot_id in (None, "latest"):
            snap = self.catalog.latest_snapshot(api_name)
            if not snap:
                return []
            snapshot_id = snap["snapshot_id"]
        rows = self.catalog.files_for_snapshot(str(snapshot_id), content_type="lake")
        return [r for r in rows if r.get("status") not in {"quarantined", "missing", "deleted"}]

    def scan_api(self, api_name: str, snapshot_id: str | None = None, filters: Mapping[str, Any] | None = None, columns: list[str] | None = None) -> pa.Table:
        files = self.list_active_files(api_name, snapshot_id)
        tables = [pq.read_table(self.root / f["relative_path"]) for f in files]
        table = self._union_tables(tables)
        table = self._apply_filters(table, filters or {})
        if columns is not None:
            table = self._select_columns(table, columns)
        return table

    def scan_partition(self, api_name: str, partition_values: Mapping[str, Any], snapshot_id: str | None = None) -> pa.Table:
        tables = []
        for row in self.list_active_files(api_name, snapshot_id):
            values = loads(row.get("partition_values_json")) or {}
            if all(values.get(k) == v for k, v in partition_values.items()):
                tables.append(pq.read_table(self.root / row["relative_path"]))
        return self._union_tables(tables)

    def _union_tables(self, tables: list[pa.Table]) -> pa.Table:
        if not tables:
            return pa.table({})
        names: list[str] = []
        for table in tables:
            for name in table.column_names:
                if name not in names:
                    names.append(name)
        aligned = [self._select_columns(table, names) for table in tables]
        return pa.concat_tables(aligned, promote_options="default")

    def _select_columns(self, table: pa.Table, columns: list[str]) -> pa.Table:
        arrays = []
        for name in columns:
            if name in table.column_names:
                arrays.append(table[name])
            else:
                arrays.append(pa.nulls(table.num_rows))
        return pa.table(arrays, names=columns)

    def _apply_filters(self, table: pa.Table, filters: Mapping[str, Any]) -> pa.Table:
        if not filters or table.num_rows == 0:
            return table
        mask = None
        for key, value in filters.items():
            if key not in table.column_names:
                return table.slice(0, 0)
            condition = pc.equal(table[key], pa.scalar(value))
            mask = condition if mask is None else pc.and_(mask, condition)
        return table.filter(mask) if mask is not None else table
