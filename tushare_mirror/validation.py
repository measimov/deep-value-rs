from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .catalog import CatalogStore
from .io_utils import read_jsonl_zst, sha256_file


class Validator:
    def __init__(self, root: Path | str, catalog: CatalogStore | None = None):
        self.root = Path(root)
        self.catalog = catalog or CatalogStore(self.root)

    def validate_file_rows(self, rows: list[dict[str, Any]], use_staged: bool = False) -> tuple[bool, list[tuple[str | None, str, str | None]]]:
        failures: list[tuple[str | None, str, str | None]] = []
        for row in rows:
            file_id = row.get("file_id")
            rel = row.get("staged_path") if use_staged and row.get("staged_path") else row.get("relative_path")
            path = Path(rel) if Path(str(rel)).is_absolute() else self.root / str(rel)
            if not path.exists():
                failures.append((file_id, "missing_file", str(path)))
                continue
            if int(row.get("size_bytes") or -1) != path.stat().st_size:
                failures.append((file_id, "size_mismatch", str(path)))
            actual_hash = sha256_file(path)
            if row.get("sha256") != actual_hash:
                failures.append((file_id, "checksum_mismatch", str(path)))
            if row.get("content_type") == "lake" and row.get("file_format") == "parquet":
                try:
                    meta = pq.ParquetFile(path).metadata
                    expected = row.get("record_count")
                    if expected is not None and meta.num_rows != int(expected):
                        failures.append((file_id, "record_count_mismatch", f"expected={expected} actual={meta.num_rows}"))
                    if not row.get("schema_id"):
                        failures.append((file_id, "schema_id_missing", str(path)))
                except Exception as e:
                    failures.append((file_id, "parquet_footer_unreadable", str(e)))
            if row.get("content_type") == "raw" and row.get("file_format") == "jsonl.zst":
                try:
                    events = read_jsonl_zst(path)
                    expected_events = row.get("raw_event_count")
                    if expected_events is not None and len(events) != int(expected_events):
                        failures.append((file_id, "raw_event_count_mismatch", f"expected={expected_events} actual={len(events)}"))
                except Exception as e:
                    failures.append((file_id, "raw_unreadable", str(e)))
        return not failures, failures

    def validate_snapshot(self, snapshot_id: str | None = None, api_name: str | None = None) -> tuple[bool, str]:
        snapshot = None
        if snapshot_id in (None, "latest"):
            snapshot = self.catalog.latest_snapshot(api_name)
            if not snapshot:
                run_id = self.catalog.record_validation(None, api_name, "failed", {"error": "snapshot_not_found"}, [(None, "snapshot_not_found", api_name)])
                return False, run_id
            snapshot_id = snapshot["snapshot_id"]
        rows = self.catalog.files_for_snapshot(str(snapshot_id))
        ok, failures = self.validate_file_rows(rows)
        status = "succeeded" if ok else "failed"
        run_id = self.catalog.record_validation(str(snapshot_id), api_name, status, {"files": len(rows), "failures": len(failures)}, failures)
        return ok, run_id
