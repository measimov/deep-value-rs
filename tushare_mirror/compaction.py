from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .catalog import CatalogStore


SMALL_FILE_BYTES = 1 * 1024 * 1024
OVERSIZED_FILE_BYTES = 1024 * 1024 * 1024
SMALL_FILE_COUNT_THRESHOLD = 4
FILE_COUNT_THRESHOLD = 8


@dataclass(frozen=True)
class CompactionPartitionCandidate:
    partition_key: str
    file_count: int
    small_file_count: int
    oversized_file_count: int
    total_size_bytes: int
    estimated_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompactionPlan:
    api_name: str
    snapshot_id: str | None
    partitions_checked: int
    candidate_partitions: list[CompactionPartitionCandidate]
    small_file_count: int
    oversized_file_count: int
    estimated_actions: list[str]
    execution_allowed: bool
    dry_run: bool
    required_infra: list[str]
    warnings: list[str]
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        data["candidate_partitions"] = [item.to_dict() for item in self.candidate_partitions]
        return data


class CompactionPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore | None = None):
        self.root = Path(root)
        self.catalog = catalog or CatalogStore(self.root, read_only=True)

    def plan(self, api_name: str) -> CompactionPlan:
        if not self.catalog.db_path.exists():
            return self._result(api_name=api_name, snapshot_id=None, blocking_errors=[f"catalog not found: {self.catalog.db_path}"])
        snapshot = self.catalog.latest_snapshot(api_name)
        if not snapshot:
            return self._result(api_name=api_name, snapshot_id=None, warnings=["no latest snapshot for api"])
        lake_files = self.catalog.files_for_snapshot(snapshot["snapshot_id"], content_type="lake")
        partitions: dict[str, list[dict[str, Any]]] = {}
        for row in lake_files:
            key = _partition_key(row)
            partitions.setdefault(key, []).append(row)
        candidates: list[CompactionPartitionCandidate] = []
        total_small = 0
        total_oversized = 0
        for key, rows in sorted(partitions.items()):
            small = sum(1 for row in rows if int(row.get("size_bytes") or 0) < SMALL_FILE_BYTES)
            oversized = sum(1 for row in rows if int(row.get("size_bytes") or 0) > OVERSIZED_FILE_BYTES)
            total_small += small
            total_oversized += oversized
            if len(rows) > FILE_COUNT_THRESHOLD or small > SMALL_FILE_COUNT_THRESHOLD or oversized:
                action = "compact_small_files" if oversized == 0 else "split_or_rewrite_oversized_files"
                candidates.append(
                    CompactionPartitionCandidate(
                        partition_key=key,
                        file_count=len(rows),
                        small_file_count=small,
                        oversized_file_count=oversized,
                        total_size_bytes=sum(int(row.get("size_bytes") or 0) for row in rows),
                        estimated_action=action,
                    )
                )
        return self._result(
            api_name=api_name,
            snapshot_id=snapshot["snapshot_id"],
            partitions_checked=len(partitions),
            candidate_partitions=candidates,
            small_file_count=total_small,
            oversized_file_count=total_oversized,
            estimated_actions=sorted(set(item.estimated_action for item in candidates)),
        )

    def _result(
        self,
        *,
        api_name: str,
        snapshot_id: str | None,
        partitions_checked: int = 0,
        candidate_partitions: list[CompactionPartitionCandidate] | None = None,
        small_file_count: int = 0,
        oversized_file_count: int = 0,
        estimated_actions: list[str] | None = None,
        warnings: list[str] | None = None,
        blocking_errors: list[str] | None = None,
    ) -> CompactionPlan:
        return CompactionPlan(
            api_name=api_name,
            snapshot_id=snapshot_id,
            partitions_checked=partitions_checked,
            candidate_partitions=list(candidate_partitions or []),
            small_file_count=small_file_count,
            oversized_file_count=oversized_file_count,
            estimated_actions=list(estimated_actions or []),
            execution_allowed=False,
            dry_run=True,
            required_infra=[
                "compaction executor",
                "snapshot rewrite protocol",
                "backup and restore-check semantics for compacted files",
                "query benchmark",
            ],
            warnings=list(warnings or []),
            blocking_errors=list(blocking_errors or []),
        )


def _partition_key(row: dict[str, Any]) -> str:
    raw = row.get("partition_values_json") or "{}"
    try:
        values = json.loads(raw)
    except json.JSONDecodeError:
        return str(raw)
    if isinstance(values, dict):
        return json.dumps(values, ensure_ascii=False, sort_keys=True)
    return str(values)
