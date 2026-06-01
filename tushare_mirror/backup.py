from __future__ import annotations

import json
import os
import shutil
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .catalog import CatalogStore
from .io_utils import ensure_dir, now_utc, read_jsonl_zst, sha256_file

MANIFEST_VERSION = 1
_BLOCKED_FILE_STATUSES = {"quarantined", "missing", "deleted", "deleted_pending", "superseded", "compacted"}


def _connect_readonly_sqlite(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve().as_posix()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _catalog_summary_readonly(root: Path, catalog_path: Path) -> dict[str, Any]:
    with _connect_readonly_sqlite(catalog_path) as conn:
        version = conn.execute("select value from catalog_meta where key='catalog_schema_version'").fetchone()
        latest = conn.execute("select snapshot_id from snapshots where status='current' order by created_at desc limit 1").fetchone()
        latest_backfill = conn.execute("select run_id from ingestion_runs where run_type='backfill' order by started_at desc limit 1").fetchone()
        return {
            "catalog_path": str(catalog_path),
            "schema_version": int(version[0]) if version else 0,
            "endpoint_count": conn.execute("select count(*) from endpoints").fetchone()[0],
            "run_count": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
            "backfill_run_count": conn.execute("select count(*) from ingestion_runs where run_type='backfill'").fetchone()[0],
            "latest_backfill_run_id": latest_backfill[0] if latest_backfill else None,
            "job_count": conn.execute("select count(*) from jobs").fetchone()[0],
            "file_count": conn.execute("select count(*) from files").fetchone()[0],
            "snapshot_count": conn.execute("select count(*) from snapshots").fetchone()[0],
            "latest_snapshot": latest[0] if latest else None,
            "validation_count": conn.execute("select count(*) from validation_runs").fetchone()[0],
            "quarantine_count": conn.execute("select count(*) from quarantine_files").fetchone()[0],
        }


@dataclass(frozen=True)
class BackupFileItem:
    file_id: str
    api_name: str
    storage_layer: str
    source_relative_path: str
    backup_relative_path: str
    snapshot_ids: list[str]
    record_count: int | None
    raw_event_count: int | None
    size_bytes: int
    sha256: str
    exists: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BackupPlan:
    backup_id: str
    source_root: Path
    target: Path
    snapshot_scope: str
    api_names: list[str]
    snapshot_ids: list[str]
    catalog_path: Path
    endpoint_config_paths: list[Path]
    files: list[BackupFileItem]
    file_count: int
    raw_file_count: int
    lake_file_count: int
    object_file_count: int
    total_size_bytes: int
    catalog_included: bool
    warnings: list[str]
    rejected_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "source_root": str(self.source_root),
            "target": str(self.target),
            "snapshot_scope": self.snapshot_scope,
            "api_names": self.api_names,
            "snapshot_ids": self.snapshot_ids,
            "snapshot_count": len(self.snapshot_ids),
            "catalog_path": str(self.catalog_path),
            "endpoint_config_paths": [str(path.relative_to(self.source_root)) for path in self.endpoint_config_paths],
            "files": [item.to_dict() for item in self.files],
            "file_count": self.file_count,
            "raw_file_count": self.raw_file_count,
            "lake_file_count": self.lake_file_count,
            "object_file_count": self.object_file_count,
            "total_size_bytes": self.total_size_bytes,
            "catalog_included": self.catalog_included,
            "warnings": self.warnings,
            "rejected_reason": self.rejected_reason,
        }

    def summary(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "source_root": str(self.source_root),
            "target": str(self.target),
            "snapshot_scope": self.snapshot_scope,
            "api_names": self.api_names,
            "snapshot_count": len(self.snapshot_ids),
            "file_count": self.file_count,
            "raw_file_count": self.raw_file_count,
            "lake_file_count": self.lake_file_count,
            "object_file_count": self.object_file_count,
            "catalog_included": self.catalog_included,
            "total_size_bytes": self.total_size_bytes,
            "warnings": self.warnings,
            "rejected_reason": self.rejected_reason,
        }


@dataclass(frozen=True)
class BackupResult:
    backup_id: str
    target: Path
    manifest_path: Path
    status: str
    file_count: int
    total_size_bytes: int
    restore_check: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "target": str(self.target),
            "manifest_path": str(self.manifest_path),
            "status": self.status,
            "file_count": self.file_count,
            "total_size_bytes": self.total_size_bytes,
            "restore_check": self.restore_check,
        }

    def summary(self) -> dict[str, Any]:
        return self.to_dict()


@dataclass(frozen=True)
class ManifestValidationResult:
    backup_id: str | None
    status: str
    manifest_version: int | None
    manifest: dict[str, Any] | None
    errors: list[dict[str, Any]]
    warnings: list[dict[str, Any]]
    unsupported_manifest_version: bool = False

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "status": self.status,
            "manifest_version": self.manifest_version,
            "unsupported_manifest_version": self.unsupported_manifest_version,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "errors": self.errors,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class BackupInspectResult:
    backup_id: str | None
    status: str
    manifest_version: int | None
    created_at: str | None
    snapshot_scope: str | None
    catalog_schema_version: int | None
    api_names: list[str]
    snapshot_count: int
    file_count: int
    raw_file_count: int
    lake_file_count: int
    object_file_count: int
    endpoint_config_count: int
    total_size_bytes: int | None
    catalog_relative_path: str | None
    catalog_present: bool
    catalog_checksum_status: str | None
    possible_mutation: bool
    manifest_validation_status: str
    manifest_error_count: int
    manifest_warning_count: int
    warnings: list[dict[str, Any]]
    errors: list[dict[str, Any]]
    catalog_counts: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        data = self.to_dict()
        data.pop("errors", None)
        data.pop("catalog_counts", None)
        return data


@dataclass(frozen=True)
class RestoreCheckResult:
    backup_id: str | None
    status: str
    manifest_version: int | None
    catalog_status: str
    checked_file_count: int
    checked_raw_file_count: int
    checked_lake_file_count: int
    missing_file_count: int
    checksum_failure_count: int
    size_failure_count: int
    record_count_failure_count: int
    raw_event_count_failure_count: int
    parquet_failure_count: int
    raw_failure_count: int
    endpoint_config_failure_count: int
    file_count_failure_count: int
    manifest_validation_status: str
    unsupported_manifest_version: bool
    catalog_checksum_status: str | None
    possible_mutation: bool
    manifest_error_count: int
    manifest_warning_count: int
    failures: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def summary(self) -> dict[str, Any]:
        data = self.to_dict()
        data.pop("failures", None)
        return data


class BackupManifestValidator:
    TOP_LEVEL_REQUIRED = {
        "manifest_version",
        "backup_id",
        "created_at",
        "catalog_schema_version",
        "snapshot_scope",
        "api_names",
        "snapshot_ids",
        "file_count",
        "total_size_bytes",
        "catalog",
        "endpoint_configs",
        "files",
    }
    TOP_LEVEL_OPTIONAL = {"source_root"}
    CATALOG_REQUIRED = {"relative_path", "size_bytes", "sha256"}
    ENDPOINT_CONFIG_REQUIRED = {"relative_path", "size_bytes", "sha256"}
    FILE_REQUIRED = {
        "file_id",
        "api_name",
        "storage_layer",
        "source_relative_path",
        "backup_relative_path",
        "snapshot_ids",
        "size_bytes",
        "sha256",
    }
    FILE_OPTIONAL = {"record_count", "raw_event_count", "exists"}

    def load_and_validate(self, backup_root: Path | str) -> ManifestValidationResult:
        root = Path(backup_root)
        manifest_path = root / "manifest.json"
        if not manifest_path.exists():
            return ManifestValidationResult(
                None,
                "failed",
                None,
                None,
                [{"reason": "manifest_missing", "field": "manifest", "path": str(manifest_path)}],
                [],
            )
        try:
            payload = json.loads(manifest_path.read_text())
        except Exception as exc:
            return ManifestValidationResult(
                None,
                "failed",
                None,
                None,
                [{"reason": "manifest_unreadable", "field": "manifest", "details": str(exc)}],
                [],
            )
        if not isinstance(payload, dict):
            return ManifestValidationResult(
                None,
                "failed",
                None,
                None,
                [{"reason": "manifest_not_object", "field": "manifest"}],
                [],
            )
        return self.validate_manifest_dict(payload)

    def validate_manifest_dict(self, manifest: dict[str, Any]) -> ManifestValidationResult:
        errors: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        version = manifest.get("manifest_version")
        backup_id = manifest.get("backup_id")
        unsupported = False
        for field in sorted(self.TOP_LEVEL_REQUIRED):
            if field not in manifest:
                errors.append({"reason": "missing_required_field", "field": field})
        unknown = sorted(set(manifest) - self.TOP_LEVEL_REQUIRED - self.TOP_LEVEL_OPTIONAL)
        for field in unknown:
            warnings.append({"reason": "unknown_field", "field": field})
        if "manifest_version" in manifest and version != MANIFEST_VERSION:
            unsupported = True
            errors.append({"reason": "unsupported_manifest_version", "field": "manifest_version", "expected": MANIFEST_VERSION, "actual": version})
        self._require_list(manifest, "api_names", errors)
        self._require_list(manifest, "snapshot_ids", errors)
        self._require_non_negative_int(manifest, "catalog_schema_version", errors)
        self._require_non_negative_int(manifest, "file_count", errors)
        self._require_non_negative_int(manifest, "total_size_bytes", errors)
        catalog = manifest.get("catalog")
        if isinstance(catalog, dict):
            self._validate_entry("catalog", catalog, self.CATALOG_REQUIRED, set(), errors, warnings)
        elif "catalog" in manifest:
            errors.append({"reason": "invalid_type", "field": "catalog", "expected": "object"})
        endpoint_configs = manifest.get("endpoint_configs")
        if isinstance(endpoint_configs, list):
            for idx, entry in enumerate(endpoint_configs):
                if isinstance(entry, dict):
                    self._validate_entry(f"endpoint_configs[{idx}]", entry, self.ENDPOINT_CONFIG_REQUIRED, set(), errors, warnings)
                else:
                    errors.append({"reason": "invalid_type", "field": f"endpoint_configs[{idx}]", "expected": "object"})
        elif "endpoint_configs" in manifest:
            errors.append({"reason": "invalid_type", "field": "endpoint_configs", "expected": "list"})
        files = manifest.get("files")
        if isinstance(files, list):
            if "file_count" in manifest and isinstance(manifest.get("file_count"), int) and manifest.get("file_count") != len(files):
                errors.append({"reason": "file_count_mismatch", "field": "file_count", "expected": manifest.get("file_count"), "actual": len(files)})
            for idx, entry in enumerate(files):
                if isinstance(entry, dict):
                    self._validate_entry(f"files[{idx}]", entry, self.FILE_REQUIRED, self.FILE_OPTIONAL, errors, warnings)
                    if "snapshot_ids" in entry and not isinstance(entry.get("snapshot_ids"), list):
                        errors.append({"reason": "invalid_type", "field": f"files[{idx}].snapshot_ids", "expected": "list"})
                else:
                    errors.append({"reason": "invalid_type", "field": f"files[{idx}]", "expected": "object"})
        elif "files" in manifest:
            errors.append({"reason": "invalid_type", "field": "files", "expected": "list"})
        status = "failed" if errors else "succeeded"
        return ManifestValidationResult(
            str(backup_id) if backup_id is not None else None,
            status,
            version if isinstance(version, int) else None,
            manifest,
            errors,
            warnings,
            unsupported,
        )

    def _validate_entry(self, prefix: str, entry: dict[str, Any], required: set[str], optional: set[str], errors: list[dict[str, Any]], warnings: list[dict[str, Any]]) -> None:
        for field in sorted(required):
            key = f"{prefix}.{field}"
            if field not in entry:
                errors.append({"reason": "missing_required_field", "field": key})
                continue
            if field in {"relative_path", "sha256", "file_id", "api_name", "storage_layer", "source_relative_path", "backup_relative_path"}:
                self._require_non_empty_string(entry, field, key, errors)
            if field == "size_bytes":
                self._require_non_negative_int(entry, field, errors, key)
        unknown = sorted(set(entry) - required - optional)
        for field in unknown:
            warnings.append({"reason": "unknown_field", "field": f"{prefix}.{field}"})

    def _require_list(self, data: dict[str, Any], field: str, errors: list[dict[str, Any]]) -> None:
        if field in data and not isinstance(data.get(field), list):
            errors.append({"reason": "invalid_type", "field": field, "expected": "list"})

    def _require_non_negative_int(self, data: dict[str, Any], field: str, errors: list[dict[str, Any]], label: str | None = None) -> None:
        if field not in data:
            return
        value = data.get(field)
        if not isinstance(value, int) or value < 0:
            errors.append({"reason": "invalid_value", "field": label or field, "expected": "non_negative_integer", "actual": value})

    def _require_non_empty_string(self, data: dict[str, Any], field: str, label: str, errors: list[dict[str, Any]]) -> None:
        value = data.get(field)
        if not isinstance(value, str) or not value:
            errors.append({"reason": "invalid_value", "field": label, "expected": "non_empty_string", "actual": value})


class BackupInspector:
    def inspect(self, backup_root: Path | str) -> BackupInspectResult:
        root = Path(backup_root)
        validation = BackupManifestValidator().load_and_validate(root)
        manifest = validation.manifest or {}
        files = list(manifest.get("files") or []) if isinstance(manifest.get("files"), list) else []
        endpoint_configs = list(manifest.get("endpoint_configs") or []) if isinstance(manifest.get("endpoint_configs"), list) else []
        catalog_entry = manifest.get("catalog") if isinstance(manifest.get("catalog"), dict) else {}
        catalog_rel = str(catalog_entry.get("relative_path") or "_catalog/catalog.sqlite") if catalog_entry is not None else "_catalog/catalog.sqlite"
        catalog_path = root / catalog_rel
        catalog_counts = None
        catalog_checksum_status = None
        possible_mutation = False
        if catalog_path.exists():
            try:
                catalog_counts = _catalog_summary_readonly(root, catalog_path)
            except Exception as exc:
                validation = ManifestValidationResult(
                    validation.backup_id,
                    validation.status,
                    validation.manifest_version,
                    validation.manifest,
                    validation.errors,
                    validation.warnings + [{"reason": "catalog_inspect_failed", "details": str(exc)}],
                    validation.unsupported_manifest_version,
                )
            expected_catalog_hash = catalog_entry.get("sha256") if isinstance(catalog_entry, dict) else None
            if isinstance(expected_catalog_hash, str) and expected_catalog_hash:
                catalog_checksum_status = "matched" if sha256_file(catalog_path) == expected_catalog_hash else "mismatch"
                possible_mutation = catalog_checksum_status == "mismatch"
        return BackupInspectResult(
            backup_id=validation.backup_id,
            status="succeeded" if validation.status == "succeeded" else "failed",
            manifest_version=validation.manifest_version,
            created_at=manifest.get("created_at") if isinstance(manifest.get("created_at"), str) else None,
            snapshot_scope=manifest.get("snapshot_scope") if isinstance(manifest.get("snapshot_scope"), str) else None,
            catalog_schema_version=manifest.get("catalog_schema_version") if isinstance(manifest.get("catalog_schema_version"), int) else None,
            api_names=list(manifest.get("api_names") or []) if isinstance(manifest.get("api_names"), list) else [],
            snapshot_count=len(manifest.get("snapshot_ids") or []) if isinstance(manifest.get("snapshot_ids"), list) else 0,
            file_count=len(files),
            raw_file_count=sum(1 for item in files if isinstance(item, dict) and item.get("storage_layer") == "raw"),
            lake_file_count=sum(1 for item in files if isinstance(item, dict) and item.get("storage_layer") == "lake"),
            object_file_count=sum(1 for item in files if isinstance(item, dict) and item.get("storage_layer") == "object"),
            endpoint_config_count=len(endpoint_configs),
            total_size_bytes=manifest.get("total_size_bytes") if isinstance(manifest.get("total_size_bytes"), int) else None,
            catalog_relative_path=catalog_rel,
            catalog_present=catalog_path.exists(),
            catalog_checksum_status=catalog_checksum_status,
            possible_mutation=possible_mutation,
            manifest_validation_status=validation.status,
            manifest_error_count=validation.error_count,
            manifest_warning_count=validation.warning_count,
            warnings=validation.warnings,
            errors=validation.errors,
            catalog_counts=catalog_counts,
        )


class BackupPlanner:
    def __init__(self, source_root: Path | str, catalog: CatalogStore):
        self.source_root = Path(source_root)
        self.catalog = catalog

    def plan(self, target: Path | str, api_name: str | None = None) -> BackupPlan:
        backup_id = "backup_" + now_utc().replace("-", "").replace(":", "").replace(".", "").replace("Z", "")
        target_path = Path(target)
        snapshots = self.catalog.latest_snapshots(api_name)
        endpoint_paths = sorted((self.source_root / "_catalog" / "endpoints").glob("*.yaml"))
        warnings: list[str] = []
        if not snapshots:
            return BackupPlan(
                backup_id=backup_id,
                source_root=self.source_root,
                target=target_path,
                snapshot_scope="latest_all" if api_name is None else "latest_api",
                api_names=[] if api_name is None else [api_name],
                snapshot_ids=[],
                catalog_path=self.catalog.db_path,
                endpoint_config_paths=endpoint_paths,
                files=[],
                file_count=0,
                raw_file_count=0,
                lake_file_count=0,
                object_file_count=0,
                total_size_bytes=0,
                catalog_included=self.catalog.db_path.exists(),
                warnings=["No active snapshots to backup."],
                rejected_reason="no_active_snapshots",
            )
        files_by_id: dict[str, dict[str, Any]] = {}
        snapshot_ids_by_file: dict[str, set[str]] = {}
        for snapshot in snapshots:
            snapshot_id = str(snapshot["snapshot_id"])
            for row in self.catalog.files_for_snapshot(snapshot_id):
                if row.get("status") in _BLOCKED_FILE_STATUSES:
                    continue
                if row.get("content_type") not in {"raw", "lake", "object"}:
                    continue
                file_id = str(row["file_id"])
                files_by_id.setdefault(file_id, row)
                snapshot_ids_by_file.setdefault(file_id, set()).add(snapshot_id)
        items: list[BackupFileItem] = []
        for file_id, row in sorted(files_by_id.items(), key=lambda pair: str(pair[1].get("relative_path"))):
            rel = str(row.get("relative_path"))
            source_path = self.source_root / rel
            items.append(
                BackupFileItem(
                    file_id=file_id,
                    api_name=str(row.get("api_name") or ""),
                    storage_layer=str(row.get("content_type") or ""),
                    source_relative_path=rel,
                    backup_relative_path=rel,
                    snapshot_ids=sorted(snapshot_ids_by_file[file_id]),
                    record_count=row.get("record_count"),
                    raw_event_count=row.get("raw_event_count"),
                    size_bytes=int(row.get("size_bytes") or 0),
                    sha256=str(row.get("sha256") or ""),
                    exists=source_path.exists(),
                )
            )
            if not source_path.exists():
                warnings.append(f"missing source file: {rel}")
        api_names = sorted({str(snapshot.get("api_name")) for snapshot in snapshots if snapshot.get("api_name")})
        return BackupPlan(
            backup_id=backup_id,
            source_root=self.source_root,
            target=target_path,
            snapshot_scope="latest_all" if api_name is None else "latest_api",
            api_names=api_names,
            snapshot_ids=sorted(str(snapshot["snapshot_id"]) for snapshot in snapshots),
            catalog_path=self.catalog.db_path,
            endpoint_config_paths=endpoint_paths,
            files=items,
            file_count=len(items),
            raw_file_count=sum(1 for item in items if item.storage_layer == "raw"),
            lake_file_count=sum(1 for item in items if item.storage_layer == "lake"),
            object_file_count=sum(1 for item in items if item.storage_layer == "object"),
            total_size_bytes=sum(item.size_bytes for item in items),
            catalog_included=self.catalog.db_path.exists(),
            warnings=warnings,
            rejected_reason=None,
        )


class BackupExecutor:
    def __init__(self, source_root: Path | str, catalog: CatalogStore):
        self.source_root = Path(source_root)
        self.catalog = catalog

    def backup(self, plan: BackupPlan, overwrite: bool = False) -> BackupResult:
        if plan.rejected_reason:
            raise ValueError(plan.warnings[0] if plan.warnings else plan.rejected_reason)
        target = plan.target
        self._guard_target(target, overwrite)
        staging = target.parent / f"{target.name}.tmp.{plan.backup_id}"
        if staging.exists():
            shutil.rmtree(staging)
        try:
            ensure_dir(staging)
            self._copy_catalog(staging / "_catalog" / "catalog.sqlite")
            endpoint_entries = self._copy_endpoint_configs(plan, staging)
            copied_files = self._copy_files(plan, staging)
            catalog_entry = self._catalog_entry(staging)
            manifest = self._manifest(plan, catalog_entry, endpoint_entries, copied_files)
            manifest_path = staging / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2))
            check = RestoreChecker().check(staging)
            if check.status != "succeeded":
                raise ValueError(f"restore-check failed for staging backup: {check.failures}")
            if target.exists():
                self._remove_existing_target(target)
            os.replace(staging, target)
            return BackupResult(plan.backup_id, target, target / "manifest.json", "succeeded", plan.file_count, plan.total_size_bytes, check.to_dict())
        except Exception:
            if staging.exists():
                shutil.rmtree(staging)
            raise

    def _guard_target(self, target: Path, overwrite: bool) -> None:
        source_resolved = self.source_root.resolve()
        target_resolved = target.resolve() if target.exists() else target.absolute().resolve().parent / target.name
        if target_resolved == source_resolved:
            raise ValueError("backup target must not be the source root")
        try:
            target_resolved.relative_to(source_resolved)
        except ValueError:
            pass
        else:
            raise ValueError("backup target must not be inside the source root")
        if target.exists() and not overwrite:
            if target.is_dir() and not any(target.iterdir()):
                return
            raise FileExistsError(f"backup target already exists: {target}; pass --overwrite to replace it")

    def _remove_existing_target(self, target: Path) -> None:
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()

    def _copy_catalog(self, dest: Path) -> None:
        ensure_dir(dest.parent)
        source = sqlite3.connect(self.catalog.db_path)
        try:
            target = sqlite3.connect(dest)
            try:
                source.backup(target)
            finally:
                target.close()
        finally:
            source.close()

    def _copy_endpoint_configs(self, plan: BackupPlan, staging: Path) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for source in plan.endpoint_config_paths:
            rel = source.relative_to(plan.source_root).as_posix()
            dest = staging / rel
            ensure_dir(dest.parent)
            shutil.copy2(source, dest)
            entries.append({"relative_path": rel, "size_bytes": dest.stat().st_size, "sha256": sha256_file(dest)})
        return entries

    def _copy_files(self, plan: BackupPlan, staging: Path) -> list[dict[str, Any]]:
        copied: list[dict[str, Any]] = []
        for item in plan.files:
            src = plan.source_root / item.source_relative_path
            if not src.exists():
                raise FileNotFoundError(src)
            dest = staging / item.backup_relative_path
            ensure_dir(dest.parent)
            shutil.copy2(src, dest)
            size = dest.stat().st_size
            actual_hash = sha256_file(dest)
            if size != item.size_bytes:
                raise ValueError(f"backup size mismatch for {item.backup_relative_path}")
            if actual_hash != item.sha256:
                raise ValueError(f"backup checksum mismatch for {item.backup_relative_path}")
            copied.append(item.to_dict())
        return copied

    def _catalog_entry(self, staging: Path) -> dict[str, Any]:
        rel = "_catalog/catalog.sqlite"
        path = staging / rel
        return {"relative_path": rel, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)}

    def _manifest(self, plan: BackupPlan, catalog_entry: dict[str, Any], endpoint_entries: list[dict[str, Any]], copied_files: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "manifest_version": MANIFEST_VERSION,
            "backup_id": plan.backup_id,
            "created_at": now_utc(),
            "source_root": str(plan.source_root),
            "catalog_schema_version": self._catalog_schema_version(),
            "snapshot_scope": plan.snapshot_scope,
            "api_names": plan.api_names,
            "snapshot_ids": plan.snapshot_ids,
            "file_count": len(copied_files),
            "total_size_bytes": sum(int(item.get("size_bytes") or 0) for item in copied_files),
            "catalog": catalog_entry,
            "endpoint_configs": endpoint_entries,
            "files": copied_files,
        }

    def _catalog_schema_version(self) -> int:
        with sqlite3.connect(self.catalog.db_path) as conn:
            row = conn.execute("select value from catalog_meta where key='catalog_schema_version'").fetchone()
        return int(row[0]) if row else 0


class RestoreChecker:
    def check(self, backup_root: Path | str) -> RestoreCheckResult:
        root = Path(backup_root)
        validation = BackupManifestValidator().load_and_validate(root)
        manifest = validation.manifest or {}
        if validation.status != "succeeded":
            file_count_failures = sum(1 for failure in validation.errors if failure.get("reason") == "file_count_mismatch")
            return RestoreCheckResult(
                backup_id=validation.backup_id,
                status="failed",
                manifest_version=validation.manifest_version,
                catalog_status="not_checked",
                checked_file_count=0,
                checked_raw_file_count=0,
                checked_lake_file_count=0,
                missing_file_count=0,
                checksum_failure_count=0,
                size_failure_count=0,
                record_count_failure_count=0,
                raw_event_count_failure_count=0,
                parquet_failure_count=0,
                raw_failure_count=0,
                endpoint_config_failure_count=0,
                file_count_failure_count=file_count_failures,
                manifest_validation_status=validation.status,
                unsupported_manifest_version=validation.unsupported_manifest_version,
                catalog_checksum_status=None,
                possible_mutation=False,
                manifest_error_count=validation.error_count,
                manifest_warning_count=validation.warning_count,
                failures=validation.errors,
            )
        failures: list[dict[str, Any]] = []
        catalog_status = self._check_catalog(root, manifest, failures)
        self._check_endpoint_configs(root, manifest, failures)
        counts = {
            "checked": 0,
            "raw": 0,
            "lake": 0,
            "missing": 0,
            "checksum": 0,
            "size": 0,
            "record": 0,
            "raw_event": 0,
            "parquet": 0,
            "raw_failure": 0,
        }
        files = list(manifest.get("files") or [])
        for item in files:
            self._check_file(root, item, failures, counts)
        endpoint_failures = sum(1 for failure in failures if str(failure.get("reason", "")).startswith("endpoint_config"))
        file_count_failures = sum(1 for failure in failures if failure.get("reason") == "file_count_mismatch")
        catalog_checksum_failures = [failure for failure in failures if failure.get("reason") == "catalog_checksum_mismatch"]
        catalog_checksum_status = "mismatch" if catalog_checksum_failures else ("matched" if catalog_status == "succeeded" else None)
        possible_mutation = bool(catalog_checksum_failures)
        status = "succeeded" if not failures else "failed"
        return RestoreCheckResult(
            backup_id=manifest.get("backup_id"),
            status=status,
            manifest_version=validation.manifest_version,
            catalog_status=catalog_status,
            checked_file_count=counts["checked"],
            checked_raw_file_count=counts["raw"],
            checked_lake_file_count=counts["lake"],
            missing_file_count=counts["missing"],
            checksum_failure_count=counts["checksum"],
            size_failure_count=counts["size"],
            record_count_failure_count=counts["record"],
            raw_event_count_failure_count=counts["raw_event"],
            parquet_failure_count=counts["parquet"],
            raw_failure_count=counts["raw_failure"],
            endpoint_config_failure_count=endpoint_failures,
            file_count_failure_count=file_count_failures,
            manifest_validation_status=validation.status,
            unsupported_manifest_version=validation.unsupported_manifest_version,
            catalog_checksum_status=catalog_checksum_status,
            possible_mutation=possible_mutation,
            manifest_error_count=validation.error_count,
            manifest_warning_count=validation.warning_count,
            failures=failures,
        )

    def _check_catalog(self, root: Path, manifest: dict[str, Any], failures: list[dict[str, Any]]) -> str:
        entry = manifest.get("catalog") or {}
        rel = entry.get("relative_path") or "_catalog/catalog.sqlite"
        path = root / str(rel)
        if not path.exists():
            failures.append({"reason": "catalog_missing", "path": str(rel)})
            return "missing"
        status = "succeeded"
        if int(entry.get("size_bytes") or -1) != path.stat().st_size:
            failures.append({"reason": "catalog_size_mismatch", "path": str(rel)})
            status = "failed"
        if entry.get("sha256") != sha256_file(path):
            failures.append({
                "reason": "catalog_checksum_mismatch",
                "path": str(rel),
                "details": "catalog checksum mismatch: backup catalog may have been modified after backup creation",
                "possible_mutation": True,
            })
            status = "failed"
        try:
            conn = _connect_readonly_sqlite(path)
            conn.execute("select 1").fetchone()
            conn.close()
        except Exception as exc:
            failures.append({"reason": "catalog_unreadable", "path": str(rel), "details": str(exc)})
            status = "failed"
        return status

    def _check_endpoint_configs(self, root: Path, manifest: dict[str, Any], failures: list[dict[str, Any]]) -> None:
        for entry in manifest.get("endpoint_configs") or []:
            rel = str(entry.get("relative_path"))
            path = root / rel
            if not path.exists():
                failures.append({"reason": "endpoint_config_missing", "path": rel})
                continue
            if int(entry.get("size_bytes") or -1) != path.stat().st_size:
                failures.append({"reason": "endpoint_config_size_mismatch", "path": rel})
            if entry.get("sha256") != sha256_file(path):
                failures.append({"reason": "endpoint_config_checksum_mismatch", "path": rel})

    def _check_file(self, root: Path, item: dict[str, Any], failures: list[dict[str, Any]], counts: dict[str, int]) -> None:
        rel = str(item.get("backup_relative_path") or item.get("source_relative_path"))
        path = root / rel
        layer = str(item.get("storage_layer") or "")
        if layer == "raw":
            counts["raw"] += 1
        if layer == "lake":
            counts["lake"] += 1
        if not path.exists():
            counts["missing"] += 1
            failures.append({"file_id": item.get("file_id"), "reason": "missing_file", "path": rel})
            return
        counts["checked"] += 1
        if int(item.get("size_bytes") or -1) != path.stat().st_size:
            counts["size"] += 1
            failures.append({"file_id": item.get("file_id"), "reason": "size_mismatch", "path": rel})
        if item.get("sha256") != sha256_file(path):
            counts["checksum"] += 1
            failures.append({"file_id": item.get("file_id"), "reason": "checksum_mismatch", "path": rel})
        if layer == "lake":
            try:
                meta = pq.ParquetFile(path).metadata
                expected = item.get("record_count")
                if expected is not None and meta.num_rows != int(expected):
                    counts["record"] += 1
                    failures.append({"file_id": item.get("file_id"), "reason": "record_count_mismatch", "path": rel, "expected": expected, "actual": meta.num_rows})
            except Exception as exc:
                counts["parquet"] += 1
                failures.append({"file_id": item.get("file_id"), "reason": "parquet_footer_unreadable", "path": rel, "details": str(exc)})
        if layer == "raw":
            try:
                events = read_jsonl_zst(path)
                expected_events = item.get("raw_event_count")
                if expected_events is not None and len(events) != int(expected_events):
                    counts["raw_event"] += 1
                    failures.append({"file_id": item.get("file_id"), "reason": "raw_event_count_mismatch", "path": rel, "expected": expected_events, "actual": len(events)})
            except Exception as exc:
                counts["raw_failure"] += 1
                failures.append({"file_id": item.get("file_id"), "reason": "raw_unreadable", "path": rel, "details": str(exc)})
