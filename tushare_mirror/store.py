from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import CatalogStore
from .client import QueryResult
from .errors import ErrorType, MirrorError, classify_exception, retry_delay_seconds, should_retry
from .hashing import job_key as make_job_key, params_hash, row_hash, sha256_hex
from .io_utils import atomic_move, ensure_dir, move_tree_to_quarantine, now_utc, sha256_file, write_jsonl_zst
from .schema import SchemaRegistry
from .validation import Validator

STANDARD_METADATA_COLUMNS = [
    "_api_name",
    "_params_hash",
    "_row_hash",
    "_fetched_at",
    "_source_fields",
    "_job_key",
    "_run_id",
]


@dataclass
class FetchResult:
    run_id: str
    job_key: str
    snapshot_id: str | None
    record_count: int
    skipped: bool = False


class FileLakeStore:
    def __init__(self, root: Path | str, catalog: CatalogStore | None = None, retry_sleep: Callable[[float], None] | None = None):
        self.root = Path(root)
        self.catalog = catalog or CatalogStore(self.root)
        self.retry_sleep = retry_sleep or time.sleep

    def plan_fetch(self, api_name: str, params: Mapping[str, Any]) -> dict[str, Any]:
        cfg = self.catalog.get_endpoint_config(api_name)
        fields = list(cfg.get("default_fields") or [])
        partition_spec_id = cfg["partition_spec_id"]
        key = make_job_key(api_name, params, fields, partition_spec_id)
        partition_values = self._partition_values(cfg, params)
        permission = self.catalog.latest_permission(api_name)
        permission_status = permission.get("status") if permission else "unknown"
        permission_expired = True
        if permission and permission.get("valid_until"):
            permission_expired = self._is_expired(str(permission["valid_until"]))
        existing_job = self.catalog.get_job(key)
        active_lake_files = self.catalog.active_files_for_job(key, api_name, content_type="lake")
        existing_active = bool(existing_job and existing_job.get("status") == "done" and active_lake_files)
        planned_actions = ["no_op_existing_active_data"] if existing_active else ["request_tushare", "write_raw_jsonl_zst", "write_lake_parquet", "validate", "commit_snapshot"]
        return {
            "api_name": api_name,
            "params_hash": params_hash(params),
            "job_key": key,
            "volume_class": cfg.get("volume_class"),
            "partition_values": partition_values,
            "raw_path": self._raw_relative_path(api_name, key),
            "lake_path_prefix": str(Path(self._lake_relative_path(cfg, params, key)).parent),
            "permission_status": permission_status,
            "permission_expired": permission_expired,
            "existing_active_data": existing_active,
            "planned_actions": planned_actions,
        }

    def fetch(self, api_name: str, params: Mapping[str, Any], client, max_attempts: int = 3) -> FetchResult:
        cfg = self.catalog.get_endpoint_config(api_name)
        fields = list(cfg.get("default_fields") or [])
        table_id = cfg["table_id"]
        partition_spec_id = cfg["partition_spec_id"]
        key = make_job_key(api_name, params, fields, partition_spec_id)
        existing = self.catalog.get_job(key)
        active_lake_files = self.catalog.active_files_for_job(key, api_name, content_type="lake")
        if existing and existing.get("status") == "done" and active_lake_files:
            snap = self.catalog.latest_snapshot(api_name)
            return FetchResult("", key, snap["snapshot_id"] if snap else None, int(existing.get("record_count") or 0), skipped=True)

        run_id = self.catalog.create_run("fetch")
        self.catalog.upsert_job(key, run_id, api_name, params, fields, "running")
        tmp_dir = self.root / "_tmp" / f"run_id={run_id}"
        raw_tmp = tmp_dir / "raw" / f"{key}.jsonl.zst"
        parquet_tmp = tmp_dir / "lake" / f"{key}.parquet"
        try:
            page_size = cfg.get("page_size") or 5000
            result = self._query_with_retry(client, api_name, params, fields, page_size, max_attempts)
            raw_events = [self._raw_event(run_id, key, api_name, event, fields) for event in result.events]
            try:
                raw_event_count = write_jsonl_zst(raw_tmp, raw_events)
            except Exception as e:
                raise MirrorError(ErrorType.WRITE_FAILED, f"raw write failed: {e}") from e
            source_item_count = len(result.items)
            error_event_count = sum(1 for e in raw_events if e.get("tushare_code") not in (0, None))

            schema_registry = SchemaRegistry(self.catalog)
            decision = schema_registry.decide(api_name, result.fields, result.items)
            if not decision.compatible:
                self.catalog.record_schema_change(
                    api_name,
                    decision.details.get("old_schema_id"),
                    decision.schema_id,
                    decision.change_type,
                    decision.details,
                    approved=False,
                )
                raw_hash = sha256_file(raw_tmp)
                self._quarantine(run_id, key, api_name, ErrorType.SCHEMA_INCOMPATIBLE.value, tmp_dir, raw_tmp, raw_hash)
                self.catalog.update_job_failed(key, f"schema incompatible: {decision.details}", ErrorType.SCHEMA_INCOMPATIBLE.value)
                self.catalog.finish_run(run_id, "failed", "schema incompatible", ErrorType.SCHEMA_INCOMPATIBLE.value, {"job_key": key})
                return FetchResult(run_id, key, None, 0)

            schema_registry.commit(api_name, decision)
            rows = self._rows(api_name, params, key, run_id, decision.schema_id, result.fields, result.items)
            record_count = len(rows)
            try:
                self._write_parquet(parquet_tmp, rows)
            except Exception as e:
                raise MirrorError(ErrorType.WRITE_FAILED, f"lake parquet write failed: {e}") from e

            partition_values = self._partition_values(cfg, params)
            raw_rel = self._raw_relative_path(api_name, key)
            lake_rel = self._lake_relative_path(cfg, params, key)
            raw_file_id = self.catalog.insert_file(
                table_id=table_id,
                api_name=api_name,
                content_type="raw",
                file_format="jsonl.zst",
                relative_path=raw_rel,
                staged_path=str(raw_tmp),
                partition_values=partition_values,
                record_count=None,
                source_item_count=source_item_count,
                raw_event_count=raw_event_count,
                error_event_count=error_event_count,
                size_bytes=raw_tmp.stat().st_size,
                sha256=sha256_file(raw_tmp),
                schema_id=None,
                status="staged",
                run_id=run_id,
                job_key=key,
            )
            lake_file_id = self.catalog.insert_file(
                table_id=table_id,
                api_name=api_name,
                content_type="lake",
                file_format="parquet",
                relative_path=lake_rel,
                staged_path=str(parquet_tmp),
                partition_values=partition_values,
                record_count=record_count,
                source_item_count=source_item_count,
                raw_event_count=None,
                error_event_count=error_event_count,
                size_bytes=parquet_tmp.stat().st_size,
                sha256=sha256_file(parquet_tmp),
                schema_id=decision.schema_id,
                status="staged",
                run_id=run_id,
                job_key=key,
            )

            validator = Validator(self.root, self.catalog)
            ok, failures = validator.validate_file_rows([self.catalog.get_file(raw_file_id), self.catalog.get_file(lake_file_id)], use_staged=True)
            if not ok:
                self._quarantine(run_id, key, api_name, ErrorType.VALIDATION_FAILED.value, tmp_dir, raw_tmp, None)
                self.catalog.update_job_failed(key, json.dumps(failures), ErrorType.VALIDATION_FAILED.value)
                self.catalog.finish_run(run_id, "failed", "validation failed", ErrorType.VALIDATION_FAILED.value, {"failures": failures})
                return FetchResult(run_id, key, None, 0)

            raw_final = self.root / raw_rel
            lake_final = self.root / lake_rel
            try:
                atomic_move(raw_tmp, raw_final)
                atomic_move(parquet_tmp, lake_final)
            except Exception as e:
                raise MirrorError(ErrorType.WRITE_FAILED, f"final file commit failed: {e}") from e
            self.catalog.update_file_status(raw_file_id, "staged", None)
            self.catalog.update_file_status(lake_file_id, "staged", None)
            try:
                snapshot_id = self.catalog.commit_snapshot(
                    api_name=api_name,
                    table_id=table_id,
                    file_ids=[raw_file_id, lake_file_id],
                    run_id=run_id,
                    checkpoint_key=key,
                    cursor=json.dumps(params, sort_keys=True),
                )
            except Exception as e:
                raise MirrorError(ErrorType.CATALOG_COMMIT_FAILED, f"catalog commit failed: {e}") from e
            self.catalog.update_job_done(key, record_count, source_item_count, raw_event_count, error_event_count)
            self.catalog.finish_run(run_id, "succeeded", summary={"job_key": key, "record_count": record_count, "raw_event_count": raw_event_count})
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return FetchResult(run_id, key, snapshot_id, record_count)
        except Exception as e:
            error_type = classify_exception(e)
            self.catalog.update_job_failed(key, str(e), error_type.value)
            self.catalog.finish_run(run_id, "failed", str(e), error_type.value, {"job_key": key})
            qdir = self.root / "_quarantine" / f"run_id={run_id}" / f"api={api_name}" / f"job={key}" / f"reason={error_type.value}"
            move_tree_to_quarantine(tmp_dir, qdir)
            if qdir.exists():
                self.catalog.record_quarantine(run_id, key, api_name, error_type.value, str(qdir.relative_to(self.root)), None, None)
            raise

    def _query_with_retry(self, client, api_name: str, params: Mapping[str, Any], fields: list[str], page_size: int, max_attempts: int) -> QueryResult:
        attempt = 1
        while True:
            try:
                return client.query_paginated(api_name, params, fields, page_size=page_size)
            except Exception as e:
                error_type = classify_exception(e)
                if should_retry(error_type, attempt, max_attempts):
                    self.retry_sleep(retry_delay_seconds(error_type, attempt))
                    attempt += 1
                    continue
                raise

    def _is_expired(self, value: str) -> bool:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return True
        return parsed <= datetime.now(timezone.utc)

    def _raw_event(self, run_id: str, key: str, api_name: str, event: Mapping[str, Any], fields: list[str]) -> dict[str, Any]:
        data = event.get("data") or {}
        return {
            "run_id": run_id,
            "job_key": key,
            "api_name": api_name,
            "params": event.get("_request_params") or {},
            "fields": ",".join(fields),
            "fetched_at": now_utc(),
            "http_status": event.get("_http_status"),
            "tushare_code": event.get("code"),
            "tushare_msg": event.get("msg"),
            "response_fields": data.get("fields") or [],
            "items": data.get("items") or [],
            "has_more": data.get("has_more"),
            "page_index": event.get("_page_index", 0),
        }

    def _rows(self, api_name: str, params: Mapping[str, Any], key: str, run_id: str, schema_id: str, fields: list[str], items: list[list[Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        fetched_at = now_utc()
        source_fields_hash = sha256_hex({"fields": fields})
        p_hash = params_hash(params)
        for item in items:
            source = {field: (item[idx] if idx < len(item) else None) for idx, field in enumerate(fields)}
            row = dict(source)
            row.update(
                {
                    "_api_name": api_name,
                    "_params_hash": p_hash,
                    "_row_hash": row_hash(api_name, schema_id, fields, source),
                    "_fetched_at": fetched_at,
                    "_source_fields": source_fields_hash,
                    "_job_key": key,
                    "_run_id": run_id,
                }
            )
            rows.append(row)
        return rows

    def _write_parquet(self, path: Path, rows: list[dict[str, Any]]) -> None:
        ensure_dir(path.parent)
        table = pa.Table.from_pylist(rows) if rows else pa.table({col: pa.array([], type=pa.string()) for col in STANDARD_METADATA_COLUMNS})
        pq.write_table(table, path, compression="zstd", use_dictionary=True)

    def _partition_values(self, cfg: Mapping[str, Any], params: Mapping[str, Any]) -> dict[str, Any]:
        date_field = (cfg.get("partition") or {}).get("date_field", "trade_date")
        date_value = str(params.get(date_field, "unknown"))
        year = date_value[:4] if len(date_value) >= 4 else "unknown"
        month = date_value[4:6] if len(date_value) >= 6 else "unknown"
        return {
            "market": cfg.get("market"),
            "domain": cfg.get("domain"),
            "api_name": cfg.get("api_name"),
            "year": year,
            "month": month,
            date_field: date_value,
        }

    def _raw_relative_path(self, api_name: str, key: str) -> str:
        date = now_utc()[:10].replace("-", "")
        return f"raw/api={api_name}/ingest_date={date}/job={key}.jsonl.zst"

    def _lake_relative_path(self, cfg: Mapping[str, Any], params: Mapping[str, Any], key: str) -> str:
        parts = self._partition_values(cfg, params)
        return (
            f"lake/market={parts['market']}/domain={parts['domain']}/api={parts['api_name']}/"
            f"year={parts['year']}/month={parts['month']}/part-{key[-12:]}.parquet"
        )

    def _quarantine(self, run_id: str, key: str, api_name: str, reason: str, tmp_dir: Path, raw_path: Path, raw_hash: str | None) -> None:
        qdir = self.root / "_quarantine" / f"run_id={run_id}" / f"api={api_name}" / f"job={key}" / f"reason={reason}"
        size = raw_path.stat().st_size if raw_path.exists() else None
        move_tree_to_quarantine(tmp_dir, qdir)
        self.catalog.record_quarantine(run_id, key, api_name, reason, str(qdir.relative_to(self.root)), size, raw_hash)
