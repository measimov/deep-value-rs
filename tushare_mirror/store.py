from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import CatalogStore
from .client import QueryResult
from .errors import ErrorType, MirrorError, classify_exception, retry_delay_seconds, should_retry
from .hashing import params_hash, row_hash, sha256_hex
from .io_utils import atomic_move, ensure_dir, move_tree_to_quarantine, now_utc, sha256_file, write_jsonl_zst
from .planner import JobPlanner
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

    def plan_fetch(self, api_name: str, params: Mapping[str, Any], fields: list[str] | None = None) -> dict[str, Any]:
        return JobPlanner(self.root, self.catalog).plan_single_fetch(api_name, params, fields).to_dict()

    def fetch(
        self,
        api_name: str,
        params: Mapping[str, Any],
        client,
        max_attempts: int = 3,
        fields: list[str] | None = None,
        run_id: str | None = None,
        finish_run: bool = True,
        run_type: str = "fetch",
    ) -> FetchResult:
        cfg = self.catalog.get_endpoint_config(api_name)
        plan = JobPlanner(self.root, self.catalog).plan_single_fetch(api_name, params, fields)
        if plan.existing_active_data:
            snap = self.catalog.latest_snapshot(api_name)
            existing = self.catalog.get_job(plan.job_key)
            return FetchResult(run_id or "", plan.job_key, snap["snapshot_id"] if snap else None, int((existing or {}).get("record_count") or 0), skipped=True)

        if run_id is None:
            run_id = self.catalog.create_run(run_type)
        self.catalog.upsert_job(plan.job_key, run_id, api_name, plan.params, plan.fields, "running")
        tmp_dir = self.root / "_tmp" / f"run_id={run_id}"
        raw_tmp = tmp_dir / "raw" / f"{plan.job_key}.jsonl.zst"
        parquet_tmp = tmp_dir / "lake" / f"{plan.job_key}.parquet"
        try:
            page_size = cfg.get("page_size") or 5000
            result = self._query_with_retry(client, api_name, plan.params, plan.fields, page_size, max_attempts)
            raw_events = [self._raw_event(run_id, plan.job_key, api_name, event, plan.fields) for event in result.events]
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
                self._quarantine(run_id, plan.job_key, api_name, ErrorType.SCHEMA_INCOMPATIBLE.value, tmp_dir, raw_tmp, raw_hash)
                self.catalog.update_job_failed(plan.job_key, f"schema incompatible: {decision.details}", ErrorType.SCHEMA_INCOMPATIBLE.value)
                if finish_run:
                    self.catalog.finish_run(run_id, "failed", "schema incompatible", ErrorType.SCHEMA_INCOMPATIBLE.value, {"job_key": plan.job_key})
                return FetchResult(run_id, plan.job_key, None, 0)

            schema_registry.commit(api_name, decision)
            rows = self._rows(api_name, plan.params, plan.job_key, run_id, decision.schema_id, result.fields, result.items)
            record_count = len(rows)
            try:
                self._write_parquet(parquet_tmp, rows)
            except Exception as e:
                raise MirrorError(ErrorType.WRITE_FAILED, f"lake parquet write failed: {e}") from e

            raw_file_id = self.catalog.insert_file(
                table_id=plan.table_id,
                api_name=api_name,
                content_type="raw",
                file_format="jsonl.zst",
                relative_path=plan.raw_path,
                staged_path=str(raw_tmp),
                partition_values=plan.partition_values,
                record_count=None,
                source_item_count=source_item_count,
                raw_event_count=raw_event_count,
                error_event_count=error_event_count,
                size_bytes=raw_tmp.stat().st_size,
                sha256=sha256_file(raw_tmp),
                schema_id=None,
                status="staged",
                run_id=run_id,
                job_key=plan.job_key,
            )
            lake_file_id = self.catalog.insert_file(
                table_id=plan.table_id,
                api_name=api_name,
                content_type="lake",
                file_format="parquet",
                relative_path=plan.lake_path,
                staged_path=str(parquet_tmp),
                partition_values=plan.partition_values,
                record_count=record_count,
                source_item_count=source_item_count,
                raw_event_count=None,
                error_event_count=error_event_count,
                size_bytes=parquet_tmp.stat().st_size,
                sha256=sha256_file(parquet_tmp),
                schema_id=decision.schema_id,
                status="staged",
                run_id=run_id,
                job_key=plan.job_key,
            )

            validator = Validator(self.root, self.catalog)
            ok, failures = validator.validate_file_rows([self.catalog.get_file(raw_file_id), self.catalog.get_file(lake_file_id)], use_staged=True)
            if not ok:
                self._quarantine(run_id, plan.job_key, api_name, ErrorType.VALIDATION_FAILED.value, tmp_dir, raw_tmp, None)
                self.catalog.update_job_failed(plan.job_key, json.dumps(failures), ErrorType.VALIDATION_FAILED.value)
                if finish_run:
                    self.catalog.finish_run(run_id, "failed", "validation failed", ErrorType.VALIDATION_FAILED.value, {"failures": failures})
                return FetchResult(run_id, plan.job_key, None, 0)

            raw_final = self.root / plan.raw_path
            lake_final = self.root / plan.lake_path
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
                    table_id=plan.table_id,
                    file_ids=[raw_file_id, lake_file_id],
                    run_id=run_id,
                    checkpoint_key=plan.job_key,
                    cursor=json.dumps(plan.params, sort_keys=True),
                )
            except Exception as e:
                raise MirrorError(ErrorType.CATALOG_COMMIT_FAILED, f"catalog commit failed: {e}") from e
            self.catalog.update_job_done(plan.job_key, record_count, source_item_count, raw_event_count, error_event_count)
            if finish_run:
                self.catalog.finish_run(run_id, "succeeded", summary={"job_key": plan.job_key, "record_count": record_count, "raw_event_count": raw_event_count})
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return FetchResult(run_id, plan.job_key, snapshot_id, record_count)
        except Exception as e:
            error_type = classify_exception(e)
            self.catalog.update_job_failed(plan.job_key, str(e), error_type.value)
            if finish_run:
                self.catalog.finish_run(run_id, "failed", str(e), error_type.value, {"job_key": plan.job_key})
            qdir = self.root / "_quarantine" / f"run_id={run_id}" / f"api={api_name}" / f"job={plan.job_key}" / f"reason={error_type.value}"
            move_tree_to_quarantine(tmp_dir, qdir)
            if qdir.exists():
                self.catalog.record_quarantine(run_id, plan.job_key, api_name, error_type.value, str(qdir.relative_to(self.root)), None, None)
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
        return JobPlanner(self.root, self.catalog).partition_values(cfg, params)

    def _raw_relative_path(self, api_name: str, key: str) -> str:
        return JobPlanner(self.root, self.catalog).raw_relative_path(api_name, key)

    def _lake_relative_path(self, cfg: Mapping[str, Any], params: Mapping[str, Any], key: str) -> str:
        return JobPlanner(self.root, self.catalog).lake_relative_path(cfg, params, key)

    def _quarantine(self, run_id: str, key: str, api_name: str, reason: str, tmp_dir: Path, raw_path: Path, raw_hash: str | None) -> None:
        qdir = self.root / "_quarantine" / f"run_id={run_id}" / f"api={api_name}" / f"job={key}" / f"reason={reason}"
        size = raw_path.stat().st_size if raw_path.exists() else None
        move_tree_to_quarantine(tmp_dir, qdir)
        self.catalog.record_quarantine(run_id, key, api_name, reason, str(qdir.relative_to(self.root)), size, raw_hash)
