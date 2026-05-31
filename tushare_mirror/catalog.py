from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping

from .hashing import params_hash
from .io_utils import now_utc

CATALOG_SCHEMA_VERSION = 2

SCHEMA_SQL = """
pragma journal_mode = wal;
pragma foreign_keys = on;

create table if not exists catalog_meta (
    key text primary key,
    value text not null
);

create table if not exists endpoints (
    api_name text primary key,
    family text not null,
    market text not null,
    domain text not null,
    namespace text not null,
    volume_class text not null,
    table_id text not null,
    partition_spec_id text not null,
    config_json text not null,
    updated_at text not null
);

create table if not exists partition_specs (
    partition_spec_id text primary key,
    name text not null,
    template text not null,
    bucket_count integer,
    spec_json text not null,
    updated_at text not null
);

create table if not exists permission_probes (
    probe_id text primary key,
    api_name text not null,
    token_hash text not null,
    status text not null,
    probe_params_json text not null,
    probe_fields_json text not null,
    row_count integer,
    error_type text,
    error_message text,
    raw_response_json text,
    probed_at text not null,
    valid_until text not null
);

create index if not exists idx_permission_probes_api_token
    on permission_probes(api_name, token_hash, probed_at desc);

create table if not exists ingestion_runs (
    run_id text primary key,
    run_type text not null default 'fetch',
    status text not null,
    started_at text not null,
    finished_at text,
    last_error_type text,
    error_message text,
    summary_json text
);

create table if not exists jobs (
    job_key text primary key,
    run_id text,
    api_name text not null,
    params_hash text,
    params_json text not null,
    fields_json text not null,
    status text not null,
    attempts integer not null default 0,
    record_count integer,
    source_item_count integer,
    raw_event_count integer,
    error_event_count integer,
    last_error_type text,
    last_error text,
    created_at text not null,
    updated_at text not null
);

create table if not exists checkpoint_state (
    checkpoint_key text primary key,
    api_name text not null,
    last_committed_cursor text,
    updated_at text not null
);

create table if not exists schemas (
    schema_id text primary key,
    api_name text not null,
    fields_json text not null,
    logical_types_json text not null,
    nullable_json text not null,
    created_at text not null
);

create table if not exists schema_changes (
    change_id text primary key,
    api_name text not null,
    old_schema_id text,
    new_schema_id text not null,
    change_type text not null,
    details_json text not null,
    approved integer not null default 0,
    approved_by text,
    approved_at text,
    detected_at text not null
);

create table if not exists files (
    file_id text primary key,
    table_id text,
    api_name text not null,
    content_type text not null,
    file_format text not null,
    relative_path text not null,
    staged_path text,
    partition_values_json text not null,
    record_count integer,
    source_item_count integer,
    raw_event_count integer,
    error_event_count integer,
    size_bytes integer not null,
    sha256 text not null,
    schema_id text,
    status text not null,
    created_at text not null,
    run_id text not null,
    job_key text not null,
    added_by_snapshot_id text
);

create index if not exists idx_files_api_status on files(api_name, status);
create index if not exists idx_files_job on files(job_key);

create table if not exists snapshots (
    snapshot_id text primary key,
    scope text not null,
    table_id text,
    api_name text,
    parent_snapshot_id text,
    sequence_number integer not null,
    status text not null,
    operation text not null,
    created_at text not null,
    run_id text
);

create table if not exists snapshot_files (
    snapshot_id text not null,
    file_id text not null,
    primary key (snapshot_id, file_id)
);

create table if not exists snapshot_refs (
    global_snapshot_id text not null,
    table_id text not null,
    table_snapshot_id text not null,
    primary key (global_snapshot_id, table_id)
);

create table if not exists validation_runs (
    validation_run_id text primary key,
    snapshot_id text,
    api_name text,
    status text not null,
    started_at text not null,
    finished_at text,
    summary_json text not null
);

create table if not exists validation_failures (
    validation_run_id text not null,
    file_id text,
    reason text not null,
    details text
);

create table if not exists quarantine_files (
    quarantine_id text primary key,
    run_id text not null,
    job_key text,
    api_name text,
    reason text not null,
    relative_path text not null,
    size_bytes integer,
    sha256 text,
    created_at text not null
);

create table if not exists compaction_runs (
    compaction_run_id text primary key,
    status text not null,
    plan_json text not null,
    created_at text not null
);

create table if not exists backup_manifests (
    backup_id text primary key,
    snapshot_id text,
    target text not null,
    manifest_path text not null,
    created_at text not null
);

create table if not exists postgres_loads (
    load_id text primary key,
    snapshot_id text not null,
    table_name text not null,
    schema_id text,
    loaded_at text not null
);
"""


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def loads(value: str | None) -> Any:
    return json.loads(value) if value else None


class CatalogStore:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.catalog_dir = self.root / "_catalog"
        self.db_path = self.catalog_dir / "catalog.sqlite"

    def connect(self) -> sqlite3.Connection:
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("pragma foreign_keys = on")
        return conn

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            conn.execute("begin immediate")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init(self) -> None:
        with self.transaction() as conn:
            conn.executescript(SCHEMA_SQL)
            self._migrate(conn)
            conn.execute(
                "insert or replace into catalog_meta(key, value) values('catalog_schema_version', ?)",
                (str(CATALOG_SCHEMA_VERSION),),
            )

    def _migrate(self, conn: sqlite3.Connection) -> None:
        self._add_column(conn, "permission_probes", "row_count", "integer")
        self._add_column(conn, "permission_probes", "error_type", "text")
        self._add_column(conn, "ingestion_runs", "run_type", "text not null default 'fetch'")
        self._add_column(conn, "ingestion_runs", "last_error_type", "text")
        self._add_column(conn, "ingestion_runs", "summary_json", "text")
        self._add_column(conn, "jobs", "params_hash", "text")
        self._add_column(conn, "jobs", "last_error_type", "text")
        for row in conn.execute("select job_key, params_json from jobs where params_hash is null").fetchall():
            conn.execute(
                "update jobs set params_hash=? where job_key=?",
                (params_hash(loads(row["params_json"]) or {}), row["job_key"]),
            )

    def _add_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        cols = {r[1] for r in conn.execute(f"pragma table_info({table})").fetchall()}
        if column not in cols:
            conn.execute(f"alter table {table} add column {column} {definition}")

    def schema_version(self) -> int:
        self.init()
        with self.connect() as conn:
            row = conn.execute("select value from catalog_meta where key='catalog_schema_version'").fetchone()
        return int(row[0]) if row else 0

    def backup(self, output_path: Path | str) -> Path:
        self.init()
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        source = self.connect()
        try:
            dest = sqlite3.connect(out)
            try:
                source.backup(dest)
            finally:
                dest.close()
        finally:
            source.close()
        return out

    def upsert_endpoint(self, cfg: Mapping[str, Any], table_id: str, partition_spec_id: str) -> None:
        now = now_utc()
        part = cfg.get("partition", {})
        with self.transaction() as conn:
            conn.execute(
                """
                insert into endpoints(api_name,family,market,domain,namespace,volume_class,table_id,partition_spec_id,config_json,updated_at)
                values(?,?,?,?,?,?,?,?,?,?)
                on conflict(api_name) do update set
                  family=excluded.family, market=excluded.market, domain=excluded.domain,
                  namespace=excluded.namespace, volume_class=excluded.volume_class,
                  table_id=excluded.table_id, partition_spec_id=excluded.partition_spec_id,
                  config_json=excluded.config_json, updated_at=excluded.updated_at
                """,
                (
                    cfg["api_name"], cfg["family"], cfg["market"], cfg["domain"], cfg["namespace"],
                    cfg["volume_class"], table_id, partition_spec_id, dumps(cfg), now,
                ),
            )
            conn.execute(
                """
                insert into partition_specs(partition_spec_id,name,template,bucket_count,spec_json,updated_at)
                values(?,?,?,?,?,?)
                on conflict(partition_spec_id) do update set
                  name=excluded.name, template=excluded.template, bucket_count=excluded.bucket_count,
                  spec_json=excluded.spec_json, updated_at=excluded.updated_at
                """,
                (
                    partition_spec_id,
                    part.get("name", partition_spec_id),
                    part.get("template", ""),
                    part.get("bucket_count"),
                    dumps(part),
                    now,
                ),
            )

    def get_endpoint(self, api_name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("select * from endpoints where api_name=?", (api_name,)).fetchone()
        return dict(row) if row else None

    def get_endpoint_config(self, api_name: str) -> dict[str, Any]:
        row = self.get_endpoint(api_name)
        if not row:
            raise KeyError(f"endpoint not found: {api_name}")
        cfg = loads(row["config_json"])
        cfg["table_id"] = row["table_id"]
        cfg["partition_spec_id"] = row["partition_spec_id"]
        return cfg

    def list_endpoints(self, family: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if family:
                rows = conn.execute("select * from endpoints where family=? order by api_name", (family,)).fetchall()
            else:
                rows = conn.execute("select * from endpoints order by api_name").fetchall()
        return [dict(r) for r in rows]

    def latest_permission(self, api_name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "select * from permission_probes where api_name=? order by probed_at desc limit 1",
                (api_name,),
            ).fetchone()
        return dict(row) if row else None

    def record_probe(
        self,
        api_name: str,
        token_hash: str,
        status: str,
        params: Mapping[str, Any],
        fields: Iterable[str],
        valid_until: str,
        error_message: str | None = None,
        raw_response: Mapping[str, Any] | None = None,
        row_count: int | None = None,
        error_type: str | None = None,
    ) -> str:
        probe_id = "probe_" + uuid.uuid4().hex
        with self.transaction() as conn:
            conn.execute(
                """
                insert into permission_probes(probe_id,api_name,token_hash,status,probe_params_json,probe_fields_json,row_count,error_type,error_message,raw_response_json,probed_at,valid_until)
                values(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (probe_id, api_name, token_hash, status, dumps(params), dumps(list(fields)), row_count, error_type, error_message, dumps(raw_response) if raw_response else None, now_utc(), valid_until),
            )
        return probe_id

    def create_run(self, run_type: str = "fetch") -> str:
        run_id = "run_" + uuid.uuid4().hex
        with self.transaction() as conn:
            conn.execute("insert into ingestion_runs(run_id,run_type,status,started_at) values(?,?,?,?)", (run_id, run_type, "running", now_utc()))
        return run_id

    def finish_run(self, run_id: str, status: str, error_message: str | None = None, error_type: str | None = None, summary: Mapping[str, Any] | None = None) -> None:
        with self.transaction() as conn:
            conn.execute(
                "update ingestion_runs set status=?, finished_at=?, error_message=?, last_error_type=?, summary_json=? where run_id=?",
                (status, now_utc(), error_message, error_type, dumps(summary) if summary is not None else None, run_id),
            )

    def get_job(self, job_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("select * from jobs where job_key=?", (job_key,)).fetchone()
        return dict(row) if row else None

    def upsert_job(self, job_key: str, run_id: str, api_name: str, params: Mapping[str, Any], fields: Iterable[str], status: str) -> None:
        now = now_utc()
        with self.transaction() as conn:
            conn.execute(
                """
                insert into jobs(job_key,run_id,api_name,params_hash,params_json,fields_json,status,attempts,created_at,updated_at)
                values(?,?,?,?,?,?,?,1,?,?)
                on conflict(job_key) do update set
                  run_id=excluded.run_id, status=excluded.status, attempts=jobs.attempts+1,
                  params_hash=excluded.params_hash, last_error_type=null, last_error=null,
                  updated_at=excluded.updated_at
                """,
                (job_key, run_id, api_name, params_hash(params), dumps(params), dumps(list(fields)), status, now, now),
            )

    def update_job_done(self, job_key: str, record_count: int, source_item_count: int, raw_event_count: int, error_event_count: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                update jobs set status='done', record_count=?, source_item_count=?, raw_event_count=?, error_event_count=?, last_error_type=null, last_error=null, updated_at=? where job_key=?
                """,
                (record_count, source_item_count, raw_event_count, error_event_count, now_utc(), job_key),
            )

    def update_job_failed(self, job_key: str, error: str, error_type: str | None = None) -> None:
        with self.transaction() as conn:
            conn.execute("update jobs set status='failed', last_error_type=?, last_error=?, updated_at=? where job_key=?", (error_type, error, now_utc(), job_key))

    def insert_schema(self, schema_id: str, api_name: str, fields: Iterable[str], logical_types: Mapping[str, str], nullable: Mapping[str, bool]) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                insert or ignore into schemas(schema_id,api_name,fields_json,logical_types_json,nullable_json,created_at)
                values(?,?,?,?,?,?)
                """,
                (schema_id, api_name, dumps(list(fields)), dumps(logical_types), dumps(nullable), now_utc()),
            )

    def latest_schema_for_api(self, api_name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("select * from schemas where api_name=? order by created_at desc limit 1", (api_name,)).fetchone()
        return dict(row) if row else None

    def record_schema_change(self, api_name: str, old_schema_id: str | None, new_schema_id: str, change_type: str, details: Mapping[str, Any], approved: bool = False) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                insert into schema_changes(change_id,api_name,old_schema_id,new_schema_id,change_type,details_json,approved,detected_at)
                values(?,?,?,?,?,?,?,?)
                """,
                ("chg_" + uuid.uuid4().hex, api_name, old_schema_id, new_schema_id, change_type, dumps(details), 1 if approved else 0, now_utc()),
            )

    def insert_file(self, *, table_id: str | None, api_name: str, content_type: str, file_format: str, relative_path: str, staged_path: str | None, partition_values: Mapping[str, Any], record_count: int | None, source_item_count: int | None, raw_event_count: int | None, error_event_count: int | None, size_bytes: int, sha256: str, schema_id: str | None, status: str, run_id: str, job_key: str) -> str:
        file_id = "file_" + uuid.uuid4().hex
        with self.transaction() as conn:
            conn.execute(
                """
                insert into files(file_id,table_id,api_name,content_type,file_format,relative_path,staged_path,partition_values_json,record_count,source_item_count,raw_event_count,error_event_count,size_bytes,sha256,schema_id,status,created_at,run_id,job_key)
                values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (file_id, table_id, api_name, content_type, file_format, relative_path, staged_path, dumps(partition_values), record_count, source_item_count, raw_event_count, error_event_count, size_bytes, sha256, schema_id, status, now_utc(), run_id, job_key),
            )
        return file_id

    def get_file(self, file_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("select * from files where file_id=?", (file_id,)).fetchone()
        if not row:
            raise KeyError(file_id)
        return dict(row)

    def update_file_status(self, file_id: str, status: str, staged_path: str | None = None) -> None:
        with self.transaction() as conn:
            conn.execute("update files set status=?, staged_path=? where file_id=?", (status, staged_path, file_id))

    def latest_snapshot(self, api_name: str | None = None) -> dict[str, Any] | None:
        with self.connect() as conn:
            if api_name:
                row = conn.execute("select * from snapshots where api_name=? and status='current' order by sequence_number desc, created_at desc limit 1", (api_name,)).fetchone()
            else:
                row = conn.execute("select * from snapshots where status='current' order by sequence_number desc, created_at desc limit 1").fetchone()
        return dict(row) if row else None

    def files_for_snapshot(self, snapshot_id: str, content_type: str | None = None) -> list[dict[str, Any]]:
        sql = """
            select f.* from snapshot_files sf join files f on f.file_id=sf.file_id
            where sf.snapshot_id=?
        """
        args: list[Any] = [snapshot_id]
        if content_type:
            sql += " and f.content_type=?"
            args.append(content_type)
        sql += " order by f.relative_path"
        with self.connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def active_files_for_job(self, job_key: str, api_name: str, content_type: str | None = None) -> list[dict[str, Any]]:
        snapshot = self.latest_snapshot(api_name)
        if not snapshot:
            return []
        rows = self.files_for_snapshot(snapshot["snapshot_id"], content_type=content_type)
        return [row for row in rows if row.get("job_key") == job_key and row.get("status") not in {"quarantined", "missing", "deleted", "deleted_pending"}]

    def commit_snapshot(self, *, api_name: str, table_id: str, file_ids: Iterable[str], run_id: str, checkpoint_key: str, cursor: str) -> str:
        new_file_ids = list(file_ids)
        parent = self.latest_snapshot(api_name)
        parent_id = parent["snapshot_id"] if parent else None
        sequence = (int(parent["sequence_number"]) + 1) if parent else 1
        snapshot_id = "snap_" + uuid.uuid4().hex
        with self.transaction() as conn:
            conn.execute(
                "insert into snapshots(snapshot_id,scope,table_id,api_name,parent_snapshot_id,sequence_number,status,operation,created_at,run_id) values(?,?,?,?,?,?,?,?,?,?)",
                (snapshot_id, "table", table_id, api_name, parent_id, sequence, "current", "append", now_utc(), run_id),
            )
            inherited: list[str] = []
            if parent_id:
                inherited = [r[0] for r in conn.execute("select file_id from snapshot_files where snapshot_id=?", (parent_id,)).fetchall()]
                conn.execute("update snapshots set status='superseded' where snapshot_id=?", (parent_id,))
            for file_id in inherited + new_file_ids:
                conn.execute("insert or ignore into snapshot_files(snapshot_id,file_id) values(?,?)", (snapshot_id, file_id))
            for file_id in new_file_ids:
                conn.execute("update files set status='current', added_by_snapshot_id=? where file_id=?", (snapshot_id, file_id))
            conn.execute(
                "insert into checkpoint_state(checkpoint_key,api_name,last_committed_cursor,updated_at) values(?,?,?,?) on conflict(checkpoint_key) do update set last_committed_cursor=excluded.last_committed_cursor, updated_at=excluded.updated_at",
                (checkpoint_key, api_name, cursor, now_utc()),
            )
        return snapshot_id

    def record_validation(self, snapshot_id: str | None, api_name: str | None, status: str, summary: Mapping[str, Any], failures: Iterable[tuple[str | None, str, str | None]]) -> str:
        validation_run_id = "val_" + uuid.uuid4().hex
        with self.transaction() as conn:
            conn.execute(
                "insert into validation_runs(validation_run_id,snapshot_id,api_name,status,started_at,finished_at,summary_json) values(?,?,?,?,?,?,?)",
                (validation_run_id, snapshot_id, api_name, status, now_utc(), now_utc(), dumps(summary)),
            )
            for file_id, reason, details in failures:
                conn.execute("insert into validation_failures(validation_run_id,file_id,reason,details) values(?,?,?,?)", (validation_run_id, file_id, reason, details))
        return validation_run_id

    def record_quarantine(self, run_id: str, job_key: str | None, api_name: str | None, reason: str, relative_path: str, size_bytes: int | None, sha256: str | None) -> None:
        with self.transaction() as conn:
            conn.execute(
                "insert into quarantine_files(quarantine_id,run_id,job_key,api_name,reason,relative_path,size_bytes,sha256,created_at) values(?,?,?,?,?,?,?,?,?)",
                ("q_" + uuid.uuid4().hex, run_id, job_key, api_name, reason, relative_path, size_bytes, sha256, now_utc()),
            )

    def inspect_summary(self) -> dict[str, Any]:
        self.init()
        latest = self.latest_snapshot()
        with self.connect() as conn:
            return {
                "catalog_path": str(self.db_path),
                "schema_version": self.schema_version(),
                "endpoint_count": conn.execute("select count(*) from endpoints").fetchone()[0],
                "run_count": conn.execute("select count(*) from ingestion_runs").fetchone()[0],
                "job_count": conn.execute("select count(*) from jobs").fetchone()[0],
                "file_count": conn.execute("select count(*) from files").fetchone()[0],
                "snapshot_count": conn.execute("select count(*) from snapshots").fetchone()[0],
                "latest_snapshot": latest["snapshot_id"] if latest else None,
                "validation_count": conn.execute("select count(*) from validation_runs").fetchone()[0],
                "quarantine_count": conn.execute("select count(*) from quarantine_files").fetchone()[0],
            }

    def list_runs(self, api_name: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        where = ""
        args: list[Any] = []
        if api_name:
            where = "where exists (select 1 from jobs j where j.run_id=r.run_id and j.api_name=?)"
            args.append(api_name)
        args.append(limit)
        sql = f"""
            select r.run_id,r.run_type,r.status,r.started_at,r.finished_at,r.last_error_type,r.error_message,r.summary_json,
                   (select count(*) from jobs j where j.run_id=r.run_id) as job_count
            from ingestion_runs r {where}
            order by r.started_at desc limit ?
        """
        with self.connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["summary"] = loads(d.pop("summary_json"))
            result.append(d)
        return result

    def list_jobs(self, api_name: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        args: list[Any] = []
        sql = """
            select j.job_key,j.run_id,j.api_name,j.status,j.params_hash,j.record_count,j.raw_event_count,
                   r.started_at,r.finished_at,j.last_error_type,j.last_error
            from jobs j left join ingestion_runs r on r.run_id=j.run_id
        """
        if api_name:
            sql += " where j.api_name=?"
            args.append(api_name)
        sql += " order by j.updated_at desc limit ?"
        args.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def list_snapshots(self, api_name: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        args: list[Any] = []
        sql = """
            select s.snapshot_id,s.table_id,s.api_name,s.status,s.created_at,s.parent_snapshot_id,
                   count(sf.file_id) as file_count,
                   coalesce(sum(case when f.content_type='lake' then f.record_count else 0 end), 0) as record_count
            from snapshots s
            left join snapshot_files sf on sf.snapshot_id=s.snapshot_id
            left join files f on f.file_id=sf.file_id
        """
        if api_name:
            sql += " where s.api_name=?"
            args.append(api_name)
        sql += " group by s.snapshot_id order by s.sequence_number desc limit ?"
        args.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def list_validations(self, api_name: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        args: list[Any] = []
        sql = """
            select v.validation_run_id as validation_id,v.snapshot_id,v.status,v.started_at,v.finished_at,
                   json_extract(v.summary_json, '$.files') as checked_file_count,
                   json_extract(v.summary_json, '$.failures') as failure_count,
                   v.api_name
            from validation_runs v
        """
        if api_name:
            sql += " where v.api_name=?"
            args.append(api_name)
        sql += " order by v.started_at desc limit ?"
        args.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def list_permissions(self, api_name: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        args: list[Any] = []
        sql = "select api_name,status,probed_at,valid_until,row_count,error_type,error_message from permission_probes"
        if api_name:
            sql += " where api_name=?"
            args.append(api_name)
        sql += " order by probed_at desc limit ?"
        args.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]
