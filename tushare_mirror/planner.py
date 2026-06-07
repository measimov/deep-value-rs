from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .catalog import CatalogStore
from .hashing import job_key as make_job_key, params_hash
from .io_utils import now_utc


@dataclass(frozen=True)
class FetchPlan:
    api_name: str
    params: dict[str, Any]
    fields: list[str]
    params_hash: str
    job_key: str
    table_id: str
    partition_spec_id: str
    volume_class: str | None
    partition_values: dict[str, Any]
    raw_path: str
    lake_path: str
    lake_path_prefix: str
    permission_status: str
    permission_valid_until: str | None
    permission_expired: bool
    existing_active_job: bool
    existing_active_files: int
    existing_active_data: bool
    planned_actions: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProbePlan:
    api_name: str
    params: dict[str, Any]
    fields: list[str]
    permission_status: str
    permission_valid_until: str | None
    permission_expired: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class JobPlanner:
    def __init__(self, root: Path | str, catalog: CatalogStore):
        self.root = Path(root)
        self.catalog = catalog

    def plan_single_fetch(self, api_name: str, params: Mapping[str, Any], fields: list[str] | None = None) -> FetchPlan:
        cfg = self.catalog.get_endpoint_config(api_name)
        effective_params = self._effective_params(cfg, params)
        effective_fields = list(fields if fields is not None else cfg.get("default_fields") or [])
        key = make_job_key(api_name, effective_params, effective_fields, cfg["partition_spec_id"])
        partition_values = self.partition_values(cfg, effective_params)
        raw_path = self.raw_relative_path(api_name, key)
        lake_path = self.lake_relative_path(cfg, effective_params, key, partition_values)
        permission = self.catalog.latest_permission(api_name)
        permission_status = permission.get("status") if permission else "unknown"
        permission_valid_until = permission.get("valid_until") if permission else None
        permission_expired = True if not permission_valid_until else self._is_expired(str(permission_valid_until))
        existing_job = self.catalog.get_job(key)
        active_lake_files = self.catalog.active_files_for_job(key, api_name, content_type="lake")
        existing_active_job = bool(existing_job and existing_job.get("status") == "done")
        existing_active_data = bool(existing_active_job and active_lake_files)
        planned_actions = (
            ["no_op_existing_active_data"]
            if existing_active_data
            else ["request_tushare", "write_raw_jsonl_zst", "write_lake_parquet", "validate", "commit_snapshot"]
        )
        return FetchPlan(
            api_name=api_name,
            params=effective_params,
            fields=effective_fields,
            params_hash=params_hash(effective_params),
            job_key=key,
            table_id=cfg["table_id"],
            partition_spec_id=cfg["partition_spec_id"],
            volume_class=cfg.get("volume_class"),
            partition_values=partition_values,
            raw_path=raw_path,
            lake_path=lake_path,
            lake_path_prefix=str(Path(lake_path).parent),
            permission_status=str(permission_status),
            permission_valid_until=str(permission_valid_until) if permission_valid_until else None,
            permission_expired=permission_expired,
            existing_active_job=existing_active_job,
            existing_active_files=len(active_lake_files),
            existing_active_data=existing_active_data,
            planned_actions=planned_actions,
        )

    def plan_probe(self, api_name: str) -> ProbePlan:
        cfg = self.catalog.get_endpoint_config(api_name)
        probe = cfg.get("probe") or {}
        params = self._effective_params(cfg, probe.get("params") or {})
        fields = list(probe.get("fields") or cfg.get("default_fields") or [])
        permission = self.catalog.latest_permission(api_name)
        permission_status = permission.get("status") if permission else "unknown"
        permission_valid_until = permission.get("valid_until") if permission else None
        permission_expired = True if not permission_valid_until else self._is_expired(str(permission_valid_until))
        return ProbePlan(
            api_name=api_name,
            params=params,
            fields=fields,
            permission_status=str(permission_status),
            permission_valid_until=str(permission_valid_until) if permission_valid_until else None,
            permission_expired=permission_expired,
        )

    def partition_values(self, cfg: Mapping[str, Any], params: Mapping[str, Any]) -> dict[str, Any]:
        template = self._partition_template(cfg)
        base = {
            "market": cfg.get("market"),
            "domain": cfg.get("domain"),
            "api_name": cfg.get("api_name"),
        }
        if template == "snapshot_date":
            snapshot_date = self._date_value(params, ["snapshot_date"])
            return {**base, "snapshot_date": snapshot_date}
        if template == "exchange_year":
            exchange = str(params.get("exchange") or (cfg.get("default_params") or {}).get("exchange") or "unknown")
            date_value = self._date_value(params, ["cal_date", "start_date", "end_date"])
            return {**base, "exchange": exchange, "year": self._year(date_value)}
        if template == "event_year_month":
            date_value = self._date_value(params, ["ann_date", "trade_date", "start_date", "end_date"])
            return {**base, "year": self._year(date_value), "month": self._month(date_value), "event_date": date_value}
        if template == "family_code_snapshot":
            hs_type = str(params.get("hs_type") or (cfg.get("default_params") or {}).get("hs_type") or "unknown")
            snapshot_date = self._date_value(params, ["snapshot_date"])
            return {**base, "hs_type": hs_type, "snapshot_date": snapshot_date}
        if template == "period_year":
            date_value = self._date_value(params, ["period", "end_date", "ann_date", "start_date"])
            return {**base, "period_year": self._year(date_value), "period_date": date_value}
        date_field = self._primary_date_field(cfg) or "trade_date"
        date_value = self._date_value(params, [date_field])
        return {**base, "year": self._year(date_value), "month": self._month(date_value), date_field: date_value}

    def raw_relative_path(self, api_name: str, key: str) -> str:
        date = now_utc()[:10].replace("-", "")
        return f"raw/api={api_name}/ingest_date={date}/job={key}.jsonl.zst"

    def lake_relative_path(self, cfg: Mapping[str, Any], params: Mapping[str, Any], key: str, partition_values: Mapping[str, Any] | None = None) -> str:
        parts = dict(partition_values or self.partition_values(cfg, params))
        prefix = f"lake/market={parts['market']}/domain={parts['domain']}/api={parts['api_name']}"
        template = self._partition_template(cfg)
        if template == "snapshot_date":
            prefix += f"/snapshot_date={parts['snapshot_date']}"
        elif template == "exchange_year":
            prefix += f"/exchange={parts['exchange']}/year={parts['year']}"
        elif template == "family_code_snapshot":
            prefix += f"/hs_type={parts['hs_type']}/snapshot_date={parts['snapshot_date']}"
        elif template == "period_year":
            prefix += f"/period_year={parts['period_year']}"
        else:
            prefix += f"/year={parts['year']}/month={parts['month']}"
        return f"{prefix}/part-{key[-12:]}.parquet"

    def _effective_params(self, cfg: Mapping[str, Any], params: Mapping[str, Any]) -> dict[str, Any]:
        merged = dict(cfg.get("default_params") or {})
        merged.update({k: v for k, v in dict(params).items() if v is not None})
        return merged

    def _partition_template(self, cfg: Mapping[str, Any]) -> str:
        if cfg.get("partition_template"):
            return str(cfg["partition_template"])
        part = cfg.get("partition") or {}
        name = str(part.get("name") or "")
        for template in ("snapshot_date", "exchange_year", "event_year_month", "family_code_snapshot", "period_year"):
            if template in name:
                return template
        return "year_month"

    def _primary_date_field(self, cfg: Mapping[str, Any]) -> str | None:
        if "primary_date_field" in cfg:
            return cfg.get("primary_date_field")
        return (cfg.get("partition") or {}).get("date_field")


    def _date_value(self, params: Mapping[str, Any], candidates: list[str]) -> str:
        for key in candidates:
            value = params.get(key)
            if value:
                return str(value)
        return now_utc()[:10].replace("-", "")

    def _year(self, date_value: str) -> str:
        return date_value[:4] if len(date_value) >= 4 else "unknown"

    def _month(self, date_value: str) -> str:
        return date_value[4:6] if len(date_value) >= 6 else "unknown"

    def _is_expired(self, value: str) -> bool:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return True
        return parsed <= datetime.now(timezone.utc)
