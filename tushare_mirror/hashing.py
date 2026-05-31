from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

NULL_SENTINEL = {"__tushare_mirror_null__": True}


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return NULL_SENTINEL
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return format(value, ".17g")
    return value


def canonicalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): canonicalize(value[k]) for k in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [canonicalize(v) for v in value]
    return _normalize_scalar(value)


def canonical_json(value: Any) -> str:
    return json.dumps(canonicalize(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex(value: Any) -> str:
    data = canonical_json(value).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def params_hash(params: Mapping[str, Any]) -> str:
    return sha256_hex({k: v for k, v in params.items() if k != "token"})


def token_hash(token: str, secret: str | None = None) -> str:
    secret = secret or os.environ.get("TUSHARE_MIRROR_HASH_SECRET") or "local-dev-secret"
    return hmac.new(secret.encode("utf-8"), token.encode("utf-8"), hashlib.sha256).hexdigest()


def schema_id(api_name: str, fields: Iterable[str], logical_types: Mapping[str, str], nullable: Mapping[str, bool]) -> str:
    payload = {
        "api_name": api_name,
        "fields": list(fields),
        "logical_types": {k: logical_types[k] for k in sorted(logical_types)},
        "nullable": {k: bool(nullable.get(k, True)) for k in sorted(logical_types)},
    }
    return "sch_" + sha256_hex(payload)[:24]


def row_hash(api_name: str, schema: str, field_order: Iterable[str], row: Mapping[str, Any]) -> str:
    payload = {
        "api_name": api_name,
        "schema_id": schema,
        "values": [[field, canonicalize(row.get(field))] for field in field_order],
    }
    return "row_" + sha256_hex(payload)[:32]


def job_key(api_name: str, params: Mapping[str, Any], fields: Iterable[str], partition_spec_id: str) -> str:
    payload = {
        "api_name": api_name,
        "params": {k: v for k, v in params.items() if k != "token"},
        "fields": list(fields),
        "partition_spec_id": partition_spec_id,
    }
    return "job_" + sha256_hex(payload)[:32]


def table_id(namespace: str, api_name: str) -> str:
    return "tbl_" + sha256_hex({"namespace": namespace, "api_name": api_name})[:24]


def partition_spec_id(name: str, spec: Mapping[str, Any]) -> str:
    return "ps_" + sha256_hex({"name": name, "spec": spec})[:24]
