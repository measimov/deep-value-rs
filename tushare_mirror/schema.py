from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .hashing import schema_id as make_schema_id
from .catalog import loads


@dataclass
class SchemaDecision:
    schema_id: str
    fields: list[str]
    logical_types: dict[str, str]
    nullable: dict[str, bool]
    compatible: bool
    change_type: str
    details: dict[str, Any]


@dataclass
class SchemaPreparation:
    decision: SchemaDecision
    items: list[list[Any]]


def infer_value_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def widen_type(current: str, value_type: str) -> str:
    if current == value_type:
        return current
    if current == "null":
        return value_type
    if value_type == "null":
        return current
    if {current, value_type} <= {"int", "float"}:
        return "float"
    return "string" if current == "string" or value_type == "string" else value_type


def infer_schema(api_name: str, fields: Sequence[str], items: Sequence[Sequence[Any]], type_hints: Mapping[str, str] | None = None) -> tuple[str, list[str], dict[str, str], dict[str, bool]]:
    field_list = list(fields)
    logical = {field: "null" for field in field_list}
    nullable = {field: False for field in field_list}
    for row in items:
        for idx, field in enumerate(field_list):
            value = row[idx] if idx < len(row) else None
            if value is None:
                nullable[field] = True
            logical[field] = widen_type(logical[field], infer_value_type(value))
    hints = type_hints or {}
    logical = {k: (hints.get(k, "string") if v == "null" else v) for k, v in logical.items()}
    sid = make_schema_id(api_name, field_list, logical, nullable)
    return sid, field_list, logical, nullable


def coerce_value_to_logical_type(value: Any, expected_type: str) -> Any:
    if value is None:
        return None
    if expected_type == "string":
        return str(value)
    if expected_type == "float":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                return None
            try:
                return float(stripped)
            except ValueError:
                return value
    if expected_type == "int":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                return None
            try:
                return int(stripped)
            except ValueError:
                try:
                    parsed = float(stripped)
                except ValueError:
                    return value
                return int(parsed) if parsed.is_integer() else value
    if expected_type == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "t", "yes", "y", "1"}:
                return True
            if lowered in {"false", "f", "no", "n", "0"}:
                return False
    return value


def normalize_items_for_schema(fields: Sequence[str], items: Sequence[Sequence[Any]], target_types: Mapping[str, str]) -> list[list[Any]]:
    field_list = list(fields)
    normalized: list[list[Any]] = []
    for row in items:
        values = list(row)
        normalized_row: list[Any] = []
        for idx, field in enumerate(field_list):
            value = values[idx] if idx < len(values) else None
            expected_type = target_types.get(field)
            if expected_type:
                value = coerce_value_to_logical_type(value, expected_type)
            normalized_row.append(value)
        normalized.append(normalized_row)
    return normalized


def compare_schemas(old_fields: list[str], old_types: Mapping[str, str], new_fields: list[str], new_types: Mapping[str, str]) -> tuple[bool, str, dict[str, Any]]:
    old_set = set(old_fields)
    new_set = set(new_fields)
    added = sorted(new_set - old_set)
    missing = sorted(old_set - new_set)
    type_changes: dict[str, tuple[str, str]] = {}
    incompatible: dict[str, tuple[str, str]] = {}
    for field in sorted(old_set & new_set):
        old = old_types[field]
        new = new_types[field]
        if old == new:
            continue
        type_changes[field] = (old, new)
        if old == "int" and new == "float":
            continue
        if old == "null":
            continue
        incompatible[field] = (old, new)
    reordered = [f for f in new_fields if f in old_set] != [f for f in old_fields if f in new_set]
    details = {"added": added, "missing": missing, "type_changes": type_changes, "reordered": reordered}
    if incompatible:
        details["incompatible"] = incompatible
        return False, "incompatible_type_change", details
    if missing:
        return True, "missing_column_warning", details
    if type_changes:
        return True, "type_widening", details
    if added:
        return True, "add_column", details
    if reordered:
        return True, "reorder", details
    return True, "same", details


class SchemaRegistry:
    def __init__(self, catalog):
        self.catalog = catalog

    def decide(self, api_name: str, fields: Sequence[str], items: Sequence[Sequence[Any]]) -> SchemaDecision:
        return self.prepare(api_name, fields, items, normalize=False).decision

    def prepare(self, api_name: str, fields: Sequence[str], items: Sequence[Sequence[Any]], normalize: bool = True) -> SchemaPreparation:
        old = self.catalog.latest_schema_for_api(api_name)
        old_fields: list[str] | None = None
        old_types: dict[str, str] = {}
        if old:
            old_fields = loads(old["fields_json"])
            old_types = loads(old["logical_types_json"])
        normalized_items = normalize_items_for_schema(fields, items, old_types) if old and normalize else [list(row) for row in items]
        sid, field_list, logical, nullable = infer_schema(api_name, fields, normalized_items, old_types if old and normalize else None)
        if not old:
            decision = SchemaDecision(sid, field_list, logical, nullable, True, "new_schema", {})
            return SchemaPreparation(decision, normalized_items)
        assert old_fields is not None
        compatible, change_type, details = compare_schemas(old_fields, old_types, field_list, logical)
        decision = SchemaDecision(sid, field_list, logical, nullable, compatible, change_type, {"old_schema_id": old["schema_id"], **details})
        return SchemaPreparation(decision, normalized_items)

    def commit(self, api_name: str, decision: SchemaDecision) -> None:
        old_schema_id = decision.details.get("old_schema_id")
        self.catalog.insert_schema(decision.schema_id, api_name, decision.fields, decision.logical_types, decision.nullable)
        if decision.change_type != "same":
            self.catalog.record_schema_change(api_name, old_schema_id, decision.schema_id, decision.change_type, decision.details, approved=decision.compatible)
