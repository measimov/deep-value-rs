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


def infer_schema(api_name: str, fields: Sequence[str], items: Sequence[Sequence[Any]]) -> tuple[str, list[str], dict[str, str], dict[str, bool]]:
    field_list = list(fields)
    logical = {field: "null" for field in field_list}
    nullable = {field: False for field in field_list}
    for row in items:
        for idx, field in enumerate(field_list):
            value = row[idx] if idx < len(row) else None
            if value is None:
                nullable[field] = True
            logical[field] = widen_type(logical[field], infer_value_type(value))
    logical = {k: ("string" if v == "null" else v) for k, v in logical.items()}
    sid = make_schema_id(api_name, field_list, logical, nullable)
    return sid, field_list, logical, nullable


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
        sid, field_list, logical, nullable = infer_schema(api_name, fields, items)
        old = self.catalog.latest_schema_for_api(api_name)
        if not old:
            return SchemaDecision(sid, field_list, logical, nullable, True, "new_schema", {})
        old_fields = loads(old["fields_json"])
        old_types = loads(old["logical_types_json"])
        compatible, change_type, details = compare_schemas(old_fields, old_types, field_list, logical)
        return SchemaDecision(sid, field_list, logical, nullable, compatible, change_type, {"old_schema_id": old["schema_id"], **details})

    def commit(self, api_name: str, decision: SchemaDecision) -> None:
        old_schema_id = decision.details.get("old_schema_id")
        self.catalog.insert_schema(decision.schema_id, api_name, decision.fields, decision.logical_types, decision.nullable)
        if decision.change_type != "same":
            self.catalog.record_schema_change(api_name, old_schema_id, decision.schema_id, decision.change_type, decision.details, approved=decision.compatible)
