from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


OBJECT_TEXT_ENDPOINT_KINDS = {
    "object_document",
    "text_news",
    "research_report",
    "announcement",
    "html_text",
    "unknown_object_text",
}


@dataclass(frozen=True)
class ObjectTextMetadata:
    object_index_required: bool
    object_download_required: bool
    content_addressed_storage: bool
    sha256_dedup_required: bool
    content_type_field: str | None
    source_url_field: str | None
    publish_time_fields: list[str]
    title_fields: list[str]
    object_id_fields: list[str]
    metadata_lake_required: bool
    binary_storage_layer: str | None
    execution_blocked_until_object_store_enabled: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ObjectTextValidationResult:
    status: str
    endpoint_kind: str
    metadata: ObjectTextMetadata
    errors: list[str]
    warnings: list[str]

    @property
    def blocked(self) -> bool:
        return self.status == "blocked"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def object_text_metadata_from_config(endpoint_config: Mapping[str, Any]) -> ObjectTextMetadata:
    raw = endpoint_config.get("object_strategy")
    strategy = dict(raw) if isinstance(raw, Mapping) else {}
    endpoint_kind = str(endpoint_config.get("endpoint_kind") or "")
    object_like = endpoint_kind in OBJECT_TEXT_ENDPOINT_KINDS
    return ObjectTextMetadata(
        object_index_required=bool(strategy.get("object_index_required", object_like)),
        object_download_required=bool(strategy.get("object_download_required", endpoint_kind in {"object_document", "announcement", "research_report"})),
        content_addressed_storage=bool(strategy.get("content_addressed_storage", object_like)),
        sha256_dedup_required=bool(strategy.get("sha256_dedup_required", object_like)),
        content_type_field=_optional_string(strategy.get("content_type_field")),
        source_url_field=_optional_string(strategy.get("source_url_field")),
        publish_time_fields=_string_list(strategy.get("publish_time_fields")),
        title_fields=_string_list(strategy.get("title_fields")),
        object_id_fields=_string_list(strategy.get("object_id_fields")),
        metadata_lake_required=bool(strategy.get("metadata_lake_required", object_like)),
        binary_storage_layer=_optional_string(strategy.get("binary_storage_layer")),
        execution_blocked_until_object_store_enabled=bool(strategy.get("execution_blocked_until_object_store_enabled", object_like)),
    )


def validate_object_text_metadata(endpoint_config: Mapping[str, Any]) -> ObjectTextValidationResult:
    endpoint_kind = str(endpoint_config.get("endpoint_kind") or "unknown")
    metadata = object_text_metadata_from_config(endpoint_config)
    errors: list[str] = []
    warnings: list[str] = []

    if endpoint_kind not in OBJECT_TEXT_ENDPOINT_KINDS:
        warnings.append("endpoint_kind is not object/text; metadata is advisory only")
    if metadata.object_index_required and not metadata.object_id_fields:
        errors.append("missing_object_id_fields")
    if metadata.object_download_required and not metadata.source_url_field:
        errors.append("missing_source_url_field")
    if metadata.object_download_required and not metadata.binary_storage_layer:
        errors.append("missing_binary_storage_layer")
    if metadata.object_download_required:
        errors.append("object_download_execution_blocked")
    if metadata.content_addressed_storage and not metadata.sha256_dedup_required:
        warnings.append("content_addressed_storage without sha256_dedup_required is unsafe")
    if metadata.object_index_required and not metadata.publish_time_fields:
        warnings.append("missing_publish_time_fields")
    if metadata.object_index_required and not metadata.title_fields:
        warnings.append("missing_title_fields")

    return ObjectTextValidationResult(
        status="blocked" if errors else "complete",
        endpoint_kind=endpoint_kind,
        metadata=metadata,
        errors=errors,
        warnings=warnings,
    )


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]
