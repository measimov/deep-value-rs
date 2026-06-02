from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


INTRADAY_ENDPOINT_KINDS = {"minute_bar", "tick", "order", "realtime"}
INTRADAY_ALLOWED_BUCKET_COUNTS = {32, 64, 128}


@dataclass(frozen=True)
class IntradayBucketMetadata:
    freq: str | None
    bucket_strategy: str
    bucket_count: int
    partition_template: str
    target_file_size_mb: int
    max_file_size_mb: int
    compaction_required: bool
    query_benchmark_required: bool
    storage_estimate_required: bool
    execution_blocked_until_bucket_policy_enabled: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IntradayBucketValidationResult:
    status: str
    endpoint_kind: str
    metadata: IntradayBucketMetadata
    errors: list[str]
    warnings: list[str]

    @property
    def blocked(self) -> bool:
        return self.status == "blocked"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def intraday_metadata_from_config(endpoint_config: Mapping[str, Any]) -> IntradayBucketMetadata:
    raw = endpoint_config.get("intraday_strategy")
    strategy = dict(raw) if isinstance(raw, Mapping) else {}
    endpoint_kind = str(endpoint_config.get("endpoint_kind") or "")
    default_bucket = _default_bucket_count(endpoint_kind)
    default_freq = "1min" if endpoint_kind == "minute_bar" else None
    return IntradayBucketMetadata(
        freq=_optional_string(strategy.get("freq") or endpoint_config.get("freq") or default_freq),
        bucket_strategy=str(strategy.get("bucket_strategy") or "hash_by_code_date"),
        bucket_count=int(strategy.get("bucket_count") or default_bucket),
        partition_template=str(strategy.get("partition_template") or "date_bucket"),
        target_file_size_mb=int(strategy.get("target_file_size_mb") or 256),
        max_file_size_mb=int(strategy.get("max_file_size_mb") or 1024),
        compaction_required=bool(strategy.get("compaction_required", endpoint_kind in INTRADAY_ENDPOINT_KINDS)),
        query_benchmark_required=bool(strategy.get("query_benchmark_required", endpoint_kind in INTRADAY_ENDPOINT_KINDS)),
        storage_estimate_required=bool(strategy.get("storage_estimate_required", endpoint_kind in INTRADAY_ENDPOINT_KINDS)),
        execution_blocked_until_bucket_policy_enabled=bool(
            strategy.get("execution_blocked_until_bucket_policy_enabled", endpoint_kind in INTRADAY_ENDPOINT_KINDS)
        ),
    )


def validate_intraday_metadata(endpoint_config: Mapping[str, Any]) -> IntradayBucketValidationResult:
    endpoint_kind = str(endpoint_config.get("endpoint_kind") or "unknown")
    metadata = intraday_metadata_from_config(endpoint_config)
    errors: list[str] = []
    warnings: list[str] = []

    if endpoint_kind not in INTRADAY_ENDPOINT_KINDS:
        warnings.append("endpoint_kind is not intraday; metadata is advisory only")
    if metadata.bucket_count not in INTRADAY_ALLOWED_BUCKET_COUNTS:
        errors.append("invalid_bucket_count")
    if metadata.target_file_size_mb < 128 or metadata.target_file_size_mb > 512:
        errors.append("invalid_target_file_size_mb")
    if metadata.max_file_size_mb < metadata.target_file_size_mb:
        errors.append("max_file_size_below_target")
    if not metadata.compaction_required and endpoint_kind in {"minute_bar", "tick", "order"}:
        errors.append("compaction_required_false")
    if not metadata.execution_blocked_until_bucket_policy_enabled:
        errors.append("execution_not_blocked")
    if endpoint_kind == "minute_bar" and not metadata.freq:
        errors.append("missing_freq")
    if endpoint_kind == "realtime":
        warnings.append("realtime polling policy is required before execution")

    return IntradayBucketValidationResult(
        status="blocked" if errors else "complete",
        endpoint_kind=endpoint_kind,
        metadata=metadata,
        errors=errors,
        warnings=warnings,
    )


def _default_bucket_count(endpoint_kind: str) -> int:
    if endpoint_kind == "minute_bar":
        return 64
    if endpoint_kind in {"tick", "order"}:
        return 128
    return 64


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
