from __future__ import annotations

import http.client
import urllib.error
from enum import Enum
from typing import Any, Mapping


class ErrorType(str, Enum):
    PERMISSION_DENIED = "permission_denied"
    RATE_LIMITED = "rate_limited"
    NETWORK_ERROR = "network_error"
    SERVER_ERROR = "server_error"
    INVALID_PARAMS = "invalid_params"
    INVALID_ENDPOINT = "invalid_endpoint"
    EMPTY_RESULT = "empty_result"
    SCHEMA_INCOMPATIBLE = "schema_incompatible"
    WRITE_FAILED = "write_failed"
    CHECKSUM_FAILED = "checksum_failed"
    CATALOG_COMMIT_FAILED = "catalog_commit_failed"
    VALIDATION_FAILED = "validation_failed"
    UNKNOWN_ERROR = "unknown_error"


class MirrorError(RuntimeError):
    def __init__(self, error_type: ErrorType | str, message: str):
        self.error_type = ErrorType(error_type)
        super().__init__(message)


NON_RETRYABLE = {
    ErrorType.PERMISSION_DENIED,
    ErrorType.INVALID_PARAMS,
    ErrorType.INVALID_ENDPOINT,
    ErrorType.EMPTY_RESULT,
    ErrorType.SCHEMA_INCOMPATIBLE,
    ErrorType.CHECKSUM_FAILED,
    ErrorType.VALIDATION_FAILED,
    ErrorType.CATALOG_COMMIT_FAILED,
}

RETRYABLE = {
    ErrorType.RATE_LIMITED,
    ErrorType.NETWORK_ERROR,
    ErrorType.SERVER_ERROR,
    ErrorType.UNKNOWN_ERROR,
}


def classify_tushare_response(response: Mapping[str, Any]) -> tuple[str, str | None]:
    code = response.get("code")
    msg = str(response.get("msg") or "")
    msg_lower = msg.lower()
    if code == 0:
        items = (((response.get("data") or {}).get("items")) or [])
        return ("accessible" if items else "empty_but_accessible", None)
    if "权限" in msg or "permission" in msg_lower or "积分" in msg:
        return ErrorType.PERMISSION_DENIED.value, msg
    if "频" in msg or "rate" in msg_lower or "limit" in msg_lower or "每分钟" in msg:
        return ErrorType.RATE_LIMITED.value, msg
    if "不存在" in msg or "invalid api" in msg_lower or "api_name" in msg_lower:
        return ErrorType.INVALID_ENDPOINT.value, msg
    if "参数" in msg or "param" in msg_lower:
        return ErrorType.INVALID_PARAMS.value, msg
    if code in (500, 502, 503, 504):
        return ErrorType.SERVER_ERROR.value, msg
    return ErrorType.UNKNOWN_ERROR.value, msg


def classify_exception(exc: Exception) -> ErrorType:
    from .client import TushareError

    if isinstance(exc, MirrorError):
        return exc.error_type
    if isinstance(exc, TushareError):
        status, _ = classify_tushare_response(exc.response or {"code": exc.code, "msg": exc.message})
        try:
            return ErrorType(status)
        except ValueError:
            return ErrorType.UNKNOWN_ERROR
    if isinstance(exc, (TimeoutError, ConnectionError, urllib.error.URLError, http.client.IncompleteRead, http.client.RemoteDisconnected)):
        return ErrorType.NETWORK_ERROR
    return ErrorType.UNKNOWN_ERROR


def should_retry(error_type: ErrorType | str, attempt: int, max_attempts: int) -> bool:
    err = ErrorType(error_type)
    return err in RETRYABLE and attempt < max_attempts


def retry_delay_seconds(error_type: ErrorType | str, attempt: int) -> float:
    err = ErrorType(error_type)
    if err == ErrorType.RATE_LIMITED:
        return min(30.0, float(2 ** max(attempt - 1, 0)))
    if err in {ErrorType.NETWORK_ERROR, ErrorType.SERVER_ERROR, ErrorType.UNKNOWN_ERROR}:
        return min(10.0, float(attempt))
    return 0.0
