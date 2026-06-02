from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class RateFailurePolicy:
    scope: str | None
    category: str | None
    max_requests_per_batch: int
    retryable_errors: list[str]
    non_retryable_errors: list[str]
    backoff_strategy: str
    stop_conditions: list[str]
    batch_abort_conditions: list[str]
    permission_denied_policy: str
    rate_limited_policy: str
    schema_incompatible_policy: str
    quarantine_policy: str
    execution_allowed: bool
    dry_run: bool
    warnings: list[str]
    blocking_errors: list[str]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors) or not self.execution_allowed

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocked"] = self.blocked
        return data


class RatePolicyReporter:
    def report(self, *, scope: str | None = None, category: str | None = None) -> RateFailurePolicy:
        blocking_errors: list[str] = []
        if scope and category:
            blocking_errors.append("choose either scope or category, not both")
        if not scope and not category:
            blocking_errors.append("scope or category is required")
        if scope and scope != "low-risk-a-share":
            blocking_errors.append(f"unsupported scope: {scope}")
        if category and category not in {"intraday", "financial", "object_text", "realtime"}:
            blocking_errors.append(f"unsupported category: {category}")
        if blocking_errors:
            return self._policy(scope=scope, category=category, max_requests=0, execution_allowed=False, warnings=[], blocking_errors=blocking_errors)
        if scope == "low-risk-a-share":
            return self._policy(
                scope=scope,
                category=None,
                max_requests=20,
                execution_allowed=True,
                warnings=[
                    "policy describes bounded mirror/backfill batches only",
                    "real execution still requires explicit user-confirmed command guardrails",
                ],
            )
        assert category is not None
        max_requests = 0 if category in {"intraday", "financial", "object_text", "realtime"} else 20
        return self._policy(
            scope=None,
            category=category,
            max_requests=max_requests,
            execution_allowed=False,
            warnings=[f"{category} execution remains blocked until dedicated infrastructure and real smoke tests exist"],
            blocking_errors=[f"{category}_execution_blocked"],
        )

    def _policy(
        self,
        *,
        scope: str | None,
        category: str | None,
        max_requests: int,
        execution_allowed: bool,
        warnings: list[str],
        blocking_errors: list[str] | None = None,
    ) -> RateFailurePolicy:
        return RateFailurePolicy(
            scope=scope,
            category=category,
            max_requests_per_batch=max_requests,
            retryable_errors=["rate_limited", "network_error", "server_error"],
            non_retryable_errors=["permission_denied", "invalid_params", "schema_incompatible"],
            backoff_strategy="bounded_exponential_backoff",
            stop_conditions=[
                "token missing",
                "permission denied for required dependency",
                "schema incompatible",
                "quarantine created",
                "restore-check failed",
            ],
            batch_abort_conditions=[
                "max request budget exceeded",
                "trade_cal dependency failed",
                "validation failed",
                "backup failed",
            ],
            permission_denied_policy="record endpoint status and do not retry unless dependency requires abort",
            rate_limited_policy="retry within bounded attempts, then stop batch without expansion",
            schema_incompatible_policy="quarantine and require code/schema review",
            quarantine_policy="do not overwrite quarantined data automatically",
            execution_allowed=execution_allowed,
            dry_run=True,
            warnings=warnings,
            blocking_errors=list(blocking_errors or []),
        )
