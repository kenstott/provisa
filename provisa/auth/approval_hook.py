# Copyright (c) 2026 Kenneth Stott
# Canary: 79e8861e-37fd-4012-b630-a8ae2f7098e1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ABAC approval hook — external policy evaluation before query execution."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import httpx


class FallbackPolicy(str, Enum):
    ALLOW = "allow"
    DENY = "deny"


class HookType(str, Enum):
    WEBHOOK = "webhook"
    GRPC = "grpc"
    UNIX_SOCKET = "unix_socket"


@dataclass
class ApprovalRequest:
    """Payload sent to the approval hook."""

    user: str
    roles: list[str]
    tables: list[str]
    columns: list[str]
    operation: str


@dataclass
class ApprovalResponse:
    """Result from the approval hook."""

    approved: bool
    reason: str = ""


@dataclass
class ApprovalHookConfig:
    """Configuration for the approval hook."""

    type: HookType = HookType.WEBHOOK
    url: str = ""
    socket_path: str = ""
    timeout_ms: int = 5000
    fallback: FallbackPolicy = FallbackPolicy.DENY
    scope: str = ""
    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown_s: float = 30.0


class CircuitBreaker:
    """Track consecutive failures; open after threshold, half-open after cooldown."""

    def __init__(self, threshold: int = 5, cooldown_s: float = 30.0) -> None:
        self._threshold = threshold
        self._cooldown_s = cooldown_s
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    @property
    def is_open(self) -> bool:
        if self._consecutive_failures < self._threshold:
            return False
        if self._opened_at is None:
            return True
        elapsed = time.monotonic() - self._opened_at
        if elapsed >= self._cooldown_s:
            return False  # half-open: allow one attempt
        return True

    @property
    def is_half_open(self) -> bool:
        if self._consecutive_failures < self._threshold:
            return False
        if self._opened_at is None:
            return False
        return (time.monotonic() - self._opened_at) >= self._cooldown_s

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold and self._opened_at is None:
            self._opened_at = time.monotonic()


class ApprovalHook(ABC):
    """Abstract base for approval hook implementations."""

    @abstractmethod
    async def evaluate(self, request: ApprovalRequest) -> ApprovalResponse: ...


class WebhookApprovalHook(ApprovalHook):
    """HTTP POST approval hook via httpx."""

    def __init__(self, config: ApprovalHookConfig) -> None:
        self._url = config.url
        self._timeout = config.timeout_ms / 1000.0
        self._fallback = config.fallback
        self._breaker = CircuitBreaker(
            config.circuit_breaker_threshold, config.circuit_breaker_cooldown_s
        )

    async def evaluate(self, request: ApprovalRequest) -> ApprovalResponse:
        if self._breaker.is_open and not self._breaker.is_half_open:
            return self._fallback_response("circuit breaker open")

        payload = _request_to_dict(request)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    self._url, json=payload, timeout=self._timeout
                )
                resp.raise_for_status()
                data = resp.json()
                self._breaker.record_success()
                return ApprovalResponse(
                    approved=data.get("approved", False),
                    reason=data.get("reason", ""),
                )
        except Exception:
            self._breaker.record_failure()
            return self._fallback_response("webhook call failed")

    def _fallback_response(self, reason: str) -> ApprovalResponse:
        if self._fallback == FallbackPolicy.ALLOW:
            return ApprovalResponse(approved=True, reason=f"fallback allow: {reason}")
        return ApprovalResponse(approved=False, reason=f"fallback deny: {reason}")


class GrpcApprovalHook(ApprovalHook):
    """gRPC approval hook with persistent channel."""

    def __init__(self, config: ApprovalHookConfig) -> None:
        self._url = config.url
        self._timeout = config.timeout_ms / 1000.0
        self._fallback = config.fallback
        self._channel: Any = None
        self._stub: Any = None
        self._breaker = CircuitBreaker(
            config.circuit_breaker_threshold, config.circuit_breaker_cooldown_s
        )

    def _ensure_channel(self) -> None:
        if self._channel is not None:
            return
        try:
            import grpc  # type: ignore[import-untyped]

            self._channel = grpc.aio.insecure_channel(self._url)
        except ImportError:
            raise RuntimeError("grpcio required for gRPC approval hook")

    async def evaluate(self, request: ApprovalRequest) -> ApprovalResponse:
        if self._breaker.is_open and not self._breaker.is_half_open:
            return self._fallback_response("circuit breaker open")

        try:
            self._ensure_channel()
            # gRPC stub call would go here once proto stubs are generated.
            # For now, raise so fallback kicks in until stubs exist.
            raise NotImplementedError("gRPC stubs not yet generated")
        except Exception:
            self._breaker.record_failure()
            return self._fallback_response("grpc call failed")

    def _fallback_response(self, reason: str) -> ApprovalResponse:
        if self._fallback == FallbackPolicy.ALLOW:
            return ApprovalResponse(approved=True, reason=f"fallback allow: {reason}")
        return ApprovalResponse(approved=False, reason=f"fallback deny: {reason}")


class UnixSocketApprovalHook(ApprovalHook):
    """HTTP POST over Unix domain socket (for OPA / same-machine sidecars)."""

    def __init__(self, config: ApprovalHookConfig) -> None:
        self._socket_path = config.socket_path
        self._url = config.url or "http://localhost/evaluate"
        self._timeout = config.timeout_ms / 1000.0
        self._fallback = config.fallback
        self._breaker = CircuitBreaker(
            config.circuit_breaker_threshold, config.circuit_breaker_cooldown_s
        )

    async def evaluate(self, request: ApprovalRequest) -> ApprovalResponse:
        if self._breaker.is_open and not self._breaker.is_half_open:
            return self._fallback_response("circuit breaker open")

        payload = _request_to_dict(request)
        try:
            transport = httpx.AsyncHTTPTransport(uds=self._socket_path)
            async with httpx.AsyncClient(transport=transport) as client:
                resp = await client.post(
                    self._url, json=payload, timeout=self._timeout
                )
                resp.raise_for_status()
                data = resp.json()
                self._breaker.record_success()
                return ApprovalResponse(
                    approved=data.get("approved", False),
                    reason=data.get("reason", ""),
                )
        except Exception:
            self._breaker.record_failure()
            return self._fallback_response("unix socket call failed")

    def _fallback_response(self, reason: str) -> ApprovalResponse:
        if self._fallback == FallbackPolicy.ALLOW:
            return ApprovalResponse(approved=True, reason=f"fallback allow: {reason}")
        return ApprovalResponse(approved=False, reason=f"fallback deny: {reason}")


def create_hook(config: ApprovalHookConfig) -> ApprovalHook:
    """Factory: create the appropriate hook from config."""
    if config.type == HookType.WEBHOOK:
        return WebhookApprovalHook(config)
    if config.type == HookType.GRPC:
        return GrpcApprovalHook(config)
    if config.type == HookType.UNIX_SOCKET:
        return UnixSocketApprovalHook(config)
    raise ValueError(f"Unknown hook type: {config.type}")


def should_check(
    table_ids: list[str],
    source_ids: list[str],
    config: ApprovalHookConfig,
    *,
    table_hooks: dict[str, bool] | None = None,
    source_hooks: dict[str, bool] | None = None,
) -> bool:
    """Determine whether approval hook should fire for this query.

    Args:
        table_ids: tables referenced in the query.
        source_ids: sources for those tables.
        config: global approval hook config.
        table_hooks: per-table approval_hook flag (table_id -> bool).
        source_hooks: per-source approval_hook flag (source_id -> bool).
    """
    if config.scope == "all":
        return True

    table_hooks = table_hooks or {}
    source_hooks = source_hooks or {}

    for tid in table_ids:
        if table_hooks.get(tid):
            return True

    for sid in source_ids:
        if source_hooks.get(sid):
            return True

    return False


def _request_to_dict(request: ApprovalRequest) -> dict:
    return {
        "user": request.user,
        "roles": request.roles,
        "tables": request.tables,
        "columns": request.columns,
        "operation": request.operation,
    }
