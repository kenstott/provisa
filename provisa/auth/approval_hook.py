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

# Requirements: REQ-203, REQ-204, REQ-246, REQ-247


class FallbackPolicy(str, Enum):  # REQ-556
    ALLOW = "allow"
    DENY = "deny"


class HookType(str, Enum):  # REQ-246
    WEBHOOK = "webhook"
    GRPC = "grpc"
    UNIX_SOCKET = "unix_socket"


@dataclass
class ApprovalRequest:  # REQ-555
    """Payload sent to the approval hook (REQ-203)."""

    user: str
    roles: list[str]
    tables: list[str]
    columns: list[str]
    operation: str
    session_vars: dict[str, str] = field(default_factory=dict)


@dataclass
class ApprovalResponse:  # REQ-555
    """Result from the approval hook (REQ-203).

    ``additional_filter`` is an optional raw SQL predicate that the caller ANDs into
    the query's WHERE clause after governance, narrowing the result further.
    """

    approved: bool
    reason: str = ""
    additional_filter: str | None = None


@dataclass
class ApprovalHookConfig:  # REQ-247
    """Configuration for the approval hook."""

    type: HookType = HookType.WEBHOOK
    url: str = ""
    socket_path: str = ""
    timeout_ms: int = 5000
    fallback: FallbackPolicy = FallbackPolicy.DENY
    scope: str = ""
    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown_s: float = 30.0


class CircuitBreaker:  # REQ-556
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


class ApprovalHook(ABC):  # REQ-203
    """Abstract base for approval hook implementations."""

    @abstractmethod
    async def evaluate(self, request: ApprovalRequest) -> ApprovalResponse: ...


class WebhookApprovalHook(ApprovalHook):  # REQ-246
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
                resp = await client.post(self._url, json=payload, timeout=self._timeout)
                resp.raise_for_status()
                data = resp.json()
                self._breaker.record_success()
                return ApprovalResponse(
                    approved=data.get("approved", False),
                    reason=data.get("reason", ""),
                    additional_filter=data.get("additional_filter"),
                )
        except Exception:
            self._breaker.record_failure()
            return self._fallback_response("webhook call failed")

    def _fallback_response(self, reason: str) -> ApprovalResponse:
        if self._fallback == FallbackPolicy.ALLOW:
            return ApprovalResponse(approved=True, reason=f"fallback allow: {reason}")
        return ApprovalResponse(approved=False, reason=f"fallback deny: {reason}")


class GrpcApprovalHook(ApprovalHook):  # REQ-246
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
            import grpc.aio  # type: ignore[import-untyped]
            from provisa.auth import approval_pb2_grpc

            self._channel = grpc.aio.insecure_channel(self._url)
            self._stub = approval_pb2_grpc.ApprovalServiceStub(self._channel)
        except ImportError:
            raise RuntimeError("grpcio required for gRPC approval hook")

    async def evaluate(self, request: ApprovalRequest) -> ApprovalResponse:
        if self._breaker.is_open and not self._breaker.is_half_open:
            return self._fallback_response("circuit breaker open")

        try:
            self._ensure_channel()
            from provisa.auth import approval_pb2

            proto_req = approval_pb2.ApprovalRequest(  # type: ignore[attr-defined]
                user=request.user,
                roles=list(request.roles),
                tables=list(request.tables),
                columns=list(request.columns),
                operation=request.operation,
                session_vars=dict(request.session_vars),
            )
            proto_resp = await self._stub.Evaluate(proto_req, timeout=self._timeout)
            self._breaker.record_success()
            return ApprovalResponse(
                approved=proto_resp.approved,
                reason=proto_resp.reason,
                additional_filter=proto_resp.additional_filter or None,
            )
        except Exception:
            self._breaker.record_failure()
            return self._fallback_response("grpc call failed")

    def _fallback_response(self, reason: str) -> ApprovalResponse:
        if self._fallback == FallbackPolicy.ALLOW:
            return ApprovalResponse(approved=True, reason=f"fallback allow: {reason}")
        return ApprovalResponse(approved=False, reason=f"fallback deny: {reason}")


class UnixSocketApprovalHook(ApprovalHook):  # REQ-246
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
                resp = await client.post(self._url, json=payload, timeout=self._timeout)
                resp.raise_for_status()
                data = resp.json()
                self._breaker.record_success()
                return ApprovalResponse(
                    approved=data.get("approved", False),
                    reason=data.get("reason", ""),
                    additional_filter=data.get("additional_filter"),
                )
        except Exception:
            self._breaker.record_failure()
            return self._fallback_response("unix socket call failed")

    def _fallback_response(self, reason: str) -> ApprovalResponse:
        if self._fallback == FallbackPolicy.ALLOW:
            return ApprovalResponse(approved=True, reason=f"fallback allow: {reason}")
        return ApprovalResponse(approved=False, reason=f"fallback deny: {reason}")


def create_hook(config: ApprovalHookConfig) -> ApprovalHook:  # REQ-246
    """Factory: create the appropriate hook from config."""
    if config.type == HookType.WEBHOOK:
        return WebhookApprovalHook(config)
    if config.type == HookType.GRPC:
        return GrpcApprovalHook(config)
    if config.type == HookType.UNIX_SOCKET:
        return UnixSocketApprovalHook(config)
    raise ValueError(f"Unknown hook type: {config.type}")


def load_approval_hook_config(block: dict | None) -> ApprovalHookConfig | None:  # REQ-247
    """Build an ApprovalHookConfig from the `auth.approval_hook` YAML block (REQ-247).

    Returns None when no block is configured (hook disabled).
    """
    if not block:
        return None
    return ApprovalHookConfig(
        type=HookType(block.get("type", "webhook")),
        url=block.get("url", ""),
        socket_path=block.get("socket_path", ""),
        timeout_ms=int(block.get("timeout_ms", 5000)),
        fallback=FallbackPolicy(block.get("fallback", "deny")),
        scope=block.get("scope", ""),
        circuit_breaker_threshold=int(block.get("circuit_breaker_threshold", 5)),
        circuit_breaker_cooldown_s=float(block.get("circuit_breaker_cooldown_s", 30.0)),
    )


def should_check(  # REQ-204
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
        "session_vars": request.session_vars,
    }
