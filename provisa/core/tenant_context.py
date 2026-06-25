# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-tenant compiled state container and in-memory cache."""

# Requirements: REQ-008, REQ-253

from __future__ import annotations

import time
from dataclasses import dataclass, field

from provisa.core.models import ProvisaConfig
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.rls import RLSContext


@dataclass
class TenantContext:  # REQ-008, REQ-253
    tenant_id: str
    config: ProvisaConfig
    compilation_contexts: dict[str, CompilationContext] = field(default_factory=dict)
    rls_contexts: dict[str, RLSContext] = field(default_factory=dict)
    schemas: dict[str, object] = field(default_factory=dict)
    built_at: float = field(default_factory=time.monotonic)

    def is_fresh(self, ttl_seconds: int = 300) -> bool:
        return (time.monotonic() - self.built_at) < ttl_seconds


class TenantContextCache:  # REQ-008, REQ-253
    """In-memory cache of per-tenant compiled contexts. TTL 5 min."""

    def __init__(self, ttl_seconds: int = 300) -> None:
        self._cache: dict[str, TenantContext] = {}
        self._ttl = ttl_seconds

    def get(self, tenant_id: str) -> TenantContext | None:
        ctx = self._cache.get(tenant_id)
        if ctx and ctx.is_fresh(self._ttl):
            return ctx
        return None

    def set(self, tenant_id: str, ctx: TenantContext) -> None:
        self._cache[tenant_id] = ctx

    def invalidate(self, tenant_id: str) -> None:
        self._cache.pop(tenant_id, None)
