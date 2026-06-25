# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Helpers to resolve per-tenant or global compiled state from a request."""

# Requirements: REQ-456, REQ-592

from __future__ import annotations

from fastapi import Request

from provisa.core.tenant_context import TenantContext
from provisa.api.app import state


def get_tenant_context(request: Request) -> TenantContext | None:  # REQ-456, REQ-592
    """Returns per-tenant context in SaaS mode, None in single-tenant mode."""
    return getattr(request.state, "tenant_context", None)


def get_schemas(request: Request) -> dict:  # REQ-456
    ctx = get_tenant_context(request)
    return ctx.schemas if ctx else state.schemas


def get_compilation_context(request: Request, role_id: str):  # REQ-456
    ctx = get_tenant_context(request)
    return ctx.compilation_contexts[role_id] if ctx else state.contexts[role_id]


def get_rls_context(request: Request, role_id: str):  # REQ-456
    ctx = get_tenant_context(request)
    return ctx.rls_contexts[role_id] if ctx else state.rls_contexts[role_id]
