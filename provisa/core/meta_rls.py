# Copyright (c) 2026 Kenneth Stott
# Canary: 699438a6-e5b3-4f63-9ead-9ce65d3dde0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""App-layer meta-governance RLS (REQ-828, REQ-041, REQ-402).

Tenant isolation on the control-plane (``_META_TABLES``) moved OUT of Postgres row-level
security and INTO the app layer, so it holds identically on every admin-store backend —
Postgres, SQLite, and the embedded DuckDB dialect (REQ-828 coupling #3). Postgres RLS
cannot travel to DuckDB/SQLite; this guard is the store-independent enforcement, and the
PG policies (``_init_meta_rls``) remain only as defense-in-depth where Postgres is used.

The guard is applied at the single choke point every repository statement flows through —
``Database.Connection.execute_core`` — so no call site can bypass it. When a tenant is in
scope (multitenancy), a read/modify of a tenant-scoped meta table is confined to that
tenant's own rows plus shared ``tenant_id IS NULL`` rows (mirroring the PG policy's USING
clause), and inserts are stamped with the tenant. When NO tenant is in scope (single-tenant
desktop / ``multitenancy=False``, and startup seeding of shared rows) the guard is a no-op.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any

from sqlalchemy import Column, Delete, Insert, Select, Table, Update, Uuid, or_

# The control-plane tables governed by tenant isolation (mirrors app_loaders._META_TABLES).
# Only those carrying a ``tenant_id`` column are DIRECTLY scoped here; the rest
# (roles_domain_access, tracked_webhooks, tracked_functions) have no tenant_id and are
# isolated TRANSITIVELY via their FK to an already-scoped parent row.
META_TABLES = frozenset(
    {
        "registered_tables",
        "table_columns",
        "domains",
        "relationships",
        "rls_rules",
        "roles",
        "roles_domain_access",
        "tracked_webhooks",
        "tracked_functions",
    }
)

_current_tenant: ContextVar[str | None] = ContextVar("provisa_meta_tenant", default=None)


def set_meta_tenant(tenant_id: str | None) -> Token:
    """Bind the tenant whose control-plane rows are visible/writable for this context. Returns a
    token for :func:`reset_meta_tenant`. Set at the request boundary (tenant middleware) and around
    any tenant-scoped programmatic control-plane access."""
    return _current_tenant.set(tenant_id)


def reset_meta_tenant(token: Token) -> None:
    _current_tenant.reset(token)


def current_meta_tenant() -> str | None:
    return _current_tenant.get()


@contextmanager
def meta_tenant_scope(tenant_id: str | None):
    """Scope control-plane access to ``tenant_id`` for the duration of the block."""
    token = _current_tenant.set(tenant_id)
    try:
        yield
    finally:
        _current_tenant.reset(token)


def _scoped_table(target: Any) -> Table | None:
    """The target as a tenant-scoped meta Table, or None when it is not one (or lacks tenant_id)."""
    if isinstance(target, Table) and target.name in META_TABLES and "tenant_id" in target.c:
        return target
    return None


def _tenant_value(col: Column, tenant: str) -> Any:
    """Coerce the context tenant (a string, e.g. from the JWT claim) to the tenant_id column's
    Python type — a ``uuid.UUID`` for the UUID columns the control-plane uses, else the raw string.
    Without this the Uuid bind processor rejects the string (``'str' has no attribute 'hex'``)."""
    if isinstance(col.type, Uuid):
        return uuid.UUID(tenant)
    return tenant


def apply_meta_tenant_guard(stmt: Any) -> Any:
    """Return ``stmt`` scoped to the tenant currently in context (REQ-828).

    - No tenant in context → returned unchanged (single-tenant / shared-row seeding).
    - Non-meta or non-Core statement → returned unchanged.
    - SELECT / UPDATE / DELETE on a tenant-scoped meta table → a
      ``tenant_id = <tenant> OR tenant_id IS NULL`` predicate is AND-ed in.
    - INSERT (values form) into a tenant-scoped meta table → ``tenant_id`` is stamped with the
      tenant, so a row can never be written under another tenant.
    """
    tenant = _current_tenant.get()
    if tenant is None:
        return stmt

    if isinstance(stmt, (Update, Delete)):
        tbl = _scoped_table(stmt.table)
        if tbl is not None:
            col = tbl.c.tenant_id
            return stmt.where(or_(col == _tenant_value(col, tenant), col.is_(None)))
        return stmt

    if isinstance(stmt, Select):
        for frm in stmt.get_final_froms():
            tbl = _scoped_table(frm)
            if tbl is not None:
                col = tbl.c.tenant_id
                stmt = stmt.where(or_(col == _tenant_value(col, tenant), col.is_(None)))
        return stmt

    if isinstance(stmt, Insert):
        tbl = _scoped_table(stmt.table)
        # Only value-based inserts are stamped; INSERT ... FROM SELECT (stmt.select set) is left
        # to the SELECT's own guard on read. Stamping overrides any caller tenant_id — a write can
        # never land under a different tenant while a tenant is in scope.
        if tbl is not None and getattr(stmt, "select", None) is None:
            return stmt.values(tenant_id=_tenant_value(tbl.c.tenant_id, tenant))
        return stmt

    return stmt
