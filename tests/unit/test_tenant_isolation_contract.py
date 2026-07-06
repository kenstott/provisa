# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e1c92-6d03-4a58-9f21-8c0e2d63a915
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-838: tenant control-plane isolation is a CONTRACT, mechanism is backend-relative.

The invariant — a tenant-scoped connection reaches only its own org's data — is identical
across control-plane backends; only the REALIZATION differs, chosen from the store's dialect:

- PostgreSQL   -> per-connection SET search_path to org_<org_id> + a per-org role
                  (physical schema isolation).
- single-tenant portable backend (SQLite / MySQL from schema_org)
               -> the sole default schema of a per-tenant database (no search_path,
                  no role system to harden).

These tests assert the DISPATCH — that the right mechanism is selected per backend and that an
unavailable mechanism degrades explicitly (a no-op, not a silent partial isolation) — rather
than assuming any single mechanism. test_org_isolation.py covers the PG realization in depth.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

# ---------------------------------------------------------------------------
# PostgreSQL realization: physical schema isolation (search_path + per-org role)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pg_realization_scopes_connection_to_org_schema():
    """On PostgreSQL, a per-connection init binds the org's physical schema (search_path)."""
    from provisa.core.db import _make_init_conn

    conn = AsyncMock()
    await _make_init_conn("acme")(conn)

    issued = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("SET search_path" in s and "org_acme" in s for s in issued)


@pytest.mark.asyncio
async def test_pg_realization_hardens_with_a_per_org_role():
    """On PostgreSQL the mechanism also creates a role scoped to the org schema."""
    from provisa.core.db import create_org_role

    conn = AsyncMock()
    conn.capabilities.dialect = "postgresql"
    await create_org_role(conn, "acme")

    issued = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("role_acme" in s for s in issued)
    assert any("GRANT USAGE, CREATE ON SCHEMA org_acme TO role_acme" in s for s in issued)


# ---------------------------------------------------------------------------
# Portable (single-tenant) realization: default schema, no PG-only mechanism
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_pg_backend_does_not_use_the_pg_role_mechanism():
    """A backend with no role system realizes isolation differently — the role step is a no-op.

    Explicit degrade (REQ-889): isolation still holds (single-tenant default schema), the
    PG-specific hardening is simply not attempted — never a silent half-applied mechanism.
    """
    from provisa.core.db import create_org_role

    conn = AsyncMock()
    conn.capabilities.dialect = "sqlite"
    await create_org_role(conn, "acme")

    conn.execute.assert_not_awaited()  # no role DDL issued on a non-PG store


@pytest.mark.asyncio
async def test_non_pg_backend_bootstraps_from_portable_schema_without_search_path(monkeypatch):
    """On a non-PG control-plane store, init_schema takes the portable path (no search_path DDL).

    The isolation mechanism here is the per-tenant database's sole default schema, so no
    org_<id> schema / SET search_path is issued — a different realization of the same contract.
    """
    import provisa.core.db as db

    portable_called = {"n": 0}

    async def _fake_portable(pool):
        assert pool.dialect == "sqlite"  # the portable path received the non-PG pool
        portable_called["n"] += 1

    monkeypatch.setattr(db, "_init_schema_portable", _fake_portable)

    class _PortablePool:
        dialect = "sqlite"

    await db.init_schema(_PortablePool(), "CREATE TABLE t (id INT)", org_id="acme")  # type: ignore[arg-type]

    assert portable_called["n"] == 1  # routed to the portable, non-search_path realization


# ---------------------------------------------------------------------------
# The contract holds regardless of which mechanism was selected
# ---------------------------------------------------------------------------


def test_org_id_is_validated_before_any_mechanism_binds():
    """Whatever the backend, the org identity is validated first — no injection into any
    isolation mechanism (search_path, schema name, role name)."""
    from provisa.core.db import _validate_org_id

    for bad in ("bad-org", "org with spaces", "org;drop", "a'b"):
        with pytest.raises(ValueError):
            _validate_org_id(bad)

    for ok in ("default", "acme123", "org_1"):
        assert _validate_org_id(ok) is None


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
