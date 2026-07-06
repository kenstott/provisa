# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3a1c07-5d42-4b96-9f18-2c7b0e64a935
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-852: OrgRouter — the multi-tenant mechanism for NOT-schema-capable backends.

OrgRouter maps org_id -> Database, lazily caching one engine per org_<id> database file
(file-per-org). It guards against construction on schema-capable backends (PG/MySQL/Oracle),
where an org is a namespace on a shared connection (Capabilities.enter_org_sql) and a router
would be wrong. Pure unit test — engines are built lazily via create_engine_from_url, patched
so no real files/pools are opened.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from provisa.core.database import OrgRouter


# ---------------------------------------------------------------------------
# Guard: file-per-org router is invalid on schema-capable backends
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("url", [
    "postgresql+asyncpg://u:p@h/db",
    "mysql+aiomysql://u:p@h/db",
])
def test_rejects_schema_capable_backends(url):
    """A schema-capable backend scopes orgs on a shared engine — a router must refuse to build."""
    with pytest.raises(ValueError, match="not-schema-capable"):
        OrgRouter(url)


def test_accepts_not_schema_capable_backend():
    """SQLite (file-per-org, no namespaces) is exactly what OrgRouter is for."""
    router = OrgRouter("sqlite+aiosqlite:////data/control.db")
    assert router is not None


# ---------------------------------------------------------------------------
# Per-org file URL derivation
# ---------------------------------------------------------------------------


def test_per_org_url_is_a_sibling_file_named_for_the_org():
    router = OrgRouter("sqlite+aiosqlite:////data/control.db")
    assert router._org_url("acme") == "sqlite+aiosqlite:////data/org_acme.db"


def test_database_for_rejects_invalid_org_id():
    # database_for validates the org id before deriving any file path (no injection into the URL).
    router = OrgRouter("sqlite+aiosqlite:////data/control.db")
    with pytest.raises(ValueError):
        router.database_for("bad-org")
    with pytest.raises(ValueError):
        router.database_for("bad;org")


# ---------------------------------------------------------------------------
# Lazy build + one-engine-per-org caching
# ---------------------------------------------------------------------------


def test_database_for_lazily_builds_and_caches_one_engine_per_org():
    from unittest.mock import MagicMock

    engine = MagicMock(name="engine")
    with patch(
        "provisa.core.database.create_engine_from_url", return_value=engine
    ) as make_engine:
        router = OrgRouter("sqlite+aiosqlite:////data/control.db")

        a1 = router.database_for("acme")
        a2 = router.database_for("acme")  # cache hit — no second engine
        b1 = router.database_for("beta")

    assert a1 is a2  # same org -> same cached Database
    assert a1 is not b1  # different org -> different Database
    # exactly one engine built per distinct org, each pointed at that org's file
    built = [c.args[0] for c in make_engine.call_args_list]
    assert built == [
        "sqlite+aiosqlite:////data/org_acme.db",
        "sqlite+aiosqlite:////data/org_beta.db",
    ]


def test_named_database_reflects_the_org():
    from unittest.mock import MagicMock

    with patch("provisa.core.database.create_engine_from_url", return_value=MagicMock()):
        router = OrgRouter("sqlite+aiosqlite:////data/control.db")
        db = router.database_for("acme")
    assert db.name == "org_acme"


@pytest.mark.asyncio
async def test_close_closes_and_clears_every_cached_org_database():
    from unittest.mock import AsyncMock

    from unittest.mock import MagicMock

    closed: list[str] = []

    with patch("provisa.core.database.create_engine_from_url", return_value=MagicMock()):
        router = OrgRouter("sqlite+aiosqlite:////data/control.db")
        for org in ("acme", "beta"):
            db = router.database_for(org)
            db.close = AsyncMock(side_effect=lambda o=org: closed.append(o))

        await router.close()

    assert sorted(closed) == ["acme", "beta"]
    assert router._cache == {}  # cache cleared after close


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
