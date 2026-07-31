# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b1c90-2a47-4d18-9e05-8b3c7d41ea62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1352: source catalog registration drops and recreates, so credentials refresh.

Trino cannot report a live catalog's properties back, and ``CREATE CATALOG IF NOT EXISTS`` is a
no-op against an existing catalog. Registration that skipped when the catalog existed therefore
pinned every source to the credentials it first booted with: rotating the Cloud SQL password left
cloud.provisa.dev's demo catalogs authenticating with the dead one, and every federated query died
with ``JDBC_ERROR "FATAL: password authentication failed for user \"provisa\""`` while the control
plane — whose system catalogs already drop-and-recreate — kept working.
"""

from __future__ import annotations

import pytest
import trino

from provisa.core import catalog
from provisa.core.models import Source, SourceType


class _FakeCursor:
    def __init__(self, recorder: list[str], undroppable: str | None) -> None:
        self._recorder = recorder
        self._undroppable = undroppable

    def execute(self, sql: str, *_args, **_kwargs):
        self._recorder.append(sql)
        if self._undroppable and sql.startswith("DROP CATALOG") and self._undroppable in sql:
            raise trino.exceptions.TrinoQueryError(
                {
                    "errorName": "CATALOG_NOT_FOUND",
                    "message": "catalog is loaded from a static properties file",
                },
                query_id="q1",
            )
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None


class _FakeConn:
    def __init__(self, undroppable: str | None = None) -> None:
        self.executed: list[str] = []
        self._undroppable = undroppable

    def cursor(self):  # type: ignore[override]
        return _FakeCursor(self.executed, self._undroppable)


def _pg_source() -> Source:
    return Source(
        id="pet-store",
        type=SourceType.postgresql,
        host="10.93.0.3",
        port=5432,
        database="provisa",
        username="provisa",
        password="rotated-secret",
    )


def test_registration_drops_before_creating():
    conn = _FakeConn()
    catalog.create_catalog(conn, _pg_source(), "rotated-secret")

    drops = [s for s in conn.executed if s.startswith("DROP CATALOG")]
    creates = [s for s in conn.executed if s.startswith("CREATE CATALOG")]
    assert drops == ["DROP CATALOG IF EXISTS pet_store"], conn.executed
    assert len(creates) == 1, conn.executed
    assert conn.executed.index(drops[0]) < conn.executed.index(creates[0])


def test_create_is_unconditional_so_properties_actually_refresh():
    # IF NOT EXISTS would silently keep the pre-rotation catalog after the drop raced or the
    # catalog was recreated by another writer — the whole defect this guards.
    conn = _FakeConn()
    catalog.create_catalog(conn, _pg_source(), "rotated-secret")

    create = next(s for s in conn.executed if s.startswith("CREATE CATALOG"))
    assert "IF NOT EXISTS" not in create
    assert "rotated-secret" in create


def test_registration_no_longer_probes_for_an_existing_catalog():
    # SHOW CATALOGS was the skip check; its absence is what makes registration authoritative.
    conn = _FakeConn()
    catalog.create_catalog(conn, _pg_source(), "rotated-secret")

    assert not any(s.startswith("SHOW CATALOGS") for s in conn.executed), conn.executed


def test_an_undroppable_catalog_fails_loudly_instead_of_shadowing():
    conn = _FakeConn(undroppable="pet_store")

    with pytest.raises(RuntimeError, match="cannot be dropped"):
        catalog.create_catalog(conn, _pg_source(), "rotated-secret")

    assert not any(s.startswith("CREATE CATALOG") for s in conn.executed), conn.executed


def test_org_prefixed_catalog_name_is_the_one_dropped():
    # REQ-1266: multi-org registration must refresh the org's physical catalog, not the bare name.
    conn = _FakeConn()
    catalog.create_catalog(
        conn, _pg_source(), "rotated-secret", catalog_name="org_kstott__pet_store"
    )

    assert "DROP CATALOG IF EXISTS org_kstott__pet_store" in conn.executed
    assert any(s.startswith("CREATE CATALOG org_kstott__pet_store ") for s in conn.executed)
