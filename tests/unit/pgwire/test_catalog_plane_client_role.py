# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3c2a91-5d0e-4b8a-9c61-2e4f8b0d13a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin terminal runs a client's catalog SQL as the client role (REQ-2190): a control plane a
test serves that terminal from has to grant that role what the product's seed grants it, or
DuckDB's ATTACH catalog scan (REQ-883) fails "permission denied for schema org_default"."""

# Requirements: REQ-883, REQ-2190

from __future__ import annotations

import psycopg
import pytest
from sqlalchemy.exc import ProgrammingError

from provisa.transpiler.metadata_settings import enter_client_role
from tests.unit.pgwire.catalog_plane import grant_catalog_client, plane_engine

_NAMESPACES = "SELECT oid, nspname FROM org_default.catalog_pg_namespace ORDER BY oid"


def _read_as_client(engine) -> list[tuple]:
    with engine.connect() as conn:
        with conn.begin():
            conn.exec_driver_sql(enter_client_role())
            return [tuple(r) for r in conn.exec_driver_sql(_NAMESPACES).fetchall()]


def test_an_ungranted_plane_refuses_the_client_role(control_plane):
    """The failure the fixture hit: the bare plane gives the client role nothing."""
    engine = plane_engine(control_plane)
    with pytest.raises(ProgrammingError) as raised:
        _read_as_client(engine)
    assert isinstance(raised.value.orig, psycopg.errors.InsufficientPrivilege)
    assert "permission denied for schema org_default" in str(raised.value)


def test_a_granted_plane_answers_the_client_roles_catalog_read(control_plane):
    engine = plane_engine(control_plane)
    grant_catalog_client(engine)
    names = {name for _oid, name in _read_as_client(engine)}
    assert {"pg_catalog", "information_schema", "public"} <= names
