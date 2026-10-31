# Copyright (c) 2026 Kenneth Stott
# Canary: b7d3c521-08e6-4a1f-9e42-6f35a0c8d117
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The control plane a test of what a catalog answers reads (REQ-2190)."""

# Requirements: REQ-2190

from __future__ import annotations

import pytest

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.transpiler.metadata_views import CATALOG_VIEW_NAMES, NEUTRAL_VIEW_NAMES

_SCHEMA = "org_default"


@pytest.fixture(scope="session")
def _catalog_engine(embedded_postgres, control_plane_template):
    from tests.pg_control_plane import clone

    held = clone(embedded_postgres, control_plane_template)
    engine = held.engine(f"{held.url}?options=-csearch_path%3D{_SCHEMA},public")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS meta")
        for ddl in _META_TABLE_VIEWS.values():
            conn.exec_driver_sql(ddl.replace("%", "%%"))
        for name in (*NEUTRAL_VIEW_NAMES, *CATALOG_VIEW_NAMES):
            conn.exec_driver_sql(f"CREATE VIEW meta.{name} AS SELECT * FROM {_SCHEMA}.{name}")
        conn.exec_driver_sql(
            "CREATE OR REPLACE FUNCTION provisa_raise(sqlstate TEXT, message TEXT) RETURNS TEXT "
            "LANGUAGE plpgsql AS $$ BEGIN "
            "RAISE EXCEPTION USING ERRCODE = sqlstate, MESSAGE = message; END $$"
        )
    yield engine
    held.close()


@pytest.fixture(autouse=True)
def _catalog_reads_the_plane(request, monkeypatch):
    """Every test here reaching ``catalog.answer``, directly or through a server, reads a real
    control plane."""
    from provisa.pgwire import catalog
    from tests.unit.pgwire.catalog_plane import Plane

    plane = Plane(request.getfixturevalue("_catalog_engine"))
    monkeypatch.setattr(catalog, "_read_meta", plane.read)
    yield
