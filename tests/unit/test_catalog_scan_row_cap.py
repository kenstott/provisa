# Copyright (c) 2026 Kenneth Stott
# Canary: b15bd467-c091-4b6b-81a5-09d8e8583c5a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A client's catalog statement is answered whole, whatever the asking role's row cap (REQ-005,
REQ-2190, REQ-883).

DuckDB's ``ATTACH ... TYPE postgres`` discovers a database's tables with ONE statement over
pg_class, pg_namespace, pg_attribute and pg_type, ordered by namespace oid: pg_catalog's own
relations (namespace 11) come first, some two thousand rows of them, and a registered table (public,
2200) after. Governed as a read of the meta domain, that statement was capped at the role's default
row limit (``server.limits.default_row_limit``, 100 as shipped) like any data read, so a role
without ``full_results`` was answered pg_catalog's first hundred attributes and no table of its own:
``information_schema.tables`` in DuckDB listed nothing.

Governed for real here: the model the pgwire_pg_backend fixture serves, written to an embedded
control plane, the statement through ``catalog.answer`` → ``govern_catalog_plan`` → the admin
terminal as the catalog client role."""

# Requirements: REQ-005, REQ-883, REQ-2190

from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.pgwire_pg_model import ROLE, TABLE, build_state, meta_catalog
from tests.unit.pgwire.catalog_plane import grant_catalog_client, plane_engine, write_model

# DuckDB 1.5.5's postgres extension, verbatim: its table discovery on ATTACH.
_DUCKDB_DISCOVERY = """SELECT pg_namespace.oid AS namespace_id, relname, relpages, attname,
    pg_type.typname type_name, atttypmod type_modifier, pg_attribute.attndims ndim,
    attnum, pg_attribute.attnotnull AS notnull, NULL constraint_id,
    NULL constraint_type, NULL constraint_key, type_ns.nspname AS type_schema,
    col_desc.description AS column_comment,
    tbl_desc.description AS table_comment
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_attribute ON pg_class.oid=pg_attribute.attrelid
JOIN pg_type ON atttypid=pg_type.oid
JOIN pg_namespace type_ns ON pg_type.typnamespace = type_ns.oid
LEFT JOIN pg_description col_desc ON col_desc.objoid=pg_class.oid AND col_desc.objsubid=pg_attribute.attnum
LEFT JOIN pg_description tbl_desc ON tbl_desc.objoid=pg_class.oid AND tbl_desc.objsubid=0
WHERE attnum > 0 AND relkind IN ('r', 'v', 'm', 'f', 'p')
UNION ALL
SELECT pg_namespace.oid AS namespace_id, relname, NULL relpages, NULL attname, NULL type_name,
    NULL type_modifier, NULL ndim, NULL attnum, NULL AS notnull,
    pg_constraint.oid AS constraint_id, contype AS constraint_type,
    conkey AS constraint_key, NULL AS type_schema,
    NULL AS column_comment, NULL AS table_comment
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_constraint ON (pg_class.oid=pg_constraint.conrelid)
WHERE relkind IN ('r', 'v', 'm', 'f', 'p') AND contype IN ('p', 'u')
ORDER BY namespace_id, relname, attnum, constraint_id"""

_PUBLIC_OID = 2200
# The shipped default (provisa.core.settings_catalog, limits.default_row_limit), stated here so the
# test does not read whatever the session's environment or config holds.
_SHIPPED_ROW_LIMIT = "100"


@pytest.fixture
def served(control_plane, monkeypatch):
    """The pgwire_pg_backend model on an embedded control plane, read through the real pipeline:
    its state, and ``run``, which takes a coroutine onto the loop the model store is read on."""
    import provisa.api.app as app_mod
    import provisa.pgwire.server as srv
    from provisa.core.database import Database

    monkeypatch.setenv("PROVISA_DEFAULT_ROW_LIMIT", _SHIPPED_ROW_LIMIT)
    engine = plane_engine(control_plane)
    grant_catalog_client(engine)
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()

    def run(coro):
        return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=60)

    model_db = Database(engine, name="model")
    meta_tables, meta_col_types = meta_catalog(model_db, loop)
    state = build_state(MagicMock(), meta_tables, meta_col_types)
    write_model(engine, ROLE["id"], state)
    state.model_db = model_db
    state.admin_db = None  # no platform plane behind this harness (REQ-1454)
    state.model_stamp = 1  # REQ-1914: the first model
    state.org_id = "default"
    # The audit row is the tenant plane's, which this harness does not hold.
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    with patch.object(app_mod, "state", state), patch.object(srv, "state", state, create=True):
        yield state, run
    loop.call_soon_threadsafe(loop.stop)


@pytest.mark.usefixtures("served")
def test_the_role_is_capped_at_the_shipped_row_limit():
    """The premise: the asking role holds no full_results, so its reads are capped at 100 rows."""
    from provisa.compiler.stage2 import resolve_row_cap

    assert "full_results" not in ROLE["capabilities"]
    assert resolve_row_cap(ROLE) == int(_SHIPPED_ROW_LIMIT)


def test_duckdb_discovery_answers_the_registered_table_past_the_row_cap(served):
    from provisa.pgwire import catalog

    state, run = served
    result = catalog.answer(_DUCKDB_DISCOVERY, ROLE["id"], state, None, run=run)
    columns = {
        row[3] for row in result.rows if row[0] == _PUBLIC_OID and row[1] == TABLE and row[3]
    }
    assert {"id", "amount", "region"} <= columns
    # pg_catalog's own relations precede it: the answer is longer than the cap it is not held to.
    assert len(result.rows) > int(_SHIPPED_ROW_LIMIT)
    assert result.rows[0][0] == 11  # pg_catalog, first by namespace oid


def test_the_same_roles_data_read_is_still_capped(served):
    """Only the catalog statement is answered whole: a read of the registered table's data is held
    to the role's cap as before."""
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.pgwire._pipeline import plan_pgwire_sql

    _state, run = served
    plan = run(plan_pgwire_sql(f"SELECT id FROM public.{TABLE}", ROLE["id"]))
    limit = sqlglot.parse_one(plan.sql, read="postgres").args.get("limit")
    assert isinstance(limit, exp.Limit)
    assert limit.expression.sql() == _SHIPPED_ROW_LIMIT
