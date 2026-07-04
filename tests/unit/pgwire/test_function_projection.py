# Copyright (c) 2026 Kenneth Stott
# Canary: 2d9f4a71-6c08-4b53-9e21-7a4c0d6f8b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-872: registered tracked_functions projected into the SQL surface catalog.

pgwire's pg_proc and information_schema.routines/parameters are built from the
tracked_functions registry, so a SQL client (psql \\df, DBeaver, Explore) can
DISCOVER registered functions. Only functions visible to the role appear.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from provisa.pgwire.catalog import _build_catalog_db


def _state_with_functions(functions: dict):
    state = MagicMock()
    mc = MagicMock()
    mc.tables = {}
    state.contexts = {"alice": mc}
    state.schema_build_cache = {"column_types": {}}
    state.tracked_functions = functions
    return state


_CREATE_ORDER = {
    "id": 1,
    "name": "createOrder",
    "arguments": [{"name": "customer_id", "type": "integer"}, {"name": "total", "type": "number"}],
    "returns": "provisa.public.orders",
    "return_schema": "[]",  # table-valued (set-returning)
    "visible_to": [],
    "kind": "mutation",
}
_SECRET_FN = {
    "id": 2,
    "name": "adminReset",
    "arguments": [],
    "returns": "boolean",
    "visible_to": ["ops"],  # not visible to alice
    "kind": "mutation",
}


def test_routines_lists_registered_function():
    db = _build_catalog_db("alice", _state_with_functions({"createOrder": _CREATE_ORDER}))
    rows = db.execute(
        "SELECT routine_name, routine_type, data_type FROM _is_routines WHERE routine_name='createOrder'"
    ).fetchall()
    db.close()
    assert rows == [("createOrder", "FUNCTION", "record")]


def test_pg_proc_lists_registered_function_with_arity():
    db = _build_catalog_db("alice", _state_with_functions({"createOrder": _CREATE_ORDER}))
    rows = db.execute(
        "SELECT proname, prokind, pronargs, proretset FROM _pg_proc WHERE proname='createOrder'"
    ).fetchall()
    db.close()
    assert rows == [("createOrder", "f", 2, True)]


def test_parameters_projected_in_order():
    db = _build_catalog_db("alice", _state_with_functions({"createOrder": _CREATE_ORDER}))
    rows = db.execute(
        "SELECT ordinal_position, parameter_name, parameter_mode, data_type "
        "FROM _is_parameters WHERE specific_name LIKE 'createOrder_%' ORDER BY ordinal_position"
    ).fetchall()
    db.close()
    assert rows == [
        (1, "customer_id", "IN", "integer"),
        (2, "total", "IN", "double precision"),
    ]


def test_role_scoped_visibility():
    fns = {"createOrder": _CREATE_ORDER, "adminReset": _SECRET_FN}
    db = _build_catalog_db("alice", _state_with_functions(fns))
    names = {r[0] for r in db.execute("SELECT proname FROM _pg_proc").fetchall()}
    db.close()
    assert "createOrder" in names  # unrestricted → visible
    assert "adminReset" not in names  # visible_to=['ops'], not alice


def test_visible_to_grants_access():
    db = _build_catalog_db("ops", _state_with_functions({"adminReset": _SECRET_FN}))
    names = {r[0] for r in db.execute("SELECT proname FROM _pg_proc").fetchall()}
    db.close()
    assert "adminReset" in names


def test_duplicate_alias_projected_once():
    # Same function under two registry keys (bare + domain-prefixed) → one catalog row.
    fns = {"createOrder": _CREATE_ORDER, "sales__createOrder": _CREATE_ORDER}
    db = _build_catalog_db("alice", _state_with_functions(fns))
    n = db.execute("SELECT count(*) FROM _pg_proc WHERE proname='createOrder'").fetchone()[0]
    db.close()
    assert n == 1


def test_no_functions_leaves_catalog_empty():
    db = _build_catalog_db("alice", _state_with_functions({}))
    n = db.execute("SELECT count(*) FROM _pg_proc").fetchone()[0]
    db.close()
    assert n == 0
