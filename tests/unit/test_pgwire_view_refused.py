# Copyright (c) 2026 Kenneth Stott
# Canary: 2a9c6e15-7f48-4b03-9d61-c5e8a0f3b742
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``CREATE VIEW`` over pgwire is refused: a view is created through the model.

The DDL handler used to send a view's statement on as written — to the engine when the domain's
DDL catalog is an engine catalog, to the source when it is a source's — so the view's body read
live addresses with no row filter, mask or column visibility, and the result was a relation the
model did not know (kept only in the creating role's in-memory context). The statement is now
recognised from its tokens and refused before either path, naming the admin action."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from provisa.pgwire import ddl_handler
from provisa.pgwire.ddl_handler import DdlHandler, ViewNotCreatedOverPgwire, creates_view

_VIEW_STATEMENTS = [
    "CREATE VIEW leak AS SELECT * FROM sales_pg.public.orders",
    "create or replace view leak as select * from orders",
    "CREATE MATERIALIZED VIEW leak AS SELECT * FROM orders",
    "CREATE TEMP VIEW leak AS SELECT 1",
    "CREATE OR REPLACE TEMPORARY VIEW leak AS SELECT 1",
    "CREATE RECURSIVE VIEW leak (n) AS SELECT 1",
    "/* nightly */ CREATE\n  VIEW leak AS SELECT 1",
    "-- nightly\nCREATE VIEW leak AS SELECT 1",
    "CREATE VIEW leak AS SELECT 'unterminated",  # malformed: refused all the same
]
_NOT_VIEW_STATEMENTS = [
    "CREATE TABLE view_log (id INT)",
    "CREATE TABLE t (view TEXT)",
    "CREATE INDEX view_idx ON t (a)",
    "SELECT 'CREATE VIEW v AS SELECT 1'",
    "DROP VIEW leak",
    "ALTER VIEW leak RENAME TO other",
]


@pytest.mark.parametrize("sql", _VIEW_STATEMENTS)
def test_a_statement_that_creates_a_view_is_recognised(sql):
    assert creates_view(sql)


@pytest.mark.parametrize("sql", _NOT_VIEW_STATEMENTS)
def test_a_statement_that_only_mentions_a_view_is_not(sql):
    assert not creates_view(sql)


def _state(write_target: tuple[str, str], *, source_catalog: str | None):
    """A deployment whose domain writes DDL to ``write_target``. With ``source_catalog`` set the
    target is a registered source's catalog (the direct path); otherwise an engine catalog."""
    state = MagicMock()
    state.roles = {"maker": {"capabilities": ["ddl"], "domain_access": ["sales"]}}
    state.domain_write_targets = {"sales": write_target}
    state.source_catalogs = {"sales-pg": source_catalog} if source_catalog else {}
    state.source_types = {"sales-pg": "postgresql"} if source_catalog else {}
    state.contexts = {"maker": MagicMock(tables={})}
    return state


def _handle(state, sql: str, role: str = "maker"):
    handler = object.__new__(DdlHandler)
    handler._handler = MagicMock()
    ctx = MagicMock()
    ctx.session.role_id = role
    with (
        patch("provisa.pgwire.ddl_handler.state", state),
        patch("provisa.pgwire.ddl_handler.run_on_connection_loop") as ran,
        patch("provisa.pgwire.ddl_handler._register_ddl_object") as registered,
    ):
        try:
            return handler.handle(ctx, sql), ran, registered
        except Exception as exc:  # returned to the test, which asserts on it
            return exc, ran, registered


_TARGETS = {
    "engine catalog": {"write_target": ("iceberg", "sales"), "source_catalog": None},
    "source catalog": {"write_target": ("sales_pg", "public"), "source_catalog": "sales_pg"},
}


@pytest.mark.parametrize("target", sorted(_TARGETS))
@pytest.mark.parametrize("sql", _VIEW_STATEMENTS)
def test_create_view_is_refused_and_reaches_neither_the_engine_nor_the_source(target, sql):
    state = _state(**_TARGETS[target])
    outcome, ran, registered = _handle(state, sql)

    assert isinstance(outcome, ViewNotCreatedOverPgwire)
    assert not isinstance(outcome, PermissionError)  # the server answers 0A000, not 42501
    assert "CREATE VIEW is not available over pgwire" in str(outcome)
    assert "registerTable" in str(outcome) and "admin UI" in str(outcome)
    ran.assert_not_called()  # nothing was run on the engine or on a source connection
    state.federation_engine.execute_engine.assert_not_called()
    state.source_pools.acquire.assert_not_called()
    registered.assert_not_called()  # and nothing was added to any role's context
    assert state.contexts["maker"].tables == {}


def test_the_refusal_does_not_depend_on_the_roles_rights():
    """A role with no ``ddl`` capability gets the same answer: the statement is available to
    nobody, so it says so before it says anything about the role."""
    state = _state(**_TARGETS["engine catalog"])
    state.roles["reader"] = {"capabilities": [], "domain_access": ["sales"]}
    outcome, ran, _ = _handle(state, "CREATE VIEW leak AS SELECT 1", role="reader")
    assert isinstance(outcome, ViewNotCreatedOverPgwire)
    ran.assert_not_called()


def test_create_table_still_takes_the_engine_path():
    """The refusal is the view's alone: a table with column definitions goes where it went."""
    state = _state(**_TARGETS["engine catalog"])
    outcome, ran, registered = _handle(state, "CREATE TABLE notes (id INT)")
    assert outcome == "CREATE TABLE"
    ran.assert_called_once()
    state.federation_engine.execute_engine.assert_called_once_with(
        "CREATE TABLE iceberg.sales.notes (id INT)"
    )
    registered.assert_called_once_with("maker", "notes", "iceberg", "sales", "TABLE")


def test_the_handler_holds_no_view_statement_pattern():
    """No pattern in the handler rewrites or forwards a view statement any more."""
    import inspect

    source = inspect.getsource(ddl_handler)
    assert "_VIEW_RE" not in source.replace("_OPENS_AS_VIEW_RE", "").replace(
        "_MAY_CREATE_VIEW_RE", ""
    )
    assert 'verb = "VIEW"' not in source
