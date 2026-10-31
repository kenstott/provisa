# Copyright (c) 2026 Kenneth Stott
# Canary: 19b3e7d4-62fa-4a85-b7c1-0d4f8e2a6c95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2190: a client's catalog statement is addressed to the meta domain by name, its functions
are the compiler's forms, and nothing it asks about the session reaches the control plane."""

# Requirements: REQ-2190, REQ-127

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.pgwire.catalog_pg17 import RELATIONS
from provisa.pgwire.catalog_route import CatalogRequest, addressed_statement
from provisa.transpiler.metadata_facts import catalog_facts
from provisa.transpiler.metadata_functions import MetadataFunctionRefused
from provisa.transpiler.metadata_settings import CatalogViewRefused
from provisa.transpiler.metadata_views import catalog_view_name
from provisa.transpiler.router import Route

_REQUEST = CatalogRequest(
    role_id="reader",
    database="sales_db",
    facts=catalog_facts("reader", {1: {"SELECT"}}),
    settings={"server_version": "17.0"},
    pid=4242,
)


def _addressed(sql: str, request: CatalogRequest = _REQUEST) -> str:
    return addressed_statement(sql, request)


def _tables(sql: str) -> set[str]:
    return {f"{t.db}.{t.name}" for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)}


@pytest.mark.parametrize(("schema", "relation"), sorted(RELATIONS))
def test_every_relation_is_addressed_to_its_view(schema, relation):
    assert _tables(_addressed(f"SELECT 1 FROM {schema}.{relation}")) == {
        f"meta.{catalog_view_name(schema, relation)}"
    }


def test_a_bare_name_is_pg_catalogs_first():
    assert _tables(_addressed("SELECT 1 FROM pg_class")) == {"meta.catalog_pg_class"}
    assert _tables(_addressed("SELECT 1 FROM routines")) == {"meta.is_routines"}


def test_the_statements_own_name_still_qualifies_its_columns():
    sql = _addressed("SELECT pg_class.oid, pg_catalog.pg_class.relname FROM pg_catalog.pg_class")
    assert "AS pg_class" in sql
    assert "pg_class.relname" in sql
    assert "pg_catalog.pg_class" not in sql


def test_a_cte_of_a_catalogs_name_stays_the_statements():
    sql = _addressed("WITH pg_class AS (SELECT 1 AS a) SELECT a FROM pg_class")
    assert "meta." not in sql


@pytest.mark.parametrize(
    "call",
    [
        "current_user",
        "session_user",
        "current_database()",
        "current_schema()",
        "current_catalog",
        "version()",
        "pg_backend_pid()",
        "current_setting('server_version')",
        "set_config('a.b', 'x', true)",
        "getpgusername()",
        "pg_catalog.current_setting('server_version')",
    ],
)
def test_a_session_function_is_answered_for_the_session(call):
    written = _addressed(f"SELECT {call} FROM pg_catalog.pg_am")
    tree = sqlglot.parse_one(written, read="postgres")
    assert not {type(n).__name__ for n in tree.walk()} & {
        "CurrentUser",
        "SessionUser",
        "CurrentDatabase",
        "CurrentSchema",
        "CurrentCatalog",
        "CurrentVersion",
    }
    assert not {f.name.lower() for f in tree.find_all(exp.Anonymous)} & {
        "version",
        "pg_backend_pid",
        "current_setting",
        "set_config",
    }


def test_the_session_is_the_requests():
    written = _addressed("SELECT current_user, current_database(), pg_backend_pid()")
    assert "'reader'" in written and "'sales_db'" in written and "4242" in written


def test_a_regclass_literal_never_reads_the_server():
    written = _addressed("SELECT 1 FROM pg_class c WHERE c.oid = 'sales.orders'::regclass")
    assert "regclass" not in written.lower()
    assert "meta.catalog_pg_class" in written
    assert "provisa_raise('42p01'" in written.lower()


def test_a_catalogs_own_relation_is_postgresqls_oid():
    assert "1259" in _addressed("SELECT 'pg_class'::regclass::oid")


def test_a_regclass_read_as_text_is_the_listed_name():
    written = _addressed("SELECT c.oid::regclass::text FROM pg_class c")
    assert "regclass" not in written.lower()
    assert written.count("catalog_pg_class") >= 2


def test_a_row_version_column_is_no_views():
    assert "c.xmin" not in _addressed("SELECT c.xmin FROM pg_class c")


def test_a_function_on_a_catalog_is_its_form():
    written = _addressed("SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c")
    assert "meta.table_keys" in written


def test_a_server_file_is_never_read_from_the_server():
    tree = sqlglot.parse_one(_addressed("SELECT pg_read_file('/etc/passwd')"), read="postgres")
    assert "pg_read_file" not in {f.name.lower() for f in tree.find_all(exp.Anonymous)}
    assert "'absolute path not allowed'" in tree.sql(dialect="postgres")


def test_statistics_answer_nothing_of_the_server():
    written = _addressed("SELECT pg_stat_get_db_numbackends(1)")
    assert written.lower().startswith("select cast(0 as ")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT query_to_xml('select * from pg_stat_activity', true, true, '')",
        "SELECT * FROM query_to_xml('select 1', true, true, '') t",
        "SELECT ts_stat('select 1')",
        "SELECT pg_terminate_backend(1)",
    ],
)
def test_what_runs_sql_on_the_server_is_refused_by_name(sql):
    with pytest.raises(MetadataFunctionRefused):
        _addressed(sql)


def test_a_table_form_keeps_the_statements_alias():
    tree = sqlglot.parse_one(_addressed("SELECT t.name FROM pg_ls_waldir() AS t"), read="postgres")
    assert not list(tree.find_all(exp.Anonymous))
    subquery = tree.find(exp.Subquery)
    assert subquery is not None and subquery.alias == "t"


def test_an_array_cast_sqlglot_cannot_read_is_read():
    assert "_oid" in _addressed("SELECT 1 FROM pg_class WHERE oid = ANY(NULL::oid[])").lower()


async def test_a_catalog_statements_route_is_never_kept_for_its_shape(monkeypatch):
    """A form can read exactly as a client's own statement does (pg_last_xact_replay_timestamp() is
    CAST(NULL AS TIMESTAMPTZ)): a route kept for that shape would send the client's statement to
    the control plane."""
    from unittest.mock import MagicMock

    from provisa.api.data import materialization
    from provisa.pgwire import _pipeline
    from provisa.pgwire.catalog_route import current_request

    async def routed(*_args, **_kwargs):
        return "routed as itself"

    monkeypatch.setattr(_pipeline, "_optimize_and_route", routed)
    monkeypatch.setattr(materialization, "would_materialize_optimize", lambda *a, **k: False)
    monkeypatch.setattr(_pipeline, "_view_answering", lambda *a, **k: None)
    state = MagicMock()
    token = current_request.set(_REQUEST)
    try:
        answer = await _pipeline._optimize_and_route_cached(
            "SELECT 1", "SELECT 1", MagicMock(), MagicMock(), state, "reader", table_ids=()
        )
    finally:
        current_request.reset(token)
    assert answer == "routed as itself"
    assert state.compiled_query_cache.mock_calls == []


_CATALOG_VIEW, _USER_TABLE = 90, 7


def _meta_and_a_source():
    from provisa.compiler.sql_gen import CompilationContext
    from provisa.compiler.sql_types import TableMeta

    ctx = CompilationContext()
    ctx.tables = {
        "catalog_pg_class": TableMeta(
            table_id=_CATALOG_VIEW,
            field_name="meta__catalog_pg_class",
            type_name="MetaCatalogPgClass",
            source_id="provisa-admin",
            catalog_name="provisa_admin",
            schema_name="meta",
            table_name="catalog_pg_class",
            domain_id="meta",
        ),
        "orders": TableMeta(
            table_id=_USER_TABLE,
            field_name="sales__orders",
            type_name="SalesOrders",
            source_id="sales",
            catalog_name="sales",
            schema_name="public",
            table_name="orders",
            domain_id="sales",
        ),
    }
    return ctx


def _two_stores():
    from types import SimpleNamespace

    from provisa.federation.replica_address import ReplicaRoutes
    from provisa.mv.registry import MVRegistry

    return SimpleNamespace(
        _active_runtime=lambda: SimpleNamespace(unbound_sources=frozenset()),
        mv_registry=MVRegistry(),
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"provisa-admin": "postgresql", "sales": "postgresql"},
        source_dialects={"provisa-admin": "postgres", "sales": "postgres"},
        source_dsns={},
        source_pools=SimpleNamespace(source_ids={"provisa-admin", "sales"}, has=lambda _: True),
        replica_routes=ReplicaRoutes(),
        federation_engine=SimpleNamespace(engine=None),
        tables=[],
    )


async def _routed(sql: str, table_ids: tuple[int, ...], *, catalog_statement: bool):
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import build_governance_context
    from provisa.pgwire._pipeline import _optimize_and_route
    from provisa.pgwire.catalog_route import current_request
    from tests.helpers import unscoped_role

    ctx = _meta_and_a_source()
    gov = build_governance_context(
        "reader", RLSContext.empty(), {}, ctx, tables=[], role=unscoped_role("reader")
    )
    token = current_request.set(_REQUEST) if catalog_statement else None
    try:
        return await _optimize_and_route(
            sql, sql, gov, ctx, _two_stores(), table_ids=table_ids, nf_args={}
        )
    finally:
        if token is not None:
            current_request.reset(token)


@pytest.mark.parametrize("catalog_statement", [True, False])
async def test_a_catalog_view_read_with_a_user_table_is_refused_by_name(catalog_statement):
    """Row scope reads CURRENT_SETTING, which only the admin terminal gives a statement: on the
    engine it would fail as an engine's error about a function, and no client could tell why."""
    with pytest.raises(CatalogViewRefused, match="meta.catalog_pg_class") as refused:
        await _routed(
            "SELECT c.relname FROM meta.catalog_pg_class c JOIN sales.orders o ON o.id = c.oid",
            (_CATALOG_VIEW, _USER_TABLE),
            catalog_statement=catalog_statement,
        )
    assert refused.value.sqlstate == "0A000"
    assert refused.value.params == {"view": "meta.catalog_pg_class"}


async def test_a_catalog_statement_alone_is_the_admin_terminals():
    _, decision, *_ = await _routed(
        "SELECT c.relname FROM meta.catalog_pg_class c",
        (_CATALOG_VIEW,),
        catalog_statement=True,
    )
    assert (decision.route, decision.source_id) == (Route.DIRECT, "provisa-admin")
