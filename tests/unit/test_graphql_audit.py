# Copyright (c) 2026 Kenneth Stott
# Canary: 2e9b6d41-7c3a-4f85-a1d0-8b5e3c7f9a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL request is audited like every other transport's (REQ-074/REQ-1386).

``/data/graphql`` produced no ``query_audit_log`` row at all. Each request now hands one record to
the audit writer — a query, a query answered from its cached plan, a mutation, a refused request —
and the response does not wait for the INSERT.
"""

# Requirements: REQ-074, REQ-1386

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest
from fastapi.responses import JSONResponse

from provisa.audit.context import audit_identity_scope
from provisa.audit.graphql import document_table_ids
from provisa.compiler.sql_types import CompilationContext, JoinMeta, TableMeta
from tests.unit.test_graphql_plan_cache import _endpoint_harness


def _table(table_id: int, name: str, type_name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=type_name,
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name=name,
    )


_ORDERS = _table(7, "orders", "Orders")
_CUSTOMERS = _table(9, "customers", "Customers")
_CTX = CompilationContext(
    tables={"orders": _ORDERS, "customers": _CUSTOMERS},
    joins={
        ("Orders", "customer"): JoinMeta(
            source_column="customer_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=_CUSTOMERS,
            cardinality="many-to-one",
        )
    },
)


@pytest.fixture
def audited(monkeypatch):
    """The endpoint harness with a tenant database, an acting principal and the audit rows the
    writer inserted. ``rows()`` waits for the writer; ``gate`` holds the INSERT back."""
    from provisa.audit.writer import audit_writer_status, flush_audit
    from provisa.encryption import NullEncryption

    written: list[dict] = []
    gate = threading.Event()
    gate.set()

    async def _log_queries(pool, batch):
        assert gate.wait(10)
        written.extend(batch)

    monkeypatch.setattr("provisa.audit.query_log.log_queries", _log_queries)
    monkeypatch.setattr("provisa.encryption.runtime.encryption_service", NullEncryption)
    # The harness's compilation context and row-filter context are opaque stand-ins, so what the
    # role's governance enforced is stood in for too — as the tables it was asked about, which is
    # what this file checks the row carries (tests/unit/test_audit_provenance.py builds it for real).
    monkeypatch.setattr(
        "provisa.audit.provenance.enforced_for_request",
        lambda _state, _role, _ctx: lambda table_ids: {"tables": list(table_ids)},
    )
    harness = _endpoint_harness(monkeypatch)
    harness.state.tenant_db = object()
    harness.state.record_db = harness.state.tenant_db
    harness.state.hot_counts = None  # REQ-826: no Hot-count store; counting has its own tests
    harness.state.org_id = "acme"
    harness.state.contexts = {"analyst": _CTX}

    def call(*args, **kwargs):
        with audit_identity_scope("alice", "http"):
            return harness.call(*args, **kwargs)

    def rows() -> list[dict]:
        assert flush_audit(5.0), audit_writer_status()
        return written

    yield SimpleNamespace(call=call, rows=rows, gate=gate, harness=harness, written=written)
    gate.set()


def _facts(row: dict) -> tuple:
    return (
        row["tenant_id"],
        row["user_id"],
        row["role_id"],
        row["table_ids"],
        row["source"],
        row["status_code"],
        row["query_text_enc"].decode(),
    )


_QUERY = "{ orders { orderId customer { name } } }"


def test_a_query_produces_exactly_one_audit_row(audited):
    audited.call(_QUERY)
    assert [_facts(r) for r in audited.rows()] == [
        ("acme", "alice", "analyst", [7, 9], "http", 200, _QUERY)
    ]


def test_a_query_answered_from_its_cached_plan_is_audited_too(audited):
    audited.call(_QUERY)
    audited.call(_QUERY)
    assert audited.harness.calls["compile"] == 1  # the second request was a plan hit
    assert [_facts(r) for r in audited.rows()] == [
        ("acme", "alice", "analyst", [7, 9], "http", 200, _QUERY)
    ] * 2


def test_a_mutation_produces_exactly_one_audit_row(audited, monkeypatch):
    from graphql import parse

    from provisa.api.data import endpoint

    mutation = 'mutation { insert_orders(objects: [{region: "eu"}]) { affected_rows } }'

    async def _mutate(*args, **kwargs):
        return JSONResponse({"data": {"insert_orders": {"affected_rows": 1}}})

    monkeypatch.setattr(endpoint, "parse_query", lambda schema, query, variables=None: parse(query))
    monkeypatch.setattr(endpoint, "_handle_mutation", _mutate)
    audited.call(mutation)
    assert [_facts(r) for r in audited.rows()] == [
        ("acme", "alice", "analyst", [7], "http", 200, mutation)
    ]


def test_a_refused_request_is_audited_with_its_status(audited, monkeypatch):
    from provisa.api.data import endpoint
    from provisa.api.errors import ApiError

    async def _refuse(*args, **kwargs):
        raise ApiError(403, "data.forbidden", "role may not read orders")

    monkeypatch.setattr(endpoint, "_handle_query", _refuse)
    with pytest.raises(ApiError):
        audited.call(_QUERY)
    assert [(r["status_code"], r["table_ids"]) for r in audited.rows()] == [(403, [7, 9])]


def test_a_normalized_read_is_audited(audited, monkeypatch):
    from provisa.api.data import endpoint

    async def _normalized(*args, **kwargs):
        return JSONResponse({"normalized": {}})

    monkeypatch.setattr(endpoint, "_handle_normalized", _normalized)
    audited.call(_QUERY, x_provisa_normalized="true")
    assert [(r["status_code"], r["table_ids"]) for r in audited.rows()] == [(200, [7, 9])]


def test_opening_a_subscription_is_audited(audited, monkeypatch):
    from graphql import parse

    import provisa.api.data.subscription_sse as sse
    from provisa.api.data import endpoint

    subscription = "subscription { orders { orderId } }"

    async def _open(*args, **kwargs):
        return JSONResponse({"stream": "opened"})

    monkeypatch.setattr(endpoint, "parse_query", lambda schema, query, variables=None: parse(query))
    monkeypatch.setattr(sse, "handle_subscription_sse", _open)
    audited.call(subscription)
    assert [_facts(r) for r in audited.rows()] == [
        ("acme", "alice", "analyst", [7], "http", 200, subscription)
    ]


def test_an_action_field_request_is_one_row_the_actions_governed_statement(audited, monkeypatch):
    """An action's rows are governed by a statement of their own, which is audited like a table
    read. That row IS the request's record: the request writes no second one."""
    from provisa.api.data import endpoint
    from provisa.audit.pipeline import PendingAudit, write_audit
    from provisa.audit.context import note_statement_audited

    async def _action_request(*args, **kwargs):
        # What action_governance._audit does when the action's governed statement has run.
        pending = PendingAudit(
            "alice", "http", "analyst", "SELECT * FROM send_invoice", [], 0.0, 1, {}
        )
        await write_audit(pending, 200, audited.harness.state, route="engine", row_count=2)
        note_statement_audited()
        return JSONResponse({"data": {"send_invoice": [{"ok": True}, {"ok": True}]}})

    monkeypatch.setattr(endpoint, "_handle_query", _action_request)
    audited.call("{ send_invoice(id: 1) { ok } }")
    assert [(r["query_text_enc"].decode(), r["route"], r["row_count"]) for r in audited.rows()] == [
        ("SELECT * FROM send_invoice", "engine", 2)
    ]


def test_a_query_records_the_rows_it_returned(audited):
    audited.call(_QUERY)
    (row,) = audited.rows()
    assert row["row_count"] == 1  # the harness's field returns one row


def test_a_response_cache_hit_records_the_cache_route(audited, monkeypatch):
    from provisa.api.data import endpoint

    async def _hit(compiled, *args, **kwargs):
        entry = SimpleNamespace(age_seconds=1)  # a response-cache entry: the field was a HIT
        return compiled.root_field, [{"orderId": 1}, {"orderId": 2}], None, "ck", entry

    monkeypatch.setattr(endpoint, "_execute_one_field", _hit)
    audited.call(_QUERY)
    (row,) = audited.rows()
    assert (row["route"], row["row_count"]) == ("cache", 2)


def test_a_refused_request_records_no_route_and_no_rows(audited, monkeypatch):
    from provisa.api.data import endpoint
    from provisa.api.errors import ApiError

    async def _refuse(*args, **kwargs):
        raise ApiError(403, "data.forbidden", "role may not read orders")

    monkeypatch.setattr(endpoint, "_handle_query", _refuse)
    with pytest.raises(ApiError):
        audited.call(_QUERY)
    (row,) = audited.rows()
    assert (row["route"], row["row_count"], row["status_code"]) == (None, None, 403)


def test_the_route_a_field_was_answered_by_is_noted_where_it_is_decided():
    """The executed-field path notes its route on the request's audit outcome right after the
    route decision (the harness above stubs that function out)."""
    import inspect

    from provisa.api.data import endpoint

    source = inspect.getsource(endpoint._execute_one_field)
    decided = source.index("decision = decide_route(")
    noted = source.index("note_request_route(")
    assert (
        decided < noted < source.index("if decision.route == Route.CACHE and cached is not None:")
    )


def test_the_response_does_not_wait_for_the_insert(audited):
    audited.gate.clear()  # the INSERT cannot complete
    response = audited.call(_QUERY)
    assert response.status_code == 200  # the request returned regardless
    assert audited.written == []
    audited.gate.set()
    assert len(audited.rows()) == 1


def test_a_request_with_no_acting_principal_records_nothing(audited):
    audited.harness.call(_QUERY)  # no identity bound: not a user request
    assert audited.rows() == []


def test_table_ids_follow_relationships_fragments_and_mutation_roots():
    assert document_table_ids("{ orders { id } customers { id } }", _CTX) == (7, 9)
    assert document_table_ids(
        "query { orders { ...F } } fragment F on Orders { customer { name } }", _CTX
    ) == (7, 9)
    assert document_table_ids(
        "mutation { delete_customers(where: {}) { affected_rows } }", _CTX
    ) == (9,)
    assert document_table_ids("mutation { send_invoice(id: 1) { ok } }", _CTX) == ()  # an action
    assert document_table_ids("{ orders { ", _CTX) == ()  # does not parse: reached no table
