# Copyright (c) 2026 Kenneth Stott
# Canary: 0823df66-e3fc-4c38-86c5-acf1e294edf4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view's inputs are in service for its readers (REQ-1921).

A view is answered from the tables its SQL names. A table out of service -- a draft -- is
offered to no reader, by any name: a statement over a view that reads one is refused naming the
table, exactly as a statement naming the table is. The view itself stays; it is answered again
when its input is released."""

# Requirements: REQ-1921

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.definitions import TableIsDraft
from provisa.compiler.introspect import ColumnMetadata
from provisa.mv.view_read import require_inputs_in_service
from tests.unit.test_empty_domain_access_reaches_nothing import DOMAIN_OF, _tables
from tests.unit.test_table_read_as_caller import _Routed, _refunds

pytestmark = pytest.mark.unit

_VIEW = 10
_OVER_THE_DRAFT = 'SELECT id, note FROM "sales"."refunds"'
_OVER_A_SERVED_TABLE = 'SELECT id FROM "sales"."orders"'


def _view(view_sql: str, **over) -> dict:
    return _refunds(
        **{
            "id": _VIEW,
            "source_id": "__derived__",
            "schema_name": "sales",
            "table_name": "refund_notes",
            "view_sql": view_sql,
            "draft": False,
            **over,
        }
    )


def _build(monkeypatch, *, view: dict, drafts: list[dict], role: dict | None = None):
    """The real role build over the tables in service, the view among them, and the drafts."""
    import provisa.api.app as appmod
    from provisa.api.app_loaders import _build_and_register_schemas

    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(state._active_runtime(), name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(
        state, "source_types", {"pg": "postgresql", "__derived__": "derived"}, raising=False
    )
    monkeypatch.setattr(
        state, "source_catalogs", {"pg": "pg", "__derived__": "__derived__"}, raising=False
    )
    tables = [*_tables(), view]
    columns = {
        tid: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
        for tid in DOMAIN_OF
    }
    columns[view["id"]] = [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="note", data_type="varchar", is_nullable=True),
    ]
    _build_and_register_schemas(
        roles=[
            role
            or {
                "id": "r",
                "description": "Test role.",
                "capabilities": ["usage"],
                "domain_access": ["sales"],
            }
        ],
        tables=tables,
        relationships=[],
        col_types_converted=columns,
        naming_rules=[],
        domains=[{"id": d} for d in DOMAIN_OF.values()],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=drafts,
    )
    monkeypatch.setattr(state, "tables", tables, raising=False)
    monkeypatch.setattr(
        state, "view_sql_map", {view["table_name"]: view["view_sql"]}, raising=False
    )
    return state


async def _read_the_view(state) -> str:
    """The statement a reader's read of the view hands to routing: one statement, the view's SQL
    in place of its reference, governed and lowered whole."""
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire import _pipeline

    async def _route(*args, **kwargs):
        raise _Routed(str(args[0]))

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        with pytest.raises(_Routed) as routed:
            await _pipeline._govern_and_route_compiled(
                'SELECT * FROM "sales"."refund_notes" LIMIT 5',
                "r",
                state=state,
                cache_hint=NO_CACHE_HINT,
                sdl_joins=False,
            )
    return routed.value.sql


async def test_a_view_over_a_table_out_of_service_is_refused_naming_the_table(monkeypatch):
    state = _build(monkeypatch, view=_view(_OVER_THE_DRAFT), drafts=[_refunds()])
    with pytest.raises(TableIsDraft) as refused:
        await _read_the_view(state)
    assert refused.value.table == "refunds"


async def test_the_view_is_answered_again_once_its_input_is_released(monkeypatch):
    state = _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[_refunds()])
    assert '"pg"."public"."orders"' in await _read_the_view(state)


@pytest.mark.parametrize(
    "said",
    ['"sales"."refunds"', "sales.refunds", "public.refunds", "refunds", '"refunds" AS r'],
)
def test_a_table_out_of_service_is_refused_by_every_name_a_view_could_give_it(said):
    state = SimpleNamespace(tables=_tables(), role_build_inputs={"draft_tables": [_refunds()]})
    with pytest.raises(TableIsDraft) as refused:
        require_inputs_in_service(f"SELECT id FROM {said}", state)
    assert refused.value.table == "refunds"


def test_it_is_found_wherever_the_views_sql_reads_it():
    state = SimpleNamespace(tables=_tables(), role_build_inputs={"draft_tables": [_refunds()]})
    for sql in (
        'SELECT o.id FROM "sales"."orders" o JOIN "sales"."refunds" r ON r.id = o.id',
        'SELECT id FROM "sales"."orders" WHERE id IN (SELECT id FROM sales.refunds)',
        'WITH r AS (SELECT id FROM "sales"."refunds") SELECT id FROM r',
    ):
        with pytest.raises(TableIsDraft):
            require_inputs_in_service(sql, state)


def test_a_view_whose_inputs_are_all_in_service_is_not_refused():
    state = SimpleNamespace(tables=_tables(), role_build_inputs={"draft_tables": [_refunds()]})
    require_inputs_in_service('SELECT id FROM "sales"."orders"', state)
    require_inputs_in_service("SELECT 1", state)


def test_a_name_a_table_in_service_answers_to_is_that_tables():
    """A draft in another domain with a served table's name takes nothing from the served one."""
    twin = _refunds(id=11, domain_id="finance", table_name="orders")
    state = SimpleNamespace(tables=_tables(), role_build_inputs={"draft_tables": [twin]})
    require_inputs_in_service('SELECT id FROM "sales"."orders"', state)
    require_inputs_in_service("SELECT id FROM orders", state)
    with pytest.raises(TableIsDraft):
        require_inputs_in_service('SELECT id FROM "finance"."orders"', state)


async def test_a_draft_views_preview_follows_the_same_rule(monkeypatch):
    from provisa.api.admin import table_read

    view = _view(_OVER_THE_DRAFT, draft=True)
    served = _view(_OVER_A_SERVED_TABLE, id=12, table_name="order_ids")
    state = _build(monkeypatch, view=served, drafts=[_refunds(), view])
    with pytest.raises(TableIsDraft) as refused:
        await table_read.read_as_caller(state, _VIEW, "r", limit=5)
    assert refused.value.table == "refunds"


# --- a table the reader may not read is not read through a view either (#222) ---------------------

_OVER_ANOTHER_DOMAIN = 'SELECT id FROM "finance"."ledger"'


async def test_a_view_over_a_table_its_reader_does_not_reach_is_refused_naming_it(monkeypatch):
    """The reader reaches ``sales`` and the view is in ``sales``; the view reads ``finance``.
    Refused where the view's body is chosen, by the rule and in the words a statement naming
    the table is refused with -- no statement is written for the engine."""
    state = _build(monkeypatch, view=_view(_OVER_ANOTHER_DOMAIN), drafts=[])
    with pytest.raises(PermissionError) as refused:
        await _read_the_view(state)
    assert str(refused.value) == "[V000] Table 'finance.ledger' not accessible for role 'r'"


async def test_a_view_over_tables_its_reader_reaches_is_answered_under_its_rules(monkeypatch):
    from provisa.compiler.rls import RLSContext

    state = _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[])
    state.rls_contexts["r"] = RLSContext(rules={1: "id > 100"}, domain_rules={}, action_rules={})
    routed = (await _read_the_view(state)).replace('"', "")
    assert "FROM pg.public.orders" in routed and "orders.id > 100" in routed


async def test_a_reader_who_reaches_the_other_domain_reads_the_same_view(monkeypatch):
    state = _build(
        monkeypatch,
        view=_view(_OVER_ANOTHER_DOMAIN),
        drafts=[],
        role={
            "id": "r",
            "description": "Test role.",
            "capabilities": ["usage"],
            "domain_access": ["sales", "finance"],
        },
    )
    assert '"pg"."public"."ledger"' in await _read_the_view(state)


def test_a_table_the_reader_sees_part_of_is_governed_not_refused():
    """Only a table of which the reader may read nothing is refused; hidden columns of a table
    it does read are removed by governance, as before."""
    from provisa.compiler.stage2 import GovernanceContext
    from provisa.mv.view_read import require_inputs_reached

    gov = GovernanceContext(
        table_map={"sales.orders": 1, "orders": 1, "finance.ledger": 2, "ops.queries": 4},
        visible_columns={1: frozenset({"id"}), 2: frozenset(), 4: frozenset()},
        public_tables=frozenset({4}),
    )
    gov.role_id = "r"
    require_inputs_reached('SELECT id FROM "sales"."orders"', gov)  # sees some of it
    require_inputs_reached("SELECT 1 FROM unknown_relation", gov)  # not this rule's to judge
    require_inputs_reached('SELECT id FROM "ops"."queries"', gov)  # publishes to the role
    for sql in (
        'SELECT o.id FROM "sales"."orders" o JOIN finance.ledger l ON l.id = o.id',
        'SELECT id FROM "sales"."orders" WHERE id IN (SELECT id FROM "finance"."ledger")',
    ):
        with pytest.raises(PermissionError, match="'finance.ledger' not accessible for role 'r'"):
            require_inputs_reached(sql, gov)
