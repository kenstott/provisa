# Copyright (c) 2026 Kenneth Stott
# Canary: 4e1b7c92-5d3a-4f08-9a6e-2c8f0b1d7e53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view whose definition calls a code command -- a code resolver's bridge -- is always landed
and read only from its stored table, secured as that table, as a view applying a column transform
is (REQ-2131). The command runs when the view is built, never when it is read.

So, for every reader, through the one governed pipeline:

* the V-rules judge the view's definition, where the registered code command is admitted as a
  governed relation (V006) and its table arguments are judged as the view's inputs -- while an
  unregistered table function there, and a call in a statement's own FROM the reader may not
  make, are refused as before;
* the statement that reaches routing reads the view, never the call;
* each reader's own row and column rules hold at each hop: the bridge read brings back no row of
  either input table the reader's rules exclude."""

# Requirements: REQ-2249, REQ-2131

from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext, read_rule
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_types import JoinMeta, JunctionMeta
from provisa.federation.replica_address import ReplicaRoutes
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from tests.role_state import RoleState

pytestmark = pytest.mark.unit

_STORED = '"mat_store"."org_default_mv_cache"."mv_order_clips"'
_RESOLVER = "SELECT * FROM match_region('orders', 'clips') AS m"
_HOST_FILE = "SELECT * FROM read_csv('/etc/passwd') AS m"


def _meta(table_id: int, name: str, domain: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.title(),
        source_id="pg" if table_id != 3 else "__derived__",
        catalog_name="pg",
        schema_name="sales",
        table_name=name,
        domain_id=domain,
    )


def _table(table_id: int, name: str, domain: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "schema_name": "sales",
        "table_name": name,
        "domain_id": domain,
        "columns": [
            {"column_name": c, "data_type": "integer", "visible_to": ["analyst"]} for c in columns
        ],
    }


_COLUMNS = {
    "orders": ["id", "region"],
    "clips": ["id", "region", "secret"],
    "order_clips": ["order_id", "clip_id", "score"],
}

#: The command a resolver is, as the commands are loaded (state.tracked_functions).
_MATCH_REGION = {
    "name": "match_region",
    "impl_kind": "code_command",
    "kind": "query",
    "domain_id": "sales",
    "visible_to": [],  # assigned to no role: no reader may call it
    "language_bindings": {},
    "arguments": [
        {"name": "orders", "arg_kind": "table_ref"},
        {"name": "clips", "arg_kind": "table_ref"},
    ],
    "output_columns": [{"name": c} for c in _COLUMNS["order_clips"]],
}


def _state(
    *,
    definition: str = _RESOLVER,
    clips_domain: str = "sales",
    domain_access: list[str] | None = None,
    rules: dict[int, str] | None = None,
    hidden: dict[int, set[str]] | None = None,
    guarded: bool = False,
) -> SimpleNamespace:
    """A reader, ``analyst``, over orders (1), clips (2) and the bridge order_clips (3): a view
    over the resolver's call, landed (its stored table) and read from it."""
    ctx = CompilationContext()
    domains = {"orders": "sales", "clips": clips_domain, "order_clips": "sales"}
    ctx.tables = {
        name: _meta(i, name, domains[name])
        for i, name in enumerate(("orders", "clips", "order_clips"), start=1)
    }
    if guarded:
        # The set's relationships: orders to clips through the bridge, and its two hops.
        orders, clips, bridge = (ctx.tables[n] for n in ("orders", "clips", "order_clips"))
        # The registry holds every junction with its edge's label source (REQ-1586).
        via = JunctionMeta(
            table=bridge,
            source_columns=("order_id",),
            target_columns=("clip_id",),
            label_source="table",
        )
        ctx.joins = {
            ("Orders", "ORDER_CLIPS"): JoinMeta(
                "id", "id", "integer", "integer", clips, "many-to-many", via=via
            ),
            ("Orders", "order_clips"): JoinMeta(
                "id", "order_id", "integer", "integer", bridge, "one-to-many"
            ),
            ("Order_Clips", "clips"): JoinMeta(
                "clip_id", "id", "integer", "integer", clips, "many-to-one"
            ),
        }
    tables = [
        _table(i, name, domains[name], _COLUMNS[name])
        for i, name in enumerate(("orders", "clips", "order_clips"), start=1)
    ]
    for table in tables:
        for column in table["columns"]:
            if column["column_name"] in (hidden or {}).get(table["id"], set()):
                column["visible_to"] = []
    registry = MVRegistry()
    registry.register(
        MVDefinition(
            id="view-order_clips",
            source_tables=[],
            target_catalog="mat_store",
            target_schema="org_default_mv_cache",
            target_table="mv_order_clips",
            sql=definition,
            semantic_sql=definition,
            view_table="order_clips",
            status=MVStatus.FRESH,
        )
    )
    return RoleState(
        admin_db=None,
        contexts={"analyst": ctx},
        rls_contexts={
            "analyst": RLSContext(rules={t: read_rule(r) for t, r in (rules or {}).items()})
        },
        roles={
            "analyst": {
                "id": "analyst",
                # The hops' relationships are judged only where a test registers them.
                "capabilities": [] if guarded else ["ignore_relationships"],
                "domain_access": domain_access or ["*"],
            }
        },
        masking_rules={},
        tables=copy.deepcopy(tables),
        source_types={"pg": "postgresql"},
        source_catalogs={},
        # The set's two plain relationships, as registered (REQ-2249): orders to the bridge, the
        # bridge to clips.
        relationships=[
            {
                "source_table_id": 1,
                "target_table_id": 3,
                "source_column": "id",
                "target_column": "order_id",
                "via_table_id": None,
                "target_function_name": None,
            },
            {
                "source_table_id": 3,
                "target_table_id": 2,
                "source_column": "clip_id",
                "target_column": "id",
                "via_table_id": None,
                "target_function_name": None,
            },
        ],
        metrics={},
        federation_engine=None,
        view_sql_map={"order_clips": f"SELECT * FROM {_STORED}"},
        stored_view_reads={"order_clips"},
        mv_registry=registry,
        role_build_inputs={
            "withdrawn_tables": [],
            # Every role's row rules, as stored (transform_view_rules.roles_rules).
            "rls_rules": [
                {"role_id": "analyst", "table_id": t, "filter_expr": read_rule(r)}
                for t, r in (rules or {}).items()
            ],
            "tables": copy.deepcopy(tables),
        },
        tracked_functions={"match_region": copy.deepcopy(_MATCH_REGION)},
        security_high=False,
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
        model_stamp=1,
    )


class _Routed(Exception):
    """Raised by the routing stand-in: the statement cleared governance and reached routing."""

    def __init__(self, governed_sql: str) -> None:
        super().__init__(governed_sql)
        self.governed_sql = governed_sql


async def _govern(monkeypatch, sql: str, state: SimpleNamespace) -> str:
    """The governed statement that reaches routing; else what governance raised."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", state, raising=False)

    async def _routing(_exec_sql, governed_sql, *_args, **_kwargs):  # noqa: ARG001
        raise _Routed(governed_sql)

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_routing)):
        with pytest.raises(_Routed) as routed:
            await _pipeline._govern_and_route(sql, "analyst")
    return routed.value.governed_sql


async def _refused(monkeypatch, sql: str, state: SimpleNamespace) -> str:
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", state, raising=False)
    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock()):
        with pytest.raises(PermissionError) as refused:
            await _pipeline._govern_and_route(sql, "analyst")
    return str(refused.value)


_READ_BRIDGE = "SELECT m.order_id, m.clip_id, m.score FROM order_clips m"
_FOLLOW = (
    "SELECT o.id, m.score, c.id AS clip, c.region FROM orders o "
    "JOIN order_clips m ON m.order_id = o.id JOIN clips c ON c.id = m.clip_id"
)


# --- V006: the call, admitted in a view's definition only ------------------------------------


@pytest.mark.asyncio
async def test_a_view_over_a_registered_code_command_is_admitted(monkeypatch):
    governed = await _govern(monkeypatch, _READ_BRIDGE, _state())
    assert "order_clips" in governed


@pytest.mark.asyncio
async def test_an_unregistered_table_function_in_a_view_definition_is_refused(monkeypatch):
    refused = await _refused(monkeypatch, _READ_BRIDGE, _state(definition=_HOST_FILE))
    assert "V006" in refused and "READ_CSV" in refused.upper(), refused


@pytest.mark.asyncio
async def test_a_code_command_in_a_statements_own_from_is_refused_as_before(monkeypatch):
    """The reader may not call the command: in its own statement the call is no relation."""
    refused = await _refused(monkeypatch, _RESOLVER, _state())
    assert "V006" in refused and "MATCH_REGION" in refused.upper(), refused


@pytest.mark.asyncio
async def test_a_view_command_call_outside_the_view_body_is_not_admitted(monkeypatch):
    """Admitted where a view's definition is expanded, never beside it in the same statement."""
    sql = f"SELECT b.order_id FROM order_clips b JOIN ({_RESOLVER}) x ON x.order_id = b.order_id"
    refused = await _refused(monkeypatch, sql, _state())
    assert "V006" in refused and "MATCH_REGION" in refused.upper(), refused


@pytest.mark.asyncio
async def test_the_calls_table_arguments_are_judged_as_the_views_inputs(monkeypatch):
    """clips is outside the reader's domains: read through the bridge's definition, it is refused
    as a statement naming it is."""
    refused = await _refused(
        monkeypatch, _READ_BRIDGE, _state(clips_domain="media", domain_access=["sales"])
    )
    assert "clips" in refused and ("V001" in refused or "V000" in refused), refused


@pytest.mark.asyncio
async def test_a_table_argument_naming_no_registered_table_is_refused(monkeypatch):
    refused = await _refused(
        monkeypatch,
        _READ_BRIDGE,
        _state(definition="SELECT * FROM match_region('orders', 'nowhere') AS m"),
    )
    assert "V006" in refused and "nowhere" in refused, refused


@pytest.mark.asyncio
async def test_the_edge_through_the_bridge_is_held_to_the_sets_relationships(monkeypatch):
    """The relationship guard traces the bridge's columns through its definition: the call's rows
    carry the columns the command declares, so the joins on them are the set's relationships."""
    governed = await _govern(monkeypatch, _FOLLOW, _state(guarded=True))
    assert "order_clips" in governed


@pytest.mark.asyncio
async def test_a_join_through_the_bridge_on_no_relationship_is_still_refused(monkeypatch):
    sql = _FOLLOW.replace("c.id = m.clip_id", "c.id = m.order_id")
    refused = await _refused(monkeypatch, sql, _state(guarded=True))
    assert "V002" in refused, refused


# --- read from its stored table -------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_bridge_reaches_routing_as_its_table_never_the_call(monkeypatch):
    governed = await _govern(monkeypatch, _READ_BRIDGE, _state())
    assert "match_region" not in governed.lower()
    assert "order_clips" in governed


def test_a_view_calling_a_code_command_is_landed_and_read_from_its_stored_table():
    from provisa.mv.view_read import lands_stored_read

    assert lands_stored_read(_RESOLVER, set(), {"match_region"})
    upper = _RESOLVER.replace("match_region", "MATCH_REGION")
    assert lands_stored_read(upper, set(), {"match_region"})
    assert not lands_stored_read("SELECT id FROM orders", set(), {"match_region"})
    assert not lands_stored_read(_HOST_FILE, set(), {"match_region"})


def test_a_view_applying_a_column_transform_is_still_landed():
    from provisa.mv.view_read import lands_stored_read

    assert lands_stored_read("SELECT id, clean(raw) FROM s.t", {"clean"}, set())


# --- each reader's rules at each hop --------------------------------------------------------


@pytest.mark.asyncio
async def test_a_row_rule_on_the_target_holds_at_its_hop(monkeypatch):
    governed = await _govern(monkeypatch, _FOLLOW, _state(rules={2: "id = 1"}))
    assert "id = 1" in governed.replace('"', ""), governed


@pytest.mark.asyncio
async def test_a_row_rule_on_the_source_holds_at_its_hop(monkeypatch):
    governed = await _govern(monkeypatch, _FOLLOW, _state(rules={1: "region = 'emea'"}))
    assert "region = 'emea'" in governed.replace('"', ""), governed


@pytest.mark.asyncio
async def test_a_direct_read_of_the_bridge_is_filtered_by_the_rule_on_each_input(monkeypatch):
    """REQ-2131 (amended b0823d303a): the bridge's stored rows carry no row of either input, so
    the reader's rule on each is held through that input's relationship to the bridge."""
    governed = await _govern(monkeypatch, _READ_BRIDGE, _state(rules={2: "id = 1"}))
    flat = governed.replace('"', "")
    assert "EXISTS(SELECT 1 FROM clips AS __rule_2 WHERE __rule_2.id = m.clip_id" in flat, governed
    assert "__rule_2.id = 1" in flat, governed


@pytest.mark.asyncio
async def test_a_column_the_reader_lacks_on_the_target_is_not_read_through_the_bridge(
    monkeypatch,
):
    sql = _FOLLOW.replace("c.region", "c.secret")
    refused = await _refused(monkeypatch, sql, _state(hidden={2: {"secret"}}))
    assert "V003" in refused and "secret" in refused, refused
