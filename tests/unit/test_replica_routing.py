# Copyright (c) 2026 Kenneth Stott
# Canary: 6f95a12d-a8a0-4bdd-9f61-4384d6f00439
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``replicate`` setting and the one decision made from it (REQ-826).

``replicate`` is an integer per table, inherited from its source: not set = Default (the global
threshold), -1 = Never, N > 0 = Hot-N, 0 = Always. Only Always is a guarantee; Never and the Hot
values are best effort. These cases pin, for every value:

* which tables an engine serves from a replica (``replica_routing.reads_replica``) — by setting,
  and where the engine cannot read the source in place, whatever the setting;
* which setting a refusal names (``table_floor``);
* that the floor is judged PER STATEMENT (``registry_view.operator_floor``): a statement that
  reads no replica-served table keeps its direct route even when its source has replicated tables.
"""

# Requirements: REQ-826, REQ-030, REQ-1141, REQ-1912

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.models import Column, Source, SourceType, Table
from provisa.core.operator_floor import floor_setting
from provisa.core.replicate import (
    ALWAYS,
    NEVER,
    check_replicate,
    contradiction,
    floor_of,
    may_replicate,
    resolved_load_protected,
    resolved_replicate,
)
from provisa.federation.engine import build_engine
from provisa.federation.registry_view import operator_floor
from provisa.federation.replica_address import ReplicaRoutes
from provisa.federation.replica_routing import (
    has_live_attach,
    live_while_building,
    reads_replica,
    table_floor,
)
from provisa.transpiler.router import OperatorFloorViolation, Route, decide_route

pytestmark = pytest.mark.unit

_ENGINE = build_engine("trino")


def _attachable(**settings) -> Source:
    """A source Trino reads in place."""
    return Source(
        id="pg",
        type=SourceType.postgresql,
        host="h",
        port=5432,
        database="d",
        username="u",
        cache_ttl=60,
        **settings,
    )


def _replicated_only(**settings) -> Source:
    """A source Trino reaches only by replicating it."""
    return Source(id="api", type=SourceType.openapi, path="https://x.test/spec.json", **settings)


def _table(source_id: str = "pg", **settings) -> SimpleNamespace:
    base = {"replicate": None, "load_protected": None}
    base.update(settings)
    return SimpleNamespace(source_id=source_id, **base)


# -- the value -------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", [None, NEVER, ALWAYS, 1, 50, 750, 10_000])
def test_every_value_of_the_setting_is_accepted(value):
    assert check_replicate(value) == value
    Source(id="s", type=SourceType.postgresql, replicate=value)


@pytest.mark.parametrize("value", [-2, -100, True, False, 1.5, "0"])
def test_a_value_the_setting_does_not_have_is_refused(value):
    with pytest.raises(ValueError, match="replicate must be"):
        check_replicate(value)


def test_a_table_inherits_its_sources_value_and_its_own_wins():
    assert resolved_replicate(_attachable(replicate=500), _table()) == 500
    assert resolved_replicate(_attachable(replicate=500), _table(replicate=NEVER)) == NEVER
    assert resolved_replicate(_attachable(), _table()) is None
    assert resolved_load_protected(_attachable(load_protected=True), _table()) is True
    assert resolved_load_protected(_attachable(), _table(load_protected=True)) is True
    assert resolved_load_protected(_attachable(), _table()) is False


def test_load_protected_with_never_is_refused_everywhere_it_can_be_written():
    assert contradiction(NEVER, True) is not None
    for value in (None, ALWAYS, 500):
        assert contradiction(value, True) is None
    assert contradiction(NEVER, False) is None
    # on one object: the model refuses it
    with pytest.raises(ValueError, match="contradictory"):
        Source(
            id="s", type=SourceType.postgresql, replicate=NEVER, load_protected=True, cache_ttl=60
        )
    with pytest.raises(ValueError, match="contradictory"):
        Table(
            source_id="s",
            domain_id="d",
            table="t",
            schema="public",
            columns=[Column(name="id", data_type="integer", visible_to=["*"])],
            replicate=NEVER,
            load_protected=True,
        )


def test_a_table_inheriting_load_protection_may_not_say_never():
    """Resolved across source and table: judged at config load (and at save)."""
    from provisa.core.config_loader import _validate_replicate

    table = Table(
        source_id="pg",
        domain_id="d",
        table="orders",
        schema="public",
        columns=[Column(name="id", data_type="integer", visible_to=["*"])],
        replicate=NEVER,
    )
    config = SimpleNamespace(sources=[_attachable(load_protected=True)], tables=[table])
    with pytest.raises(ValueError, match=r"orders.*contradictory"):
        _validate_replicate(config)
    config.sources = [_attachable()]
    _validate_replicate(config)


# -- the decision table ----------------------------------------------------------------------------

_VALUES = [None, NEVER, 500, ALWAYS]


def _expected_on_an_attachable_source(replicate, load_protected: bool, promoted: bool) -> bool:
    if load_protected or replicate == ALWAYS:
        return True
    if replicate == NEVER:
        return False
    return promoted  # Default or Hot-N: only once it passed its threshold


# Every value x load protection x promoted. load_protected with never is left out: it is
# contradictory and refused before it reaches the decision (asserted above).
_ATTACHABLE_CASES = [
    (replicate, load_protected, promoted)
    for replicate in _VALUES
    for load_protected in (False, True)
    for promoted in (False, True)
    if contradiction(replicate, load_protected) is None
]


@pytest.mark.parametrize(("replicate", "load_protected", "promoted"), _ATTACHABLE_CASES)
def test_a_table_of_a_source_the_engine_reads_in_place(replicate, load_protected, promoted):
    table = _table(replicate=replicate, load_protected=load_protected or None)
    expected = _expected_on_an_attachable_source(replicate, load_protected, promoted)
    assert reads_replica(_attachable(), table, _ENGINE, promoted=promoted) is expected
    assert (table_floor(_attachable(), table, promoted=promoted) is not None) is expected


@pytest.mark.parametrize("replicate", _VALUES)
@pytest.mark.parametrize("promoted", [False, True])
def test_a_table_of_a_source_the_engine_cannot_read_in_place_is_always_replica_served(
    replicate, promoted
):
    """The replica is the only way to reach it. Never (-1) is best effort and cannot be met
    here: the table is still served from its replica."""
    source = _replicated_only()
    table = _table("api", replicate=replicate)
    assert reads_replica(source, table, _ENGINE, promoted=promoted) is True
    # ...but only an operator SETTING is a floor a route=direct hint is refused by.
    expected_floor = replicate == ALWAYS or (promoted and replicate != NEVER)
    assert (table_floor(source, table, promoted=promoted) is not None) is expected_floor


def test_a_source_set_to_always_floors_every_table_whatever_the_table_says():
    """A source that is always replicated has no live attach, so a table of it cannot opt out."""
    source = _attachable(replicate=ALWAYS)
    assert floor_setting(source) == "replicate"
    assert has_live_attach(source, _ENGINE) is False
    for own in (None, NEVER, 500):
        table = _table(replicate=own)
        assert reads_replica(source, table, _ENGINE, promoted=False) is True
        assert table_floor(source, table, promoted=False) == "replicate"


def test_a_source_under_a_threshold_keeps_its_live_attach():
    for value in (None, NEVER, 500):
        source = _attachable(replicate=value)
        assert floor_setting(source) is None
        assert has_live_attach(source, _ENGINE) is True


def test_the_refusal_names_the_setting_that_floors_the_table():
    assert (
        table_floor(_attachable(load_protected=True), _table(), promoted=False) == "load_protected"
    )
    assert table_floor(_attachable(), _table(replicate=ALWAYS), promoted=False) == "replicate"
    assert table_floor(_attachable(), _table(replicate=500), promoted=True) == "replicate"
    assert table_floor(_attachable(), _table(replicate=500), promoted=False) is None
    assert floor_of(None, False, promoted=False) is None


def test_only_a_busy_table_may_be_read_live_while_its_replica_is_built():
    live = _attachable()
    assert live_while_building(live, _table(replicate=500), _ENGINE) is True
    assert live_while_building(live, _table(), _ENGINE) is True  # Default
    assert live_while_building(live, _table(replicate=ALWAYS), _ENGINE) is False
    assert live_while_building(live, _table(load_protected=True), _ENGINE) is False
    assert live_while_building(_replicated_only(), _table("api", replicate=500), _ENGINE) is False


def test_which_values_say_a_table_is_to_be_replicated():
    """The tables the replication-clock rule judges at config load and at save (REQ-1907)."""
    assert may_replicate(ALWAYS, False) and may_replicate(500, False) and may_replicate(None, True)
    assert not may_replicate(None, False) and not may_replicate(NEVER, False)


# -- per statement ---------------------------------------------------------------------------------

_ALWAYS_TABLE, _DEFAULT_TABLE, _OTHER_SOURCE_TABLE = 1, 2, 3


def _published() -> SimpleNamespace:
    """A state as the schema build leaves it: source ``pg`` has one Always table (1) and one
    Default table (2); source ``crm`` has a Default table (3)."""
    return SimpleNamespace(
        replica_routes=ReplicaRoutes(floored={_ALWAYS_TABLE: ("pg", "replicate")})
    )


def _route(state: SimpleNamespace, table_ids, sources, hint=None):
    return decide_route(
        set(sources),
        {"pg": "postgresql", "crm": "postgresql"},
        {"pg": "postgres", "crm": "postgres"},
        steward_hint=hint,
        operator_floor=operator_floor(state, table_ids),
    )


def test_a_statement_that_reads_only_a_live_table_keeps_its_direct_route():
    """Its source has an Always table; the statement does not read it."""
    state = _published()
    assert operator_floor(state, [_DEFAULT_TABLE]) == {}
    decision = _route(state, [_DEFAULT_TABLE], {"pg"})
    assert decision.route == Route.DIRECT and decision.source_id == "pg"
    # and an explicit route=direct hint on it is allowed
    assert _route(state, [_DEFAULT_TABLE], {"pg"}, hint="direct").route == Route.DIRECT


def test_a_statement_that_reads_a_replica_served_table_routes_to_the_engine():
    state = _published()
    assert operator_floor(state, [_ALWAYS_TABLE, _DEFAULT_TABLE]) == {"pg": "replicate"}
    decision = _route(state, [_ALWAYS_TABLE, _DEFAULT_TABLE], {"pg"})
    assert decision.route == Route.ENGINE and "replicate on pg" in decision.reason


def test_a_direct_hint_on_a_replica_served_table_is_refused_naming_the_setting():
    state = _published()
    with pytest.raises(OperatorFloorViolation) as refused:
        _route(state, [_ALWAYS_TABLE], {"pg"}, hint="direct")
    assert "replicate" in str(refused.value) and "pg" in str(refused.value)


def test_another_sources_statement_is_not_floored_by_it():
    state = _published()
    assert operator_floor(state, [_OTHER_SOURCE_TABLE]) == {}
    assert _route(state, [_OTHER_SOURCE_TABLE], {"crm"}).route == Route.DIRECT


# -- promoted, not yet built (REQ-826) -------------------------------------------------------------


async def test_a_promoted_table_is_the_replicators_before_its_reads_go_to_the_replica(monkeypatch):
    """From its promotion the table is in the set the replicator builds, keeps and retires by
    (``replica_tables``) — so it is built and is not retired — while reads keep going to the
    source until its first build completes (the serving set routing reads)."""
    from provisa.federation import replica_routing

    reg = {
        "id": 1,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": "orders",
        "replicate": 500,
        "load_protected": None,
        "columns": [{"column_name": "id", "native_filter_type": None}],
    }
    key = ("pg", "public", "orders")
    registry = replica_routing._Registry(
        [reg], {"pg": _attachable()}, serving=frozenset(), promoted=frozenset({key}), synthetic={}
    )

    async def _registry(_state):
        return registry

    monkeypatch.setattr(replica_routing, "_registry", _registry)
    assert [r["table_name"] for _s, r in await replica_routing.replica_tables(_ENGINE, None)] == [
        "orders"
    ]
    # reads: not yet
    assert replica_routing._served_from_replica(_ENGINE, registry, registry.serving) == []
    assert replica_routing._floored(registry) == {}
    # once built in this engine's store, both
    built = registry._replace(serving=frozenset({key}))
    assert [
        r["table_name"]
        for _s, r in replica_routing._served_from_replica(_ENGINE, built, built.serving)
    ] == ["orders"]
    assert replica_routing._floored(built) == {1: ("pg", "replicate")}


async def test_a_deployment_with_no_store_and_nothing_replicated_reads_its_routes(
    tmp_path, monkeypatch
):
    """Trino is not its own store; with no materialization store configured and none to fall
    back on, ``store_identity`` raises. A deployment that replicates nothing must still build
    its routes and floored set, as it did before Hot replication read the replica state."""
    import asyncio as _asyncio

    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.federation import replica_routing
    from provisa.federation.engine import MaterializeStoreUnconfigured

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    await init_schema(db, "", org_id="default")
    monkeypatch.delenv("TENANT_DATABASE_URL", raising=False)
    monkeypatch.delenv("PROVISA_MATERIALIZE_URL", raising=False)
    monkeypatch.setattr("provisa.federation.engine.configured_materialize_url", lambda: None)
    trino = build_engine("trino")
    with pytest.raises(MaterializeStoreUnconfigured):
        trino.materialize_store()

    reg = {
        "id": 1,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": "orders",
        "replicate": None,
        "load_protected": None,
        "columns": [{"column_name": "id", "native_filter_type": None}],
    }

    async def _tables(_conn):
        return [reg]

    async def _sources(_state, _conn=None):
        return [_attachable()]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _tables)
    # REQ-1939: no synthetic dataset is generated in this model.
    monkeypatch.setattr("provisa.synthetic.datasets.generated_tables", no_synthetic_tables)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    state = SimpleNamespace(
        model_db=db,
        tenant_db=db,
        config=SimpleNamespace(),
        federation_engine=SimpleNamespace(engine=trino),
    )
    try:
        assert await replica_routing.replica_tables(trino, state) == []
        assert await replica_routing.floored_tables(state) == {}
    finally:
        engine.dispose()
    del _asyncio


async def no_synthetic_tables(_conn):
    return {}
