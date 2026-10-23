# Copyright (c) 2026 Kenneth Stott
# Canary: 1b67cbc9-9313-4054-91e5-6e11fc58dc71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement is landed and run by the engine unless it passes every gate, in order (REQ-863):
one source, a dialect that runs every construct in it, a direct access path, colocated sources
under one login, no forced-engine condition, and a source trusted to answer it. The first gate it
fails names why it is landed. A direct route hint tries the gates and is ignored -- said -- when
one fails; an engine hint skips them (REQ-030)."""

# Requirements: REQ-863, REQ-030, REQ-2000, REQ-2100

import pytest

from provisa.federation.colocation import colocation_key
from provisa.transpiler.router import GATES, Route, decide_route

_TYPES = {
    "pg": "postgresql",
    "pg2": "postgresql",
    "lake": "iceberg",
    "api": "openapi",
    "py": "python",
    "vec": "qdrant",
    "odd": "nosuchdb",
}
_DIALECTS = {"pg": "postgres", "pg2": "postgres", "api": "", "py": "duckdb", "vec": "qdrant"}


def _route(sources, statement="SELECT a FROM t", **kw):
    return decide_route(
        set(sources),
        _TYPES,
        _DIALECTS,
        statement=statement,
        operator_floor=kw.pop("operator_floor", {}),
        **kw,
    )


def test_the_gates_are_these_in_this_order():
    # A new gate is an explicit addition here.
    assert [g.name for g in GATES] == [
        "one_source",
        "dialect_supports",
        "access_path",
        "colocated_login",
        "not_forced",
        "trusted_to_answer",
    ]


def test_a_statement_that_passes_every_gate_is_read_on_its_source():
    decision = _route({"pg"})
    assert (decision.route, decision.source_id) == (Route.DIRECT, "pg")


def test_one_source():
    assert _route({"pg", "pg2"}).reason == "landed: one_source: reads 2 sources"


def test_dialect_supports():
    window = "SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t"
    assert _route({"pg"}, window).route == Route.DIRECT
    landed = decide_route(
        {"odd"}, {"odd": "clickhouse"}, {"odd": "clickhouse"},
        statement="SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)",
        operator_floor={},
    )  # fmt: skip
    assert landed.reason == "landed: dialect_supports: source 'odd' cannot run EXISTS"


def test_access_path():
    assert (
        _route({"odd"}).reason == "landed: access_path: no direct driver for source type (nosuchdb)"
    )
    assert _route({"lake"}).reason == "landed: access_path: virtual source (iceberg)"


def _keyed(user_b: str) -> dict:
    return {
        "pg": colocation_key("h", 5432, "db", "app", "pw"),
        "pg2": colocation_key("h", 5432, "db", user_b, "pw"),
    }


def test_colocated_login():
    assert _route({"pg", "pg2"}, source_dsns=_keyed("app")).route == Route.DIRECT
    other = _route({"pg", "pg2"}, source_dsns=_keyed("reporting"))
    assert other.reason == (
        "landed: colocated_login: its sources share a database under different logins"
    )


@pytest.mark.parametrize(
    "kw, why",
    [
        ({"operator_floor": {"pg": "replicate"}}, "operator floor: replicate on pg"),
        ({"reads_fakes": True}, "reads faked columns"),
        ({"forced": ("every source it reads was inlined as rows",)}, "every source it reads"),
        ({"forced": ("read from materialized view 'mv1'",)}, "read from materialized view"),
    ],
)
def test_not_forced(kw, why):
    decision = _route({"pg"}, **kw)
    assert decision.route == Route.ENGINE
    assert decision.reason.startswith(f"landed: not_forced: {why}")


def test_trusted_to_answer():
    # A connector that does not answer directly lands, whatever it could push.
    assert _route({"api"}).reason == (
        "landed: trusted_to_answer: the openapi connector does not answer directly: its rows "
        "are landed"
    )
    assert _route({"py"}).reason.startswith(
        "landed: trusted_to_answer: the python connector does not answer directly"
    )


def test_a_connector_that_answers_directly_answers_only_what_it_satisfies():
    satisfied = _route({"vec"}, unsatisfied=())
    assert (satisfied.route, satisfied.source_id) == (Route.DIRECT, "vec")
    left = _route(
        {"vec"}, unsatisfied=("a read of the collection that is no similarity top-k search",)
    )
    assert left.reason == (
        "landed: trusted_to_answer: unsatisfied: a read of the collection that is no "
        "similarity top-k search"
    )


def test_a_direct_hint_is_honoured_where_every_gate_passes():
    decision = _route({"pg"}, steward_hint="direct")
    assert (decision.route, decision.reason) == (Route.DIRECT, "steward override: direct")


@pytest.mark.parametrize(
    "sources, kw, why",
    [
        ({"pg", "pg2"}, {}, "landed: one_source:"),
        (
            {"pg"},
            {"operator_floor": {"pg": "load_protected"}},
            "landed: not_forced: operator floor",
        ),
        ({"pg"}, {"reads_fakes": True}, "landed: not_forced: reads faked columns"),
        ({"api"}, {}, "landed: trusted_to_answer:"),
    ],
)
def test_a_direct_hint_that_cannot_apply_is_ignored_and_said(sources, kw, why):
    decision = _route(sources, steward_hint="direct", **kw)  # raises nothing
    assert decision.route == Route.ENGINE
    assert decision.reason.startswith(f"route hint ignored: {why}")


def test_an_engine_hint_skips_the_gates():
    decision = _route({"pg"}, steward_hint="engine")
    assert (decision.route, decision.reason) == (Route.ENGINE, "steward override: federated")


def test_a_statement_whose_every_source_was_inlined_is_the_engines():
    # Never read directly on a source it does not read: the org's default source (A4).
    from types import SimpleNamespace

    from provisa.pgwire._pipeline import _forced_engine

    state = SimpleNamespace(
        source_types={"pg": "postgresql"},
        source_pools=SimpleNamespace(has=lambda _sid: True),
        tables=[],
    )
    forced = _forced_engine({"pg"}, True, None, {}, None, state, ())
    assert forced == ("every source it reads was inlined as rows",)
    assert _forced_engine({"pg"}, False, None, {}, None, state, ()) == ()


# -- the connector contract (federation.connector_contract) ---------------------------------------


class _Pools:
    def __init__(self, driver):
        self.driver = driver

    def get(self, _sid):
        return self.driver


class _Qdrant:
    def __init__(self, kind, why=None):
        self.kind, self.why = kind, why

    async def plan_kind(self, _sql, _table):
        return self.kind, self.why


def _qdrant_state(driver):
    from types import SimpleNamespace

    return SimpleNamespace(
        tables=[{"id": 1, "table_name": "docs", "source_id": "vec"}],
        source_pools=_Pools(driver),
    )


@pytest.mark.parametrize(
    "kind, why, unsatisfied",
    [
        ("query", None, ()),
        ("scroll", None, ("a read of the collection that is no similarity top-k search",)),
        ("retrieve", None, ("a read of the collection that is no similarity top-k search",)),
        (None, "an aggregate has no Qdrant form", ("an aggregate has no Qdrant form",)),
    ],
)
async def test_the_qdrant_connector_satisfies_only_its_similarity_top_k(
    monkeypatch, kind, why, unsatisfied
):
    from provisa.federation import connector_contract

    monkeypatch.setattr(
        "provisa.compiler.sql_rewrite.rewrite_semantic_to_physical", lambda sql, _ctx: sql
    )
    plan = await connector_contract.connector_for("qdrant").plan(
        _qdrant_state(_Qdrant(kind, why)), "vec", "SELECT 1", None, (1,)
    )
    assert plan.unsatisfied == unsatisfied


def test_every_connector_declares_whether_it_answers_directly():
    from provisa.federation.connector_contract import connector_for

    assert connector_for("qdrant").answers_directly
    assert connector_for("postgresql").answers_directly  # it runs the SQL itself
    for lands in ("openapi", "graphql_api", "grpc_remote", "sparql", "node", "python"):
        assert not connector_for(lands).answers_directly
