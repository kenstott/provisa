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


def test_an_api_source_is_not_judged_by_a_dialect():
    # An API source's access path is its API caller: a construct no SQL dialect is asked to run
    # does not land it at dialect_supports.
    grouped = "SELECT region, count(*) FROM t GROUP BY region"
    reason = _route({"api"}, grouped).reason
    assert reason.startswith("landed: trusted_to_answer: incomplete dataset from "), reason


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
    # A source with a proxy answers only a complete dataset; v1 plans none.
    decision = _route({"api"}, incomplete="an API source declares no capability")
    assert decision.reason == (
        "landed: trusted_to_answer: incomplete dataset from api: an API source declares no "
        "capability"
    )
    # Not planned is not complete.
    assert _route({"vec"}).reason == "landed: trusted_to_answer: incomplete dataset from vec"
    assert _route({"vec"}, complete_dataset=True).route == Route.DIRECT


async def test_qdrant_plans_a_complete_dataset_for_what_its_api_states_exactly(monkeypatch):
    from types import SimpleNamespace

    from provisa.federation.connector_contract import Read, contract_for

    monkeypatch.setattr(
        "provisa.compiler.sql_rewrite.rewrite_semantic_to_physical", lambda sql, _ctx: sql
    )

    class _Driver:
        def __init__(self, why):
            self.why = why

        async def reason(self, _sql, _table):
            return self.why

    def _state(why):
        return SimpleNamespace(source_pools=SimpleNamespace(get=lambda _sid: _Driver(why)))

    read = Read(table={"table_name": "docs"}, name="docs", statement="SELECT 1", source_id="vec")
    exact = await contract_for("qdrant").plan(_state(None), read)
    assert exact.complete_dataset
    joined = await contract_for("qdrant").plan(_state("it joins another table"), read)
    assert (joined.complete_dataset, joined.incomplete_because) == (False, "it joins another table")


async def test_v1_every_other_connector_declares_no_capability():
    from provisa.federation.connector_contract import Read, contract_for

    read = Read(table={"table_name": "t", "source_id": "s"}, name="t", statement="SELECT 1")
    for source_type in ("openapi", "graphql_api", "grpc_api", "grpc_remote", "sparql"):
        plan = await contract_for(source_type).plan(None, read)
        assert plan.complete_dataset is False
        assert "declares no capability" in (plan.incomplete_because or "")
    assert contract_for("postgresql") is None  # a SQL database is read through its driver
