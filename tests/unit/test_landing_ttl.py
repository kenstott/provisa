# Copyright (c) 2026 Kenneth Stott
# Canary: 981bbcf8-ba69-4d31-ba50-9d191e45c8e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A per-statement table's TTL, and a TTL of 0 read live (REQ-2003).

0 means 0: the TTL is the first one set, so a table's 0 never falls through to its source's or the
default. Above 0 the rows are landed and reused for the TTL; at 0 a small read is inlined with no
write, and a large one lands in a scratch table dropped when the statement ends.
"""

# Requirements: REQ-2003

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api.data import materialization as mat
from provisa.cache.values_cte import InlineRows

_TABLE = {"table_name": "movie_hits", "source_id": "atlas"}


def _state(source_ttl=None, default=300, source="atlas"):
    engine = SimpleNamespace(land_source_table=AsyncMock())

    @contextmanager
    def _sync():
        yield object()

    engine.isolated_sync = _sync
    return SimpleNamespace(
        federation_engine=engine,
        source_cache={source: {"cache_ttl": source_ttl}} if source_ttl is not None else {},
        response_cache_default_ttl=default,
    )


# -- 0 means 0 ---------------------------------------------------------------------------------


def test_a_tables_ttl_of_0_is_0_whatever_its_source_says():
    assert mat.landing_ttl({**_TABLE, "cache_ttl": 0}, _state(source_ttl=300)) == 0


def test_a_sources_ttl_of_0_is_0_whatever_the_default_says():
    assert mat.landing_ttl(_TABLE, _state(source_ttl=0, default=300)) == 0


def test_an_unset_ttl_is_the_next_one_set():
    assert mat.landing_ttl(_TABLE, _state(source_ttl=120)) == 120
    assert mat.landing_ttl(_TABLE, _state()) == 300
    assert mat.landing_ttl({**_TABLE, "cache_ttl": 45}, _state(source_ttl=120)) == 45


def test_an_endpoints_ttl_is_the_last_one_asked():
    assert mat.landing_ttl(_TABLE, _state(default=None), endpoint_ttl=90) == 90
    assert mat.landing_ttl(_TABLE, _state(default=600), endpoint_ttl=90) == 600


# -- a parameterized table keeps only for a TTL of its own (REQ-1986) ----------------------------

_PARAMETER = {"column_name": "_nf_q", "native_filter_type": "query_param"}


def _parameterized(**table):
    return {**_TABLE, "columns": [{"column_name": "title"}, _PARAMETER], **table}


@pytest.mark.parametrize(
    "kind", ["path_param", "query_param", "graphql_arg", "grpc_input", "query_arg", "partition_key"]
)
def test_a_table_with_any_kind_of_parameter_is_parameterized(kind):
    table = {**_TABLE, "columns": [{"column_name": "_nf_x", "native_filter_type": kind}]}
    assert mat.is_parameterized(table)
    assert not mat.is_parameterized({**_TABLE, "columns": [{"column_name": "title"}]})


def test_a_parameterized_table_with_no_ttl_of_its_own_keeps_nothing():
    """Not its source's TTL, not the organisation's default, not the endpoint's: read live per
    statement and never reused."""
    state = _state(source_ttl=120, default=300)
    assert mat.landing_ttl(_parameterized(), state, endpoint_ttl=60) == 0


def test_a_parameterized_table_with_a_ttl_of_its_own_keeps_for_it():
    state = _state(source_ttl=120, default=300)
    assert mat.landing_ttl(_parameterized(cache_ttl=45), state, endpoint_ttl=60) == 45


# -- landing ----------------------------------------------------------------------------------


@pytest.fixture
def engine_cache(monkeypatch):
    from provisa.api_source import engine_cache as ec

    loc = SimpleNamespace(catalog="mat", schema="org_cache")
    calls: dict = {"known_live": False, "drops": [], "dropped": []}
    monkeypatch.setattr(ec, "cache_location", lambda *_a: loc)
    monkeypatch.setattr(ec, "resolved_cache_catalog", lambda _e: "mat")
    monkeypatch.setattr(ec, "org_cache_schema", lambda _s: "org_cache")
    monkeypatch.setattr(ec, "ensure_cache_schema", lambda *_a: None)
    monkeypatch.setattr(ec, "analyze_cache_table", AsyncMock())
    monkeypatch.setattr(ec, "table_known_live", lambda *_a: calls["known_live"])
    monkeypatch.setattr(
        ec, "schedule_drop", lambda _e, _l, table, ttl, _c: calls["drops"].append((table, ttl))
    )

    async def _drop(_engine, _loc, table, ttl, _config):
        calls["dropped"].append((table, ttl))

    monkeypatch.setattr(ec, "drop_cache_table", _drop)
    calls["loc"] = loc
    return calls


def _threshold(monkeypatch, rows: int) -> None:
    from provisa.executor import redirect

    monkeypatch.setattr(
        redirect.RedirectConfig,
        "from_env",
        classmethod(lambda _cls: SimpleNamespace(threshold=rows)),
    )


def _answer(n: int):
    rows = [{"title": f"t{i}"} for i in range(n)]
    read = SimpleNamespace(rows=rows, columns=[("title", "text")])
    called = {"n": 0}

    async def answer():
        called["n"] += 1
        return read

    return answer, called


async def _land(table, state, answer, rewrites, inline):
    await mat._land_per_statement("movie_hits", table, state, rewrites, inline, {"q": "up"}, answer)


@pytest.mark.asyncio
async def test_a_small_read_at_ttl_0_is_inlined_with_no_write(monkeypatch, engine_cache):
    _threshold(monkeypatch, 10)
    state = _state(source_ttl=300)
    answer, _ = _answer(3)
    rewrites: dict = {}
    inline: dict = {}
    await _land({**_TABLE, "cache_ttl": 0}, state, answer, rewrites, inline)
    entry = inline["movie_hits"]
    assert isinstance(entry, InlineRows) and entry.column_names == ["title"]
    assert len(entry.rows) == 3 and rewrites == {}
    state.federation_engine.land_source_table.assert_not_awaited()
    assert engine_cache["drops"] == []


@pytest.mark.asyncio
async def test_a_large_read_at_ttl_0_lands_in_a_scratch_table_of_the_statement(
    monkeypatch, engine_cache
):
    _threshold(monkeypatch, 2)
    state = _state()
    answer, _ = _answer(5)
    rewrites: dict = {}
    inline: dict = {}
    with mat.statement_scratch() as scratch:
        await _land({**_TABLE, "cache_ttl": 0}, state, answer, rewrites, inline)
    ((loc, table),) = scratch
    assert table.startswith("scratch__") and rewrites == {"movie_hits": (loc, table)}
    assert inline == {} and engine_cache["drops"] == []  # never kept for a TTL
    state.federation_engine.land_source_table.assert_awaited_once()
    await mat.drop_scratch(scratch, state)
    assert engine_cache["dropped"] == [(table, 0)]


@pytest.mark.asyncio
async def test_two_statements_never_share_a_scratch_table(monkeypatch, engine_cache):
    _threshold(monkeypatch, 0)
    state = _state()
    names = []
    for _ in range(2):
        answer, _ = _answer(1)
        with mat.statement_scratch() as scratch:
            await _land({**_TABLE, "cache_ttl": 0}, state, answer, {}, {})
        names.append(scratch[0][1])
    assert names[0] != names[1]


@pytest.mark.asyncio
async def test_a_scratch_table_is_landed_only_while_a_statement_is_planned(
    monkeypatch, engine_cache
):
    _threshold(monkeypatch, 0)
    answer, _ = _answer(1)
    with pytest.raises(RuntimeError, match="only while a statement is planned"):
        await _land({**_TABLE, "cache_ttl": 0}, _state(), answer, {}, {})


@pytest.mark.asyncio
async def test_above_0_the_rows_are_landed_kept_for_the_ttl_and_reused(monkeypatch, engine_cache):
    _threshold(monkeypatch, 1000)
    state = _state(source_ttl=0)
    answer, called = _answer(3)
    rewrites: dict = {}
    await _land({**_TABLE, "cache_ttl": 600}, state, answer, rewrites, {})
    ((table, ttl),) = engine_cache["drops"]
    assert ttl == 600 and rewrites["movie_hits"][1] == table
    # The next statement asking the same finds the landed rows live and reads nothing.
    engine_cache["known_live"] = True
    again: dict = {}
    await _land({**_TABLE, "cache_ttl": 600}, state, answer, again, {})
    assert again == rewrites and called["n"] == 1


# -- the statement --------------------------------------------------------------------------------


def test_a_plan_with_a_scratch_table_runs_through_the_pipeline_terminal():
    from provisa.pgwire._pipeline import streams_from_terminal

    plan = SimpleNamespace(
        two_step=None, code_write=None, embedded=None, scratch=(("loc", "scratch__x"),)
    )
    assert not streams_from_terminal(plan)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_the_terminal_drops_the_scratch_tables_however_the_statement_ends(monkeypatch):
    from provisa.pgwire import _pipeline

    dropped: list = []

    async def _drop(scratch, _state):
        dropped.extend(scratch)

    async def _fails(_plan, _state):
        raise RuntimeError("the engine failed")

    monkeypatch.setattr(mat, "drop_scratch", _drop)
    monkeypatch.setattr(_pipeline, "_execute_plan_with_secrets", _fails)
    plan = SimpleNamespace(
        scratch=(("loc", "scratch__x"),), embedded=None, two_step=None, code_write=None, temp=None
    )
    with pytest.raises(RuntimeError, match="the engine failed"):
        await _pipeline._execute_plan_bound(plan, object())  # type: ignore[arg-type]
    assert dropped == [("loc", "scratch__x")]


@pytest.mark.parametrize(
    ("registered", "source_ttl", "expected"),
    [(0, 300, 0), (None, 120, 120), (None, None, 300), (45, 120, 45)],
)
@pytest.mark.asyncio
async def test_a_grpc_remote_tables_ttl_is_the_first_one_set(
    monkeypatch, engine_cache, registered, source_ttl, expected
):
    """A missing TTL is not 300 by itself: it is the source's, else the default (REQ-2003). The
    table declares no parameter, so the whole chain applies (REQ-1986)."""
    from provisa.api_source import engine_cache as ec
    from provisa.source_adapters import grpc_remote_adapter

    fetched: dict = {}

    async def _fetch(**kw):
        fetched["ttl"] = kw["ttl"]
        return [{"id": 1}]

    monkeypatch.setattr(grpc_remote_adapter, "fetch", _fetch)
    monkeypatch.setattr(ec, "cache_table_name", lambda *_a: "grpc_tbl")
    monkeypatch.setattr(ec, "land_api_cache", AsyncMock())
    _threshold(monkeypatch, 1000)
    reg = {"pb2": None}
    registration = {
        "table_name": "s__m",
        "source_id": "grpc_src",
        "cache_ttl": registered,
        "columns": [{"column_name": "id"}],
    }
    query = SimpleNamespace(
        columns=[],
        full_method_path="/S/M",
        input_message="I",
        output_message="O",
        server_streaming=False,
    )
    state = _state(source_ttl=source_ttl, source="grpc_src")
    state.grpc_remote_sources = {}
    hot = SimpleNamespace(hold=lambda *_a, **_k: "held")
    inline: dict = {}
    await mat._mat_grpc_remote_table(
        "s__m", "grpc_src", reg, query, state, hot, 0, {}, inline, None, registration=registration
    )
    assert fetched["ttl"] == expected
    if expected == 0:  # read live for this statement: inlined, nothing landed or dropped
        assert engine_cache["drops"] == [] and inline["s__m"].rows == [{"id": 1}]
    else:
        assert engine_cache["drops"] == [("grpc_tbl", expected)]


@pytest.mark.parametrize(("own", "expected"), [(None, 0), (45, 45)])
@pytest.mark.asyncio
async def test_a_grpc_remote_table_with_arguments_keeps_only_for_its_own_ttl(
    monkeypatch, engine_cache, own, expected
):
    """REQ-1986: a gRPC-remote table with arguments is read live per statement unless the table
    sets its own TTL, whatever its source says."""
    from provisa.api_source import engine_cache as ec
    from provisa.source_adapters import grpc_remote_adapter

    calls: list[int] = []

    async def _fetch(**kw):
        calls.append(kw["ttl"])
        return [{"id": 1}]

    monkeypatch.setattr(grpc_remote_adapter, "fetch", _fetch)
    monkeypatch.setattr(ec, "cache_table_name", lambda *_a: "grpc_tbl")
    monkeypatch.setattr(ec, "land_api_cache", AsyncMock())
    _threshold(monkeypatch, 1000)
    registration = {
        "table_name": "s__m",
        "source_id": "grpc_src",
        "cache_ttl": own,
        "columns": [{"column_name": "_nf_id", "native_filter_type": "grpc_input"}],
    }
    query = SimpleNamespace(
        columns=[],
        full_method_path="/S/M",
        input_message="I",
        output_message="O",
        server_streaming=False,
    )
    state = _state(source_ttl=300, source="grpc_src")
    state.grpc_remote_sources = {}
    hot = SimpleNamespace(hold=lambda *_a, **_k: "held")
    for _ in range(2):
        engine_cache["known_live"] = bool(calls) and expected > 0
        await mat._mat_grpc_remote_table(
            "s__m",
            "grpc_src",
            {"pb2": None},
            query,
            state,
            hot,
            0,
            {},
            {},
            None,
            registration=registration,
        )
    if expected == 0:  # a call per statement, nothing kept
        assert calls == [0, 0] and engine_cache["drops"] == []
    else:  # kept for its own TTL, so the second statement reuses it
        assert calls == [45] and engine_cache["drops"] == [("grpc_tbl", 45)]


@pytest.mark.asyncio
async def test_a_grpc_remote_tables_failed_landing_fails_the_read(monkeypatch, engine_cache):
    """A landing that fails is raised, never logged and the statement rewritten to the cache
    table it did not write (REQ-1661): no rewrite, no inline rows, no drop scheduled."""
    from provisa.api_source import engine_cache as ec
    from provisa.source_adapters import grpc_remote_adapter

    async def _fetch(**_kw):
        return [{"id": i} for i in range(5)]

    monkeypatch.setattr(grpc_remote_adapter, "fetch", _fetch)
    monkeypatch.setattr(ec, "cache_table_name", lambda *_a: "grpc_tbl")
    monkeypatch.setattr(ec, "land_api_cache", AsyncMock(side_effect=OSError("store refused")))
    _threshold(monkeypatch, 1000)
    query = SimpleNamespace(
        columns=[],
        full_method_path="/S/M",
        input_message="I",
        output_message="O",
        server_streaming=False,
    )
    state = _state(source="grpc_src")
    state.grpc_remote_sources = {}
    hot = SimpleNamespace(hold=lambda *_a, **_k: "held")
    rewrites: dict = {}
    inline: dict = {}
    with pytest.raises(RuntimeError, match="gRPC remote landing failed for 's__m'") as raised:
        await mat._mat_grpc_remote_table(
            "s__m",
            "grpc_src",
            {"pb2": None},
            query,
            state,
            hot,
            0,
            rewrites,
            inline,
            None,
            registration={"table_name": "s__m", "source_id": "grpc_src", "columns": []},
        )
    assert isinstance(raised.value.__cause__, OSError)
    assert rewrites == {} and inline == {}
    assert engine_cache["drops"] == []
