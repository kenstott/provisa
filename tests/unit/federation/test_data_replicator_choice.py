# Copyright (c) 2026 Kenneth Stott
# Canary: e95ee53f-d2d1-4fe5-b3d4-31b3add0d125
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: the method that builds a replica is a pure decision from declared capabilities.

The whole table of source, store and engine declarations is checked here with no source, store
or engine running.
"""

from itertools import chain, combinations, product

import pytest

from provisa.federation.data_replicator import (
    EngineCaps,
    EngineRun,
    Method,
    NoReplicationMethod,
    SourceCaps,
    SourceRead,
    TargetCaps,
    TargetWrite,
    choose_method,
)

R, W, E = SourceRead, TargetWrite, EngineRun


def _subsets(members):
    items = list(members)
    return [
        frozenset(c)
        for c in chain.from_iterable(combinations(items, n) for n in range(len(items) + 1))
    ]


def _every_combination():
    for reads, writes, swap, reaches, runs in product(
        _subsets(R), _subsets(W), (True, False), (True, False), _subsets(E)
    ):
        yield SourceCaps(reads), TargetCaps(writes, swap), EngineCaps(reaches, runs)


def _source(*reads):
    return SourceCaps(frozenset(reads))


def _target(*writes, swap=True):
    return TargetCaps(frozenset(writes), swap)


def _engine(reaches, *runs):
    return EngineCaps(reaches, frozenset(runs))


# The combinations that exist today, by name: how a replica of that source is built there.
NAMED = [
    (
        "PostgreSQL engine, a source it reaches through postgres_fdw",
        _source(R.ENGINE_REACHABLE, R.CURSOR),
        _target(W.STATEMENT_COPY, W.COPY_STREAM),
        _engine(True, E.STATEMENT),
        Method.ENGINE_STATEMENT,
    ),
    (
        "PostgreSQL engine, a source it has no connector for",
        _source(R.CURSOR),
        _target(W.STATEMENT_COPY, W.COPY_STREAM),
        _engine(False, E.STATEMENT),
        Method.STREAM_BATCHES,
    ),
    (
        "Trino over the PostgreSQL store, an API source that returns one document",
        _source(R.SINGLE_DOCUMENT),
        _target(W.COPY_STREAM),
        _engine(False, E.STATEMENT),
        Method.STREAM_BATCHES,
    ),
    (
        "an engine that reads the source in place but whose store takes only batches",
        _source(R.ENGINE_REACHABLE, R.ARROW_STREAM),
        _target(W.BULK_BATCH),
        _engine(True, E.STATEMENT),
        Method.STREAM_BATCHES,
    ),
    (
        "a warehouse store with its bulk write, a source read through a cursor",
        _source(R.CURSOR),
        _target(W.BULK_BATCH),
        _engine(False),
        Method.STREAM_BATCHES,
    ),
]


@pytest.mark.parametrize(("name", "source", "target", "engine", "method"), NAMED)
def test_named_combinations(name, source, target, engine, method):
    assert choose_method(source, target, engine) is method, name


def test_the_engine_copies_exactly_when_all_three_parties_allow_it():
    for source, target, engine in _every_combination():
        allowed = (
            target.atomic_swap
            and R.ENGINE_REACHABLE in source.reads
            and engine.reaches_source
            and E.STATEMENT in engine.runs
            and W.STATEMENT_COPY in target.writes
        )
        try:
            chosen = choose_method(source, target, engine)
        except NoReplicationMethod:
            chosen = None
        assert (chosen is Method.ENGINE_STATEMENT) == allowed, (source, target, engine)


def test_a_stream_is_the_method_when_the_engine_cannot_copy_and_both_ends_stream():
    for source, target, engine in _every_combination():
        try:
            chosen = choose_method(source, target, engine)
        except NoReplicationMethod:
            continue
        if chosen is Method.STREAM_BATCHES:
            assert target.atomic_swap
            assert source.reads & {R.ARROW_STREAM, R.CURSOR, R.SINGLE_DOCUMENT}
            assert target.writes & {W.COPY_STREAM, W.BULK_BATCH}


def test_every_refusal_names_what_is_missing_and_every_other_combination_has_a_method():
    refused = served = 0
    for source, target, engine in _every_combination():
        try:
            assert choose_method(source, target, engine) in Method
            served += 1
        except NoReplicationMethod as exc:
            refused += 1
            assert exc.missing, (source, target, engine)
            assert all(reason in str(exc) for reason in exc.missing)
    assert served and refused


def test_a_store_that_cannot_swap_serves_no_method_and_says_so():
    with pytest.raises(NoReplicationMethod) as raised:
        choose_method(
            _source(R.ENGINE_REACHABLE, R.CURSOR),
            _target(W.STATEMENT_COPY, W.COPY_STREAM, swap=False),
            _engine(True, E.STATEMENT),
        )
    assert raised.value.missing == ["the store cannot swap a finished table in atomically"]


def test_a_source_with_no_stream_on_an_engine_that_cannot_reach_it_names_both():
    with pytest.raises(NoReplicationMethod) as raised:
        choose_method(_source(), _target(W.COPY_STREAM), _engine(False, E.STATEMENT))
    assert raised.value.missing == [
        "the source offers no stream (Arrow batches, a cursor, or a single document)",
        "the engine does not reach the source",
    ]
