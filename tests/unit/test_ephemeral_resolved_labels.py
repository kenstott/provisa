# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2f902e-cc7e-4ec6-a6e9-9a91a94f704a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A label's resolution lives in the session's lookup tables (REQ-2176): store.Graph holds each
label's session side -- the anchor tables its ephemeral nodes are in, their columns merged with
their types widened, and the model types a tag gives it to -- kept at write time, so the read
overlay computes nothing per statement. Each entry is what the overlay computed per statement
before (the oracle below, its code as it was)."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from provisa.ephemeral import store
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}


def _oracle_widened(a: str, b: str) -> str:
    from provisa.ephemeral import types

    if a == b:
        return a
    if {a, b} <= {"bigint", "integer", "smallint", "double"}:
        return "double" if "double" in {a, b} else "bigint"
    return types.widened(a, b) or a


def _oracle(session) -> dict[str, store.Resolved]:
    """What read.overlay computed per statement before the lookup tables held it."""
    g = store.graph(session)
    keys = set(g.labels)
    for _, source, target in g.rel_pairs:
        keys.update((source, target))
    keys.discard("")
    if "" in g.label_anchors:
        keys.add("")
    out = {}
    for key in keys:
        anchors = g.label_anchors.get(key, set())
        held: dict[str, str] = {}
        for anchor in sorted(anchors):
            for name, kind in session.tables[anchor].columns:
                if name == store.NODE_ID:
                    continue
                held[name] = _oracle_widened(held[name], kind) if name in held else kind
        out[key] = store.Resolved(
            anchors=tuple(sorted(anchors)),
            columns=tuple(held.items()),
            tagged=tuple(sorted(g.tag_types.get(key, set()))),
        )
    return out


@pytest.mark.parametrize(
    "statements",
    [
        ["CREATE (:T {k: 1}), (:T:U {k: 2.5, w: 'x'})"],
        ["CREATE (:T {k: 1})", "MATCH (n:T) SET n:Hot"],
        ["MATCH (p:Person) WHERE p.name = 'Ann' SET p:Vip"],
        ["CREATE (:T {k: 1})", "MATCH (n:T) SET n:Hot", "MATCH (n:Hot) REMOVE n:Hot"],
        ["CREATE ({k: 1})-[:R]->(:X)", "CREATE (:Person:Temp {name: 'Cy'})"],
        ["CREATE (:T {k: 1}), (:T {k: 2})", "MATCH (n:T) WHERE n.k = 1 DELETE n"],
    ],
)
def test_the_lookup_tables_hold_what_the_overlay_computed(monkeypatch, statements):
    s = Session(monkeypatch, _PEOPLE)
    for statement in statements:
        s.run(statement)
        assert store.graph(s.session).resolved == _oracle(s.session), statement
