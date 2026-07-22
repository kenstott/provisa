# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2e5a71-4b90-4d6f-b3a8-1e7c9f024d5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1161 clause 5: the federation graph is built INCREMENTALLY — only a view whose definition
changed is re-parsed; the rest are unioned from cache, and an unchanged input is never rebuilt."""

from __future__ import annotations

import pytest

from provisa.lineage import merge
from provisa.lineage.merge import (
    build_federation_graph,
    build_federation_graph_incremental,
    clear_federation_cache,
)

_VIEWS = [
    ("main.mv_daily", "SELECT customer_id AS id, sum(amount) AS total FROM orders GROUP BY 1"),
    ("main.report", "SELECT id, total AS grand FROM mv_daily"),
]


def _norm(d: dict) -> dict:
    """Order-insensitive view of a merged-graph dict for comparison."""
    return {
        "nodes": sorted(d["nodes"], key=lambda n: n["id"]),
        "edges": sorted(d["edges"], key=lambda e: (e["source"], e["target"], e.get("transform", ""))),
        "outputs": sorted(d["outputs"]),
        "cycles": sorted([sorted(c["nodes"]) for c in d.get("cycles", [])]),
    }


@pytest.fixture(autouse=True)
def _clean_cache():
    clear_federation_cache()
    yield
    clear_federation_cache()


def test_incremental_result_matches_full_build():
    """Incremental build produces the SAME federation graph as the from-scratch build."""
    full = build_federation_graph(_VIEWS).to_dict()
    inc = build_federation_graph_incremental(_VIEWS).to_dict()
    assert _norm(inc) == _norm(full)


def test_unchanged_input_is_not_rebuilt():
    """Same input twice → the prior merged graph is returned without re-unioning (never rebuild)."""
    a = build_federation_graph_incremental(_VIEWS)
    b = build_federation_graph_incremental(_VIEWS)
    assert a is b


def test_only_changed_view_is_reparsed(monkeypatch):
    """The expensive per-view parse runs ONLY for views whose SQL changed."""
    calls: list[str] = []
    real = merge.build_column_graph

    def _spy(sql, **kw):
        calls.append(sql)
        return real(sql, **kw)

    monkeypatch.setattr(merge, "build_column_graph", _spy)

    # First build parses every view once.
    build_federation_graph_incremental(_VIEWS)
    assert len(calls) == 2

    # Identical rebuild: whole-input short-circuit → zero additional parses.
    build_federation_graph_incremental(_VIEWS)
    assert len(calls) == 2

    # Change ONE view's definition — only that view re-parses; the unchanged one is served from cache.
    changed = [_VIEWS[0], ("main.report", "SELECT id, total AS grand_total FROM mv_daily")]
    build_federation_graph_incremental(changed)
    assert len(calls) == 3
    assert calls[-1] == changed[1][1]


def test_unparseable_view_is_cached_not_reparsed(monkeypatch):
    """A view whose SQL will not parse is skipped AND cached, so it is not re-parsed each request."""
    calls: list[str] = []
    real = merge.build_column_graph

    def _spy(sql, **kw):
        calls.append(sql)
        return real(sql, **kw)

    monkeypatch.setattr(merge, "build_column_graph", _spy)

    bad = [("main.broken", "SELECT FROM WHERE ("), _VIEWS[0]]
    g1 = build_federation_graph_incremental(bad)
    n_first = len(calls)
    # rebuild with a trivial change elsewhere so the whole-input short-circuit does NOT fire
    bad2 = [("main.broken", "SELECT FROM WHERE ("), _VIEWS[1]]
    build_federation_graph_incremental(bad2)
    # the broken view was NOT parsed again (cached None); only the new/changed view parsed
    assert calls.count("SELECT FROM WHERE (") == 1
    assert g1.to_dict()["nodes"]  # the good view still contributed lineage
    assert n_first >= 1
