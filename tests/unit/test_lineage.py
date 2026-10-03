# Copyright (c) 2026 Kenneth Stott
# Canary: 1b9e46e0-d357-40df-bd71-255898ba9035
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-939: MV lineage from SQL — inputs, the reverse dependents graph, and cycle detection."""

from __future__ import annotations

from provisa.events.lineage import dependents, extract_inputs, find_cycle


def test_extract_inputs_qualified_and_ignores_ctes():
    sql = """
        WITH recent AS (SELECT * FROM sales.orders WHERE ts > now())
        SELECT r.id, c.name FROM recent r JOIN public.customers c ON r.cid = c.id
    """
    # sales.orders + public.customers are inputs; `recent` (a CTE) is not
    assert extract_inputs(sql) == {"sales.orders", "public.customers"}


def test_dependents_inverts_lineage():
    # each view's inputs as resolved against the model (events.nodes.lineage_graph)
    mvs = {
        "mv.daily": {"sales.orders"},
        "mv.by_cust": {"sales.orders", "public.customers"},
    }
    dep = dependents(mvs)
    assert dep["sales.orders"] == ["mv.by_cust", "mv.daily"]  # both listen to orders, sorted
    assert dep["public.customers"] == ["mv.by_cust"]


def test_find_cycle_none_when_acyclic():
    # a two-level DAG: mv.b reads the base source; mv.a reads mv.b — acyclic
    mvs = {
        "mv.b": {"sales.orders"},
        "mv.a": {"mv.b"},
    }
    assert find_cycle(mvs) is None


def test_find_cycle_detects_transitive_cycle():
    mvs = {
        "mv.a": {"mv.b"},
        "mv.b": {"mv.c"},
        "mv.c": {"mv.a"},  # cycle a → b → c → a
    }
    cycle = find_cycle(mvs)
    assert cycle is not None
    # the returned list closes on itself
    assert cycle[0] == cycle[-1]
    assert set(cycle) == {"mv.a", "mv.b", "mv.c"}


def test_self_reference_is_a_cycle():
    assert find_cycle({"mv.x": {"mv.x"}}) == ["mv.x", "mv.x"]
