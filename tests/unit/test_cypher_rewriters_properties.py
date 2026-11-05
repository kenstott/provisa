# Copyright (c) 2026 Kenneth Stott
# Canary: bf8592b4-81da-4b6c-a34e-82c887d3570b
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for Cypher text rewriters (REQ-571).

Two string rewrites run on every Cypher query before translation:

  * rewrite_params replaces named ``$name`` references with positional ``$1, $2 …``
    in declaration order — a bug renumbers a parameter and binds the wrong value;
  * rewrite_bare_map_literals expands ``{k: v}`` map literals to MAP(ARRAY[…],
    ARRAY[…]) bottom-up — a bug leaves a bare map the SQL engine can't parse, or
    drops one.

Generate the inputs and assert the defining invariants for every one.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.cypher.params import rewrite_params

_NAMES = ["a", "b", "c", "x", "y"]


@st.composite
def _param_query(draw):
    """A Cypher fragment referencing named params, plus the declared name list (a
    subset — references to undeclared names must be left untouched)."""
    declared = draw(st.lists(st.sampled_from(_NAMES), min_size=1, max_size=4, unique=True))
    refs = draw(st.lists(st.sampled_from(_NAMES), min_size=1, max_size=6))
    query = "WHERE " + " AND ".join(f"n.{r} = ${r}" for r in refs) + " RETURN 1"
    return query, declared, refs


@settings(max_examples=300, deadline=None)
@given(case=_param_query())
def test_rewrite_params_is_positional_and_stable(case) -> None:
    query, declared, refs = case
    out = rewrite_params(query, declared)
    idx = {name: i + 1 for i, name in enumerate(declared)}

    # Declared names become their 1-based position; undeclared names are untouched.
    for r in refs:
        if r in idx:
            assert f"${idx[r]}" in out
        else:
            assert f"${r}" in out
    # No declared named param survives ($<letter> replaced by $<digit>).
    for name in declared:
        assert f"${name}" not in out
    # Idempotent: positional refs are not themselves named params.
    assert rewrite_params(out, declared) == out
