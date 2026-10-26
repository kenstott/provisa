# Copyright (c) 2026 Kenneth Stott
# Canary: d87606cf-eee4-42d6-ab92-edcca6a6a6e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986: a statement's scratch tables are dropped when IT ends. A statement nested in another
(a code source reading its inputs while the outer statement is planned) lands its scratch tables
into the outer one's, and drops none of them: the outer statement may still read them. The outer
drops them all when it ends. Before, the nested plan carried the outer's list and dropped the outer's
scratch table under it (churnprod demo, run 40203508743: "Table with name scratch__... does not
exist")."""

from __future__ import annotations

from provisa.api.data import materialization as mat


def test_a_nested_statement_drops_none_of_the_outer_statements_scratch():
    with mat.statement_scratch() as outer:
        mat._STATEMENT_SCRATCH.get().append(("loc", "scratch__outer"))
        with mat.statement_scratch() as nested:
            mat._STATEMENT_SCRATCH.get().append(("loc", "scratch__nested"))
            assert nested == []  # nothing the nested statement drops when it ends
        assert outer == [("loc", "scratch__outer"), ("loc", "scratch__nested")]
    assert mat._STATEMENT_SCRATCH.get() is None
