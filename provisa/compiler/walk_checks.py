# Copyright (c) 2026 Kenneth Stott
# Canary: f80d3687-c5aa-41e5-a4c9-c01a46f7be19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The visited-relationship checks the graph compiler writes into a walk (REQ-348, REQ-603).

A variable-length walk keeps Cypher's trail rule: its recursive step refuses an edge already on
the walk's trail. On a relationship of a table to itself that check names two rows of the table
by something other than an equality of their columns, which the relationship guard refuses
(provisa.compiler.join_guard). The maintainer's decision (2026-11-03): the guard exempts the
walk's own trail check, and only it. The compiler records each check it writes, as PostgreSQL
SQL; the guard skips a condition only when it is one of those, written exactly so. A statement
a client writes has no recorded check of its own making."""

from __future__ import annotations

from collections import OrderedDict

import sqlglot.expressions as exp

#: How many distinct checks are kept: one per walk shape a model's relationships give.
_KEPT = 4096
_RECORDED: OrderedDict[str, None] = OrderedDict()


def _written(condition: exp.Expr) -> str:
    return condition.sql(dialect="postgres")


def record(condition: exp.Expr) -> exp.Expr:
    """``condition``, recorded as a walk's trail check the compiler wrote."""
    written = _written(condition)
    _RECORDED[written] = None
    _RECORDED.move_to_end(written)
    while len(_RECORDED) > _KEPT:
        _RECORDED.popitem(last=False)
    return condition


def compiled(condition: exp.Expr) -> bool:
    """Whether ``condition`` is a trail check the compiler wrote, exactly as it wrote it."""
    return _written(condition) in _RECORDED
