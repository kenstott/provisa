# Copyright (c) 2026 Kenneth Stott
# Canary: ed3e42d4-eccb-477c-8fae-d9265de1688c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from __future__ import annotations

import sqlglot.expressions as exp

# Requirements: REQ-264, REQ-347


def cte_names(tree: exp.Expr) -> frozenset[str]:
    """Return the set of user-defined CTE alias names in the WITH clause."""
    with_clause = tree.args.get("with")
    if not with_clause:
        return frozenset()
    return frozenset(cte.alias for cte in with_clause.expressions)


def physical_tables(tree: exp.Expr) -> list[exp.Table]:
    """Return all exp.Table nodes that are physical table refs, not CTE aliases."""
    names = cte_names(tree)
    return [t for t in tree.find_all(exp.Table) if t.name not in names]
