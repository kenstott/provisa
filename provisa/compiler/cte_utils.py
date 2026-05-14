from __future__ import annotations

import sqlglot.expressions as exp


def cte_names(tree: exp.Expression) -> frozenset[str]:
    """Return the set of user-defined CTE alias names in the WITH clause."""
    with_clause = tree.args.get("with")
    if not with_clause:
        return frozenset()
    return frozenset(cte.alias for cte in with_clause.expressions)


def physical_tables(tree: exp.Expression) -> list[exp.Table]:
    """Return all exp.Table nodes that are physical table refs, not CTE aliases."""
    names = cte_names(tree)
    return [t for t in tree.find_all(exp.Table) if t.name not in names]
