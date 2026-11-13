# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e4c92-5a18-4f3d-9c26-e8d1a7f4b503
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's answers that differed from PostgreSQL's (REQ-2236; the function support measure,
run 43361055823, REQ-2171).

array_agg: ClickHouse's groupArray skips a NULL, and answers an empty array of no rows, where
PostgreSQL keeps each NULL and answers NULL of no rows. Its form aggregates each value in a tuple,
which groupArray keeps whatever it holds, and answers NULL where the aggregate counted no row.

current_schemas: ClickHouse has its own currentSchemas, which answers its database's name, where
PostgreSQL answers the search path, and NULL of a NULL. No form names PostgreSQL's search path on
ClickHouse: refused by name there, as on Trino."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement

FAMILY = "ClickHouse answers"

_ARRAY_AGG = "array_agg($a)"
_KEEPS_NULLS = "if(count() = 0, NULL, arrayMap(_t -> _t.1, groupArray(tuple($a))))"


def _aggregated(node: exp.Expr) -> exp.Expr | None:
    """The value a plain array_agg call aggregates; None for any other node, and for a call of an
    order, of DISTINCT, under a FILTER or over a window, whose form is not this row's."""
    if isinstance(node, exp.ArrayAgg):
        value = node.this
    elif isinstance(node, exp.Anonymous) and node.name.lower() == "array_agg":
        value = node.expressions[0] if len(node.expressions) == 1 else None
    else:
        return None
    if value is None or isinstance(value, (exp.Order, exp.Distinct)):
        return None
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    if isinstance(outer, (exp.Filter, exp.Window)):
        return None
    return value


def kept_nulls(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """array_agg of every value, a NULL kept, and NULL of no rows."""
    value = _aggregated(node)
    if value is None:
        return None
    return graft(replacement(_KEEPS_NULLS, dialect, _ARRAY_AGG), {"a": value})


ARRAY_AGG = Feature(
    "array_agg",
    FAMILY,
    "The values of an aggregate as an array, a NULL kept; NULL of no rows.",
    read=Side(rules=(Rule(None, {"clickhouse": Rewrite(kept_nulls, mentions=r"\barray_agg\b")}),)),
)

_SEARCH_PATH = (
    "Refused by name: ClickHouse's own currentSchemas answers its database, not PostgreSQL's "
    "search path."
)
#: Refused wherever a statement names it: unqualified, ClickHouse would answer its own.
_NO_SEARCH_PATH = Rewrite(
    trino.refusing(
        "current_schemas",
        "current_schemas: refused by name, as no form names the search path.",
        lambda *_: True,
    ).function,
    mentions=r"\bcurrent_schemas\b",
)
CURRENT_SCHEMAS = Feature(
    "current_schemas",
    FAMILY,
    "The schemas of the search path.",
    read=Side(
        rules=(Rule(None, dict.fromkeys((*trino.TRINO, "clickhouse"), _NO_SEARCH_PATH)),),
        limitations={
            **dict.fromkeys(
                trino.TRINO, (Limitation("Refused by name: Trino has no form of it."),)
            ),
            "clickhouse": (Limitation(_SEARCH_PATH),),
        },
    ),
)

FEATURES: tuple[Feature, ...] = (ARRAY_AGG, CURRENT_SCHEMAS)
