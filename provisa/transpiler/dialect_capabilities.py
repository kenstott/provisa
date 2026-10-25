# Copyright (c) 2026 Kenneth Stott
# Canary: ada3b07d-5d65-4191-91a2-df97868fa46e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each source dialect's own SQL runs (REQ-863): a statement goes direct to its one source
only when the source's dialect supports every construct in it; a construct the dialect does not
list sends the statement to the engine, naming the construct and the source.

One list, per dialect, of the constructs a semantic statement can hold: common table expressions,
joins, GROUP BY, window functions, a subquery read as a value, EXISTS, JSON aggregation and
object construction, JSON path extraction, the engine-only functions a Cypher read is lowered to
(REQ-345) and cosine similarity (REQ-423). A dialect missing from the list supports none of them:
a plain read of one table still goes direct to it, anything more is the engine's."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.engine_functions import uncomputable

CTE = "a common table expression"
JOIN = "a join"
GROUP_BY = "GROUP BY"
WINDOW = "a window function"
SCALAR_SUBQUERY = "a subquery read as a value"
EXISTS = "EXISTS"
JSON_AGGREGATE = "JSON aggregation"
JSON_OBJECT = "a JSON object"
JSON_PATH = "JSON path extraction"
ENGINE_FUNCTION = "an engine-only function"
SIMILARITY = "cosine_similarity"

_RELATIONAL = frozenset({CTE, JOIN, GROUP_BY, WINDOW, SCALAR_SUBQUERY, EXISTS})
_JSON = frozenset({JSON_AGGREGATE, JSON_OBJECT})

#: Each dialect's constructs, as its own SQL runs them once SQLGlot has written them for it.
CAPABILITIES: dict[str, frozenset[str]] = {
    "postgres": _RELATIONAL | _JSON | {JSON_PATH, SIMILARITY},
    "redshift": _RELATIONAL,
    "mysql": _RELATIONAL | _JSON,
    "singlestore": _RELATIONAL | _JSON,
    "sqlite": _RELATIONAL | _JSON,
    "duckdb": _RELATIONAL | _JSON,
    "oracle": _RELATIONAL | _JSON,
    "tsql": _RELATIONAL,
    "snowflake": frozenset({CTE, JOIN, GROUP_BY, WINDOW, EXISTS, SIMILARITY}),
    "bigquery": frozenset({CTE, JOIN, GROUP_BY, WINDOW, EXISTS}),
    "databricks": frozenset({CTE, JOIN, GROUP_BY, WINDOW, EXISTS}),
    "hive": frozenset({CTE, JOIN, GROUP_BY, WINDOW}),
    "clickhouse": frozenset({CTE, JOIN, GROUP_BY, WINDOW}),
    "exasol": frozenset({CTE, JOIN, GROUP_BY, WINDOW, SCALAR_SUBQUERY, EXISTS}),
    "druid": frozenset({GROUP_BY}),
    "athena": _RELATIONAL | {ENGINE_FUNCTION},
    "trino": _RELATIONAL | {ENGINE_FUNCTION},
    "presto": _RELATIONAL | {ENGINE_FUNCTION},
    # REQ-2000: Qdrant's own similarity search; its route check names every other construct.
    "qdrant": frozenset({SIMILARITY}),
}

_JSON_AGGREGATES = frozenset({"json_agg", "jsonb_agg", "json_group_array", "json_arrayagg"})
_JSON_OBJECTS = frozenset({"json_build_object", "jsonb_build_object", "json_object"})


def _constructs(tree: exp.Expr, sql: str, json_path: bool) -> list[str]:
    """The constructs ``tree`` holds, in the order the list names them."""
    found: list[str] = []

    def has(*kinds: type) -> bool:
        return tree.find(*kinds) is not None

    def named(names: frozenset[str]) -> bool:
        return any(
            isinstance(n, exp.Anonymous) and n.name.lower() in names
            for n in tree.find_all(exp.Anonymous)
        )

    if has(exp.CTE):
        found.append(CTE)
    if has(exp.Join):
        found.append(JOIN)
    if has(exp.Group):
        found.append(GROUP_BY)
    if has(exp.Window):
        found.append(WINDOW)
    # A subquery read as a value (in a projection or a comparison), not one a FROM, a join, an
    # EXISTS or an IN reads as rows.
    if any(
        not isinstance(s.parent, (exp.From, exp.Join, exp.Exists, exp.In))
        for s in tree.find_all(exp.Subquery)
    ):
        found.append(SCALAR_SUBQUERY)
    if has(exp.Exists):
        found.append(EXISTS)
    if has(exp.JSONArrayAgg) or named(_JSON_AGGREGATES):
        found.append(JSON_AGGREGATE)
    if has(exp.JSONObject) or named(_JSON_OBJECTS):
        found.append(JSON_OBJECT)
    if json_path or has(exp.JSONExtract, exp.JSONExtractScalar):
        found.append(JSON_PATH)
    if uncomputable(sql, "") is not None:
        found.append(ENGINE_FUNCTION)
    from provisa.vector.similarity import mentions

    if mentions(sql):
        found.append(SIMILARITY)
    return found


def unsupported(sql: str, dialect: str, *, json_path: bool = False) -> str | None:
    """The first construct ``sql`` (PostgreSQL-dialect text) holds that a source of ``dialect``
    does not run, or None. ``json_path``: the statement extracts JSON paths (the compiler says
    so for a path column it lowered)."""
    supported = CAPABILITIES.get(dialect, frozenset())
    tree = sqlglot.parse_one(sql, read="postgres")
    for construct in _constructs(tree, sql, json_path):
        if construct not in supported:
            return construct
    return None
