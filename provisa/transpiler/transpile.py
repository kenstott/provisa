# Copyright (c) 2026 Kenneth Stott
# Canary: 2af8ab62-fcda-4876-9364-1040f6919d99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLGlot-based SQL transpilation (REQ-066, REQ-068).

Supports PG SQL → Trino, and PG SQL → any target dialect for direct execution.
"""

# Requirements: REQ-066, REQ-068, REQ-229

import functools
import re
from collections.abc import Callable

import sqlglot
from sqlglot.dialects.postgres import Postgres

from provisa.core.pg_error import MARK, raising
from provisa.transpiler import (
    function_macros,
    pg_forms_enum,
    pg_forms_geometric,
    pg_forms_type_io,
    pg_functions,
    pg_held_types,
    pg_money,
    pg_syntax,
    zoned_instants,
)
from provisa.transpiler.metadata_server_forms import SERVER_FORM_RAISE
from provisa.transpiler.native_json import resolve_native_json
from provisa.transpiler.native_subtype import resolve_reads
from provisa.transpiler.pg_forms_json import json_member
from provisa.transpiler.pg_literals import array_inputs, exact_inputs, postgres_literals
from provisa.transpiler.to_char import to_char_for_duckdb
from provisa.transpiler.transpile_correlated import (
    rewrite_correlated_subqueries_for_trino,
)
import sqlglot.expressions as exp


# Valid SQLGlot write dialects for target sources
SUPPORTED_DIALECTS: set[str] = {
    "trino",
    "postgres",
    "mysql",
    "tsql",
    "duckdb",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",  # REQ-987: Databricks as a first-class federation engine (SQLGlot databricks dialect)
    "spark",  # Databricks/Spark SQL variant
}


_WITHIN_GROUP = re.compile(r"\bWITHIN\s+GROUP\b", re.IGNORECASE)


def _within_group_unqualified(pg_sql: str) -> str:
    """``pg_sql`` with each pg_catalog.f(...) WITHIN GROUP (...) the unqualified call (REQ-2167):
    sqlglot reads the qualified one as a column of table pg_catalog and writes its name quoted and
    upper-cased, pg_catalog."RANK", which names no function. PostgreSQL resolves an unqualified
    built-in in pg_catalog first, so the call is the same."""
    if _WITHIN_GROUP.search(pg_sql) is None or "pg_catalog" not in pg_sql.lower():
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    misread = [
        c
        for c in tree.find_all(exp.Column)
        if isinstance(c.this, exp.WithinGroup) and c.table.lower() == "pg_catalog"
    ]
    for column in misread:
        call = column.this
        if isinstance(call.this, exp.Anonymous):
            named = call.this
            call.set("this", exp.Anonymous(this=named.name.lower(), expressions=named.expressions))
        column.replace(call)
    return tree.sql(dialect="postgres") if misread else pg_sql


def transpile_to_trino(pg_sql: str) -> str:  # REQ-066, REQ-068
    """Transpile PostgreSQL-dialect SQL to Trino SQL."""
    pg_sql = rewrite_correlated_subqueries_for_trino(pg_sql)
    # REQ-2171: the rewrites read the statement; a geometric form's routines are kept as written.
    return pg_forms_geometric.around(transpile(pg_sql, "trino"), _trino_rewrites)


def _trino_rewrites(result: str) -> str:
    result = _rewrite_epoch_of_timestamp_for_trino(result)
    result = _rewrite_json_build_object_for_trino(result)
    result = _rewrite_json_arrayagg_for_trino(result)
    result = _rewrite_empty_json_list_for_trino(result)
    result = _rewrite_regex_backreferences_for_trino(result)
    result = _rewrite_json_size_for_trino(result)
    return _rewrite_to_json_for_trino(result)


#: openCypher's ArgumentError (NumberOutOfRange) for a range's zero step, as the engine raises it.
RANGE_STEP_ZERO = "range() takes a step other than 0"


def _range_step(step: exp.Expression, raise_with: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """range_step(s), a range's step the query computes (expr_visitor): ``s``, or the engine's
    own error (``raise_with``: DuckDB's error, Trino's fail) where it is 0."""
    failed = exp.Anonymous(this=raise_with, expressions=[exp.Literal.string(RANGE_STEP_ZERO)])
    return exp.Case(
        ifs=[exp.If(this=exp.EQ(this=step.copy(), expression=exp.Literal.number(0)), true=failed)],
        default=step,
    )


def _type_refused(message: exp.Expression, raise_with: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """type_refused('...'), a value of a kind a function refuses (expr_visitor: openCypher's
    TypeError): the engine's own error (``raise_with``: DuckDB's error, Trino's fail) with the
    message, raised where the branch holding it is reached."""
    return exp.Anonymous(this=raise_with, expressions=[message.copy()])


def _json_field(value: exp.Expression, key: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """The JSON object ``value``'s field ``key``, null where it has none (Trino, DuckDB)."""
    return exp.Anonymous(
        this="json_extract", expressions=[value.copy(), exp.Literal.string(f"$.{key}")]
    )


def _json_has_keys(
    value: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    *keys: str,
    field: Callable[[exp.Expression, str], exp.Expression] = _json_field,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """True where the JSON object ``value`` holds every one of ``keys`` (read by ``field``)."""
    held = [exp.Not(this=exp.Is(this=field(value, k), expression=exp.Null())) for k in keys]
    return exp.and_(*held)  # pyright: ignore[reportReturnType]


def _json_order_key(
    value: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    text: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    *,
    field: Callable[[exp.Expression, str], exp.Expression] = _json_field,  # pyright: ignore[reportPrivateImportUsage]
    number: exp.Expression | None = None,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """json_order_key(x), a JSON value's place in openCypher's order (expr_visitor): its kind --
    read from the first character of ``text``, its JSON text, and for an object from its keys --
    then its number, then its text.

    openCypher's orderability (Cypher Manual, "Ordering and comparison of values"; openCypher 9
    spec, "Orderability"), ascending: MAP, NODE, RELATIONSHIP, LIST, PATH, STRING, BOOLEAN,
    NUMBER, then null last. A node, a relationship and a path are each a JSON object, told from a
    map by the keys the result assembler (gql/assembler.py) and Bolt packstream
    (bolt/packstream.py: _is_node_dict, _is_rel_dict, _is_path_dict) recognise them by. An
    object's field is read by ``field``; ``number`` is the value as a number, null for any other
    kind (by default its TRY_CAST to a double)."""
    first = exp.Anonymous(
        this="substr", expressions=[text, exp.Literal.number(1), exp.Literal.number(1)]
    )
    is_path = _json_has_keys(value, "nodes", "edges", field=field)
    is_rel = _json_has_keys(value, "type", "startNode", "endNode", field=field)
    is_node = exp.and_(
        _json_has_keys(value, "id", "label", "properties", field=field),
        exp.Is(this=field(value, "startNode"), expression=exp.Null()),
    )
    # Packstream tests a node, then a relationship, then a path; a path holds no node's keys.
    as_object = exp.Case(
        ifs=[
            exp.If(this=is_path, true=exp.Literal.number(4)),
            exp.If(this=is_rel, true=exp.Literal.number(2)),
            exp.If(this=is_node, true=exp.Literal.number(1)),
        ],
        default=exp.Literal.number(0),
    )
    ranks = [("[", 3), ('"', 5), ("t", 6), ("f", 6)]
    rank = exp.Case(
        this=first,
        ifs=[
            exp.If(this=exp.Literal.string("{"), true=as_object),
            *(exp.If(this=exp.Literal.string(c), true=exp.Literal.number(r)) for c, r in ranks),
        ],
        default=exp.Literal.number(7),
    )
    if number is None:
        number = exp.TryCast(this=value.copy(), to=exp.DataType.build("DOUBLE"))
    # A list's end before anything after it ([] before ['a'], ['a'] before ['a', 1]).
    ended = exp.Anonymous(
        this="replace",
        expressions=[
            text.copy(),
            exp.Literal.string("]"),
            exp.Anonymous(this="chr", expressions=[exp.Literal.number(1)]),
        ],
    )
    return exp.Anonymous(this="ROW", expressions=[rank, number, ended])


#: json_kind(x): a JSON value's kind as openCypher names it (expr_visitor), from the first
#: character of its JSON text.
_JSON_KIND_BY_FIRST = [
    ('"', "string"),
    ("[", "list"),
    ("{", "map"),
    ("t", "boolean"),
    ("f", "boolean"),
    ("n", "null"),
]


def _json_kind(text: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """json_kind(x), ``x``'s kind read from ``text``, its JSON text: string, list, map, boolean,
    null, else number; null for no value."""
    first = exp.Anonymous(
        this="substr", expressions=[text, exp.Literal.number(1), exp.Literal.number(1)]
    )
    return exp.Case(
        ifs=[
            exp.If(this=exp.Is(this=text.copy(), expression=exp.Null()), true=exp.Null()),
            *(
                exp.If(
                    this=exp.EQ(this=first.copy(), expression=exp.Literal.string(c)),
                    true=exp.Literal.string(k),
                )
                for c, k in _JSON_KIND_BY_FIRST
            ),
        ],
        default=exp.Literal.string("number"),
    )


#: Deepest list-in-list nesting json_list_compare compares; a deeper pair is refused by name.
_LIST_COMPARE_DEPTH = 4
LIST_COMPARE_TOO_DEEP = "list comparison: lists nested deeper than %d levels" % _LIST_COMPARE_DEPTH


def _json_list_compare(
    left: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    right: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    dialect: str,
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """json_list_compare(a, b), two JSON lists compared element by element (expr_visitor;
    openCypher 9, "Comparability and orderability"): -1, 0 or 1, null where the first element
    pair that differs cannot be compared. The first pair that is not equal decides; with every
    common pair equal the shorter list is smaller. A pair compares by value where both are
    numbers, both strings or both booleans, and recurses where both are lists; any other pair
    (two kinds, a null, a map) is incomparable. Each item is read by its own kind, never by one
    cast of the whole array. ``dialect`` is "duckdb" or "trino"."""
    duck = dialect == "duckdb"
    text = "CAST({} AS VARCHAR)" if duck else "json_format({})"
    scalar = "json_extract_string({}, '$')" if duck else "json_extract_scalar({}, '$')"
    items = "CAST({} AS JSON[])" if duck else "CAST({} AS ARRAY(JSON))"
    refuse = "error" if duck else "fail"

    def kind(value: str) -> str:
        parsed = sqlglot.parse_one(text.format(value), read=dialect)
        return _json_kind(parsed).sql(dialect=dialect)  # pyright: ignore[reportArgumentType]

    def sign(a: str, b: str) -> str:
        return f"CASE WHEN {a} < {b} THEN -1 WHEN {a} > {b} THEN 1 ELSE 0 END"

    def pair(a: str, b: str, depth: int) -> str:
        """-1, 0, 1, or 2 for a pair that cannot be compared."""
        ka, kb = kind(a), kind(b)
        num = [f"CAST({scalar.format(v)} AS DOUBLE)" for v in (a, b)]
        txt = [scalar.format(v) for v in (a, b)]
        boo = [f"CAST({scalar.format(v)} AS BOOLEAN)" for v in (a, b)]
        if depth == 0:
            # Written over the pair, so the engine does not fold the refusal away before the
            # branch is reached (DuckDB evaluates a constant error() at plan time).
            nested = f"{refuse}('{LIST_COMPARE_TOO_DEEP}' || substr({text.format(a)}, 1, 0))"
        else:
            nested = lists(a, b, depth)
        return (
            f"CASE WHEN {ka} = 'number' AND {kb} = 'number' THEN {sign(*num)} "
            f"WHEN {ka} = 'string' AND {kb} = 'string' THEN {sign(*txt)} "
            f"WHEN {ka} = 'boolean' AND {kb} = 'boolean' THEN {sign(*boo)} "
            f"WHEN {ka} = 'list' AND {kb} = 'list' THEN {nested} ELSE 2 END"
        )

    def lists(a: str, b: str, depth: int) -> str:
        arr_a, arr_b = items.format(a), items.format(b)
        p, c = f"_p{depth}", f"_c{depth}"
        shorter = (
            f"CASE WHEN len({arr_a}) < len({arr_b}) THEN -1 "
            f"WHEN len({arr_a}) > len({arr_b}) THEN 1 ELSE 0 END"
            if duck
            else f"CASE WHEN cardinality({arr_a}) < cardinality({arr_b}) THEN -1 "
            f"WHEN cardinality({arr_a}) > cardinality({arr_b}) THEN 1 ELSE 0 END"
        )
        body = pair(f"{p}[1]", f"{p}[2]", depth - 1)
        if duck:
            zipped = f"list_zip({arr_a}, {arr_b}, true)"
            decided = f"list_filter(list_transform({zipped}, {p} -> {body}), {c} -> {c} <> 0)"
            return f"list_extract(list_append({decided}, {shorter}), 1)"
        zipped = (
            f"slice(zip({arr_a}, {arr_b}), 1, least(cardinality({arr_a}), cardinality({arr_b})))"
        )
        decided = f"filter(transform({zipped}, {p} -> {body}), {c} -> {c} <> 0)"
        return f"element_at(concat({decided}, ARRAY[{shorter}]), 1)"

    result = lists(left.sql(dialect=dialect), right.sql(dialect=dialect), _LIST_COMPARE_DEPTH)
    return sqlglot.parse_one(f"NULLIF({result}, 2)", read=dialect)  # pyright: ignore[reportReturnType]


def _rewrite_json_size_for_trino(sql: str) -> str:
    """json_size(x), a JSON value's size (expr_visitor): a list's items, else a string's
    characters, as Trino reads them."""
    if not any(
        name in sql.upper()
        for name in (
            "JSON_SIZE",
            "JSON_ORDER_KEY",
            "JSON_KIND",
            "JSON_LIST_COMPARE",
            "RANGE_STEP",
            "TYPE_REFUSED",
            "JSON_KEYS",
        )
    ):
        return sql
    tree = sqlglot.parse_one(sql, read="trino")

    def _sized(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.JSONKeys) or (
            isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_KEYS"
        ):
            # A JSON object's keys (keys() of a map the client gives, expr_visitor).
            value = node.this if isinstance(node, exp.JSONKeys) else node.expressions[0]
            as_map = exp.Cast(
                this=value.copy(), to=exp.DataType.build("MAP(VARCHAR, JSON)", dialect="trino")
            )
            return exp.Anonymous(this="MAP_KEYS", expressions=[as_map])
        if isinstance(node, exp.Anonymous) and node.name.upper() == "RANGE_STEP":
            (step,) = node.expressions
            return _range_step(step, "fail")
        if isinstance(node, exp.Anonymous) and node.name.upper() == "TYPE_REFUSED":
            (message,) = node.expressions
            return _type_refused(message, "fail")
        if isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_ORDER_KEY":
            (value,) = node.expressions
            return _json_order_key(
                value, exp.Anonymous(this="json_format", expressions=[value.copy()])
            )
        if isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_KIND":
            (value,) = node.expressions
            return _json_kind(exp.Anonymous(this="json_format", expressions=[value.copy()]))
        if isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_LIST_COMPARE":
            left, right = node.expressions
            return _json_list_compare(left, right, "trino")
        if not (isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_SIZE"):
            return node
        (value,) = node.expressions
        items = exp.Anonymous(this="json_array_length", expressions=[value.copy()])
        text = exp.Anonymous(
            this="json_extract_scalar", expressions=[value.copy(), exp.Literal.string("$")]
        )
        return exp.Coalesce(
            this=items, expressions=[exp.Anonymous(this="length", expressions=[text])]
        )

    return tree.transform(_sized).sql(dialect="trino")


_BACKREFERENCE = re.compile(r"\\(\\|\d)")


def _rewrite_regex_backreferences_for_trino(sql: str) -> str:
    """A regexp_replace's replacement names a captured group as PostgreSQL writes it, \\1;
    Trino's writes it $1."""
    if "REGEXP_REPLACE" not in sql.upper() or "\\" not in sql:
        return sql
    tree = sqlglot.parse_one(sql, read="trino")
    for node in tree.find_all(exp.RegexpReplace):
        replacement = node.args.get("replacement")
        if isinstance(replacement, exp.Literal) and replacement.is_string:
            written = _BACKREFERENCE.sub(
                lambda said: said.group() if said.group(1) == "\\" else f"${said.group(1)}",
                replacement.this,
            )
            node.set("replacement", exp.Literal.string(written))
    return tree.sql(dialect="trino")


def _is_json_list_literal(node: exp.Expression) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """'[]' as json: CAST('[]' AS JSON), or JSON_PARSE('[]') as the json forms write a json cast
    of a literal for Trino (pg_forms_json._cast)."""
    if isinstance(node, exp.Cast):
        literal, is_json = node.this, node.to.is_type(exp.DataType.Type.JSON)
    elif isinstance(node, exp.ParseJSON):
        literal, is_json = node.this, True
    else:
        return False
    return is_json and isinstance(literal, exp.Literal) and literal.this == "[]"


def _is_json_text_aggregate(node: exp.Expression) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """A list field's aggregate as Trino holds it: json_format(...) text
    (_rewrite_json_arrayagg_for_trino), inline as a subquery or as the value column of the joined
    aggregate it was hoisted to (transpile_correlated: _grel_<n>._val)."""
    value = node
    if isinstance(node, exp.Subquery) and isinstance(node.this, exp.Select):
        selects = node.this.selects
        value = selects[0].unalias() if len(selects) == 1 else node
    if isinstance(value, exp.JSONFormat):
        return True
    return (
        isinstance(value, exp.Column) and value.name == "_val" and value.table.startswith("_grel_")
    )


def _rewrite_empty_json_list_for_trino(sql: str) -> str:
    """A list field's empty default (sql_selection.EMPTY_JSON_LIST, CAST('[]' AS JSON)) is
    '[]' for Trino: there the aggregate it defaults is json_format(...) text
    (_rewrite_json_arrayagg_for_trino), and Trino's CAST of text to JSON is a JSON string, not an
    array (#286). Only that default is rewritten: a statement's own json cast of '[]' stays json.
    The json forms have already written the cast as JSON_PARSE('[]') by the time this runs, so
    both forms are read."""
    if "'[]'" not in sql:
        return sql
    tree = sqlglot.parse_one(sql, read="trino")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.Coalesce) and _is_json_text_aggregate(node.this):
            for default in node.expressions:
                if _is_json_list_literal(default):
                    default.replace(exp.Literal.string("[]"))
        return node

    return tree.transform(_transform, copy=False).sql(dialect="trino", copy=False)


def _rewrite_epoch_of_timestamp_for_trino(sql: str) -> str:
    """PostgreSQL reads the epoch of a timestamp without a time zone as UTC; Trino's TO_UNIXTIME
    reads it in the session's zone, so the same statement gave a different number by the
    coordinator's zone. Read at UTC, as PostgreSQL does: TO_UNIXTIME(WITH_TIMEZONE(ts, 'UTC'))
    (REQ-1494: a faked date or time is the same on every engine and in every zone)."""
    if "TO_UNIXTIME(CAST(" not in sql:
        return sql
    tree = sqlglot.parse_one(sql, read="trino")
    changed = False
    for node in list(tree.find_all(exp.TimeToUnix)):
        arg = node.this
        if isinstance(arg, exp.Cast) and arg.to.this == exp.DataType.Type.TIMESTAMP:
            arg.replace(
                exp.Anonymous(
                    this="WITH_TIMEZONE", expressions=[arg.copy(), exp.Literal.string("UTC")]
                )
            )
            changed = True
    return tree.sql(dialect="trino") if changed else sql


def _utc_wall_outputs(
    select: exp.Select, utc_wall: dict[tuple[str, str, str], set[str]]
) -> dict[str, set[str]]:
    """Each FROM or JOIN item of ``select`` that holds a UTC-wall instant, by its alias, with the
    names it exposes them under: a table's declared columns, or the bare columns a derived table
    passes on from one -- a governed read (REQ-039) is a table inside its relation. A derived
    table's computed or masked output is no longer the column, and is not one of them."""
    outputs: dict[str, set[str]] = {}
    for source in [select.args.get("from_"), *(select.args.get("joins") or [])]:
        item = source.this if source is not None else None
        if isinstance(item, exp.Table) and item.args.get("catalog") is not None:
            key = (item.catalog, item.db.lower(), item.name.lower())
            if key in utc_wall:
                outputs[item.alias_or_name] = utc_wall[key]
        elif isinstance(item, exp.Subquery) and isinstance(item.this, exp.Select):
            inner = item.this
            below = _utc_wall_outputs(inner, utc_wall)
            only = next(iter(below)) if len(below) == 1 else ""
            passed = {
                expression.alias_or_name
                for expression in inner.expressions
                if isinstance(
                    column := (
                        expression.this if isinstance(expression, exp.Alias) else expression
                    ),
                    exp.Column,
                )
                and column.name in below.get(column.table or only, ())
            }
            if passed:
                outputs[item.alias_or_name] = passed
    return outputs


def rewrite_instants_for_trino(
    sql: str, utc_wall: dict[tuple[str, str, str], set[str]]
) -> str:  # REQ-2050
    """Each output column of ``sql`` that is an instant Trino reads as a plain TIMESTAMP written as
    ``with_timezone(col, 'UTC')``: the UTC wall time a MySQL-family TIMESTAMP or an Elasticsearch
    date arrives as, carried as the instant it is (trino_connectors.TRINO_INSTANT_AS_TIMESTAMP).

    ``utc_wall`` maps ``(catalog, schema, table)`` to such a table's declared-timestamptz columns
    (trino_connectors.trino_utc_wall_columns). Only the statement's outputs -- the outermost
    SELECT's items, each UNION branch's -- that are a bare column of such a table, directly or
    through a derived table that passes it on: a predicate, a join or an inner query reads the
    column as the connector gives it, so its pushdown stands."""
    if not utc_wall:
        return sql
    tree = sqlglot.parse_one(sql, read="trino")
    selects = [tree] if isinstance(tree, exp.Select) else []
    if isinstance(tree, exp.Union):
        selects = [s for s in tree.find_all(exp.Select) if s.find_ancestor(exp.Select) is None]
    changed = False
    for select in selects:
        sources = [select.args.get("from_")] + list(select.args.get("joins") or [])
        tables = _utc_wall_outputs(select, utc_wall)
        if not tables:
            continue
        only = next(iter(tables)) if len(sources) == 1 else None
        for item in list(select.expressions):
            column = item.this if isinstance(item, exp.Alias) else item
            if not isinstance(column, exp.Column):
                continue
            instants = tables.get(column.table or only or "")
            if not instants or column.name not in instants:
                continue
            zoned = exp.Anonymous(
                this="with_timezone", expressions=[column.copy(), exp.Literal.string("UTC")]
            )
            item.replace(exp.alias_(zoned, item.alias_or_name))
            changed = True
    return tree.sql(dialect="trino") if changed else sql


def rewrite_prometheus_labels_for_trino(
    sql: str, label_columns: dict[tuple[str, str], set[str]]
) -> str:  # REQ-1730
    """Rewrite a registered prometheus label column to ``labels['<name>']``.

    Trino's prometheus connector has no table-description mechanism (verified directly against a
    live coordinator: only ``prometheus.uri``/``prometheus.http.additional-headers``/
    ``prometheus.read-timeout`` are real catalog properties) and exposes a FIXED per-metric schema
    — ``labels MAP(VARCHAR,VARCHAR), timestamp, value`` — with no per-label flat columns under any
    configuration. A registered label column (e.g. "job") compiles to a bare identifier everywhere
    else (DuckDB introspects prometheus directly and already exposes it as a flat column), so only
    Trino needs this rewrite — applied here, at the very last physical-SQL step, rather than
    anywhere upstream in the shared governed pipeline.

    ``label_columns`` maps ``(catalog, table)`` — exactly the pairs known to be prometheus,
    populated at registration time — to that table's label column names; every other table's
    columns pass through untouched. A single-table query need not alias its label references; a
    multi-table query needs the label qualified by that table's alias to disambiguate."""
    if not label_columns:
        return sql
    tree = sqlglot.parse_one(sql, read="trino")
    alias_labels: dict[str | None, set[str]] = {}
    for tbl in tree.find_all(exp.Table):
        catalog = tbl.args.get("catalog")
        if catalog is None:
            continue
        labels = label_columns.get((catalog.name, tbl.name))
        if labels:
            alias_labels[tbl.alias_or_name] = labels
    if not alias_labels:
        return sql
    single_alias = next(iter(alias_labels)) if len(alias_labels) == 1 else None

    def _rewrite(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and not isinstance(node.parent, exp.Bracket):
            tbl_alias = node.table or single_alias
            labels = alias_labels.get(tbl_alias) if tbl_alias else None
            if labels and node.name in labels:
                return exp.Bracket(
                    this=exp.Column(this=exp.to_identifier("labels"), table=node.args.get("table")),
                    expressions=[exp.Literal.string(node.name)],
                )
        return node

    return tree.transform(_rewrite).sql(dialect="trino")


def _rewrite_to_json_for_trino(sql: str) -> str:
    """Replace to_json(x) with CAST(x AS JSON) for Trino.

    Trino has no to_json; its CAST(x AS JSON) already ENCODES a scalar/struct as a JSON value (the
    same semantics to_json gives on Postgres/DuckDB). The Cypher map-literal path emits to_json so the
    parse-semantics engines (Postgres/DuckDB) encode bare strings correctly; here it maps back to the
    Trino spelling.

    A to_json that is a JSON object's value is its JSON text marked FORMAT JSON: Trino's
    JSON_OBJECT casts a value of type JSON to varchar, which no array or object survives, and reads
    text marked FORMAT JSON as the JSON it holds (REQ-2176: a session node's labels)."""
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        value = node.expression if isinstance(node, exp.JSONKeyValue) else None
        if (
            isinstance(value, exp.Anonymous)
            and value.name.upper() == "TO_JSON"
            and len(value.expressions) == 1
        ):
            encoded = exp.Cast(
                this=value.expressions[0].transform(_transform),
                to=exp.DataType(this=exp.DataType.Type.JSON),
            )
            return exp.JSONKeyValue(
                this=node.this, expression=exp.FormatJson(this=exp.JSONFormat(this=encoded))
            )
        if isinstance(node, exp.Anonymous) and node.name.upper() == "TO_JSON":
            if len(node.expressions) == 1:
                # Recurse into the argument: sqlglot's transform walks pre-order and does not
                # revisit a replaced node's children, so a nested to_json (map literal inside a
                # map literal) would otherwise reach Trino unrewritten.
                return exp.Cast(
                    this=node.expressions[0].transform(_transform),
                    to=exp.DataType(this=exp.DataType.Type.JSON),
                )
        return node

    return tree.transform(_transform, copy=False).sql(dialect="trino", copy=False)


def _rewrite_json_build_object_for_trino(sql: str) -> str:
    """Replace JSON_BUILD_OBJECT(k, v, ...) with JSON_OBJECT('k': v, ...) for Trino.

    JSONB_BUILD_OBJECT is the same constructor over Postgres' binary JSON type — Trino registers
    neither, and its JSON type is the single one — so both spellings rewrite identically.
    """
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.Anonymous) and node.name.upper() in (
            "JSON_BUILD_OBJECT",
            "JSONB_BUILD_OBJECT",
        ):
            exprs = node.expressions
            # Recurse: transform is pre-order and skips a replaced node's children, so a nested
            # json_build_object would survive unrewritten.
            pairs = [
                exp.JSONKeyValue(
                    this=exprs[i].transform(_transform),
                    expression=exprs[i + 1].transform(_transform),
                )
                for i in range(0, len(exprs) - 1, 2)
            ]
            return exp.JSONObject(expressions=pairs)
        return node

    return tree.transform(_transform, copy=False).sql(dialect="trino", copy=False)


def _rewrite_json_arrayagg_for_trino(sql: str) -> str:
    """Replace JSON_ARRAYAGG(x) with json_format(CAST(ARRAY_AGG(JSON_PARSE(x)) AS JSON)).

    JSONB_AGG is the same aggregate over Postgres' binary JSON type — sqlglot leaves it anonymous
    (json_agg maps to JSON_ARRAYAGG, jsonb_agg does not) and Trino registers neither. A statement's
    own jsonb_agg is refused before this (REQ-2171): json's text is not the spacing of jsonb's.

    Trino 480 does not register json_arrayagg. The json_format wrapper produces
    VARCHAR so the result is safe to embed as a VALUE in JSON_OBJECT (Trino cannot
    coerce a JSON-typed array to varchar, which causes INVALID_CAST_ARGUMENT when
    the aggregate result is used inside an outer json_object(...) call).
    The Python _convert_value layer parses the VARCHAR back to a Python list/dict.
    """
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    json_type = exp.DataType(this=exp.DataType.Type.JSON)

    def _list(inner: exp.Expression, where: exp.Expression | None) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # Recurse: transform is pre-order and skips a replaced node's children, so a nested
        # json_arrayagg would survive unrewritten.
        collected: exp.Expression = exp.ArrayAgg(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            this=exp.Anonymous(this="JSON_PARSE", expressions=[inner.transform(_transform)])
        )
        if where is not None:
            # REQ-005's list cap: the filter is the aggregate's, inside the conversion.
            collected = exp.Filter(this=collected, expression=where.transform(_transform))
        return exp.Anonymous(
            this="json_format", expressions=[exp.Cast(this=collected, to=json_type.copy())]
        )

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        listed = _list_arg(node.this) if isinstance(node, exp.Filter) else None
        if listed is not None:
            return _list(listed, node.expression)
        inner = _list_arg(node)
        if inner is not None:
            return _list(inner, None)
        return node

    return tree.transform(_transform, copy=False).sql(dialect="trino", copy=False)


def transpile_to_duckdb(pg_sql: str) -> str:  # REQ-066, REQ-068
    """Transpile PostgreSQL-dialect SQL to DuckDB SQL."""
    from provisa.observability.stage_trace import trace_stage

    duckdb_sql = _duckdb_form(pg_sql)
    trace_stage("transpile.duckdb", duckdb_sql)
    return duckdb_sql


@functools.lru_cache(maxsize=2048)
def _duckdb_form(pg_sql: str) -> str:
    """``pg_sql`` in DuckDB's SQL, derived once: every step is a function of the text alone. A
    statement a client repeats reaches here on each run, its
    plan kept or not. A refusal raises and is not kept."""
    pg_sql = pg_money.lowered(pg_forms_type_io.readable(pg_sql), "duckdb")  # REQ-2171
    pg_sql = pg_forms_enum.lowered(pg_sql, "duckdb")  # REQ-2171
    written = transpile(
        pg_functions.before(
            _catalog_functions_unqualified(
                to_char_for_duckdb(postgres_literals(pg_functions.typed_calls(pg_sql)))
            )
        ),
        "duckdb",
    )
    # From here every step is of DuckDB's tree, read once: the JSON forms are several times the
    # statement's text, and each step that read it again paid for all of them.
    tree = _rewrite_json_agg_for_duckdb(sqlglot.parse_one(written, read="duckdb"))
    tree = _null_order_as_key(_rewrite_list_functions_for_duckdb(tree))
    tree = _row_delimited_string_agg(tree)
    tree = pg_functions.after_in(_postgres_semantics_for_duckdb(_lengths_for_duckdb(tree)))
    _posix_zones_for_duckdb(tree)
    # In place, the tree being this function's; a root a step lifted out of its parent keeps its copy.
    return tree.sql(dialect="duckdb", copy=tree.parent is not None)


#: string_agg of a delimiter that is no constant, as PostgreSQL appends one: each row's delimiter
#: before its value, the first row's dropped, a NULL value's row skipped and a NULL delimiter none.
#: Of any type both concatenate (text, bytea); DuckDB's own takes a constant delimiter alone.
_ROW_DELIMITED = (
    "list_reduce(list_transform(list(struct_pack(v := _v, d := _d)) FILTER (WHERE _v IS NOT NULL), "
    "(x, i) -> CASE WHEN i = 1 OR x.d IS NULL THEN x.v ELSE x.d || x.v END), (a, b) -> a || b)"
)
#: A text order key as en_US.utf8 orders it (provisa_en_us_key, refused by name for a value it
#: does not read); NULL for a value of any other type, which its own order then decides.
_EN_US_KEY = (
    "CASE WHEN typeof(_o) = 'VARCHAR' THEN provisa_en_us_key(CAST(_o AS VARCHAR), 'string_agg') END"
)


def _duckdb_form_of(sql: str, **nodes: exp.Expr) -> exp.Expr:
    """``sql`` read as DuckDB's, each column named in ``nodes`` that node."""
    return sqlglot.parse_one(sql, read="duckdb").transform(
        lambda n: nodes[n.name].copy() if isinstance(n, exp.Column) and n.name in nodes else n
    )


def _row_delimited_string_agg(tree: exp.Expr) -> exp.Expr:  # REQ-2167
    """Each string_agg whose delimiter is no constant (a column), as PostgreSQL answers it
    (``_ROW_DELIMITED``), its ORDER BY kept, a text key ordered as en_US.utf8 orders it."""

    def rewrite(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.GroupConcat) or isinstance(node.parent, exp.Filter):
            return node
        delimiter = node.args.get("separator")
        if delimiter is None or delimiter.find(exp.Column) is None:
            return node
        value, order = node.this, None
        if isinstance(value, exp.Order):
            value, order = value.this, value
        if isinstance(value, exp.Distinct):
            return node
        written = _duckdb_form_of(_ROW_DELIMITED, _v=value, _d=delimiter)
        if order is not None:
            listed = written.find(exp.ArrayAgg)  # list(...), as sqlglot reads it
            assert listed is not None
            keys: list[exp.Expr] = []
            for ordered in order.expressions:
                collated = ordered.copy()
                collated.set("this", _duckdb_form_of(_EN_US_KEY, _o=ordered.this))
                keys += [collated, ordered.copy()]
            listed.set("this", exp.Order(this=listed.this, expressions=keys))
        return written

    return tree.transform(rewrite, copy=False)


#: The calls of DuckDB's that take a zone's text, by name: the position of the zone, the number
#: of arguments, and whether PostgreSQL reads a number in it as POSIX does (west of UTC) or as a
#: numeric zone (make_timestamptz's, ISO 8601's: east). timezone()'s zone is the polyfill grid's
#: row (provisa.transpiler.polyfill.zones).
_ZONE_ARGUMENTS = {"make_timestamptz": (6, 7, False)}


def _posix_zones_for_duckdb(tree: exp.Expr) -> None:  # REQ-2167
    """In place: each zone a statement writes as a literal that PostgreSQL reads as a fixed
    offset (``'12'``, ``'+05:30'``) written as the ICU zone of that offset, which DuckDB knows
    (pg_zones); DuckDB has no zone of PostgreSQL's text."""
    zones: list[tuple[exp.Expr, bool]] = [
        (node.args["zone"], True) for node in tree.find_all(exp.AtTimeZone) if node.args.get("zone")
    ]
    for call in tree.find_all(exp.Anonymous):
        at = _ZONE_ARGUMENTS.get(call.name.lower())
        if at is not None and len(call.expressions) == at[1]:
            zones.append((call.expressions[at[0]], at[2]))
    for zone, posix in zones:
        written = zone
        while isinstance(written, exp.Cast):
            written = written.this
        if isinstance(written, exp.Literal) and written.is_string:
            # On a zone's text alone: pg_zones reads every pinned zone's abbreviations at import.
            from provisa.transpiler import pg_zones

            read = pg_zones.icu_fixed_zone if posix else pg_zones.icu_numeric_zone
            icu = read(written.this)
            if icu is not None:
                written.replace(exp.Literal.string(icu))


def _catalog_functions_unqualified(pg_sql: str) -> str:  # REQ-2167
    """A function named in pg_catalog -- ``pg_catalog."abs"(x)``, as psql, pgjdbc and the
    conformance probe name built-ins -- is that function, as PostgreSQL resolves it: written
    unqualified and read again, so a function DuckDB spells otherwise is translated as any call of
    it is. Left as written, it read as a column of a relation named pg_catalog."""
    if "pg_catalog" not in pg_sql.lower():
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read="postgres")

    def _call(node: exp.Expr) -> exp.Expr:
        if (
            isinstance(node, exp.Column)
            and isinstance(node.this, exp.WithinGroup)
            and node.table.lower() == "pg_catalog"
        ):
            # sqlglot reads the qualifier of an ordered-set aggregate as a column's table.
            ordered = node.this
            args = ", ".join(a.sql(dialect="postgres") for a in ordered.this.expressions)
            called = sqlglot.parse_one(f"{ordered.this.name}({args})", read="postgres")
            return exp.WithinGroup(this=called, expression=ordered.expression)
        if not (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
            and isinstance(node.expression, exp.Func)
        ):
            return node
        function = node.expression
        if not isinstance(function, exp.Anonymous):
            return function.transform(_call)
        part = function.expressions[0] if len(function.expressions) == 2 else None
        part = part.this if isinstance(part, exp.Cast) else part
        if function.name.lower() == "extract" and isinstance(part, exp.Literal) and part.is_string:
            # extract(text, value), as EXTRACT(field FROM value) reads it: its field a keyword,
            # where date_part's is a text (a numeric's answer, not a float8's).
            value = function.expressions[1].transform(_call)
            return exp.Extract(this=exp.var(part.this), expression=value)
        args = ", ".join(a.transform(_call).sql(dialect="postgres") for a in function.expressions)
        return sqlglot.parse_one(f"{function.name}({args})", read="postgres")

    return tree.transform(_call).sql(dialect="postgres")


#: The error a zero divisor raises on DuckDB: PostgreSQL's own, its SQLSTATE 22012 and message
#: (pg_error.raising).
DIVISION_BY_ZERO = "division by zero"


#: What a zero divisor answers on DuckDB: PostgreSQL's 22012, raised by the engine.
_DIVISION_REFUSED = sqlglot.parse_one(
    raising("22012", exp.Literal.string(DIVISION_BY_ZERO).sql(dialect="duckdb")), read="duckdb"
)


def _postgres_semantics_for_duckdb(tree: exp.Expr) -> exp.Expr:  # REQ-2167
    """What PostgreSQL says, said on DuckDB where the two differ. A division or remainder by zero
    is PostgreSQL's error: DuckDB answers infinity or NULL, so each divisor is read through a CASE
    that raises PostgreSQL's message on zero (a nonzero literal divisor is left as it is; any
    other is evaluated twice). An integer divided by an integer is an integer, truncated toward
    zero: DuckDB's ``/`` answers a DOUBLE, its ``//`` PostgreSQL's ``/`` (a non-integer operand
    divides exactly). And a set-returning function aliased in FROM with no column list names its
    one column by the alias, as PostgreSQL's does: DuckDB names the row by it."""

    def _guarded(divisor: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(divisor, exp.Literal) and not divisor.is_string and divisor.to_py() != 0:
            return divisor
        return exp.Case(
            ifs=[
                exp.If(
                    this=exp.EQ(this=divisor.copy(), expression=exp.Literal.number(0)),
                    true=_DIVISION_REFUSED.copy(),
                )
            ],
            default=divisor,
        )

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, (exp.Div, exp.IntDiv, exp.Mod)):
            node.set("expression", _guarded(node.expression))
        elif isinstance(node, exp.TableAlias) and not node.columns and node.this is not None:
            parent = node.parent
            single = (
                isinstance(parent, exp.Table) and isinstance(parent.this, exp.GenerateSeries)
            ) or (isinstance(parent, exp.Unnest) and len(parent.expressions) == 1)
            if single:
                node.set("columns", [node.this.copy()])
        return node

    tree = tree.transform(_transform, copy=False)
    # After the transform, which does not revisit what a replaced node holds (a / (b / c)).
    for div in list(tree.find_all(exp.Div)):
        div.replace(exp.IntDiv(this=div.this, expression=div.expression))
    return tree


def _lengths_for_duckdb(tree: exp.Expr) -> exp.Expr:
    """PostgreSQL's ``length`` and ``octet_length`` of a bytea, a text or a list as DuckDB computes
    them. DuckDB's ``length`` takes no BLOB and its ``octet_length`` no VARCHAR, and which the
    argument is is known only once DuckDB binds it -- every branch of a CASE is bound whatever
    the argument -- so each is a CASE on ``typeof`` (folded at bind) whose branches read the
    argument through TRY_CAST: a BLOB's byte count, a list's elements, a text's characters
    (``length``) or bytes (``strlen``)."""

    def _try_cast(arg: exp.Expression, to: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        return exp.TryCast(this=arg.copy(), to=exp.DataType.build(to, dialect="duckdb"))

    def _of(arg: exp.Expression, text_fn: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        kind = exp.Anonymous(this="typeof", expressions=[arg.copy()])
        branches = [
            exp.If(
                this=exp.EQ(this=kind, expression=exp.Literal.string("BLOB")),
                true=exp.Anonymous(this="octet_length", expressions=[_try_cast(arg, "BLOB")]),
            )
        ]
        if text_fn == "length":
            branches.append(
                exp.If(
                    this=exp.Like(this=kind.copy(), expression=exp.Literal.string("%[]")),
                    true=exp.Length(this=_try_cast(arg, "VARCHAR[]")),
                )
            )
        text_len = exp.Anonymous(this=text_fn, expressions=[_try_cast(arg, "VARCHAR")])
        return exp.Case(ifs=branches, default=text_len)

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.Length) and node.this is not None:
            return _of(node.this.transform(_transform), "length")
        if (
            isinstance(node, exp.Anonymous)
            and node.name.lower() == "octet_length"
            and len(node.expressions) == 1
        ):
            return _of(node.expressions[0].transform(_transform), "strlen")
        return node

    return tree.transform(_transform, copy=False)


#: The targets whose LIKE has no escape character unless one is written (PostgreSQL's is a
#: backslash): each LIKE is given PostgreSQL's explicitly (_like_escape_written).
_LIKE_ESCAPE_WRITTEN = frozenset({"duckdb", "trino"})


# A source whose datetime literal takes no UTC offset: an instant is written as its UTC wall time,
# which the connection's UTC session reads as that instant (REQ-2050, executor/drivers/mysql.py).
_INSTANT_AS_UTC_WALL_TIME = frozenset({"clickhouse", "mysql", "trino", "presto", "athena"})
# The engines whose session is the client's zone (REQ-2171): the wall time is written naming UTC,
# which they read as that instant in every session.
_INSTANT_NAMES_UTC = frozenset({"trino", "presto", "athena"})


def _instants_as_utc(pg_sql: str, target_dialect: str) -> str:
    """``pg_sql`` with each string literal cast to timestamptz that carries an offset written as
    the same instant in UTC, its offset gone. MariaDB reads ``TIMESTAMP('2024-01-01
    20:04:05-05:00')`` as 20:04:05, five hours early (REQ-2050). On Trino, Presto and Athena,
    whose session is the client's zone, the text names UTC."""
    said = pg_sql.upper()
    zoned = "TIMESTAMPTZ" in said or "TIMESTAMP WITH TIME ZONE" in said
    if target_dialect not in _INSTANT_AS_UTC_WALL_TIME or not zoned:
        return pg_sql
    from datetime import datetime, timezone

    tree = sqlglot.parse_one(pg_sql, read="postgres")
    for cast in tree.find_all(exp.Cast):
        literal = cast.this
        if cast.to.this != exp.DataType.Type.TIMESTAMPTZ or not (
            isinstance(literal, exp.Literal) and literal.is_string
        ):
            continue
        try:
            instant = datetime.fromisoformat(literal.this)
        except ValueError:
            continue  # not a date and time (the source refuses it as it is)
        if instant.tzinfo is None:
            continue  # no offset: read in the session's zone, UTC, as written
        utc = instant.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")
        if target_dialect in _INSTANT_NAMES_UTC:
            utc = f"{utc} UTC"
        literal.replace(exp.Literal.string(utc))
    return tree.sql(dialect="postgres")


def _like_escape_written(pg_sql: str, target_dialect: str) -> str:
    """``pg_sql`` with each LIKE and ILIKE that has no ESCAPE clause given PostgreSQL's: a
    backslash escapes the character after it. On a target whose LIKE has no escape character
    unless one is written, ``'a%b' LIKE 'a\\%b'`` was false there and true in the dialect the
    statement is in."""
    if target_dialect not in _LIKE_ESCAPE_WRITTEN or "LIKE" not in pg_sql.upper():
        return pg_sql
    from provisa.codehost.duckdb_sql import escape_likes

    return escape_likes(sqlglot.parse_one(pg_sql, read="postgres")).sql(
        dialect="postgres", copy=False
    )


_LIKE_SAID = re.compile(r"like\b|~~|\bescape\b", re.IGNORECASE)


def _like_patterns_read(pg_sql: str, target_dialect: str) -> str:
    """``pg_sql`` with each LIKE pattern in ClickHouse's form, its ESCAPE read (REQ-2167):
    ClickHouse's LIKE has no ESCAPE clause, and its backslash escapes only %, _ and itself."""
    if target_dialect != "clickhouse" or _LIKE_SAID.search(pg_sql) is None:
        return pg_sql
    from provisa.transpiler import like_patterns

    tree = like_patterns.written(sqlglot.parse_one(pg_sql, read="postgres"))
    return tree.sql(dialect="postgres", copy=False)


def _stamp_inputs_held(pg_sql: str, target_dialect: str) -> str:
    """``pg_sql`` with each text cast to a timestamp without its offset, as PostgreSQL's input
    drops it (REQ-2167): ClickHouse's cast reads it."""
    if target_dialect != "clickhouse" or "TIMESTAMP" not in pg_sql.upper():
        return pg_sql
    from provisa.transpiler.polyfill import instants

    tree = instants.wall_inputs(sqlglot.parse_one(pg_sql, read="postgres"))
    return tree.sql(dialect="postgres", copy=False)


def _null_order_as_key(tree: exp.Expr) -> exp.Expr:
    """#294: DuckDB SQL with each ORDER BY key whose nulls come first written as a null-test key
    of its own (codehost.duckdb_sql, the one form every DuckDB answering a statement runs)."""
    from provisa.codehost.duckdb_sql import null_order_as_key

    return null_order_as_key(tree)


#: A Cypher read's list functions as the IR names them (gql/expr_visitor, Trino's names) whose
#: DuckDB function differs in name or arguments.
_DUCKDB_LIST_FUNCTIONS = frozenset(
    {
        "ELEMENT_AT",
        "SLICE",
        "CARDINALITY",
        "ANY_MATCH",
        "ALL_MATCH",
        "NONE_MATCH",
        "SEQUENCE",
        "JSON_SIZE",
        "JSON_ORDER_KEY",
        "JSON_KIND",
        "JSON_LIST_COMPARE",
        "RANGE_STEP",
        "TYPE_REFUSED",
    }
)


def _three_valued(
    values: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    decided: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    then: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    otherwise: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """A quantifier over predicate ``values`` as Trino's any_match / all_match answer it: ``then``
    where ``decided``, null where a value is null (DuckDB's list_bool_or and list_bool_and skip
    them), else ``otherwise``."""
    has_null = exp.LT(
        this=exp.Anonymous(this="list_count", expressions=[values.copy()]),
        expression=exp.Anonymous(this="len", expressions=[values.copy()]),
    )
    return exp.Case(
        ifs=[exp.If(this=decided, true=then), exp.If(this=has_null, true=exp.Null())],
        default=otherwise,
    )


def _rewrite_list_functions_for_duckdb(tree: exp.Expr) -> exp.Expr:
    """A Cypher read's list functions as DuckDB computes them: a list index ``element_at(l, i)``
    (1-based, negative from the end) is ``list_extract`` and a map key ``element_at(m, 'k')``
    ``map_extract_value`` (DuckDB's element_at is a map's alone, and answers a list of the
    value); ``slice(l, start, length)`` is ``list_slice(l, start, start + length - 1)``; a list's
    ``cardinality`` is ``len``; ``any_match`` / ``all_match`` / ``none_match`` are the list's
    predicate values or'd or and'd, three-valued as Trino's (on an empty list any is false, all
    and none true; a null value with no deciding one answers null);
    ``sequence`` is ``generate_series`` (both inclusive)."""
    if not any(n.name.upper() in _DUCKDB_LIST_FUNCTIONS for n in tree.find_all(exp.Anonymous)):
        return tree

    def _fn(name: str, *args: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        return exp.Anonymous(this=name, expressions=list(args))

    def _predicates(args: list) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        return _fn("list_transform", *args)

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if not isinstance(node, exp.Anonymous) or node.name.upper() not in _DUCKDB_LIST_FUNCTIONS:
            return node
        # Recurse first: transform is pre-order and skips a replaced node's children.
        args = [a.transform(_transform) for a in node.expressions]
        name = node.name.upper()
        if name == "ELEMENT_AT":
            key = args[1]
            is_key = isinstance(key, exp.Literal) and key.is_string
            return _fn("map_extract_value" if is_key else "list_extract", *args)
        if name == "SLICE":
            start, length = args[1], args[2]
            end = exp.Sub(
                this=exp.Add(this=start.copy(), expression=length), expression=exp.Literal.number(1)
            )
            return _fn("list_slice", args[0], start, end)
        if name == "CARDINALITY":
            return _fn("len", *args)
        if name == "SEQUENCE":
            return _fn("generate_series", *args)
        if name == "RANGE_STEP":
            (step,) = args
            return _range_step(step, "error")
        if name == "TYPE_REFUSED":
            (message,) = args
            return _type_refused(message, "error")
        if name == "JSON_ORDER_KEY":
            (value,) = args
            return _json_order_key(
                value, exp.Cast(this=value.copy(), to=exp.DataType.build("VARCHAR"))
            )
        if name == "JSON_KIND":
            (value,) = args
            return _json_kind(exp.Cast(this=value.copy(), to=exp.DataType.build("VARCHAR")))
        if name == "JSON_LIST_COMPARE":
            left, right = args
            return _json_list_compare(left, right, "duckdb")
        if name == "JSON_SIZE":
            # A JSON value's size: a list's items, a string's characters (expr_visitor).
            (value,) = args
            kind = _fn("json_type", value.copy())
            return exp.Case(
                ifs=[
                    exp.If(
                        this=exp.EQ(this=kind, expression=exp.Literal.string("ARRAY")),
                        true=_fn("json_array_length", value.copy()),
                    ),
                    exp.If(
                        this=exp.EQ(this=kind.copy(), expression=exp.Literal.string("VARCHAR")),
                        true=_fn(
                            "length",
                            _fn("json_extract_string", value.copy(), exp.Literal.string("$")),
                        ),
                    ),
                ]
            )
        if name == "ALL_MATCH":
            # A false decides it; otherwise a null leaves it null.
            decided = exp.Not(
                this=exp.Coalesce(
                    this=_fn("list_bool_and", _predicates(args)), expressions=[exp.true()]
                )
            )
            return _three_valued(_predicates(args), decided, exp.false(), exp.true())
        # A true decides any (none is its negation); otherwise a null leaves it null.
        decided = exp.Coalesce(
            this=_fn("list_bool_or", _predicates(args)), expressions=[exp.false()]
        )
        found = _three_valued(_predicates(args), decided, exp.true(), exp.false())
        return found if name == "ANY_MATCH" else exp.Not(this=exp.Paren(this=found))

    # Read back: what is written here by name (len, generate_series) is a typed node to the steps
    # after this one, as it is to DuckDB's parser.
    return sqlglot.parse_one(
        tree.transform(_transform, copy=False).sql(dialect="duckdb"), read="duckdb"
    )


def _list_arg(node: exp.Expression) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]
    """The value a JSON list aggregate collects, or None for any other node."""
    if (
        isinstance(node, exp.Anonymous)
        and node.name.upper() in ("JSON_ARRAYAGG", "JSON_AGG", "JSONB_AGG")
        and node.expressions
    ):
        return node.expressions[0]
    if isinstance(node, exp.JSONArrayAgg):
        return node.this
    return None


def _json_key(key: exp.Expr) -> exp.Expr:
    """A key of json_build_object, as PostgreSQL makes one of any type: its text. DuckDB's
    json_object takes a VARCHAR key alone."""
    if isinstance(key, exp.Literal) and key.is_string:
        return key
    return exp.cast(key, "VARCHAR")


def _rewrite_json_agg_for_duckdb(tree: exp.Expr) -> exp.Expr:
    """Replace JSON_ARRAYAGG(x) / JSON_AGG(x) / JSONB_AGG(x) with json_group_array(x), and
    JSON_BUILD_OBJECT / JSONB_BUILD_OBJECT with json_object.

    The compiler emits Postgres ``json_agg`` for a one-to-many relationship selection
    (compiler/sql_selection.py:431,438); SQLGlot's DuckDB generator writes that as
    ``JSON_ARRAYAGG``, which DuckDB does not register under any of those three spellings —
    its aggregate is ``json_group_array``. Unrewritten, every nested-relationship query
    fails with "Scalar Function with name json_arrayagg does not exist".

    json_group_array over JSON-typed values nests without re-encoding, so the sibling
    ``json_object`` (which DuckDB does register, in the flat k, v form SQLGlot writes)
    needs no rewrite.
    """

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        listed = _list_arg(node.this) if isinstance(node, exp.Filter) else None
        if listed is not None:
            # json_group_array is a macro, which takes no FILTER: a filtered list is the
            # aggregate it expands to, to_json(list(x)), filtered (REQ-005's list cap).
            filtered = exp.Filter(
                this=exp.Anonymous(
                    this="list", expressions=[json_member(listed.transform(_transform))]
                ),
                expression=node.expression.transform(_transform),
            )
            return exp.Anonymous(this="to_json", expressions=[filtered])
        inner = None
        if (
            isinstance(node, exp.Anonymous)
            and node.name.upper() in ("JSON_ARRAYAGG", "JSON_AGG", "JSONB_AGG")
            and node.expressions
        ):
            inner = node.expressions[0]
        elif isinstance(node, exp.JSONArrayAgg):
            inner = node.this
        if inner is not None:
            # Recurse: transform is pre-order and skips a replaced node's children, so a
            # nested aggregate (a one-to-many inside a one-to-many) would survive unrewritten.
            return exp.Anonymous(
                this="json_group_array", expressions=[json_member(inner.transform(_transform))]
            )
        if isinstance(node, exp.Anonymous) and node.name.upper() in (
            "JSON_BUILD_OBJECT",
            "JSONB_BUILD_OBJECT",
        ):
            # PostgreSQL's constructors (JSONB_ over its binary JSON type) are DuckDB's json_object,
            # which takes the same flat key, value, ... arguments; DuckDB registers neither name.
            pairs = [e.transform(_transform) for e in node.expressions]
            return exp.Anonymous(
                this="json_object",
                expressions=[
                    json_member(e) if i % 2 else _json_key(e) for i, e in enumerate(pairs)
                ],
            )
        # Each value a statement puts in JSON, as PostgreSQL writes it at any depth. A value is
        # set in place, so transform goes on into it for the JSON it holds.
        if isinstance(node, exp.JSONObject):
            for pair in node.expressions:
                if isinstance(pair, exp.JSONKeyValue):
                    pair.set("expression", json_member(pair.expression))
        elif isinstance(node, exp.JSONArray):
            node.set("expressions", [json_member(e) for e in node.expressions])
        elif isinstance(node, exp.Anonymous) and node.name.upper() == "JSON_GROUP_OBJECT":
            value = node.expressions[1]
            if isinstance(value, exp.Order):  # value ORDER BY ...: the member is the value
                value.set("this", json_member(value.this))
            else:
                value = json_member(value)
            node.set("expressions", [node.expressions[0], value])
        return node

    return tree.transform(_transform, copy=False)


def rewrite_json_object_to_build_object(pg_sql: str) -> str:  # REQ-902
    """Collapse SQL-standard JSON_OBJECT('k': v, ...) into flat json_build_object('k', v, ...).

    pg_duckdb's transparent execution path cannot emit PostgreSQL 16 nested-JSON syntax
    (JSON_OBJECT with colon separators); its DuckDB executor rejects the colon form and requires the
    flat json_build_object(k, v) form. json_build_object is valid in BOTH plain PostgreSQL and
    pg_duckdb, so this rewrite is safe for the whole pg engine regardless of which sources a query
    touches. Applied by PgBackend.transpile_physical after the base postgres transpile.
    """
    # Parse failure must fail loud: returning input skips the required pg_duckdb JSON rewrite.
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    if tree.find(exp.JSONObject) is None:
        # Nothing to collapse. Written out again, the statement would lose what transpile wrote
        # for PostgreSQL alone, such as pg_catalog."overlaps" (pg_syntax reads it as the routine).
        return pg_sql

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.JSONObject):
            args: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
            for kv in node.expressions or []:
                if not isinstance(kv, exp.JSONKeyValue):
                    return node  # unexpected shape — leave untouched, fail loud downstream
                args.append(kv.this)
                args.append(kv.expression)
            return exp.Anonymous(this="json_build_object", expressions=args)
        return node

    # JSON_OBJECT nodes nest (a value can be a subquery selecting another JSON_OBJECT); one transform
    # pass rewrites only nodes visited before their parent is replaced, so iterate until none remain.
    while list(tree.find_all(exp.JSONObject)):
        tree = tree.transform(_transform)
    return pg_syntax.overlaps_restored(tree).sql(dialect="postgres")


def postgres_try_casts(tree: exp.Expr) -> exp.Expr:  # REQ-778
    """``tree`` for a PostgreSQL server, which has no TRY_CAST: each null-on-failure cast (the
    semantic SQL keeps them, ``semantic_dialect``) is ``CASE WHEN pg_input_is_valid(text, type)
    THEN CAST(...) END`` -- null where the value does not convert. PostgreSQL 16 or later."""

    def _lowered(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.TryCast):
            return node
        # Recurse first: transform is pre-order and skips a replaced node's children (toInteger's
        # TRY_CAST(TRUNC(TRY_CAST(v AS DECIMAL)) AS BIGINT)).
        value = node.this.transform(_lowered)
        valid = exp.Anonymous(
            this="PG_INPUT_IS_VALID",
            expressions=[
                exp.Cast(this=value.copy(), to=exp.DataType.build("TEXT")),
                exp.Literal.string(node.to.sql(dialect="postgres")),
            ],
        )
        operand = value.copy()
        if not any(operand.find_all(exp.Column, exp.Placeholder, exp.Parameter)):
            # A constant's cast is folded when the statement is planned, whatever the CASE
            # decides: PostgreSQL would refuse the value it was asked to test. Its scalar
            # subquery is not folded.
            operand = exp.Subquery(this=exp.select(operand))
        cast = exp.Cast(this=operand, to=node.to.copy())
        return exp.Case(ifs=[exp.If(this=valid, true=cast)])

    return tree.transform(_lowered)


def postgres_empty_arrays(tree: exp.Expr) -> exp.Expr:
    """``tree`` for a PostgreSQL server, which types no empty ARRAY[]: one a COALESCE falls back to
    (collect() of nothing) is '{}', which PostgreSQL reads as the other argument's array type."""

    def _typed(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Coalesce):
            for i, arg in enumerate(node.expressions):
                if isinstance(arg, exp.Array) and not arg.expressions:
                    node.expressions[i] = exp.Literal.string("{}")
        return node

    return tree.transform(_typed)


def postgres_array_filters(tree: exp.Expr) -> exp.Expr:  # REQ-778
    """``tree`` for a PostgreSQL server, which has no FILTER: each filtered list (the semantic SQL
    keeps ``FILTER(l, y -> p)``, ``semantic_dialect``) is ``ARRAY(SELECT y FROM UNNEST(l) AS
    _t(y) WHERE p)``, null for a null ``l`` (as Trino's filter), never the empty list."""

    def _lowered(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.ArrayFilter):
            return node
        cond = node.expression
        if not (isinstance(cond, exp.Lambda) and len(cond.expressions) == 1):
            raise ValueError(f"A filtered list takes one element variable: {node.sql()}")
        var = cond.expressions[0].name
        # Recurse first: transform is pre-order and skips a replaced node's children.
        values = node.this.transform(_lowered)
        filtered = (
            exp.select(var).from_(_pg_unnested(values, var)).where(cond.this.transform(_lowered))
        )
        return _pg_null_list(values, exp.Array(expressions=[filtered]))

    return tree.transform(_lowered)


def postgres_list_ranges(tree: exp.Expr) -> exp.Expr:  # REQ-345
    """``tree`` for a PostgreSQL server, which has no ``sequence``: each list a Cypher range()
    is (gql/translator_helpers names it Trino's ``sequence(start, end[, step])``) is
    ``ARRAY(SELECT generate_series(start, end[, step]))``, inclusive of both ends as range() is.
    PostgreSQL's generate_series returns rows, so they are collected into the list."""

    def _lowered(node: exp.Expr) -> exp.Expr:
        if not (isinstance(node, exp.Anonymous) and node.name.upper() == "SEQUENCE"):
            return node
        # Recurse first: transform is pre-order and skips a replaced node's children.
        args = [a.transform(_lowered) for a in node.expressions]
        series = exp.Anonymous(this="generate_series", expressions=args)
        return exp.Array(expressions=[exp.select(series)])

    return tree.transform(_lowered)


_SEQUENCE_CALLED = re.compile(r"\bsequence\s*\(", re.IGNORECASE)


def _pg_raised(sqlstate: str, message: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """PostgreSQL's error ``sqlstate`` with ``message`` as its own text, where it is reached:
    provisa_raise, which the pg engine installs (pg_runtime), as an integer. It is volatile, so
    it is never folded, and raised, when the statement is planned."""
    raised = exp.Anonymous(
        this=SERVER_FORM_RAISE, expressions=[exp.Literal.string(sqlstate), message.copy()]
    )
    return exp.Cast(this=raised, to=exp.DataType.build("INT"))


def _pg_json_text(value: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """A JSON value's JSON text on PostgreSQL, from a json or a jsonb."""
    return exp.Cast(this=value.copy(), to=exp.DataType.build("TEXT"))


def _pg_json(value: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """A JSON value as PostgreSQL's json, from a json or a jsonb."""
    return exp.Cast(this=_pg_json_text(value), to=exp.DataType.build("JSON"))


def _pg_json_scalar(value: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """A JSON scalar's text on PostgreSQL: a string unquoted, a number or boolean as written."""
    return exp.JSONBExtractScalar(
        this=exp.Cast(this=_pg_json_text(value), to=exp.DataType.build("JSONB")),
        expression=exp.Literal.string("{}"),
    )


def _pg_binary(text: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """``text`` compared by its characters' codes, as Trino and DuckDB compare text (and
    openCypher orders strings), never by the database's collation."""
    return exp.Collate(this=text, expression=exp.to_identifier("C", quoted=True))


def _pg_sign(a: exp.Expression, b: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """-1, 0 or 1 as ``a`` is below, equal to or above ``b``."""
    return exp.Case(
        ifs=[
            exp.If(this=exp.LT(this=a, expression=b), true=exp.Literal.number(-1)),
            exp.If(this=exp.GT(this=a.copy(), expression=b.copy()), true=exp.Literal.number(1)),
        ],
        default=exp.Literal.number(0),
    )


def _pg_json_list_compare(
    left: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    right: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    depth: int = _LIST_COMPARE_DEPTH,
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """json_list_compare(a, b) on PostgreSQL, as _json_list_compare answers it on Trino and
    DuckDB: the first element pair (by position) that is not equal decides, else the shorter list
    is smaller; 2 for a pair that cannot be compared, which the caller reads as null."""
    a, b, z = f"_a{depth}", f"_b{depth}", f"_z{depth}"

    def items(of: str, value: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        listed = exp.Table(
            this=exp.Anonymous(this="json_array_elements", expressions=[_pg_json(value)]),
            ordinality=True,
            alias=exp.TableAlias(
                this=exp.to_identifier(of), columns=[exp.to_identifier("e"), exp.to_identifier("n")]
            ),
        )
        return listed

    def item(of: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        return exp.column("e", table=of)

    ea, eb = item(a), item(b)
    kind_a, kind_b = (_json_kind(_pg_json_text(e)) for e in (ea, eb))

    def both(kind: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        return exp.and_(  # pyright: ignore[reportReturnType]
            exp.EQ(this=kind_a.copy(), expression=exp.Literal.string(kind)),
            exp.EQ(this=kind_b.copy(), expression=exp.Literal.string(kind)),
        )

    def as_(e: exp.Expression, to: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        return exp.Cast(this=_pg_json_scalar(e), to=exp.DataType.build(to))

    if depth == 1:
        nested = _pg_raised("54001", exp.Literal.string(LIST_COMPARE_TOO_DEEP))
    else:
        nested = _pg_json_list_compare(ea.copy(), eb.copy(), depth - 1)
    pair = exp.Case(
        ifs=[
            exp.If(
                this=both("number"),
                true=_pg_sign(as_(ea, "DOUBLE PRECISION"), as_(eb, "DOUBLE PRECISION")),
            ),
            exp.If(
                this=both("string"),
                true=_pg_sign(_pg_binary(_pg_json_scalar(ea)), _pg_binary(_pg_json_scalar(eb))),
            ),
            exp.If(this=both("boolean"), true=_pg_sign(as_(ea, "BOOLEAN"), as_(eb, "BOOLEAN"))),
            exp.If(this=both("list"), true=nested),
        ],
        default=exp.Literal.number(2),
    )
    decided = (
        exp.select(exp.column("c", table=z))
        .from_(
            exp.select(exp.column("n", table=a), exp.alias_(pair, "c"))
            .from_(items(a, left))
            .join(
                items(b, right),
                on=exp.EQ(this=exp.column("n", table=a), expression=exp.column("n", table=b)),
            )
            .subquery(z)
        )
        .where(exp.NEQ(this=exp.column("c", table=z), expression=exp.Literal.number(0)))
        .order_by(exp.column("n", table=z))
        .limit(1)
    )
    sizes = [
        exp.Anonymous(this="json_array_length", expressions=[_pg_json(v)]) for v in (left, right)
    ]
    return exp.Coalesce(this=exp.Subquery(this=decided), expressions=[_pg_sign(*sizes)])


def _pg_matched(
    name: str,
    values: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
    test: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """any_match / all_match / none_match(l[, x -> p]) on PostgreSQL, three-valued as Trino's: on
    an empty list any is false, all and none true; a value that decides answers (a true any, a
    false all); else a null predicate value answers null. A null list is null."""
    if test is None:
        var, predicate = "_x", exp.column("_x")
    else:
        if not (isinstance(test, exp.Lambda) and len(test.expressions) == 1):
            raise ValueError(f"A quantifier takes one element variable: {name}")
        var, predicate = test.expressions[0].name, test.this
    unknown = exp.GT(this=exp.Count(this=exp.Star()), expression=exp.Count(this=predicate.copy()))
    if name == "ALL_MATCH":
        decided = exp.Not(
            this=exp.Coalesce(
                this=exp.Anonymous(this="bool_and", expressions=[predicate.copy()]),
                expressions=[exp.true()],
            )
        )
        then, otherwise = exp.false(), exp.true()
    else:
        decided = exp.Coalesce(
            this=exp.Anonymous(this="bool_or", expressions=[predicate.copy()]),
            expressions=[exp.false()],
        )
        then, otherwise = exp.true(), exp.false()
        if name == "NONE_MATCH":
            then, otherwise = otherwise, then
    answer = exp.Case(
        ifs=[exp.If(this=decided, true=then), exp.If(this=unknown, true=exp.Null())],
        default=otherwise,
    )
    over = exp.select(answer).from_(_pg_unnested(values, var))
    return _pg_null_list(values, exp.Subquery(this=over))


def _pg_unnested(values: exp.Expression, var: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """The list ``values`` as rows of ``var``: UNNEST(l) AS _t(var)."""
    unnest = exp.Unnest(expressions=[values.copy()])
    unnest.set(
        "alias", exp.TableAlias(this=exp.to_identifier("_t"), columns=[exp.to_identifier(var)])
    )
    return unnest


def _pg_null_list(values: exp.Expression, listed: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """``listed``, read from the rows of ``values``, or null for a null list (whose rows are none)."""
    return exp.Case(
        ifs=[exp.If(this=exp.Is(this=values.copy(), expression=exp.Null()), true=exp.Null())],
        default=listed,
    )


def _pg_reduced(node: exp.Reduce) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """reduce(l, init, (acc, x) -> step, acc -> acc), a fold over a list, on PostgreSQL: a
    recursive scalar subquery from ``init``, each row the next item folded in, the last row's
    accumulator the answer. A null list is null."""
    values, merge, finish = node.this, node.args["merge"], node.args.get("finish")
    if not (isinstance(merge, exp.Lambda) and len(merge.expressions) == 2):
        raise ValueError(f"A fold takes an accumulator and an element variable: {node.sql()}")
    acc, var = (e.name for e in merge.expressions)
    if finish is not None and not (
        isinstance(finish, exp.Lambda)
        and [e.name for e in finish.expressions] == [acc]
        and isinstance(finish.this, (exp.Column, exp.Identifier))
        and finish.this.name == acc
    ):
        raise ValueError(f"A fold's finish is its accumulator: {node.sql()}")
    done = exp.column("_n", table="_r")
    at = exp.Add(this=done, expression=exp.Literal.number(1))
    item = exp.Bracket(this=exp.Paren(this=values.copy()), expressions=[at], offset=1)

    def _read(n: exp.Expr) -> exp.Expr:
        # A lambda's variable is a bare identifier (or an unqualified column) in its body.
        bare = (isinstance(n, exp.Column) and not n.table) or (
            isinstance(n, exp.Identifier)
            and not isinstance(n.parent, (exp.Column, exp.Table, exp.TableAlias, exp.Alias))
            and not (isinstance(n.parent, exp.Dot) and n.arg_key == "expression")
        )
        if bare and n.name == var:
            return item.copy()
        if bare and n.name == acc:
            return exp.column(acc, table="_r")
        return n

    folded = (
        exp.select(at.copy(), merge.this.transform(_read))
        .from_("_r")
        .where(
            exp.LT(
                this=done.copy(),
                expression=exp.Anonymous(this="cardinality", expressions=[values.copy()]),
            )
        )
    )
    rows = exp.union(
        exp.select(exp.Literal.number(0), node.args["initial"].copy()), folded, distinct=False
    )
    named = exp.TableAlias(
        this=exp.to_identifier("_r"), columns=[exp.to_identifier("_n"), exp.to_identifier(acc)]
    )
    last = (
        exp.select(exp.column(acc))
        .from_("_r")
        .order_by(exp.Ordered(this=exp.column("_n"), desc=True))
        .limit(1)
    )
    last.set("with_", exp.With(expressions=[exp.CTE(this=rows, alias=named)], recursive=True))
    return _pg_null_list(values, exp.Subquery(this=last))


def postgres_list_functions(tree: exp.Expr) -> exp.Expr:  # REQ-345
    """``tree`` for a PostgreSQL server, which has none of the list functions a Cypher read names
    (gql/expr_visitor writes Trino's, _rewrite_list_functions_for_duckdb DuckDB's) nor lambdas:

    - ``element_at(l, i)``, 1-based with a negative ``i`` from the end, is ``(l)[i]``, from the
      end ``(l)[cardinality(l) + 1 + i]``; null past either end, as Trino's;
    - ``slice(l, start, length)`` is ``(l)[start:start + length - 1]``;
    - ``transform(l, x -> e)`` is ``ARRAY(SELECT e FROM UNNEST(l) AS _t(x))``, null for a null l;
    - ``any_match`` / ``all_match`` / ``none_match`` aggregate the predicate over the list's
      rows (_pg_matched); ``reduce`` is a recursive fold (_pg_reduced);
    - ``range_step(s)`` is ``s``, raising where it is 0; ``type_refused('m')`` raises ``m`` where
      its branch is reached;
    - ``json_kind``, ``json_size``, ``json_order_key`` and ``json_list_compare`` read a JSON
      value's text, as on the other engines.

    A map's key (``element_at(m, 'k')``) is not a list's index: it is postgres_maps'."""

    def _lowered(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Case):
            return _refused_branches(node)
        if isinstance(node, exp.Transform):
            values = node.this.transform(_lowered)
            fn = node.expression
            if not (isinstance(fn, exp.Lambda) and len(fn.expressions) == 1):
                raise ValueError(f"A projected list takes one element variable: {node.sql()}")
            var = fn.expressions[0].name
            listed = exp.select(fn.this.transform(_lowered)).from_(_pg_unnested(values, var))
            return _pg_null_list(values, exp.Array(expressions=[listed]))
        if isinstance(node, exp.Reduce):
            return _pg_reduced(
                exp.Reduce(
                    **{k: v.transform(_lowered) for k, v in node.args.items() if v is not None}
                )
            )
        if not isinstance(node, exp.Anonymous) or node.name.upper() not in _PG_LIST_FUNCTIONS:
            return node
        # Recurse first: transform is pre-order and skips a replaced node's children.
        args = [a.transform(_lowered) for a in node.expressions]
        name = node.name.upper()
        if name == "ELEMENT_AT":
            values, at = args
            if isinstance(at, exp.Literal) and at.is_string:
                return exp.Anonymous(this=node.name, expressions=args)
            return _pg_element_at(values, at)
        if name == "SLICE":
            values, start, length = args
            end = exp.Sub(
                this=exp.Add(this=start.copy(), expression=length), expression=exp.Literal.number(1)
            )
            return exp.Bracket(
                this=exp.Paren(this=values),
                expressions=[exp.Slice(this=start, expression=end)],
                offset=1,
            )
        if name in ("ANY_MATCH", "ALL_MATCH", "NONE_MATCH"):
            return _pg_matched(name, args[0], args[1] if len(args) > 1 else None)
        if name == "RANGE_STEP":
            (step,) = args
            failed = _pg_raised("22023", exp.Literal.string(RANGE_STEP_ZERO))
            return exp.Case(
                ifs=[
                    exp.If(
                        this=exp.EQ(this=step.copy(), expression=exp.Literal.number(0)), true=failed
                    )
                ],
                default=step,
            )
        if name == "TYPE_REFUSED":
            (message,) = args
            return _pg_raised("42804", message)
        if name == "JSON_KIND":
            (value,) = args
            return _json_kind(_pg_json_text(value))
        if name == "JSON_SIZE":
            # A JSON value's size: a list's items, a string's characters (expr_visitor).
            (value,) = args
            kind = _json_kind(_pg_json_text(value))
            return exp.Case(
                ifs=[
                    exp.If(
                        this=exp.EQ(this=kind, expression=exp.Literal.string("list")),
                        true=exp.Anonymous(this="json_array_length", expressions=[_pg_json(value)]),
                    ),
                    exp.If(
                        this=exp.EQ(this=kind.copy(), expression=exp.Literal.string("string")),
                        true=exp.Length(this=_pg_json_scalar(value)),
                    ),
                ]
            )
        if name == "JSON_ORDER_KEY":
            (value,) = args
            text = _pg_json_text(value)
            number = exp.TryCast(this=text.copy(), to=exp.DataType.build("DOUBLE"))
            key = _json_order_key(
                _pg_json(value),
                text,
                field=lambda v, k: exp.Anonymous(
                    this="json_extract_path", expressions=[v.copy(), exp.Literal.string(k)]
                ),
                number=postgres_try_casts(number),  # pyright: ignore[reportArgumentType]
            )
            *ranks, ended = key.expressions
            return exp.Anonymous(this=key.name, expressions=[*ranks, _pg_binary(ended)])
        left, right = args  # JSON_LIST_COMPARE
        return exp.Nullif(this=_pg_json_list_compare(left, right), expression=exp.Literal.number(2))

    def _refused_branches(case: exp.Case) -> exp.Expr:
        """A CASE whose branch is type_refused('m'): PostgreSQL's raise is an integer, which the
        CASE's other branches need not be, so the raise is the branch's test, reached where the
        test holds (CASE WHEN (CASE WHEN test THEN raise = 0 END) THEN NULL ...)."""
        for branch in case.args.get("ifs") or []:
            refused = branch.args.get("true")
            if isinstance(refused, exp.Anonymous) and refused.name.upper() == "TYPE_REFUSED":
                (message,) = refused.expressions
                raised = exp.EQ(this=_pg_raised("42804", message), expression=exp.Literal.number(0))
                branch.set("this", exp.Case(ifs=[exp.If(this=branch.this, true=raised)]))
                branch.set("true", exp.Null())
        return case

    return tree.transform(_lowered)


def _pg_element_at(values: exp.Expression, at: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """element_at(l, i) on PostgreSQL: ``(l)[i]``, a negative ``i`` counted from the end."""

    def read(index: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        return exp.Bracket(this=exp.Paren(this=values.copy()), expressions=[index], offset=1)

    size = exp.Anonymous(this="cardinality", expressions=[values.copy()])
    from_end = exp.Add(
        this=exp.Add(this=size, expression=exp.Literal.number(1)), expression=at.copy()
    )
    if isinstance(at, exp.Literal) or (
        isinstance(at, exp.Neg) and isinstance(at.this, exp.Literal)
    ):
        return read(from_end if int(at.to_py()) < 0 else at.copy())
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.LT(this=at.copy(), expression=exp.Literal.number(0)), true=read(from_end)
            )
        ],
        default=read(at.copy()),
    )


def postgres_maps(tree: exp.Expr) -> exp.Expr:  # REQ-345
    """``tree`` for a PostgreSQL server, which has no MAP type: a Cypher map is jsonb.

    - ``MAP(ARRAY[k, ...], ARRAY[v, ...])`` (gql/expr_visitor, each value its JSON) is
      ``JSONB_BUILD_OBJECT(k, v, ...)``;
    - a key's value, ``element_at(m, 'k')``, is ``m -> 'k'``, null where the map has no such key;
      read as JSON (``CAST(element_at(m, 'k') AS JSON)``) it is that jsonb, which PostgreSQL casts
      to a number or a boolean as it casts no json;
    - ``map_keys(m)`` is ``ARRAY(SELECT jsonb_object_keys(m))``, null for a null map."""

    def _key_read(node: exp.Expr) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]
        if not (isinstance(node, exp.Anonymous) and node.name.upper() == "ELEMENT_AT"):
            return None
        held, key = node.expressions
        if not (isinstance(key, exp.Literal) and key.is_string):
            return None
        as_jsonb = exp.Cast(this=held.transform(_lowered), to=exp.DataType.build("JSONB"))
        return exp.JSONExtract(this=as_jsonb, expression=key.copy(), only_json_types=True)

    def _lowered(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.JSON):
            read = _key_read(node.this)
            if read is not None:
                return read
        read = _key_read(node)
        if read is not None:
            return read
        if isinstance(node, exp.Map):
            keys, values = node.args.get("keys"), node.args.get("values")
            if not (isinstance(keys, exp.Array) and isinstance(values, exp.Array)):
                raise ValueError(f"A map is built from its keys and values listed: {node.sql()}")
            pairs = [
                part.transform(_lowered)
                for pair in zip(keys.expressions, values.expressions, strict=True)
                for part in pair
            ]
            return exp.Anonymous(this="JSONB_BUILD_OBJECT", expressions=pairs)
        if isinstance(node, exp.MapKeys):
            held = node.this.transform(_lowered)
            named = exp.Anonymous(
                this="JSONB_OBJECT_KEYS",
                expressions=[exp.Cast(this=held.copy(), to=exp.DataType.build("JSONB"))],
            )
            return _pg_null_list(held, exp.Array(expressions=[exp.select(named)]))
        return node

    return tree.transform(_lowered)


_MAP_CALLED = re.compile(r"\b(?:map|map_keys|element_at)\s*\(", re.IGNORECASE)


def postgres_json_of_text(tree: exp.Expr) -> exp.Expr:  # REQ-345
    """``tree`` for a PostgreSQL server, which cannot tell the type of a quoted literal TO_JSON
    is given (to_json is polymorphic): each such literal (an item of a Cypher list of several
    types, gql/expr_visitor) is the text it is."""

    def _typed(node: exp.Expr) -> exp.Expr:
        if (
            isinstance(node, exp.Anonymous)
            and node.name.upper() == "TO_JSON"
            and len(node.expressions) == 1
            and isinstance(node.expressions[0], exp.Literal)
            and node.expressions[0].is_string
        ):
            text = exp.Cast(this=node.expressions[0].copy(), to=exp.DataType.build("TEXT"))
            return exp.Anonymous(this=node.name, expressions=[text])
        return node

    return tree.transform(_typed)


_TO_JSON_CALLED = re.compile(r"\bto_json\s*\(\s*'", re.IGNORECASE)

#: The list functions postgres_list_functions renders, as the IR names them.
_PG_LIST_FUNCTIONS = frozenset(
    {
        "ELEMENT_AT",
        "SLICE",
        "ANY_MATCH",
        "ALL_MATCH",
        "NONE_MATCH",
        "RANGE_STEP",
        "TYPE_REFUSED",
        "JSON_KIND",
        "JSON_SIZE",
        "JSON_ORDER_KEY",
        "JSON_LIST_COMPARE",
    }
)
_PG_LIST_CALLED = re.compile(
    r"\b(?:%s|transform|reduce)\s*\(" % "|".join(n.lower() for n in _PG_LIST_FUNCTIONS),
    re.IGNORECASE,
)


def snowflake_try_casts(tree: exp.Expr) -> exp.Expr:  # REQ-778
    """``tree`` for Snowflake, whose TRY_CAST takes text only (sqlglot writes a plain CAST of a
    value it cannot see is text): each null-on-failure cast reads its value as text first."""

    def _lowered(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.TryCast):
            return node
        text = exp.Cast(this=node.this.copy(), to=exp.DataType.build("VARCHAR"))
        return exp.TryCast(this=text, to=node.to.copy(), requires_string=True)

    return tree.transform(_lowered)


# A number as MySQL's CAST reads one -- the text it converts, rather than reading 0 or a prefix.
_MYSQL_INTEGER = "^[+-]?[0-9]+$"
_MYSQL_REAL = "^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$"


def mysql_try_casts(tree: exp.Expr) -> exp.Expr:  # REQ-778
    """``tree`` for MySQL, which has no null-on-failure cast and whose CAST of 'ab' is 0: a number
    is converted where its text reads as one, a boolean where it is 'true' or 'false', else null.
    A cast to text never fails and stays one."""

    def _lowered(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.TryCast):
            return node
        value, to = node.this, node.to
        cast = exp.Cast(this=value.copy(), to=to.copy())
        trimmed = exp.Trim(this=value.copy())
        if to.is_type(*exp.DataType.INTEGER_TYPES):
            pattern = _MYSQL_INTEGER
        elif to.is_type(*exp.DataType.REAL_TYPES):
            pattern = _MYSQL_REAL
        elif to.is_type(exp.DataType.Type.BOOLEAN):
            return exp.Case(
                this=exp.Lower(this=trimmed),
                ifs=[
                    exp.If(this=exp.Literal.string("true"), true=exp.true()),
                    exp.If(this=exp.Literal.string("false"), true=exp.false()),
                ],
            )
        else:
            return cast
        matches = exp.RegexpLike(this=trimmed, expression=exp.Literal.string(pattern))
        return exp.Case(ifs=[exp.If(this=matches, true=cast)])

    return tree.transform(_lowered)


def clickhouse_try_casts(tree: exp.Expr) -> exp.Expr:  # REQ-778
    """``tree`` for ClickHouse, whose CAST to a Nullable type still fails on text that does not
    parse: each null-on-failure cast is ``accurateCastOrNull``."""

    def _lowered(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.TryCast):
            return node
        to = node.to.copy()
        to.set("nullable", False)
        return exp.Anonymous(
            this="accurateCastOrNull",
            expressions=[node.this.copy(), exp.Literal.string(to.sql(dialect="clickhouse"))],
        )

    return tree.transform(_lowered)


#: The targets whose own null-on-failure cast sqlglot does not write (REQ-778).
#: The dialects whose cast does not read an array's text form ('{1,2}'), array_in's.
_ARRAY_TEXT_UNREAD = ("trino", "presto", "athena", "clickhouse")

_OWN_TRY_CASTS = {
    "postgres": postgres_try_casts,
    "snowflake": snowflake_try_casts,
    "mysql": mysql_try_casts,
    "clickhouse": clickhouse_try_casts,
}


def transpile(
    pg_sql: str, target_dialect: str, *, pgvector: bool = False, quote_identifiers: bool = False
) -> str:  # REQ-066, REQ-068, REQ-229, REQ-423
    """Transpile PostgreSQL-dialect SQL to a target dialect.

    Args:
        pg_sql: SQL string in PostgreSQL dialect with double-quoted identifiers.
        target_dialect: SQLGlot dialect name (e.g. "trino", "postgres", "mysql").
        pgvector: the statement runs on a PostgreSQL source with the pgvector extension, whose
            own operator a similarity search is then written with (REQ-423).
        quote_identifiers: send every identifier quoted, for a source written in another
            dialect's text whose database folds an unquoted name to upper case (REQ-1671).

    Returns:
        SQL string in target dialect.
    """
    pg_sql = pg_forms_type_io.readable(pg_sql, target_dialect)  # REQ-2171
    pg_sql = _within_group_unqualified(pg_sql)
    if target_dialect != "duckdb":  # DuckDB's form is pg_functions.typed_calls'
        pg_sql = pg_functions.pg_xml.for_dialect(pg_sql, target_dialect)  # REQ-2167
        pg_sql = _regexp_replace_by_name(pg_sql)  # its flag is read below
    if target_dialect != "postgres":
        pg_sql = exact_inputs(pg_sql)  # REQ-2171
    pg_sql = pg_forms_enum.lowered(pg_money.lowered(pg_sql, target_dialect), target_dialect)
    # REQ-1730: an identifier UNQUOTED in the source SQL (the common case for a raw/direct query —
    # governed GraphQL/Cypher-compiled queries already force `quoted=True`, see stage2.py's
    # `_expand_star`) stays unquoted through the transpile by default, which is harmless for every
    # target whose own unquoted-identifier folding already matches Provisa's lowercase DDL
    # (Postgres/Trino/DuckDB/MySQL fold to lowercase; BigQuery is case-insensitive) — but Snowflake
    # is the one dialect here that folds an UNQUOTED identifier to UPPERCASE by default, so
    # `product_id` (unquoted) resolved to a column literally named `PRODUCT_ID`, which never
    # exists — every landed/attached table's columns are created quoted-lowercase (REQ-1652's own
    # convention). Verified live (REQ-1730 engine-swap harness, 2026-09-20): first-ever live
    # mongodb -> snowflake query, "invalid identifier 'PRODUCT_ID'". `identify=True` forces every
    # identifier in the OUTPUT to be quoted regardless of how it was written in the input, which is
    # a no-op for the already-quoted governed path and the fix for the raw-SQL path.
    # REQ-1730: Oracle joins Snowflake here, for the identical reason — an unquoted identifier
    # folds to UPPERCASE by default. Unlike Snowflake (whose store DDL already always quotes,
    # REQ-1652), Oracle's own SQLAlchemy dialect used to leave a plain lowercase Python identifier
    # UNQUOTED (verified live: `CreateTable` compiled `product_id` bare) — fixed at the DDL side
    # too (materialize_exec.py's `build_table`, `quoted_name(..., quote=True)`) so both sides of
    # the seam agree on always-quoted.
    identify = quote_identifiers or target_dialect in ("snowflake", "oracle")
    if target_dialect in ("trino", "athena") and pg_sql.lstrip().upper().startswith(
        ("INSERT", "UPDATE")
    ):
        # REQ-1981: Trino's INSERT and UPDATE grammar has no target alias (``UPDATE t AS x SET``
        # is a syntax error, verified live on Athena); a DIRECT write reaches here with the
        # pipeline's self-alias on its target.
        from provisa.compiler.sql_rewrite import unalias_write_target

        pg_sql = unalias_write_target(pg_sql)
    pg_sql = _every_match_unflagged(pg_sql, target_dialect)
    pg_sql = _like_escape_written(pg_sql, target_dialect)
    pg_sql = _like_patterns_read(pg_sql, target_dialect)
    pg_sql = _stamp_inputs_held(pg_sql, target_dialect)
    pg_sql = _instants_as_utc(pg_sql, target_dialect)
    lowered = _lower_universal_regex(pg_sql, target_dialect)
    if target_dialect == "postgres" and "FILTER(" in pg_sql.upper():
        lowered = postgres_array_filters(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect == "postgres" and _SEQUENCE_CALLED.search(pg_sql):
        lowered = postgres_list_ranges(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect == "postgres" and _TO_JSON_CALLED.search(pg_sql):
        lowered = postgres_json_of_text(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect == "postgres" and _MAP_CALLED.search(pg_sql):
        lowered = postgres_maps(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect == "postgres" and _PG_LIST_CALLED.search(pg_sql):
        lowered = postgres_list_functions(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect == "postgres" and "ARRAY[]" in pg_sql.upper():
        lowered = postgres_empty_arrays(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    from provisa.transpiler.native_subtype import MARKER as _SUBTYPE_MARKER, resolve_subtypes

    if target_dialect != "duckdb" and target_dialect != "postgres" and "in_range" in pg_sql.lower():
        function_macros.refuse_named(sqlglot.parse_one(pg_sql, read="postgres"), target_dialect)
    if target_dialect != "postgres" and "range" in pg_sql.lower():
        # REQ-2171: ranges and multiranges, which only PostgreSQL has, as text and its parts.
        from provisa.transpiler import pg_ranges

        lowered = pg_ranges.rewrite(
            function_macros.range_rows_chosen(
                lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
            ),
            target_dialect,
        )
    if _SUBTYPE_MARKER in pg_sql.lower():
        # REQ-2171: each sort, filter, join or operator on a subtyped column, in this dialect's
        # native type, an emulation, or refused by name.
        lowered = resolve_subtypes(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres"),
            target_dialect,
        )
    if target_dialect in _OWN_TRY_CASTS and "TRY_CAST" in pg_sql.upper():
        lowered = _OWN_TRY_CASTS[target_dialect](
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    # REQ-2171: each read of an instant in the session's zone, on what is not told it. Before the
    # functions, so what they write of their own text is not read as a statement's.
    lowered = zoned_instants.written(pg_sql, lowered, target_dialect)
    if target_dialect in _ARRAY_TEXT_UNREAD:
        # REQ-2171: an array's text form as its elements, before a function's form reads it. Each
        # cast is read on the tree (REQ-913), never by a substring of the statement.
        lowered = array_inputs(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if function_macros.mentioned(pg_sql, target_dialect):
        # REQ-2167: each function the target does not have, in its form there, or refused by name.
        lowered = function_macros.inline(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres"),
            target_dialect,
        )
    if target_dialect not in ("postgres", "duckdb") and _CAST_LITERAL_NAMED.search(pg_sql):
        lowered = literals_as_said(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect != "postgres" and pg_held_types.mentioned(pg_sql):
        # REQ-2171: a value of a type only PostgreSQL has, as what the target holds it as. After
        # the functions: the type a statement casts to chooses the overload a call reads.
        lowered = pg_held_types.held(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect == "clickhouse":
        # REQ-2171: its instants and its refusals as PostgreSQL's, whatever wrote them.
        lowered = clickhouse_semantics(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    if target_dialect == "exasol":
        result = _exasol_stored_case(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    elif lowered is not None:
        # In place, the tree being this function's; a root a step lifted out of its parent keeps
        # its copy, for the generator reads a root's parent.
        result = lowered.sql(
            dialect=target_dialect, identify=identify, copy=lowered.parent is not None
        )
    else:
        results = sqlglot.transpile(
            pg_sql, read="postgres", write=target_dialect, identify=identify
        )
        if not results:
            raise ValueError(f"SQLGlot produced no output for: {pg_sql!r}")
        result = results[0]
    if target_dialect == "tsql":
        result = _map_tsql_reserved_schemas(result)
    if target_dialect == "sqlite":
        # SQLite has no schema/catalog concept — a direct-execution write against a sqlite
        # source must reference the bare table name (matches provisa/observability/ops_schema.py's
        # `use_schema = engine.dialect.name != "sqlite"` convention elsewhere in the codebase).
        result = _strip_schema_qualifiers(result)
    # REQ-1965: each read of a natively array/struct/map column becomes this backend's own
    # to-JSON expression here, the one place every route's dialect is known.
    result = resolve_native_json(result, target_dialect)
    # REQ-2171: each subtyped column read as a value, in PostgreSQL's canonical text.
    result = resolve_reads(result, target_dialect)
    from provisa.vector.similarity import lower as _lower_similarity

    # REQ-423: a governed similarity search, in the target's own form. REQ-2000: Qdrant's driver
    # reads the governed call itself and runs the search through Qdrant's API.
    if target_dialect != "qdrant":
        result = _lower_similarity(result, target_dialect, pgvector=pgvector)
    # REQ-2171: last, for what a later step parses has no routine definitions at its head.
    result = pg_forms_geometric.planned(result, target_dialect)
    if target_dialect == "postgres" and pg_syntax.OVERLAPS in result.lower():
        result = pg_syntax.overlaps_restored(sqlglot.parse_one(result, read="postgres")).sql(
            dialect="postgres"
        )
    from provisa.observability.stage_trace import trace_stage

    trace_stage(f"transpile.{target_dialect}", result)
    return result


_REGEXP_REPLACE_BY_NAME = re.compile(r'\bpg_catalog\s*\.\s*"?regexp_replace"?\s*\(', re.IGNORECASE)


def _regexp_replace_by_name(pg_sql: str) -> str:
    """``pg_sql`` with each pg_catalog.regexp_replace(...) the bare call (REQ-2167): sqlglot reads
    the qualified one as a function it does not know, and the bare one as REGEXP_REPLACE, whose
    flag the rewrites below read. PostgreSQL resolves the bare built-in in pg_catalog first."""
    if _REGEXP_REPLACE_BY_NAME.search(pg_sql) is None:
        return pg_sql
    build = Postgres.Parser.FUNCTIONS["REGEXP_REPLACE"]

    def bare(node: exp.Expr) -> exp.Expr:
        called = node.expression if isinstance(node, exp.Dot) else None
        if (
            isinstance(called, exp.Anonymous)
            and called.name.lower() == "regexp_replace"
            and node.this.name.lower() == "pg_catalog"
        ):
            return build([a.copy() for a in called.expressions], dialect="postgres")
        return node

    return sqlglot.parse_one(pg_sql, read="postgres").transform(bare).sql(dialect="postgres")


_CAST_LITERAL_NAMED = re.compile(
    r"\b(numeric|decimal|bytea|character|char|bpchar)\b", re.IGNORECASE
)
_BYTEA_ESCAPES = re.compile(r"\\\\|\\[0-3][0-7]{2}")


def _bytea_literal(text: str) -> exp.Expr | None:
    """byteain's reading of a hex literal; None for the escape form, which the engine's own text
    cast reads wrong and no form can."""
    if not text.startswith("\\x") or len(text) % 2:
        return None
    return exp.Unhex(this=exp.Literal.string(text[2:]))


def literals_as_said(tree: exp.Expr) -> exp.Expr:  # REQ-2171
    """A literal keeps what its cast says: a bare DECIMAL has no fraction on an engine with no
    unconstrained numeric (2.5::numeric would answer 3 on Trino and 2 on ClickHouse), a bare
    character is one character, and a bytea is its bytes, not its text."""
    for cast in list(tree.find_all(exp.Cast)):
        value = cast.this.unnest()
        value = value.this if isinstance(value, exp.Neg) else value
        if not isinstance(value, exp.Literal):
            continue
        if cast.to.is_type("decimal") and not cast.to.expressions and not value.is_string:
            scale = len(value.this.partition(".")[2])
            cast.set("to", exp.DataType.build(f"DECIMAL(38, {scale})"))
        elif cast.to.is_type("char") and not cast.to.expressions and value.is_string:
            value.replace(exp.Literal.string(value.this[:1]))
        elif cast.to.is_type("varbinary", "binary") and value.is_string:
            read = _bytea_literal(value.this)
            if read is not None:
                cast.replace(read)
    return tree


#: A timestamp on ClickHouse: its DateTime has no fraction, and Arrow reads it as a count of seconds.
_CLICKHOUSE_INSTANT = zoned_instants.CLICKHOUSE_INSTANT


def clickhouse_semantics(tree: exp.Expr) -> exp.Expr:  # REQ-2171
    """``tree`` with what ClickHouse would read otherwise said for it. A timestamp keeps its
    microseconds and reads as one, a time is read by toTime64 (its Time reads PostgreSQL's text as
    NULL), and a hex has no leading zeros, as to_hex has none. A refusal (pg_forms_types.refused), the cast of a text that is no
    number, raises: sqlglot writes a cast as Nullable, which reads such a text as NULL. Its argument
    is materialized so the refusal is not folded in a branch the statement does not take."""

    from provisa.transpiler.polyfill import instants

    def said(node: exp.Expr) -> exp.Expr:
        outside = instants.outside_cast(node)
        if outside is not None:
            # REQ-2167: a literal DateTime64 would hold at its type's end raises 0A000.
            return outside
        if isinstance(node, exp.CurrentTimestamp):
            return exp.Anonymous(
                this="now64", expressions=[exp.Literal.number(6), exp.Literal.string("UTC")]
            )
        if isinstance(node, exp.DataType) and node.this in (
            exp.DataType.Type.TIMESTAMP,
            exp.DataType.Type.TIMESTAMPTZ,
        ):
            return exp.DataType.build(_CLICKHOUSE_INSTANT, dialect="clickhouse")
        if isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.TIME):
            value = node.this
            while isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.TIME):
                value = value.this
            # Its OrNull reads what the literal's own refuses, and a NULL it cannot.
            reader = "toTime64" if isinstance(value, exp.Literal) else "toTime64OrNull"
            return exp.Anonymous(this=reader, expressions=[value, exp.Literal.number(6)])
        if isinstance(node, exp.Date) and not node.args.get("zone"):
            # REQ-2236: its DATE() is toDate, a Date of 1970 to 2149; PostgreSQL's day of a value
            # held as DateTime64(6, 'UTC') or Date32 is Date32's, exact from 1900 to 2299.
            # The new node is not walked again: its value is said here.
            return exp.Anonymous(this="toDate32", expressions=[node.this.transform(said)])
        if isinstance(node, exp.Hex):
            return exp.Anonymous(
                this="replaceRegexpOne",
                expressions=[node, exp.Literal.string("^0+(.)"), exp.Literal.string("\\1")],
            )
        if (
            isinstance(node, exp.Cast)
            and node.to.is_type(exp.DataType.Type.INT)
            and any(MARK in part.this for part in node.this.find_all(exp.Literal))
        ):
            return exp.Anonymous(
                this="accurateCast",
                expressions=[
                    exp.Anonymous(this="materialize", expressions=[node.this.copy()]),
                    exp.Literal.string("Int32"),
                ],
            )
        return node

    return tree.transform(said)


def _lower_universal_regex(pg_sql: str, target_dialect: str) -> exp.Expr | None:  # REQ-2142
    """``pg_sql`` parsed with each regex mask's call (provisa_regex_replace: the value, universal
    pattern and replacement, and what a value the pattern does not match reads as) written for the
    target: the engine's REGEXP_REPLACE in its own regex flavour, every match replaced, where the
    pattern matches; the full mask where it does not (fail closed); NULL for NULL. None when the
    statement has no such call."""
    from provisa.security.masking import UNIVERSAL_REGEX_REPLACE
    from provisa.security.universal_regex import lower

    if UNIVERSAL_REGEX_REPLACE not in pg_sql.lower():
        return None
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    calls = [
        node
        for node in tree.find_all(exp.Anonymous)
        if node.name.lower() == UNIVERSAL_REGEX_REPLACE
    ]
    if not calls:
        return None
    for node in calls:
        value, pattern, replace, otherwise = node.expressions
        engine_pattern, engine_replace = lower(pattern.this, replace.this, target_dialect)
        # Without flags sqlglot's REGEXP_REPLACE replaces every match, and it writes the
        # target's spelling of that ('g' for PostgreSQL and DuckDB).
        replaced = exp.RegexpReplace(
            this=value.copy(),
            expression=exp.Literal.string(engine_pattern),
            replacement=exp.Literal.string(engine_replace),
        )
        node.replace(
            exp.Case(
                ifs=[
                    exp.If(this=exp.Is(this=value.copy(), expression=exp.Null()), true=exp.Null()),
                    exp.If(this=_regex_found(value, engine_pattern, target_dialect), true=replaced),
                ],
                default=otherwise.copy(),
            )
        )
    return tree


# Where REGEXP_LIKE is a match of the whole value, a pattern is found anywhere by its position.
_WHOLE_VALUE_REGEXP_LIKE = frozenset({"snowflake", "exasol"})


def _regex_found(value: exp.Expr, pattern: str, target_dialect: str) -> exp.Expr:
    """Whether ``pattern`` (the target's flavour) matches anywhere in ``value``."""
    if target_dialect in _WHOLE_VALUE_REGEXP_LIKE:
        position = exp.Anonymous(
            this="REGEXP_INSTR", expressions=[value.copy(), exp.Literal.string(pattern)]
        )
        return exp.GT(this=position, expression=exp.Literal.number(0))
    return exp.RegexpLike(this=value.copy(), expression=exp.Literal.string(pattern))


# Where REGEXP_REPLACE with the 'g' flag means every match as it does in PostgreSQL. Everywhere
# else its three-argument form already replaces every match, and a fourth argument is something
# other than flags (Snowflake's and MySQL's a position).
_REGEXP_FLAGS_KEPT = frozenset({"postgres", "duckdb"})
#: Where the polyfill grid's regexp_replace row reads the flags (provisa.transpiler.polyfill.regex).
_FLAGS_IN_THE_GRID = frozenset({"trino", "presto", "athena", "clickhouse"})


def _every_match_unflagged(pg_sql: str, target_dialect: str) -> str:  # REQ-263
    """``pg_sql`` with the 'g' flag taken off each REGEXP_REPLACE for a target whose
    three-argument form replaces every match already (a regex mask masks every match)."""
    if "REGEXP_REPLACE" not in pg_sql.upper():
        return pg_sql
    if target_dialect == "duckdb":
        return _flag_past_n(pg_sql)
    if target_dialect in _FLAGS_IN_THE_GRID:
        return _no_flags_said(pg_sql)
    if target_dialect in _REGEXP_FLAGS_KEPT:
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    flagged = [
        node
        for node in tree.find_all(exp.RegexpReplace)
        if isinstance(node.args.get("modifiers"), exp.Literal)
        and node.args["modifiers"].this == "g"
    ]
    if not flagged:
        return pg_sql
    for node in flagged:
        node.set("modifiers", None)
    return tree.sql(dialect="postgres")


def _no_flags_said(pg_sql: str) -> str:
    """``pg_sql`` with each REGEXP_REPLACE the statement gives no flags given none (''), which
    PostgreSQL reads as the same call: the grid's row reads a call's flags, and a call of three
    arguments a rewrite writes (a regex mask's) is every match, as sqlglot writes one."""
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    unflagged = [
        node
        for node in tree.find_all(exp.RegexpReplace)
        if node.args.get("modifiers") is None and node.args.get("position") is None
    ]
    if not unflagged:
        return pg_sql
    for node in unflagged:
        node.set("modifiers", exp.Literal.string(""))
    return tree.sql(dialect="postgres")


def _flag_past_n(pg_sql: str) -> str:
    """``pg_sql`` for DuckDB, whose REGEXP_REPLACE sqlglot writes with a flag and without the start
    and N. PostgreSQL reads 'g' only where N is not given, so that call is the five-argument one;
    any other flag beside N has no form."""
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    # A flag given as text ('i'::text) is its literal: sqlglot writes DuckDB's flags only from one.
    for node in tree.find_all(exp.RegexpReplace):
        said = node.args.get("modifiers")
        if isinstance(said, exp.Cast) and said.this.is_string and said.to.is_type("text"):
            node.set("modifiers", said.this)
    both = [
        node
        for node in tree.find_all(exp.RegexpReplace)
        if node.args.get("occurrence") is not None and node.args.get("modifiers") is not None
    ]
    for node in both:
        said = node.args["modifiers"]
        if not (isinstance(said, exp.Literal) and said.this == "g"):
            raise function_macros.FunctionRefused("regexp_replace", "duckdb")
        node.set("modifiers", None)
    return tree.sql(dialect="postgres")


# T-SQL fixed database principals: a schema cannot share a principal's name, so `CREATE SCHEMA
# [public]` fails with 2714 ("There is already an object named 'public'") on every SQL Server-family
# warehouse (Fabric, Synapse) — yet `public` is the default schema of every Postgres-derived source.
# The registered (semantic) schema stays `public`; only the physical T-SQL name differs.
_TSQL_RESERVED_SCHEMAS = frozenset({"public"})


def tsql_physical_schema(schema: str) -> str:
    """The physical T-SQL schema for a registered schema name — the ONE mapping shared by the
    warehouse runtime's DDL (mssql_warehouse_runtime._phys_parts) and the SQL sent to it (below),
    so an object is created and queried under the same name. T-SQL identifiers compare
    case-insensitively under the default collation, hence the lowercase check."""
    return f"provisa_{schema}" if schema.lower() in _TSQL_RESERVED_SCHEMAS else schema


def _map_tsql_reserved_schemas(sql: str) -> str:
    """Rename every table reference's schema through ``tsql_physical_schema`` (tsql target only)."""
    tree = sqlglot.parse_one(sql, read="tsql")
    changed = False
    for table in tree.find_all(exp.Table):
        db = table.args.get("db")
        if db is None:
            continue
        mapped = tsql_physical_schema(db.name)
        if mapped != db.name:
            table.set("db", exp.to_identifier(mapped, quoted=True))
            changed = True
    return tree.sql(dialect="tsql") if changed else sql


def _exasol_stored_case(tree: exp.Expr) -> str:  # REQ-1731
    """Address an Exasol source's objects as Exasol stores them.

    Exasol stores an identifier written without quotes in UPPERCASE, and Provisa registers and
    compiles a source's names in lowercase (as the engine's exasol connector presents them). The
    connector addresses a lowercase name in uppercase on the Exasol side; the direct driver does
    the same here: a lowercase schema, table or column reference is sent in uppercase. Each output
    column a bare reference produced keeps its lowercase name through an alias, so the rows read
    the same on either path. A name that is not all lowercase is the object's own and is left as
    written; aliases the statement defines are its own and are left as written."""
    # Names the statement itself defines: CTE names, table and subquery aliases, output aliases.
    ctes = {c.alias for c in tree.find_all(exp.CTE)}
    table_aliases = {a.name for a in tree.find_all(exp.TableAlias)}
    output_aliases = {a.alias for a in tree.find_all(exp.Alias)}

    def _sources(column: exp.Column) -> list[exp.Expr]:
        select = column.find_ancestor(exp.Select)
        if select is None:
            return []
        return [
            src.this if isinstance(src, (exp.From, exp.Join)) else src
            for src in [select.args.get("from_"), *select.args.get("joins", [])]
            if src is not None
        ]

    def _qualifies_derived(column: exp.Column) -> bool:
        """True when the column's table qualifier names a subquery or CTE in its SELECT's FROM.

        Uses the column's immediate SELECT context rather than the full tree, so a subquery alias
        present in an outer SELECT does not incorrectly mask a same-named real table in an inner
        SELECT (REQ-1731 scope fix: global ``derived`` set caused the inner column to be treated as
        derived, suppressing the required uppercase conversion)."""
        qualifier = column.table
        if not qualifier:
            return False
        for src in _sources(column):
            if isinstance(src, exp.Subquery) and src.alias == qualifier:
                return True
            if isinstance(src, exp.Table) and src.name in ctes and src.alias_or_name == qualifier:
                return True
        return False

    def _reads_only_derived(column: exp.Column) -> bool:
        sources = _sources(column)
        return bool(sources) and all(
            isinstance(src, exp.Subquery) or (isinstance(src, exp.Table) and src.name in ctes)
            for src in sources
        )

    cte_bodies = {c.alias: c.this for c in tree.find_all(exp.CTE)}

    def _quoted_outputs(column: exp.Column) -> set[str]:
        """The names the derived relations ``column`` reads output in quotes, as written."""
        outputs: set[str] = set()
        for src in _sources(column):
            if column.table and src.alias_or_name != column.table:
                continue
            body = src.this if isinstance(src, exp.Subquery) else cte_bodies.get(src.name)
            if not isinstance(body, exp.Query):
                continue
            for projection in body.selects:
                name = projection.args.get("alias" if isinstance(projection, exp.Alias) else "this")
                if isinstance(name, exp.Identifier) and name.quoted:
                    outputs.add(name.this)
        return outputs

    def _stored(ident: exp.Expr | None, own: set[str]) -> None:
        if isinstance(ident, exp.Identifier) and ident.this == ident.this.lower():
            if ident.this not in own:
                ident.set("this", ident.this.upper())
                ident.set("quoted", True)

    for select in tree.find_all(exp.Select):
        projections = []
        for proj in select.expressions:
            if isinstance(proj, exp.Column) and proj.name == proj.name.lower():
                proj = exp.alias_(proj, exp.to_identifier(proj.name, quoted=True))
            projections.append(proj)
        select.set("expressions", projections)
    for table in tree.find_all(exp.Table):
        _stored(table.args.get("catalog"), set())
        _stored(table.args.get("db"), set())
        _stored(table.args.get("this"), ctes)  # a CTE is the statement's own
    for column in tree.find_all(exp.Column):
        _stored(column.args.get("catalog"), set())
        _stored(column.args.get("db"), set())
        _stored(column.args.get("table"), table_aliases | ctes)
        qualifier = column.table
        if _qualifies_derived(column) or (not qualifier and _reads_only_derived(column)):
            # A derived relation's column is the statement's own output name. Unquoted, Exasol
            # reads it in upper case, which a quoted lowercase output does not answer to.
            name = column.this
            if isinstance(name, exp.Identifier) and name.this in _quoted_outputs(column):
                name.set("quoted", True)
            continue
        # An ORDER BY of an output alias names the alias, not a stored column.
        in_order = column.find_ancestor(exp.Order) is not None
        _stored(column.args.get("this"), output_aliases if in_order else set())
    return tree.sql(dialect="exasol")


def _strip_schema_qualifiers(sql: str) -> str:
    """Drop db/catalog qualifiers from every table reference (sqlite target only)."""
    tree = sqlglot.parse_one(sql, read="sqlite")
    for table in tree.find_all(exp.Table):
        table.set("db", None)
        table.set("catalog", None)
    return tree.sql(dialect="sqlite")


# ── CTE rewriter ──────────────────────────────────────────────────────────────
# Trino does not support correlated scalar subqueries in SELECT.
# _rewrite_correlated_json_to_ctes hoists each json_object/json_agg correlated
# subquery from SELECT into a CTE, then replaces it with a LEFT JOIN reference.
