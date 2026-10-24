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

import sqlglot

from provisa.transpiler.native_json import resolve_native_json
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


def transpile_to_trino(pg_sql: str) -> str:  # REQ-066, REQ-068
    """Transpile PostgreSQL-dialect SQL to Trino SQL."""
    pg_sql = rewrite_correlated_subqueries_for_trino(pg_sql)
    result = transpile(pg_sql, "trino")
    result = _rewrite_epoch_of_timestamp_for_trino(result)
    result = _rewrite_json_build_object_for_trino(result)
    result = _rewrite_json_arrayagg_for_trino(result)
    result = _rewrite_empty_json_list_for_trino(result)
    return _rewrite_to_json_for_trino(result)


def _rewrite_empty_json_list_for_trino(sql: str) -> str:
    """A list field's empty default (sql_selection.EMPTY_JSON_LIST, CAST('[]' AS JSON)) is
    '[]' for Trino: there the aggregate it defaults is json_format(...) text
    (_rewrite_json_arrayagg_for_trino), and Trino's CAST of text to JSON is a JSON string, not an
    array (#286)."""
    if "'[]' AS JSON" not in sql:
        return sql
    tree = sqlglot.parse_one(sql, read="trino")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if (
            isinstance(node, exp.Cast)
            and isinstance(node.this, exp.Literal)
            and node.this.this == "[]"
            and node.to.is_type(exp.DataType.Type.JSON)
        ):
            return exp.Literal.string("[]")
        return node

    return tree.transform(_transform).sql(dialect="trino")


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


def rewrite_instants_for_trino(
    sql: str, utc_wall: dict[tuple[str, str, str], set[str]]
) -> str:  # REQ-2050
    """Each output column of ``sql`` that is an instant Trino reads as a plain TIMESTAMP written as
    ``with_timezone(col, 'UTC')``: the UTC wall time a MySQL-family TIMESTAMP or an Elasticsearch
    date arrives as, carried as the instant it is (trino_connectors.TRINO_INSTANT_AS_TIMESTAMP).

    ``utc_wall`` maps ``(catalog, schema, table)`` to such a table's declared-timestamptz columns
    (trino_connectors.trino_utc_wall_columns). Only the statement's outputs -- the outermost
    SELECT's items, each UNION branch's -- that are a bare column of such a table: a predicate, a
    join or an inner query reads the column as the connector gives it, so its pushdown stands."""
    if not utc_wall:
        return sql
    tree = sqlglot.parse_one(sql, read="trino")
    selects = [tree] if isinstance(tree, exp.Select) else []
    if isinstance(tree, exp.Union):
        selects = [s for s in tree.find_all(exp.Select) if s.find_ancestor(exp.Select) is None]
    changed = False
    for select in selects:
        tables: dict[str, set[str]] = {}
        sources = [select.args.get("from_")] + list(select.args.get("joins") or [])
        for source in sources:
            table = source.this if source is not None else None
            if not isinstance(table, exp.Table) or table.args.get("catalog") is None:
                continue
            key = (table.catalog, table.db.lower(), table.name.lower())
            if key in utc_wall:
                tables[table.alias_or_name] = utc_wall[key]
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
    Trino spelling."""
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
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

    return tree.transform(_transform).sql(dialect="trino")


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

    return tree.transform(_transform).sql(dialect="trino")


def _rewrite_json_arrayagg_for_trino(sql: str) -> str:
    """Replace JSON_ARRAYAGG(x) with json_format(CAST(ARRAY_AGG(JSON_PARSE(x)) AS JSON)).

    JSONB_AGG is the same aggregate over Postgres' binary JSON type — sqlglot leaves it anonymous
    (json_agg maps to JSON_ARRAYAGG, jsonb_agg does not) and Trino registers neither, so both
    spellings rewrite identically.

    Trino 480 does not register json_arrayagg. The json_format wrapper produces
    VARCHAR so the result is safe to embed as a VALUE in JSON_OBJECT (Trino cannot
    coerce a JSON-typed array to varchar, which causes INVALID_CAST_ARGUMENT when
    the aggregate result is used inside an outer json_object(...) call).
    The Python _convert_value layer parses the VARCHAR back to a Python list/dict.
    """
    # Parse failure must fail loud: returning input skips the required Trino rewrite.
    tree = sqlglot.parse_one(sql, read="trino")

    json_type = exp.DataType(this=exp.DataType.Type.JSON)

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
            return exp.Anonymous(
                this="json_format",
                expressions=[
                    exp.Cast(
                        this=exp.ArrayAgg(
                            # Recurse: transform is pre-order and skips a replaced node's
                            # children, so a nested json_arrayagg would survive unrewritten.
                            this=exp.Anonymous(
                                this="JSON_PARSE", expressions=[inner.transform(_transform)]
                            )
                        ),
                        to=json_type.copy(),
                    )
                ],
            )
        return node

    return tree.transform(_transform).sql(dialect="trino")


def transpile_to_duckdb(pg_sql: str) -> str:  # REQ-066, REQ-068
    """Transpile PostgreSQL-dialect SQL to DuckDB SQL."""
    return _null_order_as_key(
        _rewrite_list_functions_for_duckdb(
            _rewrite_json_agg_for_duckdb(transpile(pg_sql, "duckdb"))
        )
    )


#: The targets whose LIKE has no escape character unless one is written (PostgreSQL's is a
#: backslash): each LIKE is given PostgreSQL's explicitly (_like_escape_written).
_LIKE_ESCAPE_WRITTEN = frozenset({"duckdb", "trino"})


# A source whose datetime literal takes no UTC offset: an instant is written as its UTC wall time,
# which the connection's UTC session reads as that instant (REQ-2050, executor/drivers/mysql.py).
_INSTANT_AS_UTC_WALL_TIME = frozenset({"mysql"})


def _instants_as_utc(pg_sql: str, target_dialect: str) -> str:
    """``pg_sql`` with each string literal cast to timestamptz that carries an offset written as
    the same instant in UTC, its offset gone. MariaDB reads ``TIMESTAMP('2024-01-01
    20:04:05-05:00')`` as 20:04:05, five hours early (REQ-2050)."""
    if target_dialect not in _INSTANT_AS_UTC_WALL_TIME or "TIMESTAMPTZ" not in pg_sql.upper():
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
        utc = instant.astimezone(timezone.utc).replace(tzinfo=None)
        literal.replace(exp.Literal.string(utc.isoformat(sep=" ")))
    return tree.sql(dialect="postgres")


def _like_escape_written(pg_sql: str, target_dialect: str) -> str:
    """``pg_sql`` with each LIKE and ILIKE that has no ESCAPE clause given PostgreSQL's: a
    backslash escapes the character after it. On a target whose LIKE has no escape character
    unless one is written, ``'a%b' LIKE 'a\\%b'`` was false there and true in the dialect the
    statement is in."""
    if target_dialect not in _LIKE_ESCAPE_WRITTEN or "LIKE" not in pg_sql.upper():
        return pg_sql
    from provisa.codehost.duckdb_sql import escape_likes

    return escape_likes(sqlglot.parse_one(pg_sql, read="postgres")).sql(dialect="postgres")


def _null_order_as_key(sql: str) -> str:
    """#294: DuckDB SQL with each ORDER BY key whose nulls come first written as a null-test key
    of its own (codehost.duckdb_sql, the one form every DuckDB answering a statement runs)."""
    from provisa.codehost.duckdb_sql import null_order_as_key

    return null_order_as_key(sqlglot.parse_one(sql, read="duckdb")).sql(dialect="duckdb")


#: A Cypher read's list functions as the IR names them (gql/expr_visitor, Trino's names) whose
#: DuckDB function differs in name or arguments.
_DUCKDB_LIST_FUNCTIONS = frozenset(
    {"ELEMENT_AT", "SLICE", "CARDINALITY", "ANY_MATCH", "ALL_MATCH", "NONE_MATCH", "SEQUENCE"}
)


def _rewrite_list_functions_for_duckdb(sql: str) -> str:
    """A Cypher read's list functions as DuckDB computes them: a list index ``element_at(l, i)``
    (1-based, negative from the end) is ``list_extract`` and a map key ``element_at(m, 'k')``
    ``map_extract_value`` (DuckDB's element_at is a map's alone, and answers a list of the
    value); ``slice(l, start, length)`` is ``list_slice(l, start, start + length - 1)``; a list's
    ``cardinality`` is ``len``; ``any_match`` / ``all_match`` / ``none_match`` are the list's
    predicate values or'd or and'd (on an empty list any is false, all and none true);
    ``sequence`` is ``generate_series`` (both inclusive)."""
    if not any(name in sql.upper() for name in _DUCKDB_LIST_FUNCTIONS):
        return sql
    tree = sqlglot.parse_one(sql, read="duckdb")

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
        if name == "ANY_MATCH":
            return exp.Coalesce(
                this=_fn("list_bool_or", _predicates(args)), expressions=[exp.false()]
            )
        if name == "ALL_MATCH":
            return exp.Coalesce(
                this=_fn("list_bool_and", _predicates(args)), expressions=[exp.true()]
            )
        return exp.Not(
            this=exp.Coalesce(
                this=_fn("list_bool_or", _predicates(args)), expressions=[exp.false()]
            )
        )

    return tree.transform(_transform).sql(dialect="duckdb")


def _rewrite_json_agg_for_duckdb(sql: str) -> str:
    """Replace JSON_ARRAYAGG(x) / JSON_AGG(x) / JSONB_AGG(x) with json_group_array(x).

    The compiler emits Postgres ``json_agg`` for a one-to-many relationship selection
    (compiler/sql_selection.py:431,438); SQLGlot's DuckDB generator writes that as
    ``JSON_ARRAYAGG``, which DuckDB does not register under any of those three spellings —
    its aggregate is ``json_group_array``. Unrewritten, every nested-relationship query
    fails with "Scalar Function with name json_arrayagg does not exist".

    json_group_array over JSON-typed values nests without re-encoding, so the sibling
    ``json_object`` (which DuckDB does register, in the flat k, v form SQLGlot writes)
    needs no rewrite.
    """
    # Parse failure must fail loud: returning input skips the required DuckDB rewrite.
    tree = sqlglot.parse_one(sql, read="duckdb")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
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
            return exp.Anonymous(this="json_group_array", expressions=[inner.transform(_transform)])
        return node

    return tree.transform(_transform).sql(dialect="duckdb")


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
    return tree.sql(dialect="postgres")


def transpile(
    pg_sql: str, target_dialect: str, *, pgvector: bool = False
) -> str:  # REQ-066, REQ-068, REQ-229, REQ-423
    """Transpile PostgreSQL-dialect SQL to a target dialect.

    Args:
        pg_sql: SQL string in PostgreSQL dialect with double-quoted identifiers.
        target_dialect: SQLGlot dialect name (e.g. "trino", "postgres", "mysql").
        pgvector: the statement runs on a PostgreSQL source with the pgvector extension, whose
            own operator a similarity search is then written with (REQ-423).

    Returns:
        SQL string in target dialect.
    """
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
    identify = target_dialect in ("snowflake", "oracle")
    pg_sql = _every_match_unflagged(pg_sql, target_dialect)
    pg_sql = _like_escape_written(pg_sql, target_dialect)
    pg_sql = _instants_as_utc(pg_sql, target_dialect)
    lowered = _lower_universal_regex(pg_sql, target_dialect)
    if target_dialect == "exasol":
        result = _exasol_stored_case(
            lowered if lowered is not None else sqlglot.parse_one(pg_sql, read="postgres")
        )
    elif lowered is not None:
        result = lowered.sql(dialect=target_dialect, identify=identify)
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
    from provisa.vector.similarity import lower as _lower_similarity

    # REQ-423: a governed similarity search, in the target's own form. REQ-2000: Qdrant's driver
    # reads the governed call itself and runs the search through Qdrant's API.
    if target_dialect != "qdrant":
        result = _lower_similarity(result, target_dialect, pgvector=pgvector)
    from provisa.observability.stage_trace import trace_stage

    trace_stage(f"transpile.{target_dialect}", result)
    return result


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
# other than flags (Trino's is a function, Snowflake's and MySQL's a position).
_REGEXP_FLAGS_KEPT = frozenset({"postgres", "duckdb"})


def _every_match_unflagged(pg_sql: str, target_dialect: str) -> str:  # REQ-263
    """``pg_sql`` with the 'g' flag taken off each REGEXP_REPLACE for a target whose
    three-argument form replaces every match already (a regex mask masks every match)."""
    if target_dialect in _REGEXP_FLAGS_KEPT or "REGEXP_REPLACE" not in pg_sql.upper():
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
    # Relations the statement derives (a CTE, a subquery): their columns are the statement's own
    # output names, never a stored object's.
    derived = (
        ctes
        | {s.alias for s in tree.find_all(exp.Subquery) if s.alias}
        | {t.alias_or_name for t in tree.find_all(exp.Table) if t.name in ctes}
    )

    def _reads_only_derived(column: exp.Column) -> bool:
        select = column.find_ancestor(exp.Select)
        if select is None:
            return False
        sources = [
            src.this if isinstance(src, (exp.From, exp.Join)) else src
            for src in [select.args.get("from_"), *select.args.get("joins", [])]
            if src is not None
        ]
        return bool(sources) and all(
            isinstance(src, exp.Subquery) or (isinstance(src, exp.Table) and src.name in ctes)
            for src in sources
        )

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
        if (qualifier and qualifier in derived) or (not qualifier and _reads_only_derived(column)):
            continue  # a derived relation's column is the statement's own output name
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
