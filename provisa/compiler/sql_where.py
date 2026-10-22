# Copyright (c) 2026 Kenneth Stott
# Canary: 8dbd2b8b-4354-4af2-a56d-8690d9488a23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WHERE/ORDER BY compilation and field-argument helpers for sql_gen.

Leaf layer: turns GraphQL field args into SQL WHERE/ORDER BY fragments and
extracts limit/offset/lateral-force flags. No dependency on sql_gen or
sql_selection.
"""

from __future__ import annotations

import fnmatch as _fnmatch
import re as _re
from collections.abc import Iterable

from graphql import (
    BooleanValueNode,
    EnumValueNode,
    FieldNode,
    FloatValueNode,
    IntValueNode,
    ListValueNode,
    ObjectValueNode,
    StringValueNode,
    VariableNode,
)

from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_literals import sql_literal
from provisa.compiler.sql_types import (
    CompilationContext,
    JoinMeta,
    StatementSources,
)
from provisa.compiler.sql_rewrite import (
    _join_column_expr,
    _q,
    _sql_str_literal,
    _table_ref,
)


_VIRTUAL_COLS = frozenset({"_name_", "_domain_"})
#: meta.registered_tables' virtual column: each registration's table name as queried, the value
#: its table's _name_ holds (compiler.context._register_meta_synthetic_joins).
META_QUERY_NAME = "table_query_name"
#: The virtual columns: served with a table to every role that reads it, as GraphQL serves them,
#: and answered by the lowering (sql_rewrite._lower_virtual_columns), never by the source.
SERVED_WITH_EVERY_TABLE = _VIRTUAL_COLS | {META_QUERY_NAME}


def _parameter_of(argument: str, columns: Iterable[str]) -> str:  # REQ-1986
    """The parameter (its column without ``_nf_``) a native argument fills. The schema names the
    argument ``_<name>`` when ``<name>`` is also one of the field's own (schema_gen
    ``_build_native_filter_args``), so the underscore is not part of the parameter (#247)."""
    names = {c[4:] if c.startswith("_nf_") else c for c in columns}
    if argument in names:
        return argument
    if argument.startswith("_") and argument[1:] in names:
        return argument[1:]
    raise ValueError(f"argument {argument!r} is not a parameter of the table")


def native_args(args: dict, standard: frozenset[str], columns: Iterable[str]) -> dict:  # REQ-1986
    """A root field's native arguments (a table's parameters, every argument but ``standard``),
    by parameter name. ``columns`` are the table's parameter columns."""
    columns = list(columns)
    return {_parameter_of(k, columns): v for k, v in args.items() if k not in standard}


def native_arg_conditions(api_args: dict, alias: str | None = None) -> list[str]:  # REQ-1986
    """Each parameter (by name, from ``native_args``) as ``_nf_<name> = <literal>`` -- the
    predicate nf_extractor takes back out of the statement before the engine runs it, and the
    SQL/CQL preview shows."""
    conditions = []
    for k, v in api_args.items():
        col = _q(f"_nf_{k}")
        quoted_col = f"{_q(alias)}.{col}" if alias else col
        if isinstance(v, bool):
            lit = "TRUE" if v else "FALSE"
        elif isinstance(v, (int, float)):
            lit = str(v)
        else:
            lit = sql_literal(v, "postgres")
        conditions.append(f"{quoted_col} = {lit}")
    return conditions


# --- Build CompilationContext from SchemaInput ---


def _extract_value(
    node: object,  # object-ok: truly-any GraphQL AST value node
    variables: dict | None,
) -> object:  # object-ok: truly-any payload — GraphQL AST value nodes and Python primitives unified
    """Extract a Python value from a GraphQL AST value node."""
    if isinstance(node, StringValueNode):
        return node.value
    if isinstance(node, IntValueNode):
        return int(node.value)
    if isinstance(node, FloatValueNode):
        return float(node.value)
    if isinstance(node, BooleanValueNode):
        return node.value
    if isinstance(node, EnumValueNode):
        return node.value
    if isinstance(node, ListValueNode):
        return [_extract_value(v, variables) for v in node.values]
    if isinstance(node, ObjectValueNode):
        return {f.name.value: _extract_value(f.value, variables) for f in node.fields}
    if isinstance(node, VariableNode):
        var_name = node.name.value
        if variables and var_name in variables:
            return variables[var_name]
        raise ValueError(f"Variable ${var_name} not provided")
    raise ValueError(f"Unsupported value node type: {type(node).__name__}")


# --- WHERE clause compilation ---


_ISO_DATE_RE = _re.compile(
    r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


def _timestamp_literal_or_param(
    val: object,  # object-ok: truly-any payload — caller may pass str, int, float, bool, None
    collector,
) -> str:  # object-ok: truly-any payload — caller may pass str, int, float, bool, None
    """Return a TIMESTAMP literal if val is an ISO date, otherwise a parameter.

    Accepts: 2000-01-01, 2000-01-01T00:00:00, 2000-01-01 00:00:00,
             2000-01-01T00:00:00Z, 2000-01-01T00:00:00+05:30
    With timezone → TIMESTAMP '...' WITH TIME ZONE
    Without → TIMESTAMP '...'
    """
    if isinstance(val, str) and _ISO_DATE_RE.match(val):
        normalized = val.replace("T", " ")
        # Check for timezone suffix
        tz_match = _re.search(r"(Z|[+-]\d{2}:?\d{2})$", normalized)
        if tz_match:
            tz = tz_match.group(1)
            base = normalized[: tz_match.start()].strip()
            if tz == "Z":
                tz = "UTC"
            return f"TIMESTAMP '{base} {tz}'"
        return f"TIMESTAMP '{normalized}'"
    return collector.add(val)


def _compile_virtual_col_filter(
    vv: str,
    filter_obj: dict,
) -> list[str]:
    """Compile filter predicates for a virtual column against its compile-time value."""
    parts: list[str] = []
    for op, val in filter_obj.items():
        if op == "eq":
            parts.append("TRUE" if vv == str(val) else "FALSE")
        elif op == "neq":
            parts.append("TRUE" if vv != str(val) else "FALSE")
        elif op == "gt":
            parts.append("TRUE" if vv > str(val) else "FALSE")
        elif op == "gte":
            parts.append("TRUE" if vv >= str(val) else "FALSE")
        elif op == "lt":
            parts.append("TRUE" if vv < str(val) else "FALSE")
        elif op == "lte":
            parts.append("TRUE" if vv <= str(val) else "FALSE")
        elif op == "in":
            parts.append("TRUE" if vv in [str(v) for v in val] else "FALSE")
        elif op == "like":
            pattern = str(val).replace("%", "*").replace("_", "?")
            parts.append("TRUE" if _fnmatch.fnmatch(vv, pattern) else "FALSE")
        elif op == "ilike":  # REQ-2136
            pattern = str(val).lower().replace("%", "*").replace("_", "?")
            parts.append("TRUE" if _fnmatch.fnmatchcase(vv.lower(), pattern) else "FALSE")
        elif op == "is_null":
            parts.append("FALSE")
    return parts


def _compile_column_filter(
    col: str,
    filter_obj: dict,
    collector: ParamCollector,
) -> list[str]:
    """Compile filter predicates for a physical column expression."""
    parts: list[str] = []
    for op, val in filter_obj.items():
        if op == "eq":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} = {rhs}")
        elif op == "neq":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} != {rhs}")
        elif op == "gt":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} > {rhs}")
        elif op == "gte":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} >= {rhs}")
        elif op == "lt":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} < {rhs}")
        elif op == "lte":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} <= {rhs}")
        elif op == "in":
            placeholders = [collector.add(v) for v in val]
            parts.append(f"{col} IN ({', '.join(placeholders)})")
        elif op == "like":
            placeholder = collector.add(val)
            parts.append(f"{col} LIKE {placeholder}")
        elif op == "ilike":  # REQ-2136: LIKE with case ignored, in every engine's SQL
            placeholder = collector.add(val)
            parts.append(f"LOWER({col}) LIKE LOWER({placeholder})")
        elif op == "is_null":
            parts.append(f"{col} IS NULL" if val else f"{col} IS NOT NULL")
        else:
            # A condition this compiler does not know (a relationship's, where none is taken) is
            # refused: dropping it would read wider than asked.
            raise ValueError(f"unsupported condition {op!r} on {col}")
    return parts


def join_exprs(  # REQ-2136
    join_meta: JoinMeta,
    source_alias: str,
    target_alias: str,
    ctx: CompilationContext,
    source_table_id: int,
) -> tuple[str, str]:
    """The two sides of a relationship's join condition: its source's value (a column of
    ``source_alias``, an expression, a constant or a virtual column) and its target's."""
    if join_meta.source_expr is not None:
        src = join_meta.source_expr.replace("{alias}", _q(source_alias))
    elif join_meta.source_constant is not None:
        src = (
            _sql_str_literal(join_meta.source_constant)
            if isinstance(join_meta.source_constant, str)
            else str(join_meta.source_constant)
        )
    elif join_meta.source_column in _VIRTUAL_COLS:
        _svc = (ctx.virtual_columns.get(source_table_id) or {}).get(join_meta.source_column, "")
        src = _sql_str_literal(_svc)
    elif join_meta.source_json_key:
        src = (
            f"CAST({_q(source_alias)}.{_q(join_meta.source_column)} AS JSON)"
            f"->>'{join_meta.source_json_key}'"
        )
    else:
        src = _join_column_expr(
            source_alias,
            join_meta.source_column,
            join_meta.source_column_type,
            join_meta.target_column_type,
        )
    if join_meta.target_expr is not None:
        tgt = join_meta.target_expr.replace("{alias}", _q(target_alias))
    elif join_meta.target_column in _VIRTUAL_COLS:
        _tvc = (ctx.virtual_columns.get(join_meta.target.table_id) or {}).get(
            join_meta.target_column, ""
        )
        tgt = _sql_str_literal(_tvc)
    else:
        tgt = _join_column_expr(
            target_alias,
            join_meta.target_column,
            join_meta.target_column_type,
            join_meta.source_column_type,
        )
    return src, tgt


class RelationshipConditions:  # REQ-2136
    """A where's relationship conditions, for a where over ``type_name``'s rows read as
    ``alias``: each a row with a related row matching its own where, compiled to EXISTS over the
    relationship's target. Every target read is added to the statement's ``sources``, and the
    EXISTS body is governed as the rest of the statement is (stage2._govern_selects)."""

    def __init__(
        self,
        ctx: CompilationContext,
        type_name: str,
        use_catalog: bool,
        sources: StatementSources,
        counter: list[int] | None = None,
    ) -> None:
        self.ctx = ctx
        self.type_name = type_name
        self.use_catalog = use_catalog
        self.sources = sources
        # Shared by every level, so each EXISTS reads its target under its own alias.
        self.counter = counter if counter is not None else [0]

    def join(self, field: str) -> JoinMeta | None:
        return self.ctx.joins.get((self.type_name, field))

    def exists(
        self,
        join_meta: JoinMeta,
        where: dict,
        collector: ParamCollector,
        alias: str,
        table_id: int,
    ) -> str:
        self.counter[0] += 1
        inner = f"w{self.counter[0]}"
        target = join_meta.target
        self.sources.add_table(target)
        src, tgt = join_exprs(join_meta, alias, inner, self.ctx, table_id)
        deeper = RelationshipConditions(
            self.ctx, target.type_name, self.use_catalog, self.sources, self.counter
        )
        cond = _compile_where(
            where,
            collector,
            inner,
            self.ctx.virtual_columns.get(target.table_id),
            target.table_id,
            self.ctx.exposed_to_physical,
            deeper,
        )
        return (
            f"EXISTS (SELECT 1 FROM {_table_ref(target, self.use_catalog)} {_q(inner)}"
            f" WHERE {tgt} = {src} AND ({cond}))"
        )


def _compile_where(
    where_obj: dict,
    collector: ParamCollector,
    alias: str | None,
    virtual_vals: dict[str, str] | None = None,
    table_id: int | None = None,
    exposed_to_physical: dict | None = None,
    relationships: RelationshipConditions | None = None,
) -> str:
    """Compile a where input object to a SQL WHERE clause fragment. A relationship's condition is
    compiled only where ``relationships`` is given (a root field's where, its rows aliased);
    elsewhere it is refused, never dropped."""
    parts: list[str] = []
    _e2p = exposed_to_physical or {}

    for key, value in where_obj.items():
        if key == "_and":
            sub_parts = [
                _compile_where(
                    sub,
                    collector,
                    alias,
                    virtual_vals,
                    table_id,
                    exposed_to_physical,
                    relationships,
                )
                for sub in value
            ]
            parts.append(f"({' AND '.join(sub_parts)})")
            continue
        if key == "_or":
            sub_parts = [
                _compile_where(
                    sub,
                    collector,
                    alias,
                    virtual_vals,
                    table_id,
                    exposed_to_physical,
                    relationships,
                )
                for sub in value
            ]
            parts.append(f"({' OR '.join(sub_parts)})")
            continue

        join_meta = relationships.join(key) if relationships is not None else None
        if relationships is not None and join_meta is not None:
            if alias is None or table_id is None:
                raise ValueError(f"relationship condition {key!r} needs its rows aliased")
            parts.append(relationships.exists(join_meta, value, collector, alias, table_id))
            continue

        # Virtual column: resolve at compile time — no physical column exists
        if key in _VIRTUAL_COLS and virtual_vals is not None:
            vv = virtual_vals.get(key, "")
            parts.extend(_compile_virtual_col_filter(vv, value))
            continue

        # Column filter: map GQL name → physical name, then quote
        phys_key = _e2p.get((table_id, key), key) if table_id is not None else key
        col = _q(phys_key) if alias is None else f"{_q(alias)}.{_q(phys_key)}"
        parts.extend(_compile_column_filter(col, value, collector))

    return " AND ".join(parts) if parts else "TRUE"


# --- ORDER BY compilation ---


_DIRECTION_SQL = {
    "asc": "ASC",
    "desc": "DESC",
    "asc_nulls_first": "ASC NULLS FIRST",
    "asc_nulls_last": "ASC NULLS LAST",
    "desc_nulls_first": "DESC NULLS FIRST",
    "desc_nulls_last": "DESC NULLS LAST",
}


def _compile_order_by(
    order_by_list: list[dict],
    alias: str | None,
    table_id: int | None = None,
    exposed_to_physical: dict | None = None,
) -> str:
    """Compile order_by input list to SQL ORDER BY clause.

    Hasura v2 format: each item is {column_name: direction} where direction
    is one of: asc, desc, asc_nulls_first, asc_nulls_last, desc_nulls_first,
    desc_nulls_last.
    """
    parts: list[str] = []
    _e2p = exposed_to_physical or {}
    for item in order_by_list:
        for col_name, direction in item.items():
            if isinstance(direction, dict):
                # Nested relationship order-by: { rel_field: { col: dir } }
                # Flatten one level — qualify with the relationship field name as alias.
                for nested_col, nested_dir in direction.items():
                    sql_dir = _DIRECTION_SQL.get(nested_dir)
                    if sql_dir is None:
                        raise ValueError(f"Unknown order direction: {nested_dir!r}")
                    parts.append(f"{_q(col_name)}.{_q(nested_col)} {sql_dir}")
                continue
            sql_dir = _DIRECTION_SQL.get(direction)
            if sql_dir is None:
                raise ValueError(f"Unknown order direction: {direction!r}")
            phys_col = (
                _e2p.get((table_id, col_name), col_name) if table_id is not None else col_name
            )
            col = _q(phys_col) if alias is None else f"{_q(alias)}.{_q(phys_col)}"
            parts.append(f"{col} {sql_dir}")
    return ", ".join(parts)


def where_names_relationship(  # REQ-2136
    where: object, ctx: CompilationContext, type_name: str
) -> bool:
    """Whether ``where`` (a where argument's value) states a relationship's condition, which
    reads the field's rows under an alias."""
    if isinstance(where, list):
        return any(where_names_relationship(w, ctx, type_name) for w in where)
    if not isinstance(where, dict):
        return False
    return any(
        (k in ("_and", "_or") and where_names_relationship(v, ctx, type_name))
        or (type_name, k) in ctx.joins
        for k, v in where.items()
    )


def _has_joins(field_node: FieldNode, ctx: CompilationContext, type_name: str) -> bool:
    """Check if any selected field is a relationship (requires JOIN)."""
    if not field_node.selection_set:
        return False
    for sel in field_node.selection_set.selections:
        if isinstance(sel, FieldNode):
            if (type_name, sel.name.value) in ctx.joins:
                return True
    return False


# Args that require LATERAL JOIN — limit is handled inside ARRAY_AGG subquery
_LATERAL_FORCE_ARGS = frozenset({"where", "order_by", "offset", "distinct_on"})


def _has_lateral_force_args(field_node: FieldNode) -> bool:
    return any(arg.name.value in _LATERAL_FORCE_ARGS for arg in field_node.arguments)


def _explicit_limit(field_node: FieldNode, variables: dict | None) -> int | None:
    for arg in field_node.arguments:
        if arg.name.value == "limit":
            val = _extract_value(arg.value, variables)
            if isinstance(val, int) and not isinstance(val, bool) and val > 0:
                return val
    return None


def _extract_non_negative_int(
    value: object,  # object-ok: truly-any payload — validated via isinstance checks inside body
    name: str,
) -> int:  # object-ok: truly-any payload — validated via isinstance checks inside body
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _field_args(field_node: FieldNode, variables: dict | None) -> dict:
    return {arg.name.value: _extract_value(arg.value, variables) for arg in field_node.arguments}
