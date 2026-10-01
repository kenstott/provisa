# Copyright (c) 2026 Kenneth Stott
# Canary: 3dd08557-1e8d-4234-82cb-3dcf4dd6e0fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lower a gRPC table request directly to the query IR (a semantic SELECT).

Shared by the native gRPC servicer and the HTTP gRPC proxy so both follow the one pipeline every
transport uses — query language → IR → governed IR → plan → physical — and gRPC never round-trips
through GraphQL.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from provisa.compiler.aggregate_gen import _is_comparable, _is_numeric
from provisa.compiler.naming import active_gql_convention, apply_gql_name
from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_gen import _q
from provisa.compiler.sql_rewrite import _semantic_table_ref
from provisa.grpc.proto_gen import _physical_to_proto


def _find_table_meta(ctx: Any, type_name: str) -> Any | None:
    """Match ``type_name`` to a ``TableMeta`` case/separator-insensitively (proto collapses the
    domain separator: ``PS__Inquiries`` → ``PsInquiries``). Shared by every grpc_table_to_* lowering
    so gRPC's request-type lookup has exactly one matching rule.

    Falls back to a suffix match when no exact match exists (REQ-1730 gap, confirmed live via
    gRPC server reflection against a running server): ``state.wire_proto`` (app_loaders.py) is
    compiled for a synthetic ``__wire__`` role with EVERY domain visible at once
    (``domain_access: ["*"]``), so a table name that collides across two domains only there gets
    domain-prefixed at the wire/proto level (``PbOrders``) — but ``ctx`` here is the ACTUAL serving
    role's own context (e.g. ``org_admin``), built from that role's narrower, collision-free domain
    visibility, where the same table's ``type_name`` never needed a prefix (``Orders``). A client
    has no way to know this in advance — server reflection is the only place ``PbOrders`` is
    discoverable at all, and reflection necessarily describes the wire-level (union) schema, not
    any one role's. Restricted to a UNIQUE suffix match (never picks between two candidates) so a
    genuinely unrelated table whose bare name happens to be a suffix of another is never guessed."""

    def _n(s: str) -> str:
        return s.replace("_", "").lower()

    target = _n(type_name)
    exact = next((m for m in ctx.tables.values() if _n(m.type_name) == target), None)
    if exact is not None:
        return exact
    suffix_matches = {
        _n(m.type_name): m
        for m in ctx.tables.values()
        if target != _n(m.type_name) and target.endswith(_n(m.type_name))
    }
    return next(iter(suffix_matches.values())) if len(suffix_matches) == 1 else None


class FilterError(ValueError):
    """A ``filter`` entry the request may not send; ``field`` is the offending field name."""

    def __init__(self, field: str, type_name: str, reason: str) -> None:
        super().__init__(f"Invalid filter field {field!r} for {type_name}: {reason}")
        self.field = field


def _filter_set_fields(filter_msg: Any | None) -> list[tuple[str, Any]]:
    """``(column, value)`` pairs for a request's filter: a ``{Type}Filter`` message's
    explicitly-set fields (the native servicer), or the entries of the JSON object the HTTP gRPC
    proxy received as ``body["filter"]`` (REQ-803) — in JSON a field is set by being present.

    Every ``{Type}Filter`` field is declared ``optional`` (proto_gen.py), so ``HasField`` reliably
    distinguishes "client filtered this column to its zero value" from "client didn't set this
    column at all" — a plain (non-``optional``) proto3 scalar field can't make that distinction
    (REQ-1860)."""
    if filter_msg is None:
        return []
    if isinstance(filter_msg, Mapping):
        return list(filter_msg.items())
    return [
        (f.name, getattr(filter_msg, f.name))
        for f in filter_msg.DESCRIPTOR.fields
        if filter_msg.HasField(f.name)
    ]


# --- read_mask (REQ-803) --------------------------------------------------------------------------
# One semantics for the native servicer and the HTTP gRPC proxy: the mask is validated against the
# columns the role can read, lowered into the SELECT list, and a dotted sub-path into a JSON-valued
# column restricts that column's value.

# Registered column types whose value is a JSON document (proto ``string``, proto_gen._PROTO_TYPE_MAP).
_JSON_COLUMN_TYPES = frozenset({"json", "jsonb"})

# A sub-path selection inside one JSON value: key → the selection below it, None = the whole value.
MaskTree = dict[str, "MaskTree | None"]


class ReadMaskError(ValueError):
    """A ``read_mask`` path the request may not name; ``path`` is the offending path verbatim."""

    def __init__(self, path: str, type_name: str, reason: str) -> None:
        super().__init__(f"Invalid read_mask path {path!r} for {type_name}: {reason}")
        self.path = path


def _norm_name(name: str) -> str:
    """Result columns and proto fields are matched separator/case-insensitively (governance may
    re-case or alias a column; proto collapses ``__``) — the servicer's own matching rule."""
    return name.replace("_", "").lower()


@dataclass(frozen=True)
class ReadMask:
    """A validated ``read_mask``: the columns the query selects, and the JSON sub-path selections."""

    columns: tuple[str, ...]  # in the table's own column order, each once
    sub_paths: Mapping[str, MaskTree]  # normalized column name → selection inside its JSON value

    def restrictions(self, column_names: Sequence[str]) -> list[MaskTree | None]:
        """One entry per result column: its JSON selection, or None when its whole value is
        returned. Resolved once per result set, not per row."""
        return [self.sub_paths.get(_norm_name(c)) for c in column_names]


def _json_valued(data_type: str) -> bool:
    return data_type.lower().split("(")[0].strip() in _JSON_COLUMN_TYPES


def _add_sub_path(tree: MaskTree, segments: Sequence[str]) -> None:
    """Merge one sub-path into ``tree``. A path selecting a whole value wins over any path
    selecting inside it, whichever came first."""
    head, rest = segments[0], segments[1:]
    if not rest:
        tree[head] = None
        return
    if head in tree and tree[head] is None:
        return
    child = tree.setdefault(head, {})
    assert child is not None
    _add_sub_path(child, rest)


def _readable_fields(ctx: Any, meta: Any) -> dict[str, tuple[str, str]]:
    """Proto field name → ``(column, registered type)`` for the columns this role can read. A
    request names proto fields; a column's proto field is its name, ``__`` collapsed to ``_``
    (proto_gen._to_proto_field_name)."""
    table_columns = ctx.aggregate_columns.get(meta.table_id, [])
    by_field = {c.replace("__", "_"): (c, t) for c, t in table_columns}
    by_field.update({c: (c, t) for c, t in table_columns})
    return by_field


def _json_value_matches(value: Any, data_type: str) -> str | None:
    """None when a JSON body's filter ``value`` is the JSON type the native ``{Type}Filter``
    field carries for a column of ``data_type`` (the proto type proto_gen declares for it), else
    the JSON type expected. A boolean is not a number."""
    proto_type = _physical_to_proto(data_type)
    if proto_type == "bool":
        return None if isinstance(value, bool) else "a boolean"
    if proto_type in ("double", "float"):
        is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
        return None if is_number else "a number"
    if "int" in proto_type:
        return None if isinstance(value, int) and not isinstance(value, bool) else "an integer"
    return None if isinstance(value, str) else "a string"


def _checked_filter(
    ctx: Any, meta: Any, type_name: str, filter_msg: Any | None
) -> list[tuple[str, str, Any]]:
    """The request filter's equalities as ``(column, registered type, value)``, checked. Shared
    by every lowering of a filter (``Query{Type}`` and ``Query{Type}GroupBy``, native and proxy).

    A filtered field must be a column this role can read: the native server serves one wire proto
    whose ``{Type}Filter`` carries every column, and a predicate on a hidden one would return only
    the rows matching it — the row count confirming or refuting the hidden value (GitHub issue
    131). A ``{Type}Filter`` message's values are typed by the proto; a JSON body's value must be
    the JSON type that message's field carries for the column, and is never bound as text for a
    column of another type. Raises :class:`FilterError` naming the field."""
    readable = _readable_fields(ctx, meta)
    from_body = isinstance(filter_msg, Mapping)
    checked = []
    for field, value in _filter_set_fields(filter_msg):
        if field not in readable:
            raise FilterError(field, type_name, "not a readable field")
        column, data_type = readable[field]
        if from_body:
            expected = _json_value_matches(value, data_type)
            if expected is not None:
                raise FilterError(field, type_name, f"value must be {expected}")
        checked.append((column, data_type, value))
    return checked


# Registered column types whose filter value arrives as TEXT (the proto filter field is ``string``,
# proto_gen) but must compare as the column's own type → the SQL type its bound value is cast to.
# A text literal coerces to these implicitly on some engines; a bound text value does not on all
# (Trino has no varchar-to-timestamp comparison), so the cast is stated.
_TEXT_BOUND_CASTS = {
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMPTZ",
    "timestamptz": "TIMESTAMPTZ",
    "date": "DATE",
    "time": "TIME",
    "time with time zone": "TIMETZ",
    "timetz": "TIMETZ",
    "uuid": "UUID",
}
_TYPE_PRECISION_RE = re.compile(r"\s*\([^)]*\)")
_ISO_T_SEPARATOR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})T")


def _bound_filter_value(value: Any, data_type: str, collector: ParamCollector) -> str:
    """Bind one filter value and return the SQL it is compared as: its placeholder, cast to the
    column's type when a typed column's value arrived as text (see ``_TEXT_BOUND_CASTS``)."""
    cast_type = _TEXT_BOUND_CASTS.get(_TYPE_PRECISION_RE.sub("", data_type.lower()).strip())
    if cast_type is None or not isinstance(value, str):
        return collector.add(value)
    if cast_type.startswith("TIMESTAMP"):
        # ISO 8601's 'T' separator is not accepted by every engine's text-to-timestamp cast.
        value = _ISO_T_SEPARATOR_RE.sub(r"\1 ", value)
    return f"CAST({collector.add(value)} AS {cast_type})"


def _filter_where(
    ctx: Any, meta: Any, type_name: str, filter_msg: Any | None, collector: ParamCollector
) -> list[str]:
    """The request filter (see ``_checked_filter``; raises :class:`FilterError`) as equality
    predicates whose values are BOUND (``$N`` placeholders, the values in ``collector``) — never
    text in the statement, so requests that differ only in their filter values are one
    statement."""
    return [
        f"{_q(column)} = {_bound_filter_value(value, data_type, collector)}"
        for column, data_type, value in _checked_filter(ctx, meta, type_name, filter_msg)
    ]


def resolve_read_mask(ctx: Any, type_name: str, paths: Sequence[str]) -> ReadMask | None:
    """Validate a request's ``read_mask`` paths against the table matching ``type_name``.

    None when the mask is empty/absent (every column is selected) or no table matches (the caller's
    own lookup reports that). A path's first segment must be a column this role can read — a
    column it cannot read, a relation field (``Query{Type}`` reads the table's own columns and
    never populates one) and a name that does not exist are the same error, so the mask reveals
    nothing the role's schema does not. Further segments select inside the value and are accepted
    only on a JSON-valued column. Raises :class:`ReadMaskError` naming the offending path."""
    if not paths:
        return None
    meta = _find_table_meta(ctx, type_name)
    if meta is None:
        return None
    table_columns = ctx.aggregate_columns.get(meta.table_id, [])
    by_field = _readable_fields(ctx, meta)

    whole: set[str] = set()
    trees: dict[str, MaskTree] = {}
    for path in paths:
        segments = path.split(".")
        if not all(segments):
            raise ReadMaskError(path, type_name, "empty path segment")
        if segments[0] not in by_field:
            raise ReadMaskError(path, type_name, "not a readable field")
        column, data_type = by_field[segments[0]]
        if len(segments) == 1:
            whole.add(column)
            continue
        if not _json_valued(data_type):
            raise ReadMaskError(path, type_name, f"{segments[0]!r} is not a JSON-valued field")
        _add_sub_path(trees.setdefault(column, {}), segments[1:])

    selected = whole | trees.keys()
    return ReadMask(
        columns=tuple(c for c, _t in table_columns if c in selected),
        sub_paths={_norm_name(c): tree for c, tree in trees.items() if c not in whole},
    )


def _restrict_decoded(value: Any, tree: MaskTree) -> Any:
    if isinstance(value, dict):
        kept = {}
        for key, item in value.items():
            if key in tree:
                below = tree[key]
                kept[key] = item if below is None else _restrict_decoded(item, below)
        return kept
    if isinstance(value, list):
        return [_restrict_decoded(item, tree) for item in value]
    return value


def restrict_json(value: Any, tree: MaskTree | None) -> Any:
    """Apply a JSON sub-path selection to one column value, keeping the value's own form: JSON
    text (what a source driver returns for json/jsonb) stays text, a decoded object or list stays
    decoded. An object keeps only the selected keys; a list is restricted item by item; a scalar
    or null has no keys to select and is returned as is. ``None`` selects the whole value."""
    if tree is None:
        return value
    if isinstance(value, str):
        return json.dumps(_restrict_decoded(json.loads(value), tree))
    return _restrict_decoded(value, tree)


def grpc_table_to_semantic_sql(
    ctx: Any,
    type_name: str,
    limit: int,
    filter_msg: Any | None = None,
    read_mask: ReadMask | None = None,
) -> tuple[str, list] | None:
    """Semantic SELECT over the table matching ``type_name`` and its bound values —
    ``(sql, params)`` — or None if none matches. proto collapses the domain separator
    (``PS__Inquiries`` → ``PsInquiries``), so match case/separator-insensitively.

    ``filter_msg`` (REQ-1860) is the request's ``{Type}Filter`` sub-message, or the HTTP gRPC
    proxy's ``body["filter"]`` object (REQ-803); its set fields (see ``_filter_set_fields``) become
    an AND-joined equality WHERE clause (``_filter_where``; raises :class:`FilterError`) whose
    values are bound: ``$N`` in the statement, the values in ``params`` (REQ-1877). A positive
    ``limit`` is bound the same way, after them.

    ``read_mask`` (REQ-803, see ``resolve_read_mask``) narrows the SELECT list to the masked
    columns, so the source reads only those.

    The statement text is the compiled stage's plan key (pgwire.governed_plan). It is the
    request's SHAPE — table, masked columns, filtered fields, whether it is limited — so two
    masks never share a kept plan, and requests that differ only in their bound values share
    one."""
    meta = _find_table_meta(ctx, type_name)
    if meta is None:
        return None
    if read_mask is not None:
        selected: Sequence[str] = read_mask.columns
    else:
        selected = [c for c, _t in ctx.aggregate_columns.get(meta.table_id, [])]
    cols = ", ".join(_q(c) for c in selected) or "*"
    sql = f"SELECT {cols} FROM {_semantic_table_ref(meta)}"
    collector = ParamCollector()
    where_parts = _filter_where(ctx, meta, type_name, filter_msg, collector)
    if where_parts:
        sql = f"{sql} WHERE {' AND '.join(where_parts)}"
    if limit and limit > 0:
        # Bound like the filter values (the GraphQL compiler binds its LIMIT too): a request that
        # differs only in its limit is the same statement.
        sql = f"{sql} LIMIT {collector.add(int(limit))}"
    return sql, collector.params


def _aggregate_field_name(field_name: str) -> str:
    """The GraphQL root field name schema_gen's ``_build_aggregate_query_field`` exposes for this
    table (REQ-1359), so gRPC's synthesized query text targets exactly what the schema built."""
    if active_gql_convention() == "apollo_graphql":
        return f"{field_name}Aggregate"
    return f"{field_name}_aggregate"


def _group_by_field_name(field_name: str) -> str:
    """The GraphQL root field name schema_gen's ``_build_group_by_query_field`` exposes for this
    table (REQ-1359)."""
    if active_gql_convention() == "apollo_graphql":
        return f"{field_name}GroupBy"
    return f"{field_name}_group_by"


AGG_FUNCS = ("count", "sum", "avg", "stddev", "variance", "min", "max")


def _agg_fields_selection(
    ctx: Any,
    table_id: int,
    funcs: list[str] | None = None,
    columns: list[str] | None = None,
) -> str:
    """``{ count sum { ... } avg { ... } stddev { ... } variance { ... } min { ... } max { ... } }``
    selection text, only including sub-selections the schema actually exposes for this table —
    mirrors ``build_agg_fields_type``'s numeric/comparable classification (REQ-196), reused rather
    than reimplemented.

    ``funcs`` restricts the selection to a caller-chosen subset (REQ-1361), matching the
    ``aggregate=count,sum`` function filter JSON:API/REST already support — None/empty means
    every function the schema exposes for this table.

    ``columns`` (REQ-1882) restricts sum/avg/stddev/variance/min/max to a caller-chosen subset of
    columns — None/empty means every eligible column of that function's type, same convention as
    ``funcs``. Without this, even a caller that already narrows ``funcs`` to e.g. ``["sum"]``
    still gets sum() computed over every numeric column the table exposes, not just the one it
    wanted — live-measured as a real, avoidable cost multiplier on a wide aggregate table."""
    cols = ctx.aggregate_columns.get(table_id, [])
    want_cols = set(columns) if columns else None
    if want_cols is not None:
        cols = [(c, t) for c, t in cols if c in want_cols]
    numeric = [c for c, t in cols if _is_numeric(t)]
    comparable = [c for c, t in cols if _is_comparable(t)]
    want = set(funcs) if funcs else None
    parts = []
    if want is None or "count" in want:
        parts.append("count")
    if numeric:
        numeric_sel = "{ " + " ".join(numeric) + " }"
        for fn in ("sum", "avg", "stddev", "variance"):
            if want is None or fn in want:
                parts.append(f"{fn} {numeric_sel}")
    if comparable:
        comparable_sel = "{ " + " ".join(comparable) + " }"
        for fn in ("min", "max"):
            if want is None or fn in want:
                parts.append(f"{fn} {comparable_sel}")
    if not parts:
        parts.append("count")
    return "{ " + " ".join(parts) + " }"


def split_agg_columns(columns: list[Any], row: tuple) -> tuple[dict, dict]:
    """Split a compiled aggregate result row into (top-level scalars, {func: {col: val}}) using
    the ColumnRef.nested_in metadata compile_query already attaches — "count" is nested_in ==
    the aggregate key alone (no dot); "sum"/"avg"/etc are nested_in == "{agg_key}.{func}" (plain
    _aggregate) or "aggregate.{func}" (_group_by's nested aggregate block). Either way the last
    dot-segment is the function name, so one split rule covers both compiled shapes. Shared by
    the gRPC servicer (proto message construction) and the NL executor's gRPC result preview
    (REQ-1359) so both shape aggregate rows identically."""
    from provisa.executor.serialize import _convert_value

    top: dict[str, Any] = {}
    nested: dict[str, dict[str, Any]] = {}
    for col_ref, val in zip(columns, row):
        if val is None:
            continue
        val = _convert_value(val)
        parts = col_ref.nested_in.split(".")
        if len(parts) == 1:
            top[col_ref.column] = val
        else:
            nested.setdefault(parts[-1], {})[col_ref.column] = val
    return top, nested


def split_group_by_columns(columns: list[Any]) -> tuple[list[Any], list[int], list[Any], list[int]]:
    """Partition compiled group-by columns into (group_key_cols, group_key_idx, agg_cols, agg_idx).
    ColumnRef is an unhashable plain dataclass, so columns are split by index rather than by
    dict-keying on it. Shared by the gRPC servicer and the NL executor's gRPC result preview."""
    group_key_idx = [i for i, c in enumerate(columns) if c.nested_in == "groupKey"]
    agg_idx = [i for i, c in enumerate(columns) if c.nested_in != "groupKey"]
    return (
        [columns[i] for i in group_key_idx],
        group_key_idx,
        [columns[i] for i in agg_idx],
        agg_idx,
    )


def grpc_table_to_aggregate_graphql_text(
    ctx: Any,
    type_name: str,
    funcs: list[str] | None = None,
    columns: list[str] | None = None,
) -> str | None:
    """GraphQL query text for ``Query{Type}Aggregate`` (REQ-1359): targets the same
    ``{field}_aggregate`` root field JSON:API/REST synthesize, so gRPC runs the identical
    parse_query/compile_query pipeline instead of a third, divergent aggregate implementation.

    ``funcs`` restricts to a caller-chosen subset of aggregate functions (REQ-1361).
    ``columns`` restricts those functions to a caller-chosen subset of columns (REQ-1882)."""
    meta = _find_table_meta(ctx, type_name)
    if meta is None:
        return None
    agg_field = _aggregate_field_name(meta.field_name)
    selection = _agg_fields_selection(ctx, meta.table_id, funcs, columns)
    # {Type}Aggregate nests its functions under an "aggregate" sub-field (build_aggregate_types
    # in aggregate_gen.py: {"aggregate": ..., "nodes": ...}) — the compiler's
    # _collect_agg_aliases looks for a selection literally named "aggregate", so the synthesized
    # text must nest the same way _group_by's synthesized text already does.
    return f"{{ {agg_field} {{ aggregate {selection} }} }}"


def grpc_relation_scalars(ctx: Any, type_name: str, rel_field: str) -> list[str]:
    """Scalar column names of the related table for a many-to-one/one-to-one relationship field
    on ``type_name`` (REQ-1405), sourced from ``ctx.joins`` rather than GraphQL schema
    introspection — query_ir has no schema, only the compiler context every transport shares."""
    join_meta = ctx.joins.get((type_name, rel_field))
    if join_meta is None or join_meta.cardinality not in ("many-to-one", "one-to-one"):
        return []
    return [c for c, _t in ctx.aggregate_columns.get(join_meta.target.table_id, [])]


def _insert_include_path(
    ctx: Any, table_id: int, type_name: str, tree: dict[str, Any], segments: list[str]
) -> None:
    """Insert one dot-path's segments into a nested selection tree, recursing through
    single-object (many-to-one/one-to-one) relations at any depth (REQ-1405/REQ-1408). A leaf
    ``None`` value marks a selected scalar; a ``dict`` value marks a relation with its own nested
    selection. A relation segment with no remaining scalar/relation descendants is pruned so an
    unresolvable tail (unknown column, one-to-many hop) drops the whole entry rather than emitting
    an empty ``{ }`` block."""
    head, *rest = segments
    join_meta = ctx.joins.get((type_name, head))
    if join_meta is not None and join_meta.cardinality in ("many-to-one", "one-to-one"):
        child = tree.setdefault(head, {})
        if rest:
            _insert_include_path(
                ctx, join_meta.target.table_id, join_meta.target.type_name, child, rest
            )
        else:
            for column, _col_type in ctx.aggregate_columns.get(join_meta.target.table_id, []):
                child.setdefault(column, None)
        if not child:
            tree.pop(head, None)
        return
    if rest:
        return
    scalars = {c for c, _t in ctx.aggregate_columns.get(table_id, [])}
    if head in scalars:
        tree.setdefault(head, None)


def _render_include_tree(tree: dict[str, Any]) -> list[str]:
    fields = []
    for name, sub in tree.items():
        if sub is None:
            fields.append(apply_gql_name(name))
        else:
            fields.append(f"{name} {{ {' '.join(_render_include_tree(sub))} }}")
    return fields


def _include_node_fields(ctx: Any, meta: Any, include: list[str]) -> list[str]:
    """The ``nodes { ... }`` selection for a group-by query's ``include`` list (REQ-1408).

    Entries are either a relationship field (``user`` — every scalar of the related table, the
    REQ-1405 shape), a dot-path at any depth (``user.email``, ``assignment.employee.firstName``
    — recursing through many-to-one relations), or a base-table scalar (``status``). Dot-paths
    make the gRPC ``include`` accept the same projection REST/JSON:API express through
    ``?includeNodes=id,status,assignment.employee.firstName``, so one plan drives every surface.
    Naming no base scalar keeps all of them, which is what ``include_nodes=true`` alone means.
    Entries that resolve to neither a many-to-one relation chain nor a known column are skipped,
    same as an unknown relationship field has always been.
    """
    base_scalars = [c for c, _t in ctx.aggregate_columns.get(meta.table_id, [])]
    tree: dict[str, Any] = {}
    for entry in include:
        segments = [s for s in entry.split(".") if s]
        if segments:
            _insert_include_path(ctx, meta.table_id, meta.type_name, tree, segments)

    selected_base = [name for name, sub in tree.items() if sub is None]
    rel_fields = [
        f"{name} {{ {' '.join(_render_include_tree(sub))} }}"
        for name, sub in tree.items()
        if sub is not None
    ]
    fields = [apply_gql_name(c) for c in (selected_base or base_scalars)]
    fields.extend(rel_fields)
    return fields


def _graphql_literal(val: Any) -> str:
    """A GraphQL-syntax literal for a filter value (REQ-1860): bare ``true``/``false``/numbers,
    double-quoted strings with GraphQL string-escaping (backslash and double-quote only)."""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    escaped = str(val).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _filter_graphql_where(fields: list[tuple[str, str, Any]]) -> str:
    """``where: { col: { eq: v } ... }`` argument text for the request filter's checked
    equalities (``_checked_filter``) — a ``{Type}Filter`` message's (REQ-1860) or the HTTP gRPC
    proxy's ``body["filter"]`` object's (REQ-803) — or "" if none are set."""
    if not fields:
        return ""
    parts = " ".join(
        f"{apply_gql_name(col)}: {{ eq: {_graphql_literal(val)} }}" for col, _type, val in fields
    )
    return f"where: {{ {parts} }}"


def grpc_table_to_group_by_graphql_text(
    ctx: Any,
    type_name: str,
    by_columns: list[str],
    funcs: list[str] | None = None,
    include_nodes: bool = False,
    include: list[str] | None = None,
    filter_msg: Any | None = None,
    columns: list[str] | None = None,
) -> str | None:
    """GraphQL query text for ``Query{Type}GroupBy`` (REQ-1359): targets the same
    ``{field}_group_by(by: [...])`` root field JSON:API/REST synthesize.

    ``funcs`` restricts to a caller-chosen subset of aggregate functions (REQ-1361).
    ``columns`` restricts those functions to a caller-chosen subset of columns (REQ-1882) —
    without it, ``funcs=["sum"]`` still sums every numeric column the table exposes, not just
    the one the caller wanted.
    ``include_nodes`` (REQ-1401) appends a ``nodes { ... }`` sub-selection of the base table's
    scalar columns, mirroring JSON:API/REST's ``?includeNodes=true`` (provisa/api/jsonapi/
    generator.py::_build_group_by_graphql_query). ``include`` (REQ-1405/REQ-1408) selects what
    ``nodes`` projects — many-to-one relationship fields, ``rel.col`` dot-paths, and base-table
    scalars — mirroring JSON:API's ``?include=`` sideloading and REST's ``?includeNodes=``
    dot-path list; see ``_include_node_fields``. ``filter_msg`` (REQ-1860) is the request's
    ``{Type}Filter`` sub-message; its explicitly-set fields become a ``where: { col: { eq: v } }``
    argument, mirroring JSON:API/REST's own equality filters. The filter is checked exactly as
    ``Query{Type}``'s is (``_checked_filter``; raises :class:`FilterError`)."""
    meta = _find_table_meta(ctx, type_name)
    if meta is None:
        return None
    if not by_columns:
        return None
    filter_fields = _checked_filter(ctx, meta, type_name, filter_msg)
    gb_field = _group_by_field_name(meta.field_name)
    by_arg = "[" + ", ".join(apply_gql_name(c) for c in by_columns) + "]"
    agg_selection = _agg_fields_selection(ctx, meta.table_id, funcs, columns)
    nodes_part = ""
    if include_nodes:
        node_fields = _include_node_fields(ctx, meta, include or [])
        nodes_part = f" nodes {{ {' '.join(node_fields)} }}"
    where_part = _filter_graphql_where(filter_fields)
    gb_args = ", ".join(a for a in (f"by: {by_arg}", where_part) if a)
    return f"{{ {gb_field}({gb_args}) {{ groupKey aggregate {agg_selection}{nodes_part} }} }}"
