# Copyright (c) 2026 Kenneth Stott
# Canary: 8fa39aeb-21b3-490c-8e2d-16d607463a4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The native hop beneath each source column of the lineage graph (REQ-2116).

A registered table's column is read from something its source names natively: an API operation's
field, a collection's field path, a file, a topic, a function's result, a native table's column.
Each source column gains one node beneath it, the raw field as the source reads it, in a box for
the native address it is read from; the edge between them names the renaming on the way (a field
path, an alias, the naming convention). The address is built from what the source already records,
and nothing secret is copied: no credential, no secret reference, no URL's user or query.
"""

# Requirements: REQ-2116

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from provisa.compiler.naming import apply_sql_name
from provisa.lineage.graph import Edge, LineageGraph, Node, TransformOp

#: A native box's relation is the registered relation it lands in, under this prefix.
NATIVE_PREFIX = "native:"
#: The operation on a native edge; its name says how the raw field is renamed.
RENAME = "rename"

#: The source hints the type label is read from (sourceTypeLabel in the UI): a brand, or the
#: Kaggle dataset a files source was staged from. No other hint is copied.
_BRAND_HINTS = ("brand", "kaggle_owner", "kaggle_ref")
_FILE_TYPES = frozenset({"csv", "parquet", "files", "logs"})
_CODE_TYPES = frozenset({"node", "python"})
#: A document or search store: what its schema and table are called natively.
_DOCUMENT_NAMES = {
    "mongodb": ("database", "collection"),
    "cosmosdb": ("database", "container"),
    "elasticsearch": (None, "index"),
    "qdrant": (None, "collection"),
}


@dataclass(frozen=True)
class NativeField:
    raw: str  # the field as the source reads it
    column: str  # the registered column's SQL name
    rename: str | None  # "path" | "alias" | "naming"; None when the names are one


@dataclass(frozen=True)
class NativeTable:
    relation: str  # the registered relation the hop lands in
    source_id: str
    source_type: str
    hints: dict[str, str]
    address: tuple[tuple[str, str], ...]  # (what, value), in reading order
    fields: tuple[NativeField, ...]
    served: dict[str, Any]
    # The schema and table the source reads it from; None where the source has no such thing.
    source_schema: str | None = None
    source_table: str | None = None

    @property
    def native_relation(self) -> str:
        return f"{NATIVE_PREFIX}{self.relation}"

    def to_dict(self) -> dict:
        return {
            "relation": self.relation,
            "source": self.source_id,
            "type": self.source_type,
            "hints": dict(self.hints),
            "address": [{"what": what, "value": value} for what, value in self.address],
            "served": dict(self.served),
        }


def sql_column_name(column: dict) -> str:
    """The name a registered column is read by in SQL: its alias, else its name under the naming
    convention -- the compiler's own rule (compiler/context.py)."""
    return column.get("alias") or apply_sql_name(column["column_name"])


def _field(column: dict) -> NativeField:
    raw = column.get("path") or column["column_name"]
    sql = sql_column_name(column)
    if column.get("path"):
        rename: str | None = "path"
    elif column.get("alias"):
        rename = "alias"
    elif sql != column["column_name"]:
        rename = "naming"
    else:
        rename = None
    return NativeField(raw=raw, column=sql, rename=rename)


def _public(value: str) -> str:
    """``value`` without what could carry a credential: a URL's user, password and query."""
    parts = urlsplit(value)
    if not parts.scheme or not parts.netloc:
        return value
    host = parts.hostname or ""
    netloc = f"{host}:{parts.port}" if parts.port else host
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _parameters(params: dict | None) -> str | None:
    """An operation's fixed parameters; one given by a secret reference is named, its value not."""
    if not params:
        return None
    return ", ".join(
        k if isinstance(v, str) and "${" in v else f"{k}={v}" for k, v in sorted(params.items())
    )


def native_address(
    source_type: str,
    table: dict,
    source: Any,
    *,
    endpoint: dict | None = None,
    graphql: dict | None = None,
    topic: str | None = None,
) -> tuple[tuple[str, str], ...]:
    """Where ``table`` is read from, as its source names it (REQ-2116). ``endpoint`` is an OpenAPI
    table's api_endpoints row, ``graphql`` a remote GraphQL table's mapped spec, ``topic`` a Kafka
    table's topic."""
    schema, name = table["schema_name"], table["table_name"]
    out: list[tuple[str, str | None]]
    if endpoint is not None:
        out = [
            ("operation", f"{endpoint['method']} {endpoint['path']}"),
            ("parameters", _parameters(endpoint.get("default_params"))),
            ("rows", endpoint.get("response_root")),
        ]
    elif graphql is not None:
        out = [
            ("field", graphql.get("field_name") or graphql.get("name") or name),
            # The mapped spec's required arguments are {name, gql_type} (graphql_remote.mapper).
            (
                "arguments",
                ", ".join(a["name"] for a in graphql.get("required_args") or []) or None,
            ),
            ("rows", graphql.get("rows_path")),
        ]
    elif topic is not None:
        out = [("topic", topic)]
    elif source_type in _FILE_TYPES:
        out = [
            ("path", _public(source.path) if source.path else None),
            ("files", table.get("file_glob")),
        ]
    elif source_type in _CODE_TYPES:
        out = [("function", name)]
    elif source_type in _DOCUMENT_NAMES:
        outer, inner = _DOCUMENT_NAMES[source_type]
        out = [(outer, schema)] if outer else []
        out.append((inner, name))
    else:
        out = [("table", f"{schema}.{name}")]
    return tuple((what, value) for what, value in out if value)


def source_object(
    source_type: str, address: tuple[tuple[str, str], ...]
) -> tuple[str | None, str | None]:
    """(schema, table) a table is read from at its source, from its address: a database's
    schema-qualified table, a document store's database and collection; (None, None) for a source
    whose address names no table (an API operation, a topic, a file, a function)."""
    named = dict(address)
    if source_type in _DOCUMENT_NAMES:
        outer, inner = _DOCUMENT_NAMES[source_type]
        return (named.get(outer) if outer else None), named.get(inner)
    qualified = named.get("table")
    if qualified is None:
        return None, None
    schema, _, table = qualified.partition(".")
    return schema, table


def _iso(at: datetime | None) -> str | None:
    return at.isoformat() if at is not None else None


def served(
    replica: bool, cache_ttl: int | None, last_load: datetime | None, next_load: datetime | None
) -> dict[str, Any]:
    """How a table is served: virtual (read at each statement) or a replica, with its freshness
    (``cache_ttl``) and its last and next load."""
    if not replica:
        return {"mode": "virtual"}
    return {
        "mode": "replica",
        "cache_ttl": cache_ttl,
        "last_load": _iso(last_load),
        "next_load": _iso(next_load),
    }


def native_table(
    relation: str,
    source: Any,
    table: dict,
    address: tuple[tuple[str, str], ...],
    serving: dict[str, Any],
) -> NativeTable:
    """``table``'s native hop: its address, and the raw field of each column that holds data (a
    native-filter column is an argument of the read, not a field of its rows)."""
    stype = source.type.value if hasattr(source.type, "value") else str(source.type)
    hints = source.federation_hints or {}
    schema, name = source_object(stype, address)
    return NativeTable(
        relation=relation,
        source_id=source.id,
        source_type=stype,
        hints={k: hints[k] for k in _BRAND_HINTS if isinstance(hints.get(k), str)},
        address=address,
        fields=tuple(_field(c) for c in table["columns"] if c.get("native_filter_type") is None),
        served=serving,
        source_schema=schema,
        source_table=name,
    )


def with_native_hop(graph: LineageGraph, natives: list[NativeTable]) -> None:
    """Add the native hop beneath every source column of ``graph`` whose table is in ``natives``.

    A statement's graph names a base table by its bare name (sqlglot drops the schema), so a
    native table is found by its relation or, when no other has it, by its bare name."""
    by_relation = {n.relation: n for n in natives}
    bare: dict[str, list[NativeTable]] = {}
    for n in natives:
        bare.setdefault(n.relation.split(".")[-1], []).append(n)
    for node in list(graph.nodes.values()):
        if node.kind != "source" or node.relation is None:
            continue
        native = by_relation.get(node.relation)
        if native is None and len(bare.get(node.relation, [])) == 1:
            native = bare[node.relation][0]
        if native is None:
            continue
        field = next((f for f in native.fields if f.column == node.column), None)
        if field is None:
            continue
        node_id = f"{native.native_relation}.{field.raw}"
        graph.nodes[node_id] = Node(
            id=node_id,
            column=field.raw,
            relation=native.native_relation,
            kind="native",
            source_id=native.source_id,
            source_schema=native.source_schema,
            source_table=native.source_table,
        )
        ops = (TransformOp(field.rename, RENAME),) if field.rename else ()
        graph.add_edge(Edge(source=node_id, target=node.id, transform=field.raw, ops=ops))


def natives_payload(graph: LineageGraph, natives: list[NativeTable]) -> dict[str, dict]:
    """The native boxes ``graph`` draws, by their relation."""
    drawn = {n.relation for n in graph.nodes.values() if n.kind == "native"}
    return {n.native_relation: n.to_dict() for n in natives if n.native_relation in drawn}
