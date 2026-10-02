# Copyright (c) 2026 Kenneth Stott
# Canary: 1a33cda3-6b5f-4c87-8e8e-55f84dce50bc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Resolve every transport's spelling of the contract's tables from the deployment (REQ-1911).

The contract names a table once, by its registered identity (source id, schema, table) and its
columns by registered column name. This module asks the deployment what each transport calls them;
it does not reimplement the product's naming rules. Where the benchmark has to MATCH a name the
product serves to a registered table (a Cypher label, a gRPC message, a REST path) it compares the
two with ``norm`` (case and punctuation removed), never by building the name itself.

Endpoints read (the product code that serves each):

* ``POST /admin/graphql`` ``tables`` / ``relationships`` / ``sources``: the registry — identity,
  per-column SQL and GraphQL aliases, the GraphQL root field, the pgwire dataset
  (``provisa/api/admin/schema_query.py`` ``tables``/``relationships``, ``types.py``
  ``RegisteredTableType``/``RelationshipType``)
* ``GET /data/introspection``: GraphQL introspection for the role (``provisa/api/data/sdl.py``)
* ``GET /data/graph-schema``: Cypher labels, properties, relationship types
  (``provisa/api/rest/cypher_router.py``)
* ``GET /data/rest/openapi.json?role=``: REST paths and field enums
  (``provisa/api/rest/generator.py`` ``rest_openapi_json``, ``openapi_spec.py``)
* ``GET /data/jsonapi/openapi.json?role=``: JSON:API paths, ``fields[]``/``filter[]`` parameters
  (``provisa/api/jsonapi/generator.py``, ``spec.py``)
* ``GET /data/proto/{role}``: the role's .proto (``provisa/api/data/endpoint_dev.py`` (router
  prefix ``/data``),
  ``provisa/grpc/proto_gen.py``)
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

LANGS = ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi")

ADMIN_QUERY = """
query BenchLookup {
  sources { id type replicate cacheEnabled cacheTtl }
  tables {
    id sourceId domainId schemaName tableName alias cacheTtl replicate
    graphqlFieldName dqDataset
    columns { columnName computedSqlAlias computedGqlAlias }
  }
  relationships {
    id sourceTableId targetTableId sourceColumn targetColumn cardinality
    graphqlAlias computedCypherAlias disableCypher
  }
}
"""


class LookupError_(ValueError):
    """The deployment's answer cannot be matched to the contract."""


def norm(name: str) -> str:
    """A name with case and punctuation removed, for matching a served name to a registered one."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


@dataclass(frozen=True)
class RawLookup:
    """The deployment's raw answers, as fetched (and as recorded to replay offline)."""

    admin: Mapping[str, Any]
    introspection: Mapping[str, Any]
    graph_schema: Mapping[str, Any]
    rest_spec: Mapping[str, Any]
    jsonapi_spec: Mapping[str, Any]
    proto: str


@dataclass(frozen=True)
class TableIdentity:
    source: str
    schema: str
    table: str
    columns: tuple[str, ...]  # registered column names the contract uses

    @property
    def key(self) -> str:
        return f"{self.source}/{self.schema}.{self.table}"


@dataclass(frozen=True)
class JoinIdentity:
    left: TableIdentity
    left_column: str
    right: TableIdentity
    right_column: str

    @property
    def key(self) -> str:
        return f"{self.left.key}.{self.left_column}->{self.right.key}.{self.right_column}"


@dataclass(frozen=True)
class TableNames:
    """What each transport calls a table; None where the transport does not expose it. ``columns``
    maps a registered column name to {language: spelling}, with only the languages that expose it."""

    sql: str
    graphql_field: str | None
    cypher_label: str | None
    grpc_type: str | None
    rest_path: str | None
    jsonapi_path: str | None
    jsonapi_type: str | None
    columns: Mapping[str, Mapping[str, str]]
    # the replication setting the registry holds for the table (None: inherits the source)
    replicate: int | None = None
    cache_ttl: int | None = None
    table_id: int | None = None  # the registry id the admin API addresses the table by
    domain_id: str | None = None  # the registered domain (what the audit log's domain_id holds)


@dataclass(frozen=True)
class JoinNames:
    """How GraphQL and Cypher traverse a registered relationship (None: not exposed). ``forward``
    is whether the contract's left table is the relationship's source."""

    forward: bool
    graphql_field: str | None
    cypher_rel: str | None


@dataclass(frozen=True)
class Resolved:
    tables: Mapping[str, TableNames]
    joins: Mapping[str, JoinNames]
    sources: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)  # replication per source

    def to_json(self) -> str:
        def table(t: TableNames) -> dict[str, Any]:
            return {**t.__dict__, "columns": {c: dict(v) for c, v in t.columns.items()}}

        return json.dumps(
            {
                "tables": {k: table(t) for k, t in self.tables.items()},
                "joins": {k: j.__dict__ for k, j in self.joins.items()},
                "sources": {k: dict(v) for k, v in self.sources.items()},
            },
            indent=2,
        )

    @staticmethod
    def from_json(text: str) -> Resolved:
        raw = json.loads(text)
        return Resolved(
            tables={k: TableNames(**v) for k, v in raw["tables"].items()},
            joins={k: JoinNames(**v) for k, v in raw["joins"].items()},
            sources=raw["sources"],
        )


def write_resolved(resolved: Resolved, path: Path) -> None:
    path.write_text(resolved.to_json())


def read_resolved(path: Path) -> Resolved:
    return Resolved.from_json(path.read_text())


# --------------------------------------------------------------------------------------------
# Fetch
# --------------------------------------------------------------------------------------------


def fetch(client: Any, role: str) -> RawLookup:
    """Ask the deployment. ``client`` is an httpx.Client with the deployment's base URL (and its
    credential header); every endpoint is read as ``role``."""
    headers = {"X-Provisa-Role": role}

    def get(path: str, **params: str) -> Any:
        resp = client.get(path, headers=headers, params=params or None)
        resp.raise_for_status()
        return resp

    admin = client.post("/admin/graphql", json={"query": ADMIN_QUERY}, headers=headers)
    admin.raise_for_status()
    body = admin.json()
    if body.get("errors"):
        raise LookupError_(f"/admin/graphql: {str(body['errors'])[:300]}")
    return RawLookup(
        admin=body,
        introspection=get("/data/introspection").json(),
        graph_schema=get("/data/graph-schema").json(),
        rest_spec=get("/data/rest/openapi.json", role=role).json(),
        jsonapi_spec=get("/data/jsonapi/openapi.json", role=role).json(),
        proto=get(f"/data/proto/{role}").text,
    )


def raw_to_json(raw: RawLookup) -> str:
    return json.dumps(
        {
            "admin": raw.admin,
            "introspection": raw.introspection,
            "graph_schema": raw.graph_schema,
            "rest_spec": raw.rest_spec,
            "jsonapi_spec": raw.jsonapi_spec,
            "proto": raw.proto,
        },
        indent=2,
    )


def raw_from_json(text: str) -> RawLookup:
    return RawLookup(**json.loads(text))


# --------------------------------------------------------------------------------------------
# Resolve
# --------------------------------------------------------------------------------------------


def _unwrap(type_ref: Mapping[str, Any]) -> str | None:
    while type_ref.get("kind") in ("NON_NULL", "LIST"):
        type_ref = type_ref["ofType"]
    return type_ref.get("name")


class _Graphql:
    """GraphQL introspection: root fields, their item types, and each type's fields."""

    def __init__(self, introspection: Mapping[str, Any]) -> None:
        schema = introspection["data"]["__schema"]
        self.types = {t["name"]: t for t in schema["types"]}
        query = self.types[schema["queryType"]["name"]]
        self.roots = {f["name"]: _unwrap(f["type"]) for f in query["fields"]}

    def fields_of(self, type_name: str | None) -> dict[str, str | None]:
        if type_name is None or type_name not in self.types:
            return {}
        return {f["name"]: _unwrap(f["type"]) for f in (self.types[type_name].get("fields") or [])}


def _sql_name(dataset: str | None) -> str | None:
    """``<data source>/<schema>/<table>`` (the pgwire dataset) as ``schema.table``."""
    if not dataset:
        return None
    parts = dataset.split("/")
    if len(parts) != 3:
        raise LookupError_(f"dqDataset {dataset!r} is not '<source>/<schema>/<table>'")
    return f"{parts[1]}.{parts[2]}"


def _spec_param_enum(spec: Mapping[str, Any], op: Mapping[str, Any], name: str) -> list[str]:
    for p in op.get("parameters", []):
        if p.get("name") != name:
            continue
        items = (p.get("schema") or {}).get("items") or {}
        ref = items.get("$ref")
        if ref:
            return list(spec["components"]["schemas"][ref.rsplit("/", 1)[1]]["enum"])
        return list(items.get("enum") or [])
    return []


def _pick(candidates: Sequence[str], wanted: Sequence[str]) -> str | None:
    """The served name that matches one of the registered spellings: an exact match first, else the
    one that matches after ``norm``. Two different served names matching is ambiguous."""
    for w in wanted:
        if w in candidates:
            return w
    matches = {c for c in candidates if norm(c) in {norm(w) for w in wanted}}
    if len(matches) > 1:
        raise LookupError_(f"{sorted(matches)} all match {list(wanted)}")
    return next(iter(matches), None)


def _proto_blocks(proto: str) -> dict[str, list[str]]:
    """message name -> its field names."""
    blocks: dict[str, list[str]] = {}
    for m in re.finditer(r"message\s+(\w+)\s*\{(.*?)\n\}", proto, re.S):
        blocks[m.group(1)] = re.findall(
            r"^\s*(?:repeated\s+|optional\s+)?[\w.]+\s+(\w+)\s*=\s*\d+\s*;", m.group(2), re.M
        )
    return blocks


def _proto_roots(proto: str) -> dict[str, str]:
    """``message Query``'s field name -> message type (one per table the role can read)."""
    m = re.search(r"message\s+Query\s*\{(.*?)\n\}", proto, re.S)
    if m is None:
        return {}
    return {
        name: type_name
        for type_name, name in re.findall(r"repeated\s+([\w.]+)\s+(\w+)\s*=\s*\d+\s*;", m.group(1))
    }


def resolve(
    tables: Sequence[TableIdentity], joins: Sequence[JoinIdentity], raw: RawLookup
) -> Resolved:
    admin = raw.admin["data"]
    registry = {(t["sourceId"], t["schemaName"], t["tableName"]): t for t in admin["tables"]}
    gql = _Graphql(raw.introspection)
    nodes = raw.graph_schema["node_labels"]
    proto_roots = _proto_roots(raw.proto)
    proto_blocks = _proto_blocks(raw.proto)

    def registered(ident: TableIdentity) -> Mapping[str, Any]:
        row = registry.get((ident.source, ident.schema, ident.table))
        if row is None:
            raise LookupError_(
                f"table {ident.source}/{ident.schema}.{ident.table} is not registered in the deployment"
            )
        return row

    def columns_of(ident: TableIdentity, row: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
        known = {c["columnName"]: c for c in row["columns"]}
        for name in ident.columns:
            if name not in known:
                raise LookupError_(
                    f"column {name!r} of {ident.key} is not registered (registered: {sorted(known)})"
                )
        return {n: known[n] for n in ident.columns}

    def cypher_node(row: Mapping[str, Any]) -> Mapping[str, Any] | None:
        names = {norm(row["alias"]) if row["alias"] else None, norm(row["tableName"])} - {None}
        hits = [
            n
            for n in nodes
            if norm(n.get("domain_id") or "") == norm(row["domainId"])
            and norm(n["table_label"]) in names
        ]
        if len(hits) > 1:
            raise LookupError_(f"{[h['label'] for h in hits]} all match table {row['tableName']!r}")
        return hits[0] if hits else None

    def rest_like(spec: Mapping[str, Any], row: Mapping[str, Any]) -> tuple[str, str, Any] | None:
        """(path, table name in the path, operation) of the table in a REST-style spec."""
        for tname in [row["alias"], row["tableName"]]:
            if not tname:
                continue
            for path, item in spec["paths"].items():
                if path == f"/{row['domainId']}/{tname}" and "get" in item:
                    prefix = spec["servers"][0]["url"].rstrip("/")  # where the spec is served from
                    return prefix + path, tname, item["get"]
        return None

    out_tables: dict[str, TableNames] = {}
    graphql_item: dict[str, str | None] = {}
    for ident in tables:
        row = registered(ident)
        cols = columns_of(ident, row)
        sql = _sql_name(row.get("dqDataset"))
        if sql is None:
            raise LookupError_(f"{ident.key}: the deployment publishes no SQL name for it")
        spelled: dict[str, dict[str, str]] = {
            c: {"sql": cols[c]["computedSqlAlias"]} for c in ident.columns
        }

        # GraphQL
        root = row.get("graphqlFieldName")
        item_type = gql.roots.get(root) if root else None
        graphql_item[ident.key] = item_type
        gfields = gql.fields_of(item_type)
        graphql_field = root if item_type else None
        if graphql_field:
            for c in ident.columns:
                g = cols[c]["computedGqlAlias"]
                if g in gfields:
                    spelled[c]["graphql"] = g

        # Cypher
        node = cypher_node(row)
        label = node["label"] if node else None
        if node:
            for c in ident.columns:
                prop = _pick(
                    node["properties"],
                    [cols[c]["computedGqlAlias"], c, cols[c]["computedSqlAlias"]],
                )
                if prop:
                    spelled[c]["cypher"] = prop

        # gRPC
        grpc_type = None
        if root:
            for fname, tname in proto_roots.items():
                if norm(fname) == norm(root):
                    grpc_type = tname
        if grpc_type:
            fields = proto_blocks.get(grpc_type, [])
            for c in ident.columns:
                name = _pick(fields, [c, cols[c]["computedSqlAlias"]])
                if name:
                    spelled[c]["grpc"] = name

        # REST
        rest = rest_like(raw.rest_spec, row)
        rest_path = rest[0] if rest else None
        if rest:
            enum = _spec_param_enum(raw.rest_spec, rest[2], "fields")
            for c in ident.columns:
                name = _pick(enum, [cols[c]["computedGqlAlias"], c, cols[c]["computedSqlAlias"]])
                if name:
                    spelled[c]["rest"] = name

        # JSON:API
        japi = rest_like(raw.jsonapi_spec, row)
        japi_path = japi[0] if japi else None
        japi_type = japi[1] if japi else None
        if japi:
            filters = [
                m.group(1)
                for p in japi[2].get("parameters", [])
                if (m := re.fullmatch(r"filter\[([^\]]+)\]", p.get("name", "")))
            ]
            for c in ident.columns:
                name = _pick(filters, [cols[c]["computedGqlAlias"], c, cols[c]["computedSqlAlias"]])
                if name:
                    # The served spec names attributes by their GraphQL names (orderId), but the
                    # server's fieldset validation takes the physical column name and answers
                    # "Unknown field 'orderId'" for the spec's spelling (found by the local run, a
                    # product inconsistency reported to the maintainer). The registered column
                    # name is what the server accepts; the spec only says the column is exposed.
                    spelled[c]["jsonapi"] = c

        out_tables[ident.key] = TableNames(
            sql=sql,
            graphql_field=graphql_field,
            cypher_label=label,
            grpc_type=grpc_type,
            rest_path=rest_path,
            jsonapi_path=japi_path,
            jsonapi_type=japi_type,
            columns=spelled,
            replicate=row["replicate"],
            cache_ttl=row.get("cacheTtl"),
            table_id=row["id"],
            domain_id=row["domainId"],
        )

    # joins: each must be a registered relationship (either orientation)
    rels = admin["relationships"]
    out_joins: dict[str, JoinNames] = {}
    for j in joins:
        l_row, r_row = registered(j.left), registered(j.right)
        match, forward = None, True
        for rel in rels:
            if (
                rel["sourceTableId"],
                rel["targetTableId"],
                rel["sourceColumn"],
                rel["targetColumn"],
            ) == (
                l_row["id"],
                r_row["id"],
                j.left_column,
                j.right_column,
            ):
                match, forward = rel, True
            elif (
                rel["sourceTableId"],
                rel["targetTableId"],
                rel["sourceColumn"],
                rel["targetColumn"],
            ) == (
                r_row["id"],
                l_row["id"],
                j.right_column,
                j.left_column,
            ):
                match, forward = rel, False
        if match is None:
            raise LookupError_(f"join {j.key} is not a registered relationship in the deployment")
        src, tgt = (l_row, r_row) if forward else (r_row, l_row)
        src_key = TableIdentity(src["sourceId"], src["schemaName"], src["tableName"], ()).key
        tgt_key = TableIdentity(tgt["sourceId"], tgt["schemaName"], tgt["tableName"], ()).key
        # GraphQL nested field: the source type's field whose type is the target's item type
        graphql_field = None
        s_item, t_item = graphql_item.get(src_key), graphql_item.get(tgt_key)
        if s_item and t_item:
            cands = [n for n, t in gql.fields_of(s_item).items() if t == t_item]
            if len(cands) > 1 and match.get("graphqlAlias") in cands:
                cands = [match["graphqlAlias"]]
            if len(cands) > 1:
                raise LookupError_(f"{j.key}: GraphQL fields {cands} all reach the target type")
            graphql_field = cands[0] if cands else None
        # Cypher relationship type
        cypher_rel = None
        s_node, t_node = cypher_node(src), cypher_node(tgt)
        if s_node and t_node and not match.get("disableCypher"):
            types = [
                r["type"]
                for r in raw.graph_schema["relationship_types"]
                if r["source"] == s_node["label"] and r["target"] == t_node["label"]
            ]
            alias = match.get("computedCypherAlias")
            if alias in types:
                cypher_rel = alias
            elif len(types) == 1:
                cypher_rel = types[0]
            elif len(types) > 1:
                raise LookupError_(f"{j.key}: Cypher relationship types {types} all connect them")
        out_joins[j.key] = JoinNames(forward, graphql_field, cypher_rel)

    return Resolved(
        tables=out_tables,
        joins=out_joins,
        sources={
            s["id"]: {
                "type": s["type"],
                "replicate": s["replicate"],
                "cache_enabled": s["cacheEnabled"],
                "cache_ttl": s["cacheTtl"],
            }
            for s in admin["sources"]
        },
    )
