# Copyright (c) 2026 Kenneth Stott
# Canary: 842ff1ca-043b-404b-9fc3-e52b9ac17084
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A single-source statement on a Qdrant source, lowered to Qdrant's API (REQ-2000).

The statement arrives governed: the role's row rules are conditions of its WHERE, its masks are
expressions of its projection, and a similarity search is the governed call
``cosine_similarity(column, query, '<model>', <dimensions>)`` whose query is a vector by now. It
is lowered only where Qdrant's API states it exactly:

* one collection table, read for plain columns, no join, aggregate, grouping, DISTINCT, window or
  subquery;
* its WHERE, a Qdrant filter: AND, OR, and NOT over ``=``, ``<>``, ``IN``, ``NOT IN``, ranges and
  ``IS [NOT] NULL`` of a payload field or the point id, with SQL's own treatment of a field a point
  does not hold (it satisfies neither ``= v`` nor ``<> v``);
* point-id conditions alone and no ordering: ``retrieve``; no ordering: ``scroll``;
* ``ORDER BY cosine_similarity(<vector>, q) DESC LIMIT k`` over a cosine vector: ``query_points``,
  the filter applied before the nearest ``k`` are chosen, through Qdrant's own (approximate) index.

:func:`route_reason` says, before a statement is routed, why one is not expressible; such a
statement is the engine's, over the collection's replica, so no rule of the role's is skipped.
The driver runs what was routed to it.

Writes: INSERT (refusing an id the collection holds), ``INSERT … ON CONFLICT (id) DO UPDATE SET
c = EXCLUDED.c`` (an upsert), UPDATE and DELETE whose condition is a filter. A text written to a
vector column is embedded by the column's registered model; a vector is used as given, its
dimensions checked.
"""

# Requirements: REQ-2000
from __future__ import annotations

import asyncio
import datetime as dt
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import sqlglot
from sqlglot import exp

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult
from provisa.federation.mail_write import WriteRefused
from provisa.qdrant.fetch import ID_PATH, SCORE_PATH, VECTOR_PATH, QdrantConnection, document

_SIMILARITY = "cosine_similarity"
_TEXT = frozenset({"text", "varchar", "char", "string", "uuid"})
_INTEGER = frozenset({"smallint", "integer", "int", "bigint", "int2", "int4", "int8"})
_FLOAT = frozenset({"double", "float", "real", "float4", "float8", "numeric", "decimal"})
_TEMPORAL = frozenset({"timestamp", "timestamptz", "date", "datetime"})
_BATCH = 256


class NotExpressible(ValueError):
    """A part of a statement Qdrant's API does not state exactly."""


@dataclass(frozen=True)
class _Param:
    """A statement parameter (``$N``), bound when the statement runs."""

    index: int


@dataclass(frozen=True)
class Output:
    name: str
    data_type: str
    path: str


@dataclass
class ReadPlan:
    collection: str
    kind: str  # "retrieve" | "scroll" | "query"
    outputs: list[Output]
    filter: Any = None
    ids: list[Any] = field(default_factory=list)
    limit: int | None = None
    query: Any = None
    using: str | None = None


# -- what a statement names ---------------------------------------------------------------------


def _base(data_type: str | None) -> str:
    return (data_type or "").strip().lower().split("(")[0].strip()


def _columns(table: dict) -> dict[str, dict]:
    return {c["column_name"]: c for c in table["columns"] if c.get("native_filter_type") is None}


def _path(column: dict) -> str:
    return column.get("path") or column["column_name"]


def _is_vector(column: dict) -> bool:
    path = _path(column)
    return path == VECTOR_PATH or path.startswith(VECTOR_PATH + ".")


def _vector_name(column: dict) -> str | None:
    """The named vector a vector column reads (None: the collection's unnamed vector)."""
    path = _path(column)
    return None if path == VECTOR_PATH else path[len(VECTOR_PATH) + 1 :]


def _column(node: exp.Expr, columns: dict[str, dict]) -> dict:
    if not isinstance(node, exp.Column):
        raise NotExpressible(f"{node.sql()} is not a column of the collection")
    found = columns.get(node.name)
    if found is None:
        raise NotExpressible(f"{node.name!r} is not a column of the collection")
    return found


def _value(node: exp.Expr) -> Any:
    """A literal or a parameter, as it is compared or written."""
    if isinstance(node, exp.Paren):
        return _value(node.this)
    if isinstance(node, (exp.Parameter, exp.Placeholder)):
        index = node.this.this if isinstance(node, exp.Parameter) else node.name
        return _Param(int(index))
    if isinstance(node, exp.Cast):
        return _value(node.this)
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return -_value(node.this)
    if isinstance(node, exp.Boolean):
        return node.this
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = node.this
        return int(text) if text.lstrip("-").isdigit() else float(text)
    raise NotExpressible(f"{node.sql()} is not a value")


# -- WHERE as a Qdrant filter -------------------------------------------------------------------


def _match(path: str, value: Any) -> dict:
    return {"key": path, "match": {"value": value}}


def _absent(path: str) -> dict:
    """A point holds no value for the field: missing or null."""
    return {"is_empty": {"key": path}}


def _equals(column: dict, value: Any) -> dict:
    path, kind = _path(column), _base(column.get("data_type"))
    if path == ID_PATH:
        return {"has_id": [value]}
    if kind in _TEXT or kind in _INTEGER or kind in ("boolean", "bool"):
        return _match(path, value)
    if kind in _FLOAT or kind in _TEMPORAL:
        return {"key": path, "range": {"gte": value, "lte": value}}
    raise NotExpressible(f"{column['column_name']!r} ({kind}) has no Qdrant filter form")


def _any_of(column: dict, values: list[Any]) -> dict:
    path, kind = _path(column), _base(column.get("data_type"))
    if path == ID_PATH:
        return {"has_id": values}
    if kind in _TEXT or kind in _INTEGER:
        return {"key": path, "match": {"any": values}}
    return {"should": [_equals(column, v) for v in values]}


def _not(column: dict, held: dict) -> dict:
    """``column`` holds a value and ``held`` does not hold: SQL's ``<>`` and ``NOT IN``."""
    if _path(column) == ID_PATH:
        return {"must_not": [held]}
    return {"must_not": [held, _absent(_path(column))]}


_RANGES = {exp.GT: "gt", exp.GTE: "gte", exp.LT: "lt", exp.LTE: "lte"}
_FLIP = {"gt": "lt", "gte": "lte", "lt": "gt", "lte": "gte"}


def _comparison(node: exp.Expr, columns: dict[str, dict]) -> dict:
    left, right = node.this, node.expression
    flipped = False
    if not isinstance(left, exp.Column) and isinstance(right, exp.Column):
        left, right, flipped = right, left, True
    column = _column(left, columns)
    if _is_vector(column):
        raise NotExpressible(f"{column['column_name']!r} is a vector, not a filter field")
    value = _value(right)
    if value is None:
        raise NotExpressible(f"{node.sql()} compares with NULL")
    if isinstance(node, exp.EQ):
        return _equals(column, value)
    if isinstance(node, exp.NEQ):
        return _not(column, _equals(column, value))
    bound = _RANGES[type(node)]
    if flipped:
        bound = _FLIP[bound]
    kind = _base(column.get("data_type"))
    if _path(column) == ID_PATH or not (kind in _INTEGER or kind in _FLOAT or kind in _TEMPORAL):
        raise NotExpressible(f"{column['column_name']!r} ({kind}) has no Qdrant range")
    return {"key": _path(column), "range": {bound: value}}


def _in(node: exp.In, columns: dict[str, dict], negated: bool) -> dict:
    if node.args.get("query") is not None:
        raise NotExpressible("IN over a subquery has no Qdrant filter form")
    column = _column(node.this, columns)
    values = [_value(v) for v in node.expressions]
    if any(v is None for v in values):
        raise NotExpressible(f"{node.sql()} lists NULL")
    held = _any_of(column, values)
    return _not(column, held) if negated else held


def condition(node: exp.Expr, columns: dict[str, dict]) -> dict | None:
    """``node`` as a Qdrant filter condition; None for a condition that always holds."""
    if isinstance(node, exp.Paren):
        return condition(node.this, columns)
    if isinstance(node, exp.Boolean):
        if node.this:
            return None
        raise NotExpressible("FALSE has no Qdrant filter form")
    if isinstance(node, exp.And):
        parts = [condition(p, columns) for p in node.flatten()]
        kept = [p for p in parts if p is not None]
        return {"must": kept} if kept else None
    if isinstance(node, exp.Or):
        parts = [condition(p, columns) for p in node.flatten()]
        if any(p is None for p in parts):
            return None
        return {"should": parts}
    if isinstance(node, (exp.EQ, exp.NEQ, *_RANGES)):
        return _comparison(node, columns)
    if isinstance(node, exp.In):
        return _in(node, columns, negated=False)
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Null):
        column = _column(node.this, columns)
        return {"must": [_absent(_path(column))]}
    if isinstance(node, exp.Not):
        inner = node.this.this if isinstance(node.this, exp.Paren) else node.this
        if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
            return {"must_not": [_absent(_path(_column(inner.this, columns)))]}
        if isinstance(inner, exp.EQ):
            return _comparison(exp.NEQ(this=inner.this, expression=inner.expression), columns)
        if isinstance(inner, exp.In):
            return _in(inner, columns, negated=True)
        raise NotExpressible(f"{node.sql()} has no Qdrant filter form")
    if isinstance(node, exp.Column):
        column = _column(node, columns)
        if _base(column.get("data_type")) in ("boolean", "bool"):
            return _match(_path(column), True)
    raise NotExpressible(f"{node.sql()} has no Qdrant filter form")


_FILTER_KEYS = frozenset({"must", "should", "must_not", "min_should"})


def as_filter(held: dict | None) -> dict | None:
    """A condition as a Qdrant filter object: one condition is the filter's single ``must``."""
    if held is None or set(held) <= _FILTER_KEYS:
        return held
    return {"must": [held]}


def _ids_only(where: exp.Expr | None, columns: dict[str, dict]) -> list[Any] | None:
    """The point ids a WHERE names when it is exactly ``id = v`` or ``id IN (…)``."""
    if where is None:
        return None
    node = where.this if isinstance(where, exp.Paren) else where
    if isinstance(node, exp.EQ):
        left, right = node.this, node.expression
        if not isinstance(left, exp.Column):
            left, right = right, left
        if (
            isinstance(left, exp.Column)
            and _path(columns.get(left.name, {"column_name": ""})) == ID_PATH
        ):
            return [_value(right)]
    if (
        isinstance(node, exp.In)
        and isinstance(node.this, exp.Column)
        and not node.args.get("query")
    ):
        if _path(columns.get(node.this.name, {"column_name": ""})) == ID_PATH:
            return [_value(v) for v in node.expressions]
    return None


# -- a SELECT as a read -------------------------------------------------------------------------


def _similarity(node: exp.Expr) -> exp.Anonymous | None:
    if isinstance(node, exp.Anonymous) and str(node.this).lower() == _SIMILARITY:
        return node
    return None


def _table(tree: exp.Expr) -> exp.Table:
    from_ = tree.args.get("from") or tree.args.get("from_")
    if from_ is None or not isinstance(from_.this, exp.Table):
        raise NotExpressible("the statement reads no collection table")
    if tree.args.get("joins"):
        raise NotExpressible("the statement joins another table")
    return from_.this


def analyse(tree: exp.Expr, table: dict, distance_of: Any) -> ReadPlan:
    """``tree`` (a governed SELECT on ``table``) as one read of Qdrant's API, or
    :class:`NotExpressible` naming what Qdrant does not state exactly. ``distance_of(vector)``
    is the distance a collection's vector is searched by."""
    if not isinstance(tree, exp.Select):
        raise NotExpressible("the statement is not a single SELECT")
    for part, said in (
        ("with", "a WITH"),
        ("group", "a GROUP BY"),
        ("having", "a HAVING"),
        ("distinct", "DISTINCT"),
        ("offset", "an OFFSET"),
        ("windows", "a window"),
    ):
        if tree.args.get(part):
            raise NotExpressible(f"the statement has {said}")
    if table.get("query_template"):
        raise NotExpressible("a search table is read under its arguments")
    if tree.find(exp.AggFunc) is not None:
        raise NotExpressible("the statement aggregates")
    if tree.find(exp.Window) is not None:
        raise NotExpressible("the statement has a window")
    if any(isinstance(s, exp.Subquery) for s in tree.find_all(exp.Subquery)):
        raise NotExpressible("the statement has a subquery")
    named = _table(tree)
    if named.name != table["table_name"]:
        raise NotExpressible(f"{named.name!r} is not the collection {table['table_name']!r}")
    columns = _columns(table)

    order = tree.args.get("order")
    search: exp.Anonymous | None = None
    if order is not None:
        if len(order.expressions) != 1:
            raise NotExpressible("the statement orders by more than one expression")
        ordered = order.expressions[0]
        search = _similarity(ordered.this)
        if search is None or not ordered.args.get("desc"):
            raise NotExpressible("only the most similar first is a Qdrant ordering")

    outputs: list[Output] = []
    for projected in tree.expressions:
        name = projected.alias_or_name
        node = projected.this if isinstance(projected, exp.Alias) else projected
        if isinstance(node, exp.Star):
            raise NotExpressible("the statement selects *")
        call = _similarity(node)
        if call is not None:
            if search is None or call.sql() != search.sql():
                raise NotExpressible(
                    "a similarity is selected that the statement does not order by"
                )
            outputs.append(Output(name, "double", SCORE_PATH))
            continue
        if not isinstance(node, exp.Column):
            raise NotExpressible(f"{name!r} is computed ({node.sql()}), not a collection column")
        column = _column(node, columns)
        outputs.append(Output(name, column["data_type"], _path(column)))

    where = tree.args.get("where")
    filter_ = condition(where.this, columns) if where is not None else None
    limit_node = tree.args.get("limit")
    limit = None
    if limit_node is not None:
        value = _value(limit_node.expression)
        if not isinstance(value, int):
            raise NotExpressible("the LIMIT is not a number")
        limit = value

    if search is not None:
        if limit is None:
            raise NotExpressible("a similarity search names how many rows it reads (LIMIT)")
        vector_arg, query_arg = search.expressions[0], search.expressions[1]
        vector = _column(vector_arg, columns)
        if not _is_vector(vector):
            raise NotExpressible(f"{vector['column_name']!r} is not a vector of the collection")
        using = _vector_name(vector)
        distance = distance_of(using)
        if distance != "Cosine":
            raise NotExpressible(
                f"vector {vector['column_name']!r} is scored by {distance}, not cosine similarity"
            )
        query = _value(query_arg)
        if isinstance(query, str):
            query = [float(x) for x in json.loads(query)]
        return ReadPlan(table["table_name"], "query", outputs, filter_, [], limit, query, using)
    ids = _ids_only(where.this if where is not None else None, columns)
    if ids is not None:
        return ReadPlan(table["table_name"], "retrieve", outputs, None, ids, limit)
    return ReadPlan(table["table_name"], "scroll", outputs, filter_, [], limit)


# -- binding ------------------------------------------------------------------------------------


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return value


def bind(node: Any, params: list | None) -> Any:
    """``node`` with each parameter replaced by its value."""
    if isinstance(node, _Param):
        if params is None or node.index > len(params):
            raise WriteRefused(f"the statement names parameter ${node.index} it was not given")
        return _json_value(params[node.index - 1])
    if isinstance(node, list):
        return [bind(v, params) for v in node]
    if isinstance(node, dict):
        return {k: bind(v, params) for k, v in node.items()}
    return _json_value(node)


def _vectors_read(outputs: list[Output]) -> bool | list[str]:
    from provisa.qdrant.fetch import vectors_read

    return vectors_read([o.path for o in outputs])


def _query_vector(query: Any) -> list[float]:
    if isinstance(query, str):
        query = json.loads(query)
    return [float(x) for x in query]


# -- writes -------------------------------------------------------------------------------------


def _nested(path: str, value: Any) -> tuple[str | None, dict]:
    """A payload path's value as (the object it is set inside, {leaf: value})."""
    if "." not in path:
        return None, {path: value}
    prefix, leaf = path.rsplit(".", 1)
    return prefix, {leaf: value}


async def _vector_value(state: Any, table: dict, column: dict, value: Any) -> list[float]:
    """The vector ``value`` written to ``column`` stands for: a vector as given, or a text
    embedded by the column's registered model; checked against that model's dimensions."""
    from provisa.qdrant.table import registered_model
    from provisa.vector.query import validate_vector_dimensions, vectorize_text

    model_id = column.get("embedding_model")
    if not model_id:
        raise WriteRefused(
            f"vector column {column['column_name']!r} of {table['table_name']!r} names no "
            "embedding model, so what is written to it cannot be checked or embedded"
        )
    model = await registered_model(state, model_id)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            parsed = None
        if isinstance(parsed, list):
            value = parsed
        else:
            embedded = await vectorize_text(value, model)
            validate_vector_dimensions(embedded, model)
            return embedded
    vector = [float(x) for x in value]
    validate_vector_dimensions(vector, model)
    return vector


def _id_column(columns: dict[str, dict], table: dict) -> dict:
    for column in columns.values():
        if _path(column) == ID_PATH:
            return column
    raise WriteRefused(f"{table['table_name']!r} registers no point id column (path @id)")


def _insert_target(tree: exp.Insert) -> tuple[str, list[str]]:
    """The collection an INSERT writes and the columns it names. The physical form of a governed
    statement aliases its target (``INSERT INTO "s"."t" AS "t" ("a", ...)``), and the column list
    is then the alias's."""
    target = tree.this
    if isinstance(target, exp.Schema) and target.expressions:
        return target.this.name, [c.name for c in target.expressions]
    alias = target.args.get("alias") if isinstance(target, exp.Table) else None
    if alias is not None and alias.columns:
        return target.name, [c.name for c in alias.columns]
    raise WriteRefused("an INSERT into a Qdrant collection names its columns")


def _upsert_clause(tree: exp.Insert, id_column: dict) -> list[str] | None:
    """The columns ``INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c`` replaces; None for
    a plain INSERT. Any other conflict clause is refused by name."""
    conflict = tree.args.get("conflict")
    if conflict is None:
        return None
    keys = [k.name for k in conflict.args.get("conflict_keys") or []]
    action = str(conflict.args.get("action") or "").upper()
    if keys != [id_column["column_name"]] or action != "DO UPDATE":
        raise WriteRefused(
            "a Qdrant collection takes ON CONFLICT ("
            + id_column["column_name"]
            + ") DO UPDATE SET "
            "only, its point id the conflict"
        )
    replaced = []
    for assignment in conflict.expressions:
        target, source = assignment.this, assignment.expression
        if (
            not isinstance(assignment, exp.EQ)
            or not isinstance(target, exp.Column)
            or not isinstance(source, exp.Column)
            or str(source.table).upper() != "EXCLUDED"
            or source.name != target.name
        ):
            raise WriteRefused(
                f"{assignment.sql()}: a Qdrant upsert sets each column to EXCLUDED.<the column>"
            )
        replaced.append(target.name)
    return replaced


class QdrantDriver(DirectDriver):  # REQ-2000
    """A Qdrant source's DIRECT driver: the statements :func:`route_reason` accepted, run through
    Qdrant's client; its writes, through the same client."""

    def __init__(self) -> None:
        self._source_id: str | None = None
        self._grpc = False
        self._conn: QdrantConnection | None = None
        self._distances: dict[str, dict[str, str]] = {}

    def bind_source(self, source_id: str) -> None:
        self._source_id = source_id

    def configure(self, extra: dict[str, str]) -> None:
        self._grpc = str(extra.get("prefer_grpc", "")).lower() == "true"

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        if not host.startswith(("https://", "http://")):
            raise ValueError(f"Qdrant source {self._source_id!r}: the host is the instance's URL")
        self._conn = QdrantConnection(url=host, api_key=password or None, prefer_grpc=self._grpc)

    async def close(self) -> None:
        self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    def _client(self) -> Any:
        assert self._conn is not None, "the Qdrant driver is used after it connected"
        return self._conn.client()

    def distance_of(self, collection: str) -> Any:
        """The distance each vector of ``collection`` is searched by, read once."""
        if collection not in self._distances:
            from provisa.qdrant.fetch import collection_shape

            assert self._conn is not None
            shape = collection_shape(self._conn, collection)
            self._distances[collection] = {n: d for n, (_s, d) in shape.vectors.items()}
        held = self._distances[collection]
        return lambda using: held.get(using or "", "unknown")

    def table(self, collection: str) -> dict:
        """The registered collection table of this source named ``collection``."""
        from provisa.api.app import state

        for table in getattr(state, "tables", None) or []:
            if table.get("source_id") == self._source_id and table["table_name"] == collection:
                return table
        raise WriteRefused(
            f"{collection!r} is not a registered table of source {self._source_id!r}"
        )

    async def reason(self, sql: str, table: dict) -> str | None:
        """Why the governed ``sql`` on ``table`` is not run in Qdrant, or None when it is."""
        if table.get("query_template"):
            # a search table names no collection of its own: it is read under its arguments,
            # and its collection is never asked for its vectors here
            return "a search table is read under its arguments"
        try:
            tree = sqlglot.parse_one(sql, read="postgres")
            distances = await asyncio.to_thread(self.distance_of, table["table_name"])
            analyse(tree, table, distances)
        except NotExpressible as refused:
            return str(refused)
        return None

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        tree = sqlglot.parse_one(sql, read="postgres")
        if isinstance(tree, exp.Insert):
            return await self._insert(tree, params)
        if isinstance(tree, exp.Update):
            return await self._update(tree, params)
        if isinstance(tree, exp.Delete):
            return await self._delete(tree, params)
        table = self.table(_table(tree).name)
        distances = await asyncio.to_thread(self.distance_of, table["table_name"])
        plan = analyse(tree, table, distances)
        return await asyncio.to_thread(self._read, plan, params)

    # -- reads --

    def _read(self, plan: ReadPlan, params: list | None) -> QueryResult:
        from provisa.mongodb.pipeline_rows import OutputColumn, project

        client = self._client()
        vectors = _vectors_read(plan.outputs)
        if plan.kind == "retrieve":
            points = client.retrieve(
                collection_name=plan.collection,
                ids=bind(plan.ids, params),
                with_payload=True,
                with_vectors=vectors,
            )
            docs = [document(p) for p in points][: plan.limit]
        elif plan.kind == "query":
            from provisa.core.statement_warnings import warn
            from provisa.vector.similarity import approximate_index_warning

            response = client.query_points(
                collection_name=plan.collection,
                query=_query_vector(bind(plan.query, params)),
                using=plan.using,
                query_filter=self._filter(plan.filter, params),
                limit=plan.limit,
                with_payload=True,
                with_vectors=vectors,
            )
            docs = [document(p) for p in response.points]
            # Qdrant chose the nearest points through its own index, as a replica's index does.
            warn(approximate_index_warning())
        else:
            docs = self._scroll(plan, params, vectors)
        outputs = [OutputColumn(o.name, o.data_type, o.path) for o in plan.outputs]
        rows = [project(plan.collection, d, outputs, dates_as_text=True) for d in docs]
        names = [o.name for o in plan.outputs]
        return QueryResult(rows=[tuple(r[n] for n in names) for r in rows], column_names=names)

    def _filter(self, filter_: Any, params: list | None) -> Any:
        if filter_ is None:
            return None
        from qdrant_client import models

        return models.Filter.model_validate(bind(as_filter(filter_), params))

    def _scroll(self, plan: ReadPlan, params: list | None, vectors: Any) -> list[dict]:
        client = self._client()
        query_filter = self._filter(plan.filter, params)
        docs: list[dict] = []
        offset = None
        while True:
            want = _BATCH if plan.limit is None else min(_BATCH, plan.limit - len(docs))
            points, offset = client.scroll(
                collection_name=plan.collection,
                scroll_filter=query_filter,
                limit=want,
                offset=offset,
                with_payload=True,
                with_vectors=vectors,
            )
            docs.extend(document(p) for p in points)
            if offset is None or (plan.limit is not None and len(docs) >= plan.limit):
                return docs

    # -- writes --

    def _write_filter(self, tree: exp.Expr, table: dict, params: list | None) -> Any:
        where = tree.args.get("where")
        try:
            filter_ = condition(where.this, _columns(table)) if where is not None else None
        except NotExpressible as refused:
            raise WriteRefused(
                f"a write to {table['table_name']!r} is made in Qdrant, and its condition, with "
                f"the role's row rules, does not translate: {refused}"
            ) from None
        from qdrant_client import models

        return models.Filter.model_validate(bind(as_filter(filter_), params) if filter_ else {})

    async def _insert(self, tree: exp.Insert, params: list | None) -> QueryResult:
        from provisa.api.app import state

        target_name, names = _insert_target(tree)
        table = self.table(target_name)
        columns = _columns(table)
        id_column = _id_column(columns, table)
        if id_column["column_name"] not in names:
            raise WriteRefused(f"an INSERT into {table['table_name']!r} gives each point its id")
        values = tree.expression
        if not isinstance(values, exp.Values):
            raise WriteRefused("an INSERT into a Qdrant collection gives its rows as VALUES")
        replaced = _upsert_clause(tree, id_column)
        points = []
        for row in values.expressions:
            given = dict(zip(names, (bind(_value(v), params) for v in row.expressions)))
            point_id = given.pop(id_column["column_name"])
            payload: dict[str | None, dict] = {}
            vectors: dict[str, list[float]] = {}
            for name, value in given.items():
                column = columns.get(name) or _column(exp.column(name), columns)
                if _is_vector(column):
                    if value is not None:
                        vectors[_vector_name(column) or ""] = await _vector_value(
                            state, table, column, value
                        )
                    continue
                prefix, part = _nested(_path(column), value)
                payload.setdefault(prefix, {}).update(part)
            points.append((point_id, payload, vectors))
        await asyncio.to_thread(self._write_points, table, points, replaced is not None)
        return QueryResult(rows=[], column_names=[], rowcount=len(points))

    def _write_points(self, table: dict, points: list, upsert: bool) -> None:
        from qdrant_client import models

        client = self._client()
        collection = table["table_name"]
        ids = [p[0] for p in points]
        held = {p.id for p in client.retrieve(collection_name=collection, ids=ids)}
        if held and not upsert:
            raise WriteRefused(
                f"{collection!r} already holds point(s) {sorted(held, key=str)}: an INSERT adds new "
                "points; ON CONFLICT (id) DO UPDATE replaces them"
            )
        new = [p for p in points if p[0] not in held]
        if new:
            client.upsert(
                collection_name=collection,
                points=[
                    models.PointStruct(
                        id=pid,
                        payload=_payload(payload),
                        vector=_point_vectors(vectors),
                    )
                    for pid, payload, vectors in new
                ],
                wait=True,
            )
        for pid, payload, vectors in (p for p in points if p[0] in held):
            # An existing point keeps every field the statement does not set.
            for prefix, part in payload.items():
                client.set_payload(
                    collection_name=collection, payload=part, points=[pid], key=prefix, wait=True
                )
            if vectors:
                client.update_vectors(
                    collection_name=collection,
                    points=[models.PointVectors(id=pid, vector=_point_vectors(vectors))],
                    wait=True,
                )

    async def _update(self, tree: exp.Update, params: list | None) -> QueryResult:
        from provisa.api.app import state

        table = self.table(tree.this.name)
        columns = _columns(table)
        payload: dict[str | None, dict] = {}
        vectors: dict[str, list[float]] = {}
        for assignment in tree.expressions:
            column = _column(assignment.this, columns)
            if _path(column) == ID_PATH:
                raise WriteRefused("a point's id is not changed by a write")
            try:
                value = bind(_value(assignment.expression), params)
            except NotExpressible:
                raise WriteRefused(
                    f"{assignment.sql()}: a Qdrant write sets a value, not an expression"
                ) from None
            if _is_vector(column):
                vectors[_vector_name(column) or ""] = await _vector_value(
                    state, table, column, value
                )
                continue
            prefix, part = _nested(_path(column), value)
            payload.setdefault(prefix, {}).update(part)
        query_filter = self._write_filter(tree, table, params)

        def _run() -> int:
            from qdrant_client import models

            client = self._client()
            collection = table["table_name"]
            count = client.count(
                collection_name=collection, count_filter=query_filter, exact=True
            ).count
            for prefix, part in payload.items():
                client.set_payload(
                    collection_name=collection,
                    payload=part,
                    points=models.FilterSelector(filter=query_filter),
                    key=prefix,
                    wait=True,
                )
            if vectors:
                ids, offset = [], None
                while True:
                    found, offset = client.scroll(
                        collection_name=collection,
                        scroll_filter=query_filter,
                        limit=_BATCH,
                        offset=offset,
                        with_payload=False,
                        with_vectors=False,
                    )
                    ids.extend(p.id for p in found)
                    if offset is None:
                        break
                client.update_vectors(
                    collection_name=collection,
                    points=[models.PointVectors(id=i, vector=_point_vectors(vectors)) for i in ids],
                    wait=True,
                )
            return count

        return QueryResult(rows=[], column_names=[], rowcount=await asyncio.to_thread(_run))

    async def _delete(self, tree: exp.Delete, params: list | None) -> QueryResult:
        table = self.table(tree.this.name)
        query_filter = self._write_filter(tree, table, params)

        def _run() -> int:
            from qdrant_client import models

            client = self._client()
            collection = table["table_name"]
            count = client.count(
                collection_name=collection, count_filter=query_filter, exact=True
            ).count
            client.delete(
                collection_name=collection,
                points_selector=models.FilterSelector(filter=query_filter),
                wait=True,
            )
            return count

        return QueryResult(rows=[], column_names=[], rowcount=await asyncio.to_thread(_run))


def _payload(parts: dict[str | None, dict]) -> dict:
    """A new point's payload from its (object, {leaf: value}) parts."""
    out: dict = dict(parts.get(None, {}))
    for prefix, part in parts.items():
        if prefix is None:
            continue
        node = out
        for key in prefix.split("."):
            node = node.setdefault(key, {})
        node.update(part)
    return out


def _point_vectors(vectors: dict[str, list[float]]) -> Any:
    """The unnamed vector as a list, named ones as a dict."""
    if list(vectors) == [""]:
        return vectors[""]
    return {name: v for name, v in vectors.items() if name}


async def route_reason(
    state: Any, source_id: str, physical_sql: str, table_ids: tuple
) -> str | None:
    """Why a governed read on a Qdrant source is not run in Qdrant (the engine answers it, over
    the collection's part read live, ``federation.live_only``), or None when it is (REQ-2000)."""
    tables = [t for t in getattr(state, "tables", None) or [] if t.get("id") in set(table_ids)]
    if len(tables) != 1:
        return "the statement reads more than one table"
    driver = state.source_pools.get(source_id)
    return await driver.reason(physical_sql, tables[0])
