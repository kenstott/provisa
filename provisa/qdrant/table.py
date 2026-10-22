# Copyright (c) 2026 Kenneth Stott
# Canary: dc11b6dc-2b82-4cd6-a0a3-72cb1307ffea
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table of a Qdrant source (REQ-1983): a collection, or a search.

A collection table is named for its collection and read whole: its replica lands every point.
A search table holds a Query API request in its ``query_template``::

    {"collection": "docs", "query": "@query_vector", "using": "text",
     "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
     "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}

``query`` is always an argument; ``using`` (the named vector searched), ``filter`` (Qdrant's own
filter), ``limit``, ``score_threshold`` and ``model`` are optional, and any value in ``using``,
``filter``, ``limit`` or ``score_threshold`` that is exactly ``"@name"`` is an argument too. Each argument is a parameter of the table (``query_arg``,
REQ-1986), converted strictly to its declared type and placed into the request as a JSON value,
never into its text. A search is read per statement, under the arguments given (the
materialization step): the filter is applied by Qdrant before its nearest ``limit`` points are
chosen; a statement's other conditions narrow those points afterwards.

``model`` names a registered embedding model (``vector_models``, REQ-419): a text given for the
query is then embedded by that model (REQ-430), and a vector given for it must have that model's
dimensions (REQ-429). With no model, the query is a vector.
"""

# Requirements: REQ-1983
from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from provisa.core.parameters import parameter_name

QUERY_ARG = "query_arg"
_ARG = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)$")
_KEYS = frozenset({"collection", "query", "using", "filter", "limit", "score_threshold", "model"})
# Points a search returns when its template states no limit: Qdrant's own default.
DEFAULT_LIMIT = 10


class QdrantTableRefused(ValueError):
    """A Qdrant table that cannot be registered, said naming what is wrong."""

    code = "schema.qdrant_table_invalid"

    def __init__(self, table: str, reason: str) -> None:
        self.params = {"table": table, "reason": reason}
        super().__init__(f"Table {table!r}: {reason}")


@dataclass(frozen=True)
class QdrantSearch:
    collection: str
    request: dict  # the template, its arguments still "@name"


def _arguments_named(value: Any) -> set[str]:
    """Every argument a template value names: each string that is exactly ``"@name"``."""
    if isinstance(value, str):
        found = _ARG.match(value)
        return {found.group(1)} if found else set()
    if isinstance(value, list):
        return set().union(*(_arguments_named(v) for v in value)) if value else set()
    if isinstance(value, dict):
        return set().union(*(_arguments_named(v) for v in value.values())) if value else set()
    return set()


def parse(table: str, text: str) -> QdrantSearch:
    try:
        doc = json.loads(text)
    except ValueError as exc:
        raise QdrantTableRefused(table, f"the search is not JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise QdrantTableRefused(table, 'the search is {"collection": ..., "query": ...}')
    unknown = sorted(set(doc) - _KEYS)
    if unknown:
        raise QdrantTableRefused(
            table,
            f"a search has collection, query, using, filter, limit, score_threshold and model, "
            f"not {unknown}",
        )
    if not isinstance(doc.get("collection"), str) or not doc["collection"]:
        raise QdrantTableRefused(table, "the search names its collection")
    if not isinstance(doc.get("query"), str) or not _ARG.match(doc["query"]):
        raise QdrantTableRefused(table, 'the search\'s query is an argument, "@name"')
    if "using" in doc and not isinstance(doc["using"], str):
        raise QdrantTableRefused(table, "using names one of the collection's vectors")
    if "model" in doc and (not isinstance(doc["model"], str) or _ARG.match(doc["model"])):
        raise QdrantTableRefused(table, "model names a registered embedding model")
    if "filter" in doc and not isinstance(doc["filter"], dict):
        raise QdrantTableRefused(table, "the filter is a Qdrant filter object")
    for key in ("limit", "score_threshold"):
        value = doc.get(key)
        if value is None or isinstance(value, (int, float)) and not isinstance(value, bool):
            continue
        if not (isinstance(value, str) and _ARG.match(value)):
            raise QdrantTableRefused(table, f"{key} is a number or an argument")
    return QdrantSearch(doc["collection"], doc)


def _get(column: Any, key: str) -> Any:
    return column.get(key) if isinstance(column, dict) else getattr(column, key, None)


def _columns(table: Any) -> list[Any]:
    return list(_get(table, "columns") or [])


def _parameters(table: Any) -> list[Any]:
    return [c for c in _columns(table) if _get(c, "native_filter_type") is not None]


def check_table(table: Any) -> QdrantSearch | None:
    """A Qdrant table as registered: a search names exactly its declared arguments, its query's
    argument is a vector (``array(double)``); a collection table has no parameter; every column
    is typed. Returns the parsed search, or None for a collection table."""
    name = _get(table, "table_name")
    params = _parameters(table)
    other = sorted({_get(c, "native_filter_type") for c in params} - {QUERY_ARG})
    if other:
        raise QdrantTableRefused(name, f"a Qdrant table's parameters are query_arg, not {other}")
    untyped = [
        _get(c, "name") or _get(c, "column_name")
        for c in _columns(table)
        if not _get(c, "data_type")
    ]
    if untyped:
        raise QdrantTableRefused(name, f"the columns {untyped} have no data type")
    text = _get(table, "query_template")
    if not text:
        if params:
            raise QdrantTableRefused(name, "a collection table has no search to take arguments")
        return None
    if _get(table, "change_signal") == "native":
        raise QdrantTableRefused(
            name, "a search is read under the arguments of each statement -- use a TTL"
        )
    search = parse(name, text)
    declared = {parameter_name(c): c for c in params}
    named = _arguments_named(search.request)
    if named - set(declared):
        raise QdrantTableRefused(
            name,
            f"the search names {sorted(named - set(declared))}, which the table declares as no argument",
        )
    if set(declared) - named:
        raise QdrantTableRefused(
            name, f"the arguments {sorted(set(declared) - named)} appear nowhere in the search"
        )
    query_arg = _ARG.match(search.request["query"]).group(1)  # type: ignore[union-attr]
    if not _get(declared[query_arg], "native_filter_required"):
        raise QdrantTableRefused(
            name, f"the query's argument {query_arg!r} is required: Qdrant requires a query"
        )
    vector_type = (_get(declared[query_arg], "data_type") or "").replace(" ", "").lower()
    if vector_type not in ("array(double)", "array(float)", "array(real)"):
        raise QdrantTableRefused(
            name,
            f"the query's argument {query_arg!r} is a vector, array(double), not {vector_type!r}",
        )
    return search


def has_parameters(table: dict) -> bool:
    return any(c["native_filter_type"] == QUERY_ARG for c in table["columns"])


def is_search(table: dict, source_types: dict[str, str]) -> bool:
    """Whether a registered table (a ``state.tables`` row) is a Qdrant search, read per
    statement."""
    return source_types.get(table["source_id"]) == "qdrant" and bool(table.get("query_template"))


def _given(table: dict, given: dict | None) -> dict[str, Any]:
    """Each argument of ``table`` with the value given for it (None where none is), unconverted."""
    from provisa.compiler.naming import apply_sql_name

    by_canon = {apply_sql_name(k.lstrip("_")): v for k, v in (given or {}).items()}
    return {
        parameter_name(c): by_canon.get(apply_sql_name(parameter_name(c).lstrip("_")))
        for c in table["columns"]
        if c["native_filter_type"] == QUERY_ARG
    }


def cache_arguments(table: dict, given: dict | None) -> dict[str, str]:
    """The arguments a read gives, by name, as text: what its landed rows are kept under."""
    return {k: str(v) for k, v in _given(table, given).items() if v is not None}


def _is_text(value: Any) -> bool:
    """A query given as words rather than a vector: a string that is not a JSON array."""
    if not isinstance(value, str):
        return False
    try:
        return not isinstance(json.loads(value), list)
    except ValueError:
        return True


async def _vector(
    state: Any, table: dict, column: dict, value: Any, model_id: str | None
) -> list[float]:
    """The query vector ``value`` stands for: a vector as given, or a text embedded by the
    search's registered model; checked against that model's dimensions."""
    from provisa.core.parameter_values import convert
    from provisa.vector.query import validate_vector_dimensions, vectorize_text

    name = parameter_name(column)
    if _is_text(value):
        if not model_id:
            raise QdrantTableRefused(
                table["table_name"],
                f"{name!r} was given a text; a text query needs the search's embedding model",
            )
        model = await registered_model(state, model_id)
        embedded = await vectorize_text(value, model)
        validate_vector_dimensions(embedded, model)
        return embedded
    vector = convert(value, column["data_type"], table=table["table_name"], parameter=name)
    if model_id:
        validate_vector_dimensions(vector, await registered_model(state, model_id))
    return [float(x) for x in vector]


async def registered_model(state: Any, model_id: str) -> Any:
    """The org's registered embedding model ``model_id`` (``vector_models``, REQ-419), as the AI
    Models page holds it; one not registered or disabled is refused by name."""
    from provisa.api.mcp.tools import effective_config
    from provisa.vector.registry import VectorModelRegistry

    cfg = await effective_config(state)
    entries = [vm if isinstance(vm, dict) else vars(vm) for vm in cfg.get("vector_models") or []]
    return VectorModelRegistry.from_config(entries).get(model_id)


def _json_value(value: Any) -> Any:
    from provisa.cosmosdb.table import json_value

    return json_value(value)


def _bound(value: Any, values: dict[str, Any]) -> Any:
    """``value`` with each ``"@name"`` replaced by that argument's JSON value."""
    if isinstance(value, str):
        found = _ARG.match(value)
        return values[found.group(1)] if found else value
    if isinstance(value, list):
        return [_bound(v, values) for v in value]
    if isinstance(value, dict):
        return {k: _bound(v, values) for k, v in value.items()}
    return value


async def plan(state: Any, table: dict, given: dict | None) -> dict:
    """What a search of ``table`` sends: its collection, query vector, named vector, filter, limit
    and score threshold, every argument converted and bound."""
    from provisa.core.parameter_values import convert

    search = check_table(table)
    assert search is not None  # only a search is read per statement
    columns = {
        parameter_name(c): c for c in table["columns"] if c["native_filter_type"] == QUERY_ARG
    }
    raw = _given(table, given)
    query_arg = _ARG.match(search.request["query"]).group(1)  # type: ignore[union-attr]
    values: dict[str, Any] = {
        query_arg: await _vector(
            state, table, columns[query_arg], raw[query_arg], search.request.get("model")
        )
    }
    for name, value in raw.items():
        if name == query_arg:
            continue
        col = columns[name]
        values[name] = _json_value(
            convert(value, col["data_type"], table=table["table_name"], parameter=name)
        )
    request = _bound(search.request, values)
    limit = request.get("limit")
    return {
        "collection": search.collection,
        "query": request["query"],
        "using": request.get("using"),
        "filter": _without_absent(request.get("filter")),
        "limit": DEFAULT_LIMIT if limit is None else int(limit),
        "score_threshold": request.get("score_threshold"),
    }


def _without_absent(node: Any) -> Any:
    """A filter with each condition whose argument was not given left out: an optional argument
    not given filters nothing. A condition is a dict holding the absent value anywhere; a clause
    (must / should / must_not) left empty is dropped, and a filter left empty is None."""
    if node is None:
        return None
    if isinstance(node, dict):
        out = {}
        for key, value in node.items():
            if isinstance(value, list):
                kept = [v for v in (_without_absent(v) for v in value) if v is not None]
                if kept:
                    out[key] = kept
            elif isinstance(value, dict):
                inner = _without_absent(value)
                if inner is None:
                    return None
                out[key] = inner
            elif value is None:
                return None
            else:
                out[key] = value
        return out or None
    return node


async def read(state: Any, table: dict, given: dict | None) -> Any:
    """Search ``table`` with the arguments ``given`` and return its checked rows, nearest
    first."""
    import asyncio

    from provisa.core.ir_types import to_ir
    from provisa.federation.registry_view import registered_sources
    from provisa.mongodb.pipeline_rows import OutputColumn, project
    from provisa.mongodb.pipeline_table import PipelineRead
    from provisa.qdrant.fetch import QdrantConnection, search, vectors_read

    name = table["table_name"]
    request = await plan(state, table, given)
    outputs = [
        OutputColumn(c["column_name"], c["data_type"], c.get("path"))
        for c in table["columns"]
        if c["native_filter_type"] is None
    ]
    found = [s for s in await registered_sources(state) if s.id == table["source_id"]]
    if not found:
        raise ValueError(f"Table {name!r}: its source {table['source_id']!r} is not registered")
    conn = QdrantConnection.from_source(found[0])
    vectors = vectors_read([o.path or o.name for o in outputs])

    def _run() -> list[dict]:
        docs = search(
            conn,
            request["collection"],
            query=request["query"],
            using=request["using"],
            query_filter=request["filter"],
            limit=request["limit"],
            score_threshold=request["score_threshold"],
            vectors=vectors,
        )
        return [project(name, doc, outputs, dates_as_text=True) for doc in docs]

    rows = await asyncio.to_thread(_run)
    return PipelineRead(rows=rows, columns=[(c.name, to_ir(c.data_type)) for c in outputs])


async def read_each(
    state: Any,
    table: dict,
    argument_sets: Sequence[dict],
    carried: Mapping[str, tuple[str, str]],
) -> Any:
    """REQ-2099: ``table`` searched once for each of ``argument_sets`` -- a join's distinct sets of
    the values a relationship feeds it -- live, one query a set, and their rows together. Each row
    carries, in each column of ``carried`` (``{column: (parameter, IR type)}``), the value its set
    gave that parameter: a search's points hold nothing of the query that found them, and the
    join matches them to the row they were searched for by that value."""
    from provisa.core.ir_types import to_ir
    from provisa.mongodb.pipeline_table import PipelineRead

    columns = [
        (c["column_name"], to_ir(c["data_type"]))
        for c in table["columns"]
        if c["native_filter_type"] is None
    ] + [(column, to_ir(data_type)) for column, (_parameter, data_type) in carried.items()]
    rows: list[dict] = []
    for given in argument_sets:
        found = await read(state, table, given)
        stamp = {column: given[parameter] for column, (parameter, _t) in carried.items()}
        rows.extend({**row, **stamp} for row in found.rows)
    return PipelineRead(rows=rows, columns=columns)


async def preview_search(
    state: Any, source: Any, template: str, arguments: list[dict], rows: int
) -> tuple[list[dict], list[Any]]:
    """Run a search template with sample ``arguments`` (each ``{name, dataType, value}``) for
    Register Table: up to ``rows`` points, and the columns they suggest -- the point's id and
    score, then its payload's fields."""
    import asyncio

    from provisa.mongodb.pipeline_rows import OutputColumn, infer_columns
    from provisa.qdrant.fetch import ID_PATH, SCORE_PATH, QdrantConnection, search

    search_table = {
        "source_id": source.id,
        "table_name": "preview",
        "query_template": template,
        "change_signal": None,
        "columns": [
            {
                "column_name": f"_nf_{a['name']}",
                "data_type": a["dataType"],
                "native_filter_type": QUERY_ARG,
                # The query is the one argument Qdrant requires.
                "native_filter_required": True,
            }
            for a in arguments
        ],
    }
    parsed = parse("preview", template)
    query_arg = _ARG.match(parsed.request["query"]).group(1)  # type: ignore[union-attr]
    for c in search_table["columns"]:
        c["native_filter_required"] = parameter_name(c) == query_arg
    request = await plan(state, search_table, {a["name"]: a.get("value") for a in arguments})
    conn = QdrantConnection.from_source(source)
    docs = await asyncio.to_thread(
        lambda: search(
            conn,
            request["collection"],
            query=request["query"],
            using=request["using"],
            query_filter=request["filter"],
            limit=min(request["limit"], rows),
            score_threshold=request["score_threshold"],
            vectors=False,
        )
    )
    payloads = [{k: v for k, v in d.items() if not k.startswith("@")} for d in docs]
    ids = [d[ID_PATH] for d in docs]
    id_type = "bigint" if ids and all(isinstance(i, int) for i in ids) else "text"
    columns = [
        OutputColumn("id", id_type, ID_PATH),
        OutputColumn("score", "double", SCORE_PATH),
        *infer_columns(payloads),
    ]
    return docs, columns
