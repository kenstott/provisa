# Copyright (c) 2026 Kenneth Stott
# Canary: ad7cb300-5a7f-4937-a662-ec8ac7f97d54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register Table on a query-API source (REQ-1670 neo4j, REQ-1683 sparql): the per-type preview
behind the form and the endpoint persistence the ``registerTable``/``updateTable`` mutations run.

A query-API source has no tables to list — its table IS a query projection — so the form previews
the query (rows + inferred column types), the operator names the table, and registration writes
the same ``api_sources``/``api_endpoints`` rows config load writes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import sources

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult, QueryPreviewType
    from provisa.core.database import Connection
    from provisa.core.models import Table

QUERY_API_TYPES = frozenset({"neo4j", "sparql"})


async def source_row(conn: "Connection", source_id: str) -> dict[str, Any] | None:
    res = await conn.execute_core(
        select(
            sources.c.id,
            sources.c.type,
            sources.c.host,
            sources.c.port,
            sources.c.database,
            sources.c.mapping,
        ).where(sources.c.id == source_id)
    )
    row = res.fetchone()
    return dict(row._mapping) if row is not None else None


async def preview_sparql(conn: "Connection", source_id: str, query: str) -> "QueryPreviewType":
    """Run the SPARQL SELECT (LIMIT 5) against the source's endpoint and infer the columns: one per
    variable the answer's head names, typed by its bindings' XSD datatypes (text otherwise)."""
    import httpx

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.api_source.normalizers import sparql_bindings
    from provisa.api_source.persist import data_type_for_api_column
    from provisa.sparql.persist import sparql_config_from_source
    from provisa.sparql.reasoning import inferred_iri
    from provisa.sparql.source import infer_columns, probe_endpoint

    row = await source_row(conn, source_id)
    if row is None or row["type"] != "sparql":
        return QueryPreviewType(error=f"{source_id!r} is not a registered sparql source")
    cfg, _ = sparql_config_from_source(source_id=source_id, endpoint_url=row["host"])
    if inferred_iri(query) is not None:
        return await _preview_inferred(row, query)
    try:
        results = await probe_endpoint(cfg, query)
    except (httpx.HTTPError, ValueError) as exc:
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    columns = infer_columns(results)
    if not columns:
        return QueryPreviewType(
            error="The answer names no SELECT variables; columns cannot be inferred."
        )
    return QueryPreviewType(
        rows=sparql_bindings(results),
        columns=[
            QueryPreviewColumnType(name=c.name, data_type=data_type_for_api_column(c.type))
            for c in columns
        ],
    )


async def _preview_inferred(row: dict[str, Any], query: str) -> "QueryPreviewType":
    """REQ-2138: preview an inferred class or property of the source's reasoner: the source's
    classification, reasoned now, and the first rows of the IRI the query names -- a class's
    members (``member``), a property's pairs (``subject``, ``value``)."""
    import httpx

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.core.secrets import resolve_secrets
    from provisa.sparql import reasoning

    try:
        reasoner = reasoning.source_reasoner(row["id"], row["mapping"] or {})
    except ValueError as exc:
        return QueryPreviewType(error=str(exc))
    if reasoner is None:
        return QueryPreviewType(
            error=f"{row['id']!r} names no reasoner; a query of one IRI reads an inferred class "
            "or property"
        )
    iri = reasoning.inferred_iri(query)
    assert iri is not None
    try:
        found = await reasoning.classification(
            row["id"], reasoner[1], resolve_secrets(row["host"]), {}, ttl=0
        )
    except (reasoning.ReasonerFailed, httpx.HTTPError) as exc:
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    kind = found.signature.kind(iri)
    if kind is None:
        return QueryPreviewType(error=f"<{iri}> is not a class or property the axioms name")
    names = ["member"] if kind == "class" else ["subject", "value"]
    facts = found.facts.get(iri, [])[:PREVIEW_ROWS]
    return QueryPreviewType(
        rows=[dict(zip(names, fact)) for fact in facts],
        columns=[QueryPreviewColumnType(name=n, data_type="text") for n in names],
    )


# Documents a pipeline preview returns.
PREVIEW_ROWS = 5


async def preview_pipeline(
    conn: "Connection", source_id: str, native_query: str, arguments: list[dict]
) -> "QueryPreviewType":
    """REQ-1980: run a native query on a MongoDB source with sample ``arguments`` (each
    ``{name, dataType, value}``) and return up to five documents and the columns they suggest.
    Failures come back as ``error``: a half-written pipeline is the normal state of the editor."""
    import asyncio
    import json

    from bson import json_util

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.core.parameter_values import convert
    from provisa.federation.registry_view import registered_sources
    from provisa.mongodb.fetch import MongoConnection, aggregate_row_batches
    from provisa.mongodb.native_query import bind, check
    from provisa.mongodb.pipeline_rows import infer_columns

    from provisa.api.app import state

    row = await source_row(conn, source_id)
    if row is None or row["type"] != "mongodb":
        return QueryPreviewType(error=f"{source_id!r} is not a registered mongodb source")
    try:
        values = {
            a["name"]: (
                convert(a.get("value"), a["dataType"], table="preview", parameter=a["name"]),
                a["dataType"],
            )
            for a in arguments
        }
        query = check("preview", native_query, set(values))
        pipeline = [*bind(query, values), {"$limit": PREVIEW_ROWS}]
        source = next(s for s in await registered_sources(state, conn) if s.id == source_id)
        mongo = MongoConnection.from_source(source)
        database = source.database

        def _run() -> list[dict]:
            return [
                doc
                for batch in aggregate_row_batches(
                    mongo, database, query.input_collection, pipeline, PREVIEW_ROWS
                )
                for doc in batch
            ]

        docs = await asyncio.to_thread(_run)
    except Exception as exc:  # allow-blind-except: a preview reports every failure as its error
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    return QueryPreviewType(
        rows=[
            json.loads(json_util.dumps(d, json_options=json_util.RELAXED_JSON_OPTIONS))
            for d in docs
        ],
        columns=[
            QueryPreviewColumnType(name=c.name, data_type=c.data_type, path=c.path)
            for c in infer_columns(docs)
        ],
    )


async def preview_qdrant_search(
    conn: "Connection", source_id: str, native_query: str, arguments: list[dict]
) -> "QueryPreviewType":
    """REQ-1983: run a Qdrant search template with sample ``arguments`` (each ``{name, dataType,
    value}``, placed into the request as JSON values) and return up to five points and the columns
    they suggest. Failures come back as ``error``."""
    import json

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.federation.registry_view import registered_sources
    from provisa.qdrant.table import preview_search

    from provisa.api.app import state

    row = await source_row(conn, source_id)
    if row is None or row["type"] != "qdrant":
        return QueryPreviewType(error=f"{source_id!r} is not a registered qdrant source")
    try:
        source = next(s for s in await registered_sources(state, conn) if s.id == source_id)
        docs, columns = await preview_search(state, source, native_query, arguments, PREVIEW_ROWS)
    except Exception as exc:  # allow-blind-except: a preview reports every failure as its error
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    return QueryPreviewType(
        rows=[json.loads(json.dumps(d, default=str)) for d in docs],
        columns=[
            QueryPreviewColumnType(name=c.name, data_type=c.data_type, path=c.path) for c in columns
        ],
    )


async def preview_cosmos_query(
    conn: "Connection", source_id: str, native_query: str, arguments: list[dict]
) -> "QueryPreviewType":
    """REQ-1982: run a Cosmos DB query table's query ({"container", "query"}) with sample
    ``arguments`` (each ``{name, dataType, value}``, sent as the query's @parameters) and return
    up to five documents and the columns they suggest. Failures come back as ``error``."""
    import asyncio
    import json

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.cosmosdb.fetch import CosmosConnection, iter_query_batches
    from provisa.cosmosdb.table import json_value, parse
    from provisa.core.parameter_values import convert
    from provisa.federation.registry_view import registered_sources
    from provisa.mongodb.pipeline_rows import infer_columns

    from provisa.api.app import state

    row = await source_row(conn, source_id)
    if row is None or row["type"] != "cosmosdb":
        return QueryPreviewType(error=f"{source_id!r} is not a registered cosmosdb source")
    try:
        query = parse("preview", native_query)
        parameters = [
            {
                "name": f"@{a['name']}",
                "value": json_value(
                    convert(a.get("value"), a["dataType"], table="preview", parameter=a["name"])
                ),
            }
            for a in arguments
        ]
        source = next(s for s in await registered_sources(state, conn) if s.id == source_id)
        cosmos = CosmosConnection.from_source(source)
        docs = await asyncio.to_thread(
            lambda: next(
                iter_query_batches(cosmos, query.container, query.query, parameters, PREVIEW_ROWS),
                [],
            )[:PREVIEW_ROWS]
        )
    except Exception as exc:  # allow-blind-except: a preview reports every failure as its error
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    return QueryPreviewType(
        rows=[json.loads(json.dumps(d, default=str)) for d in docs],
        columns=[
            QueryPreviewColumnType(name=c.name, data_type=c.data_type, path=c.path)
            for c in infer_columns(docs)
        ],
    )


async def preview_es_search(
    conn: "Connection", source_id: str, native_query: str, arguments: list[dict]
) -> "QueryPreviewType":
    """REQ-2017: run a native search on an Elasticsearch source with sample ``arguments`` (each
    ``{name, dataType, value}``) and return up to five rows and the columns they suggest: the
    buckets' keys and metrics for a search with aggregations, else the mapping's fields the hits
    hold. Failures come back as ``error``."""
    import asyncio
    import itertools
    import json

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.core.parameter_values import convert
    from provisa.elasticsearch.fetch import ESConnection
    from provisa.elasticsearch.native_query import parse
    from provisa.elasticsearch.native_table import preview_columns, row_batches
    from provisa.federation.registry_view import registered_sources

    from provisa.api.app import state

    row = await source_row(conn, source_id)
    if row is None or row["type"] != "elasticsearch":
        return QueryPreviewType(error=f"{source_id!r} is not a registered elasticsearch source")
    try:
        values = {
            a["name"]: convert(a.get("value"), a["dataType"], table="preview", parameter=a["name"])
            for a in arguments
        }
        search = parse("preview", native_query)
        source = next(s for s in await registered_sources(state, conn) if s.id == source_id)
        es = ESConnection.from_source(source)

        def _run() -> tuple[list[dict], list]:
            docs = list(
                itertools.islice(
                    (
                        doc
                        for batch in row_batches(es, "preview", native_query, values, PREVIEW_ROWS)
                        for doc in batch
                    ),
                    PREVIEW_ROWS,
                )
            )
            return docs, preview_columns(es, search.index, docs, search.buckets)

        docs, columns = await asyncio.to_thread(_run)
    except Exception as exc:  # allow-blind-except: a preview reports every failure as its error
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    return QueryPreviewType(
        rows=[json.loads(json.dumps(d, default=str)) for d in docs],
        columns=[
            QueryPreviewColumnType(name=c.name, data_type=c.data_type, path=c.path) for c in columns
        ],
    )


async def pipeline_table_refusal(conn: "Connection", model: "Table") -> "MutationResult | None":
    """REQ-1980: a MongoDB table registered with a native query is a pipeline table, checked
    before its row lands: the query parses, names only its declared arguments and uses each, and
    only reads. None for every other table."""
    from provisa.api.admin.types import MutationResult
    from provisa.codesource.bundle import BundleRefused
    from provisa.codesource.table import CodeTableRefused
    from provisa.cosmosdb.table import CosmosTableRefused
    from provisa.cosmosdb.table import check_table as check_cosmos_table
    from provisa.elasticsearch.native_query import NativeSearchRefused
    from provisa.elasticsearch.native_query import check_table as check_search_table
    from provisa.mongodb.native_query import NativeQueryRefused, check_table
    from provisa.qdrant.table import QdrantTableRefused
    from provisa.qdrant.table import check_table as check_qdrant_table

    row = await source_row(conn, model.source_id)
    try:
        if row is not None and row["type"] == "cosmosdb":
            # REQ-1982: a container or a query table; its parameters are checked either way.
            check_cosmos_table(model)
        elif row is not None and row["type"] == "qdrant":
            # REQ-1983: a collection table or a search; its arguments are checked either way.
            check_qdrant_table(model)
        elif model.query_template and row is not None and row["type"] == "mongodb":
            check_table(model)
        elif model.query_template and row is not None and row["type"] == "elasticsearch":
            # REQ-2017: an index table, or a native search; a search is checked here.
            check_search_table(model)
        elif row is not None and row["type"] in ("node", "python"):
            # REQ-1984, REQ-1993: checked against the function it names, in the code as read.
            await _check_code_table(model)
    except (
        NativeQueryRefused,
        NativeSearchRefused,
        CosmosTableRefused,
        QdrantTableRefused,
        CodeTableRefused,
    ) as refused:
        return MutationResult(
            success=False, message=str(refused), code=refused.code, params=refused.params
        )
    except BundleRefused as refused:  # the source's code could not be installed or read
        return MutationResult(
            success=False, message=str(refused), code=refused.code, params=refused.params
        )
    return None


async def _check_code_table(model: "Table") -> None:
    """``model`` against the functions its code source offers, the source prepared (installed and
    read) as it is for a read."""
    from provisa.api.app import state
    from provisa.codesource.source import context_for, prepare
    from provisa.codesource.table import check_table as check_code_table
    from provisa.federation.registry_view import registered_sources

    check_code_table(model)
    (source,) = [s for s in await registered_sources(state) if s.id == model.source_id]
    prepared = await prepare(source.id, source.type.value, source.mapping, await context_for(state))
    check_code_table(model, prepared.manifest)


async def persist_query_api_registration(
    conn: "Connection", model: "Table"
) -> "MutationResult | None":
    """After the registered_tables row lands: refuse a query-API table with no query, else persist
    its endpoint and mirror it into the live endpoint map the schema builder and event loop read.
    No-op for every other source type."""
    from provisa.api.admin.types import MutationResult
    from provisa.api.app import state

    row = await source_row(conn, model.source_id)
    if row is None or row["type"] not in QUERY_API_TYPES:
        return None
    if not model.query_template:
        return MutationResult(
            success=False,
            message=f"A table on a {row['type']} source requires the query that produces its rows.",
            code="schema.query_template_required",
            params={"table": model.table_name, "source_type": row["type"]},
        )
    if row["type"] == "neo4j":
        from provisa.neo4j.persist import persist_neo4j_table

        api_source, endpoint = await persist_neo4j_table(
            conn,
            source_id=model.source_id,
            host=row["host"],
            port=row["port"],
            database=row["database"],
            base_url=None,
            table_name=model.table_name,
            query_template=model.query_template,
            columns=model.columns,
            ttl=model.cache_ttl or 300,
        )
    else:
        from provisa.sparql.persist import persist_sparql_table

        from provisa.sparql.reasoning import source_reasoner

        api_source, endpoint = await persist_sparql_table(
            conn,
            source_id=model.source_id,
            endpoint_url=row["host"],
            reasoner=source_reasoner(model.source_id, row["mapping"] or {}),  # REQ-2138
            table_name=model.table_name,
            query_template=model.query_template,
            columns=model.columns,
            ttl=model.cache_ttl or 300,
        )
    if not isinstance(getattr(state, "api_sources", None), dict):
        state.api_sources = {}
    state.api_sources[api_source.id] = api_source
    from provisa.api_source.endpoints import put_endpoint

    put_endpoint(state, endpoint)
    return None
