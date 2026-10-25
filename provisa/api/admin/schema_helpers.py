# Copyright (c) 2026 Kenneth Stott
# Canary: 9ae387e4-a2ed-414a-af67-45f45a77bb5c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin GraphQL schema helpers — pool, openapi columns, conflict checks.

Module-level helpers extracted from schema.py; called by the Query/Mutation
resolvers. state / _rebuild_schemas are imported lazily inside functions.
"""

# complexity-gate: allow-ble=4 reason="admin helpers relocated verbatim from schema.py; the broad excepts make openapi-column discovery / sqlite-migration / conflict-probe best-effort (fall back to a safe default, never fail the admin request)"

from __future__ import annotations


import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, or_, select

from provisa.core.schema_org import (
    registered_tables,
    sources,
    table_columns,
)

if TYPE_CHECKING:
    from provisa.core.database import Database

from provisa.api_source.openapi_endpoint import normalize_op_id
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.api.admin.types import (
    ImplicitMeasureType,
    RegisteredTableType,
    RoleTtlType,
    UniqueConstraintType,
    TableColumnType,
    WriteGrantType,
    WritePresetType,
    mask_entry_types,
)
from provisa.compiler.aggregate_gen import (
    _classify_columns,
)  # REQ-1360: reuse REQ-196 classification
from provisa.compiler.introspect import ColumnMetadata

from provisa.api.admin._row_mappers import (
    _delta_type_from_row,
    _live_type_from_row,
    _view_metrics_type_from_row,
)

log = logging.getLogger(__name__)


async def _get_pool() -> "Database":
    from provisa.api.app import state

    assert state.model_db is not None
    return state.model_db


async def _dynamic_openapi_columns(base_url: str, query) -> list[dict]:
    """Call a no-input GET endpoint and infer columns from the response keys."""
    import httpx
    from provisa.openapi.register import _openapi_to_provisa_type

    url = base_url.rstrip("/") + query.path
    params = {p["name"]: "" for p in query.query_params}
    try:
        r = httpx.get(url, params=params, timeout=10, follow_redirects=True)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    if isinstance(data, list):
        sample = data[0] if data else {}
    elif isinstance(data, dict):
        sample = data
    else:
        return []
    if not isinstance(sample, dict):
        return []
    additional_type = (query.response_schema or {}).get("additionalProperties", {})
    value_type = _openapi_to_provisa_type(
        additional_type.get("type") if isinstance(additional_type, dict) else None
    )
    return [{"name": k, "type": value_type} for k in sample]


def _query_template_for(source_id: str, table_name: str) -> str | None:  # REQ-1670
    """The Cypher a neo4j table runs — read from the hydrated api_endpoints map, which is where
    the registration persisted it (registered_tables carries no query column)."""
    from provisa.api.app import state
    from provisa.api_source.endpoints import endpoint_of

    ep = endpoint_of(state, source_id, table_name)
    return ep.query_template if ep is not None else None


async def _ensure_openapi_spec(source_id: str) -> bool:
    """Lazy-load an OpenAPI spec into state from the DB source record if missing."""
    from provisa.api.app import state

    if getattr(state, "openapi_specs", {}).get(source_id):
        return True
    pool = await _get_pool()
    if not pool:
        return False
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(sources.c.type, sources.c.path, sources.c.mapping).where(
                sources.c.id == source_id
            )
        )
        _r = result.fetchone()
    row = dict(_r._mapping) if _r is not None else None
    if not row or row["type"] != "openapi" or not row["path"]:
        return False
    try:
        from provisa.core.secrets import resolve_secrets as _resolve_secrets
        from provisa.openapi.exclusions import load_source_spec

        resolved_path = _resolve_secrets(row["path"])
        # REQ-1957: without what the source's own exclusions name.
        spec = load_source_spec(resolved_path, row["mapping"], source_id=source_id)
        servers = spec.get("servers", [])
        base_url = servers[0].get("url", "") if servers else ""
        if (
            base_url
            and not base_url.startswith(("http://", "https://"))
            and resolved_path.startswith(("http://", "https://"))
        ):
            from urllib.parse import urljoin

            base_url = urljoin(resolved_path, base_url)
        if not hasattr(state, "openapi_specs"):
            state.openapi_specs = {}
        state.openapi_specs[source_id] = {
            "spec_path": row["path"],
            "spec": spec,
            "base_url": base_url,
            "domain_id": "",
            "cache_ttl": 300,
        }
        return True
    except Exception:
        return False


async def _rebuild_schemas():
    from provisa.api.app import _rebuild_schemas as rebuild

    await rebuild()


def _compute_can_deploy_to_db(
    view_sql: str,
    all_tables: list,
) -> bool:
    """Return True iff view_sql references tables from exactly one source that has an active pool."""
    from provisa.api.app import state
    from provisa.compiler.naming import domain_to_sql_name

    replacements: list[tuple[str, str, str]] = []
    for t in all_tables:
        domain_sql = domain_to_sql_name(t["domain_id"])
        alias_or_name = t["alias"] or t["table_name"]
        virtual_ref = f'"{domain_sql}"."{alias_or_name}"'
        replacements.append((virtual_ref, t["source_id"], t["schema_name"]))

    hit_sources: dict[str, str] = {}
    for virtual_ref, source_id, schema_name in sorted(
        replacements, key=lambda x: len(x[0]), reverse=True
    ):
        if virtual_ref in view_sql:
            hit_sources[source_id] = schema_name

    if not hit_sources or len(hit_sources) != 1:
        return False

    target_source_id = next(iter(hit_sources))
    return state.source_pools.has(target_source_id)


def _compute_implicit_dim_measures(  # REQ-1360
    col_rows: list[dict], enable_aggregates: bool, enable_group_by: bool
) -> tuple[list[ImplicitMeasureType], list[str], set[str], set[str]]:
    """Derive metadata-only implicit measure/dimension annotations.

    Reuses the exact numeric/comparable classification `build_agg_fields_type`
    (provisa/compiler/aggregate_gen.py, REQ-196) already applies to build the GraphQL
    aggregate result type — never reimplements it. Returns
    (implicit_measures, implicit_dimensions, measure_col_names, dimension_col_names)
    where the trailing two sets let column-level is_implicit_measure/is_implicit_dimension
    flags be computed without re-deriving classification per column.
    """
    if not (enable_aggregates or enable_group_by):
        return [], [], set(), set()

    typed_cols = [r for r in col_rows if r.get("data_type")]
    visible_columns = [{"column_name": r["column_name"]} for r in typed_cols]
    column_metadata = {
        r["column_name"].lower(): ColumnMetadata(
            column_name=r["column_name"], data_type=r["data_type"], is_nullable=True
        )
        for r in typed_cols
    }
    numeric_cols, comparable_cols = _classify_columns(visible_columns, column_metadata)
    numeric_names = {name for name, _ in numeric_cols}
    comparable_names = {name for name, _ in comparable_cols}

    implicit_measures: list[ImplicitMeasureType] = []
    measure_names: set[str] = set()
    if enable_aggregates:
        for col in visible_columns:
            name = col["column_name"]
            if name not in numeric_names and name not in comparable_names:
                continue
            agg_funcs = ["count"]
            if name in numeric_names:
                agg_funcs.extend(["sum", "avg", "stddev", "variance"])
            if name in comparable_names:
                agg_funcs.extend(["min", "max"])
            implicit_measures.append(ImplicitMeasureType(column=name, agg_funcs=agg_funcs))
            measure_names.add(name)

    implicit_dimensions: list[str] = []
    dimension_names: set[str] = set()
    if enable_group_by:
        implicit_dimensions = [col["column_name"] for col in visible_columns]
        dimension_names = set(implicit_dimensions)

    return implicit_measures, implicit_dimensions, measure_names, dimension_names


def native_filter_required(
    recorded: bool | None, filter_type: str | None, source_type: str
) -> bool | None:
    """Whether a parameter column is required, for a row that may predate the record (#204).

    What a registration recorded stands. Where nothing was recorded, two cases are certain by
    design and are resolved; the rest stay None (not known to be required):
    - a path parameter: a path cannot be built without it;
    - any parameter of a remote GraphQL source: only its ``required_args`` (arguments that are
      non-null with no default) are ever registered as columns
      (``_graphql_table_registration._column_models``, ``hasura_v2.remote_schema``).
    An OpenAPI query parameter and a gRPC input carried no such guarantee."""
    if filter_type is None or recorded is not None:
        return recorded
    if filter_type == "path_param" or source_type == "graphql_remote":
        return True
    return None


@dataclass
class TableListing:
    """What the admin's view of a set of tables reads beside their own rows, read once for the
    whole set: the Tables page lists every table, and reading these table by table -- several
    control-plane queries each, and an OpenAPI source's whole spec parsed again for each of its
    tables -- took the listing past 26s on ~70 tables."""

    columns: dict[int, list[dict]]  # table id -> its columns, in order
    # table id -> its columns tagged pii (REQ-1494), each with the term it carries pii through,
    # None when the column is tagged itself (REQ-2145)
    pii: dict[int, dict[str, str | None]]
    sources: dict[str, tuple[str, str | None]]  # source id -> (type, path)
    grants: dict[int, list[dict]]  # table id -> its write grants (REQ-2074)
    endpoints: dict[str, tuple[str, dict[str, Any]]]  # OpenAPI source -> (base url, ops by id)


#: Each OpenAPI source's operations, parsed once per spec it holds: (that spec, its queries).
_parsed_specs: dict[str, tuple[dict, list]] = {}


def _openapi_operations(source_id: str) -> tuple[str, dict[str, Any]] | None:
    """The base url and the operations, by normalized operation id, of the OpenAPI source's
    spec as this process holds it (_ensure_openapi_spec); None when it cannot be parsed."""
    from provisa.api.app import state
    from provisa.openapi.mapper import parse_spec

    spec_info = state.openapi_specs.get(source_id, {})
    spec = spec_info.get("spec", {})
    cached = _parsed_specs.get(source_id)
    if cached is None or cached[0] is not spec:
        try:
            queries, _ = parse_spec(spec)
        except Exception as exc:  # allow-ble: a spec that does not parse names no endpoint for its tables, as before; logged with its cause
            log.warning("openapi source %s: spec does not parse: %s", source_id, exc)
            return None
        cached = _parsed_specs[source_id] = (spec, queries)
    operations: dict[str, Any] = {}
    for q in cached[1]:
        operations.setdefault(normalize_op_id(q.operation_id), q)
    return spec_info.get("base_url", ""), operations


async def table_listing(conn, rows: list[dict]) -> TableListing:
    """What ``rows`` (registered tables) need beside themselves, one query per kind."""
    from provisa.core.repositories import write_grant as write_grant_repo

    ids = [r["id"] for r in rows]
    columns: dict[int, list[dict]] = {}
    for r in (
        await conn.execute_core(
            select(table_columns)
            .where(table_columns.c.table_id.in_(ids))
            .order_by(table_columns.c.table_id, table_columns.c.id)
        )
    ).fetchall():
        columns.setdefault(r._mapping["table_id"], []).append(dict(r._mapping))
    # REQ-1494: the columns tagged pii -- marked in the editor's test-data mode when they declare
    # no fake, since a test-data environment shows them as NULL. REQ-2145: a column carries pii
    # through its term too, and says which.
    from provisa.core.term_tags import effective_column_tags

    pii: dict[int, dict[str, str | None]] = {}
    for (table_id, column), tags in (await effective_column_tags(conn, set(ids))).items():
        through = [t for t in tags if t["base_tag_id"] == "pii"]
        if through:
            pii.setdefault(table_id, {})[column] = next(
                (t["term"] for t in through if t["via"] == "term"), None
            )
    # REQ-1997: the source's path too -- where its file is decides whether it takes writes.
    source_rows = (
        await conn.execute_core(
            select(sources.c.id, sources.c.type, sources.c.path).where(
                sources.c.id.in_(sorted({r["source_id"] for r in rows}))
            )
        )
    ).fetchall()
    known = {r.id: (r.type, r.path) for r in source_rows}
    endpoints: dict[str, tuple[str, dict[str, Any]]] = {}
    for source_id, (source_type, path) in known.items():
        if source_type == "openapi" and path and await _ensure_openapi_spec(source_id):
            operations = _openapi_operations(source_id)
            if operations is not None:
                endpoints[source_id] = operations
    return TableListing(
        columns=columns,
        pii=pii,
        sources=known,
        grants=await write_grant_repo.for_tables(conn, ids),
        endpoints=endpoints,
    )


async def _fetch_table_with_columns(
    conn,
    row,
    all_tables: list | None = None,
    user_can_deploy: bool = True,
    listing: TableListing | None = None,
) -> RegisteredTableType:
    """The admin's view of the registered table ``row``. ``listing``: what a listing of many
    tables read for all of them at once (table_listing); one table alone reads its own."""
    if listing is None:
        listing = await table_listing(conn, [row])
    col_rows = listing.columns.get(row["id"], [])
    from provisa.compiler.naming import apply_gql_name, apply_sql_name

    _pii = listing.pii.get(row["id"], {})

    # REQ-1360: metadata-only implicit measure/dimension annotations, gated by the
    # table's own enable_aggregates/enable_group_by flags.
    implicit_measures, implicit_dimensions, _measure_names, _dimension_names = (
        _compute_implicit_dim_measures(
            col_rows,
            bool(row.get("enable_aggregates", False)),
            bool(row.get("enable_group_by", False)),
        )
    )

    _source_type, _source_path = listing.sources[row["source_id"]]

    columns = [
        TableColumnType(
            id=r["id"],
            column_name=r["column_name"],
            visible_to=list(r["visible_to"]),
            masks=mask_entry_types(r["masks"]),
            alias=r.get("alias"),
            computed_sql_alias=r.get("alias") or apply_sql_name(r["column_name"]),
            computed_gql_alias=r.get("alias")
            or apply_gql_name(r["column_name"], row.get("gql_naming_convention")),
            description=r.get("description"),
            data_type=r.get("data_type"),
            native_filter_type=r.get("native_filter_type"),
            native_filter_required=native_filter_required(
                r.get("native_filter_required"), r.get("native_filter_type"), _source_type
            ),
            required_by_steward=bool(r.get("required_by_steward") or False),  # REQ-1986
            path=r.get("path"),  # REQ-1739
            embedding=bool(r.get("embedding") or False),  # REQ-421
            embedding_model=r.get("embedding_model"),  # REQ-2000
            is_primary_key=bool(r.get("is_primary_key") or False),
            is_foreign_key=bool(r.get("is_foreign_key") or False),
            is_alternate_key=bool(r.get("is_alternate_key") or False),
            scope=r.get("scope") or "domain",
            is_implicit_measure=r["column_name"] in _measure_names,
            is_implicit_dimension=r["column_name"] in _dimension_names,
            domain_id=r.get("domain_id"),
            epoch_unit=r.get("epoch_unit"),  # REQ-1908
            fake=r.get("fake"),  # REQ-1494
            fake_stable=bool(r.get("fake_stable") or False),
            fake_stable_version=r.get("fake_stable_version"),
            synthetic_rule=r.get("synthetic_rule"),
            fake_why=r.get("fake_why"),  # REQ-2094
            is_pii=r["column_name"] in _pii,
            pii_term=_pii.get(r["column_name"]),  # REQ-2145
        )
        for r in col_rows
    ]
    write_grants = [
        WriteGrantType(
            role=g["role"],
            operation=g["operation"],
            columns=list(g["columns"]),
            rows=g["rows"],
            check=g["check"],
            presets=[
                WritePresetType(
                    column=p["column"], source=p["source"], name=p.get("name"), value=p.get("value")
                )
                for p in g["presets"]
            ],
            why=g["why"],  # REQ-2094
        )
        for g in listing.grants.get(row["id"], [])
    ]  # REQ-2074
    unique_constraints = [
        UniqueConstraintType(name=u["name"], columns=list(u["columns"]))
        for u in (row.get("unique_constraints") or [])
    ]  # REQ-1093

    api_endpoint = None
    if row["source_id"] in listing.endpoints:
        base_url, operations = listing.endpoints[row["source_id"]]
        q = operations.get(normalize_op_id(row["table_name"]))
        if q:
            api_endpoint = f"[{q.method.upper()}] {base_url.rstrip('/')}{q.path}"

    # REQ-318: what reads the table page by page, and its declared paging.
    from provisa.api.admin._table_paging import paging_type, table_paging_kind
    from provisa.api.app import state as _state

    _paging_kind = table_paging_kind(_state, _source_type, row["source_id"], row["table_name"])

    view_sql = row.get("view_sql")
    from provisa.executor.write_capability import table_write_ops

    _write_ops = sorted(
        table_write_ops(
            dict(row),
            None if view_sql else _source_type,
            _state.federation_engine.engine,
            source_path=None if view_sql else _source_path,
        )
    )
    can_deploy = False
    if (
        user_can_deploy
        and row["source_id"] == DERIVED_SOURCE_ID
        and view_sql
        and all_tables is not None
    ):
        can_deploy = _compute_can_deploy_to_db(view_sql, all_tables)

    return RegisteredTableType(
        id=row["id"],
        source_id=row["source_id"],
        domain_id=row["domain_id"],
        schema_name=row["schema_name"],
        table_name=row["table_name"],
        write_ops=_write_ops,
        alias=row.get("alias"),
        description=row.get("description"),
        cache_ttl=row.get("cache_ttl"),
        replicate=row["replicate"],  # REQ-826
        region=row["region"],  # REQ-1921
        draft=row["draft"],  # REQ-1921
        row_materialize=row["row_materialize"],  # REQ-1865
        load_protected=row.get("load_protected"),  # REQ-1141
        off_peak_window=row.get("off_peak_window"),  # REQ-1141
        off_peak_tz=row.get("off_peak_tz"),  # REQ-1141
        gql_naming_convention=row.get("gql_naming_convention"),
        watermark_column=row.get("watermark_column"),
        columns=columns,
        write_grants=write_grants,
        unique_constraints=unique_constraints,  # REQ-1093
        api_endpoint=api_endpoint,
        role_ttl=[
            RoleTtlType(role=role, ttl=ttl) for role, ttl in dict(row["role_ttl"]).items()
        ],  # REQ-1907
        paging_kind=_paging_kind,  # REQ-318
        pagination=paging_type(row["pagination"]),
        paging_ceiling_rows=_state.config.graphql_remote.max_rows,
        view_sql=view_sql,
        dq_contract=row.get("dq_contract"),  # REQ-1443
        profiler_source_id=row.get("profiler_source_id"),  # REQ-1934
        # REQ-1670: a neo4j/sparql table's query, as its endpoint serves it; REQ-1980: a MongoDB
        # pipeline table's native query, which has no endpoint and is read from its row.
        query_template=(
            row["query_template"]
            if _source_type in ("mongodb", "elasticsearch", "cosmosdb", "qdrant")
            else _query_template_for(row["source_id"], row["table_name"])
        ),
        file_glob=row.get("file_glob"),  # REQ-788
        source_file_column=row.get("source_file_column"),  # REQ-788
        import_set=row.get("import_set"),  # REQ-1954
        arguments=dict(row["arguments"] or {}),  # REQ-2061: NULL = none fixed
        landing_filter=row["landing_filter"],  # REQ-2062
        view_metrics=_view_metrics_type_from_row(row.get("view_metrics")),  # REQ-1318
        change_signal=row.get("change_signal"),
        probe_query=row.get("probe_query"),
        probe_type=row.get("probe_type"),
        materialize=bool(row.get("materialize", False)),
        mv_refresh_interval=int(row.get("mv_refresh_interval") or 300),
        mv_debounce_quiet=float(row.get("mv_debounce_quiet") or 0.0),  # REQ-963
        mv_debounce_max_delay=float(row.get("mv_debounce_max_delay") or 5.0),  # REQ-963
        push_debounce_quiet=float(row.get("push_debounce_quiet") or 0.0),  # REQ-1733
        push_debounce_max_delay=float(row.get("push_debounce_max_delay") or 5.0),  # REQ-1733
        mv_consistency=row.get("mv_consistency") or "shared",  # REQ-879
        mv_preprocess=row.get("mv_preprocess"),  # REQ-957
        mv_hold_on_failing_checks=bool(row["mv_hold_on_failing_checks"]),  # REQ-2133
        business_key=list(row.get("business_key") or []),  # REQ-2109
        valid_time=row.get("valid_time"),  # REQ-2109
        recorded_time=row.get("recorded_time"),  # REQ-2109
        mv_persist=row.get("mv_persist") or "replace",  # REQ-965
        mv_primary_key=list(row.get("mv_primary_key") or []),  # REQ-970
        mv_incremental=bool(row.get("mv_incremental", False)),  # REQ-969
        mv_calendar=row.get("mv_calendar"),  # REQ-962
        mv_grain=row.get("mv_grain"),  # REQ-962/1168
        mv_allowed_lateness=float(row.get("mv_allowed_lateness") or 0.0),  # REQ-961
        mv_expected_events=row.get("mv_expected_events"),  # REQ-961
        mv_business_day_grain=bool(row.get("mv_business_day_grain", False)),  # REQ-962
        modeling_role=row.get("modeling_role"),  # REQ-1320
        modeling_history=row.get("modeling_history"),  # REQ-1320
        stored_product_id=row.get("product_id"),  # REQ-1634
        enable_aggregates=bool(row.get("enable_aggregates", False)),
        enable_group_by=bool(row.get("enable_group_by", False)),
        can_deploy_to_db=can_deploy,
        live=_live_type_from_row(row.get("live")),
        delta=_delta_type_from_row(row.get("delta")),  # REQ-874
        implicit_measures=implicit_measures,  # REQ-1360
        implicit_dimensions=implicit_dimensions,  # REQ-1360
    )


async def _call_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
    # REQ-1395, REQ-1398: the acting org's own per-vendor keys, when set.
    from provisa.api.app import state
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.llm.client import ProvisaLLMClient

    api_keys = await read_org_api_keys(state.model_db) if state.model_db else None
    client = ProvisaLLMClient(operation, api_keys=api_keys)
    return await client.complete(
        prompt, system="You are a data catalog assistant.", max_tokens=max_tokens
    )


from provisa.api.admin._table_ops import (  # noqa: E402
    _build_column_models,  # noqa: F401  (re-export: tests + steps import from schema)
    _ensure_view_column_types,  # noqa: F401  (re-export: tests import from schema)
)


async def _domain_table_conflict(  # REQ-432
    conn,
    domain_id: str,
    table_name: str,
    source_id: str,
    schema_name: str,
    alias: str | None = None,
) -> str | None:
    """Return an error message if the effective name (alias or table_name) is already
    registered in the domain from a DIFFERENT physical table.

    Re-registering the same physical table (same source+schema) is allowed (upsert).
    Providing an alias that differs from a conflicting table_name resolves the conflict."""
    effective_name = alias or table_name
    _res = await conn.execute_core(
        select(registered_tables.c.source_id, registered_tables.c.schema_name)
        .where(
            registered_tables.c.domain_id == domain_id,
            func.coalesce(registered_tables.c.alias, registered_tables.c.table_name)
            == effective_name,
            or_(
                registered_tables.c.source_id != source_id,
                registered_tables.c.schema_name != schema_name,
            ),
        )
        .limit(1)
    )
    row = _res.fetchone()
    if row:
        return (
            f"Name {effective_name!r} is already used in domain {domain_id!r} "
            f"from {row.source_id}.{row.schema_name} — effective name (alias or table_name) must be unique."
        )
    return None


def _normalize_dataset_name(name: str) -> str:
    """Snake-case + lowercase normalization for dataset ownership comparison (REQ-433)."""
    from provisa.compiler.naming import apply_sql_name

    return apply_sql_name(name, "snake").lower()


async def _dataset_ownership_conflict(  # REQ-433
    conn, source_id: str, table_name: str, domain_id: str
) -> str | None:
    """Return an error if this dataset is already claimed by a DIFFERENT domain (REQ-433).

    First-come ownership: a physical dataset — identified by (source_id, normalized
    table name) — may be registered by only one domain. Re-registration by the owning
    domain is allowed. Virtual Provisa views (``__derived__``) are exempt: they are not
    datasource claims and many domains legitimately share that source id.
    """
    if source_id == DERIVED_SOURCE_ID:
        return None
    from provisa.core import domain_policy

    target_domain = domain_policy.resolve_domain_id(domain_id)
    norm = _normalize_dataset_name(table_name)
    _res = await conn.execute_core(
        select(registered_tables.c.domain_id, registered_tables.c.table_name).where(
            registered_tables.c.source_id == source_id
        )
    )
    for r in _res.fetchall():
        if _normalize_dataset_name(r.table_name) == norm and r.domain_id != target_domain:
            return (
                f"Table {table_name!r} on source {source_id!r} is already claimed by "
                f"domain {r.domain_id!r} (first-come ownership)."
            )
    return None


# --- Creation-request queue (REQ-434/063/366) -------------------------------
