# Copyright (c) 2026 Kenneth Stott
# Canary: dbefb303-1fbb-4d2a-acda-25310bc5d1b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""Extracted bodies for the heaviest admin Mutation resolvers.

Strawberry cannot merge resolvers across base classes, so the ``Mutation`` type
stays one class in ``schema_mutation.py`` with thin delegator methods; the large
bodies live here as free functions to keep that module under the length gate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import delete, select, update

from provisa.compiler.naming import SqlAddressTaken
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import (
    file_source_mtimes,
    registered_tables,
    sources,
    table_meta_links,
)
from provisa.api.admin.types import MutationResult, TableInput
from provisa.api.admin.schema_helpers import (
    _dataset_ownership_conflict,
    _domain_table_conflict,
    _get_pool,
    _rebuild_schemas,
)
from provisa.api.admin._table_ops import _build_columns_for_input, column_field_refusal
from provisa.api.admin._live_mappers import table_model_from_input as _table_model_from_input
from provisa.api.admin._table_settings import (
    TableSaveRefused,
    carried,
    load_protection_refusal,
    save_row_rules,
    save_carried_after_store,
    with_carried_settings,
)
from provisa.api.admin.schema_common import (
    _queue_creation_request,
    _sync_view_mv,
    check_view_mv,
)

if TYPE_CHECKING:
    from starlette.requests import Request
    from strawberry.types.info import Info as StrawberryInfo

    from provisa.core.database import Connection


async def _apply_mv_relationship_gate(
    info: StrawberryInfo, input: TableInput
) -> MutationResult | None:  # REQ-1140
    """Gate MV publication on approved relationships (REQ-1140).

    Returns None to let publication proceed, or a queued ``MutationResult`` to BLOCK it. A materialized
    view (``view_sql`` + ``materialize``) may publish only if every relationship its SQL joins over is
    approved (present in ``relationships``). Missing relationships split on the caller's rights:
    - holds ``create_relationship`` → auto-create + approve each missing relationship now,
      then proceed (returns None);
    - otherwise → queue a ``relationship`` creation request per missing dependency AND the ``view``
      creation request, and block publication (returns the queued result). Re-executing the queued
      view request after the relationships are approved re-runs this gate, which now passes.
    """
    if not (input.view_sql and input.materialize):
        return None

    from provisa.api.admin.capabilities import has_capability
    from provisa.mv.relationship_gate import evaluate_gate
    from provisa.core.models import Cardinality, Relationship
    from provisa.core.repositories import relationship as relationship_repo
    from provisa.core.repositories import table as table_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        relationships = await relationship_repo.list_all(conn)

        async def _resolve(name: str) -> int | None:
            tbl = await table_repo.find_by_table_name(conn, name)
            return tbl["id"] if tbl is not None else None

        decision = await evaluate_gate(
            view_sql=input.view_sql,
            dialect=None,
            relationships=relationships,
            resolve_table_id=_resolve,
        )
        if decision.satisfied:
            return None

        if has_capability(info, "create_relationship"):
            # Trusted author: auto-create + approve each missing relationship (a row IS approval).
            # Cardinality is unknowable from an equi-join alone; default many-to-one (source→target),
            # the FK direction, which the author may later refine.
            for m in decision.missing:
                d = m.dep
                await relationship_repo.upsert(
                    conn,
                    Relationship(
                        id=f"mv_{d.left_table}_{d.left_column}__{d.right_table}_{d.right_column}",
                        source_table_id=d.left_table,
                        target_table_id=d.right_table,
                        source_column=d.left_column,
                        target_column=d.right_column,
                        cardinality=Cardinality.many_to_one,
                        owner=getattr(_identity_user(info), "user_id", None),
                    ),
                )
            return None

    # Least-privilege author: queue every missing relationship AND the view, block publication.
    from provisa.api.admin.types import RelationshipInput

    for m in decision.missing:
        d = m.dep
        rel_input = RelationshipInput(
            id=f"mv_{d.left_table}_{d.left_column}__{d.right_table}_{d.right_column}",
            source_table_id=d.left_table,
            target_table_id=d.right_table,
            source_column=d.left_column,
            target_column=d.right_column,
            cardinality=Cardinality.many_to_one.value,
        )
        await _queue_creation_request(info, "relationship", "create_relationship", rel_input)
    return await _queue_creation_request(info, "view", "create_view", input)


def _identity_user(info: StrawberryInfo):
    from provisa.api.admin.capabilities import _identity_from_info

    return _identity_from_info(info)


async def _registered_result(input: TableInput, table_id: int | None) -> MutationResult:
    """What a successful registration answers. A table of a remote GraphQL source may have had
    fields left out because the source's credential may not read them (REQ-1923); they are
    named. Registering it may also complete a relationship between two of the source's
    registered tables (REQ-313), which is stored here, once both exist."""
    from provisa.api.admin._graphql_table_registration import sync_detected_relationships
    from provisa.api.admin._table_ops import take_omitted_fields
    from provisa.api.app import state

    if await sync_detected_relationships(state, input.source_id):
        await _rebuild_schemas()

    omitted = take_omitted_fields(input.source_id, input.table_name)
    if not omitted:
        return MutationResult(
            success=True,
            message=f"Table {input.table_name!r} registered (id={table_id})",
            code="schema.table_registered",
            params={"table": input.table_name, "id": table_id},
        )
    fields = ", ".join(sorted({o["field"] for o in omitted}))
    return MutationResult(
        success=True,
        message=(
            f"Table {input.table_name!r} registered (id={table_id}). Left out, because the "
            f"source's credential may not read them: {fields}"
        ),
        code="schema.table_registered_fields_omitted",
        params={"table": input.table_name, "id": table_id, "fields": fields, "omitted": omitted},
    )


async def reprovision_source_after_registration(pool, state, source_id: str) -> None:
    """Re-run the source's mapping-DSL synthesis and engine provisioning once a table of it is
    registered (REQ-1730) — under the org's vault (REQ-1695), as createSource does: the source's
    password is a ``${secret:...}`` reference into it, which resolves only while it is bound."""
    from provisa.api.admin.schema_common import _synthesize_mapping_dsl_tables
    from provisa.core.secrets import resolve_secrets
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.federation.registry_view import registered_sources

    model = next((s for s in await registered_sources(state) if s.id == source_id), None)
    if model is None:
        return
    async with bound_to_request_org():
        await _synthesize_mapping_dsl_tables(pool, model)
        state.federation_engine.register_source(
            model,
            resolve_secrets(model.password) if model.password else "",
            catalog_name=state.source_catalogs[source_id],
        )


def default_mail_watermark(model: Any, source_type: str | None) -> None:
    """A mail table's rows land mailbox by mailbox, in no order of the provider's own times:
    what a subscriber is sent next is told by when a row landed, not by the provider's
    ``updated_at``. So a mail table registered with the landing's stamp takes it as its
    watermark, unless the operator named another column."""
    from provisa.core import canonical_mail as mail

    if (
        source_type in ("microsoft_365", "google_workspace")
        and model.watermark_column is None
        and any(column.name == mail.LANDED_AT for column in model.columns)
    ):
        model.watermark_column = mail.LANDED_AT
    # REQ-1996: likewise a Logs table, whose rows land file by file.
    from provisa import logs

    if (
        source_type == logs.SOURCE_TYPE
        and model.watermark_column is None
        and any(column.name == logs.LANDED_AT for column in model.columns)
    ):
        model.watermark_column = logs.LANDED_AT


async def register_table(
    info: StrawberryInfo, input: TableInput
) -> (
    MutationResult
):  # REQ-013, REQ-016, REQ-252, REQ-366, REQ-413, REQ-432, REQ-433, REQ-434, REQ-1140
    import logging

    logging.getLogger(__name__).warning(
        "[DEBUG] register_table called: table_name=%s, source_id=%s, domain_id=%s",
        input.table_name,
        input.source_id,
        input.domain_id,
    )
    from provisa.api.admin._table_ops import land_transform_view

    land_transform_view(input)  # REQ-2131
    from provisa.api.admin.capabilities import require_capability

    if input.view_sql or input.view_metrics is not None:
        # view registration (free-hand SQL or REQ-1318 metric-composed spec):
        # create_view or query_development suffice
        from provisa.api.admin.capabilities import _identity_from_info, _resolved_capabilities
        from provisa.api.app import state as _cap_state

        identity = _identity_from_info(info)
        if identity is not None and getattr(identity, "user_id", "anonymous") != "anonymous":
            caps = _resolved_capabilities(identity, _cap_state)
            # REQ-1337: rights only — the two rights that author a view, and nothing in their place.
            if not (caps & {"create_view", "query_development"}):
                # REQ-434/366: lacking view-create authority queues a request.
                return await _queue_creation_request(info, "view", "create_view", input)
        # REQ-1531: the view gate has TWO halves. The domain it is registered INTO, and every table
        # its SQL READS — free-hand SQL names those tables directly, so without this a member of one
        # domain publishes another domain's rows into a view its owner does not govern. Out of
        # domain queues rather than errors, the same answer a missing right already gets.
        from provisa.api.admin.capabilities import require_domain
        from provisa.api.admin.domain_guard import require_view_within_domains

        try:
            require_domain(info, input.domain_id)
            if input.view_sql:
                _pool = await _get_pool()
                async with _pool.acquire() as _conn:
                    await require_view_within_domains(
                        info, cast("Connection", _conn), input.view_sql
                    )
        except PermissionError:
            return await _queue_creation_request(info, "view", "create_view", input)
        # REQ-1140: a materialized view publishes only over approved relationships; the gate either
        # auto-creates them (rights) or queues them + the view and blocks (returns a queued result).
        # A metric-composed view (view_metrics) joins ONLY through registered relationships by
        # construction (generate_view_metrics_sql), so the gate applies to free-hand SQL alone.
        gate_result = await _apply_mv_relationship_gate(info, input)
        if gate_result is not None:
            return gate_result
    else:
        require_capability(info, "table_registration", domain_id=input.domain_id)
    from provisa.core.repositories import table as table_repo

    pool = await _get_pool()
    from provisa.api.admin.domain_guard import require_current_domain

    async with pool.acquire() as _conn:
        await require_current_domain(info, cast("Connection", _conn), input)
    # Before the columns are settled: settling them may reach the source.
    _field_refusal = await column_field_refusal(pool, input, input.columns)
    if _field_refusal is not None:
        return _field_refusal
    columns, _col_err = await _build_columns_for_input(pool, input)
    if _col_err is not None:
        return _col_err
    alias = input.alias or None
    if not alias:
        # REQ-471: the registered virtual name is a SQL-plane identifier, so the SQL naming
        # convention is its authority. The GraphQL surface applies gql conventions at schema
        # generation — it never mints the SQL name (deriving the alias from a source's
        # gql_naming_convention put camelCase into the SQL plane, e.g. dim_pet -> dimPet).
        from provisa.api.app import state as _app_state
        from provisa.compiler.naming import apply_convention

        _sql_name = apply_convention(
            input.table_name, getattr(_app_state, "global_sql_naming_convention", "snake")
        )
        alias = None if _sql_name == input.table_name else _sql_name

    # REQ-957/964: reject a non-deterministic / unsafe preprocess hook at registration.
    from provisa.mv.preprocess import validate_preprocess

    try:
        validate_preprocess(input.mv_preprocess)
    except ValueError as _pp_err:
        return MutationResult(success=False, message=str(_pp_err))
    # REQ-2066: the window a read of the table (a materialized view's) is judged by is parsed when
    # it is saved, as the load management save does; one that does not parse is refused here. ""
    # is no window (as scheduled_refresh reads it); a table naming no zone takes its source's, and
    # only the spec is checked here, so any valid zone checks it.
    from provisa.api.admin.schema_mutation import _parsed_off_peak

    window_err = _parsed_off_peak(input.off_peak_window or None, input.off_peak_tz or "UTC")
    if window_err is not None:
        return window_err
    try:
        model = _table_model_from_input(input, columns, alias)
    except ValueError as _map_err:  # REQ-1318: view_sql+view_metrics conflict / invalid spec
        return MutationResult(success=False, message=str(_map_err))
    # REQ-1907: a table that lands needs its clock to be registered at all, so the registration
    # may state the table's own cache_ttl (#208). Afterwards updateTableCache changes it.
    if carried(input.cache_ttl) and input.cache_ttl is not None:
        from provisa.core.cache_ttl import cache_ttl_refusal  # REQ-2031

        _ttl_err = cache_ttl_refusal(input.cache_ttl)
        if _ttl_err is not None:
            return MutationResult(
                success=False,
                message=_ttl_err,
                code="schema.invalid_cache_ttl",
                params={"table": input.table_name},
            )
        model.cache_ttl = input.cache_ttl
    from provisa.api.app import state as _reg_state

    default_mail_watermark(model, _reg_state.source_types.get(input.source_id))

    from provisa.federation.replica_routing import NO_REPLICA_TYPES

    if _reg_state.source_types.get(input.source_id) in NO_REPLICA_TYPES:  # REQ-1771/1934/1443
        # ingest's Register-Table picker offers only a synthetic "default" schema (REQ-1745, a
        # UI-symmetry placeholder mirroring redis/elasticsearch/prometheus) -- it is never a real
        # physical location. ingest rows land straight into the tenant control-plane DB
        # (provisa/ingest/engine.py), which the compiler reaches only through the provisa_admin
        # catalog (catalog_name_for_source, app_loaders.py) under the org's real control-plane
        # schema. Storing "default" here would resolve to a schema provisa_admin never exposes.
        from provisa.core.environments import active_org_schema

        model.schema_name = active_org_schema(_reg_state.org_id, "")
    if _reg_state.source_types.get(input.source_id) == "govdata":
        # REQ-540: the adapter serves every schema it has; a table is registered only from a
        # schema the source was given, refused by name otherwise.
        from provisa.federation.askamerica import SchemaNotServed, require_schema_served

        async with pool.acquire() as _schemas_conn:
            _given = (
                await _schemas_conn.execute_core(
                    select(sources.c.id, sources.c.database).where(sources.c.id == input.source_id)
                )
            ).fetchone()
        if _given is not None:
            try:
                require_schema_served(_given, input.schema_name)
            except SchemaNotServed as _not_served:
                return MutationResult(success=False, message=str(_not_served))
    _effective_view_sql = input.view_sql
    try:
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            async with _conn.transaction():
                # REQ-1443: a checker table's columns, watermark and promotions come from its contract, not
                # from the input — the same derivation the YAML loader runs, so both surfaces register the
                # same table.
                from provisa.api.admin._dq_registration import apply_dq_registration

                try:
                    await apply_dq_registration(_conn, model)
                    from provisa.dq.registration import (
                        is_checker_source_type,
                        require_scan_interval,
                    )

                    if is_checker_source_type(_reg_state.source_types.get(input.source_id)):
                        require_scan_interval(model)  # REQ-1443: the interval it scans on
                    # REQ-1934: a profile result table's derivation, and a table's profiler membership.
                    from provisa.profiler.registration import apply_registration

                    await apply_registration(_conn, model)
                except ValueError as _dq_err:
                    raise TableSaveRefused(MutationResult(success=False, message=str(_dq_err)))
                from provisa.api.admin.region_defaults import registration_region

                model.region = await registration_region(  # REQ-1921
                    _conn, input, is_view=bool(input.view_sql) or input.view_metrics is not None
                )
                # REQ-1921: registered through the admin, a table or view starts as draft — out of
                # service until its domain's owners release it.
                model.draft = True
                # REQ-2109: the settings the registration carries, checked as their own mutations
                # check them; one refused refuses the registration.
                model = await with_carried_settings(info, _conn, input, model)
                _lp_refusal = await load_protection_refusal(_conn, model)  # REQ-1141
                if _lp_refusal is not None:
                    raise TableSaveRefused(_lp_refusal)
                from provisa.api.admin.capabilities import require_residency_change
                from provisa.security.residency import CREATED, ResidencyRefused

                try:
                    require_residency_change(
                        info, f"table {model.table_name}", CREATED, model.region
                    )
                except ResidencyRefused as _refused:
                    raise TableSaveRefused(
                        MutationResult(
                            success=False,
                            message=str(_refused),
                            code=_refused.code,
                            params=_refused.params,
                        )
                    )
                if model.view_metrics is not None:
                    # REQ-1318: compile the spec into the view SELECT against the live registries —
                    # the generated SQL persists in view_sql and flows everywhere free-hand SQL does.
                    from provisa.api.admin._metric_views import compile_view_metrics_sql

                    try:
                        model.view_sql = await compile_view_metrics_sql(_conn, model.view_metrics)
                    except ValueError as _vm_err:
                        raise TableSaveRefused(MutationResult(success=False, message=str(_vm_err)))
                    _effective_view_sql = model.view_sql
                if _effective_view_sql and input.materialize:
                    # A materialized view's queryable schema must be where it actually lands, not a fixed
                    # placeholder — same seam _sync_view_mv/schema_common.py uses for the MV's own refresh
                    # target. Hardcoding "views" here broke query-time resolution independently of engine
                    # (a DuckDB deployment's MV lands under its attached mat_store schema, not "views").
                    from provisa.api.app import state as _mv_state

                    _, model.schema_name = _mv_state.federation_engine.materialize_store_target(
                        _mv_state.active_org_id
                    )
                if _effective_view_sql and input.materialize:
                    # REQ-964, REQ-2109: the view's definition is judged before the save commits, so a
                    # refused one leaves nothing saved.
                    try:
                        check_view_mv(
                            input.table_name,
                            _effective_view_sql,
                            consistency=input.mv_consistency,
                            preprocess=input.mv_preprocess,
                            business_key=list(input.business_key),
                            persist=input.mv_persist,
                        )
                    except ValueError as _mv_err:
                        raise TableSaveRefused(MutationResult(success=False, message=str(_mv_err)))
                from provisa.api.admin._landing_ttl import TableTtl, landing_ttl_refusal

                _ttl_refusal = await landing_ttl_refusal(  # REQ-1907
                    _conn,
                    model.source_id,
                    table=TableTtl(
                        model.schema_name,
                        model.table_name,
                        model.change_signal,
                        model.cache_ttl,
                        model.materialize,
                        model.row_materialize,
                        model.replicate,
                        model.load_protected,
                    ),
                )
                if _ttl_refusal is not None:
                    raise TableSaveRefused(_ttl_refusal)
                from provisa.api.admin._landing_ttl import row_materialize_save_refusal

                _row_refusal = await row_materialize_save_refusal(  # REQ-1865
                    _conn,
                    model.source_id,
                    model.table_name,
                    model.row_materialize,
                    [c.name for c in model.columns if c.is_primary_key],
                )
                if _row_refusal is not None:
                    raise TableSaveRefused(_row_refusal)
                from provisa.api.admin._change_feed import table_change_feed_refusal

                _feed_refusal = await table_change_feed_refusal(  # REQ-1861
                    _conn,
                    model.source_id,
                    model.change_signal,
                    kafka_topic=model.live.kafka.topic if model.live and model.live.kafka else None,
                    table_name=model.table_name,
                )
                if _feed_refusal is not None:
                    raise TableSaveRefused(_feed_refusal)
                from provisa.api.admin._file_glob import table_file_glob_refusal

                _glob_refusal = await table_file_glob_refusal(_conn, model)  # REQ-788
                if _glob_refusal is not None:
                    raise TableSaveRefused(_glob_refusal)
                from provisa.api.admin._fake_guard import table_fake_refusal

                _fake_refusal = await table_fake_refusal(_conn, model)  # REQ-1494
                if _fake_refusal is not None:
                    raise TableSaveRefused(_fake_refusal)
                from provisa.api.admin._delta_guard import table_delta_refusal

                _delta_refusal = await table_delta_refusal(_conn, model)  # REQ-874
                if _delta_refusal is not None:
                    raise TableSaveRefused(_delta_refusal)
                _conflict = await _domain_table_conflict(
                    _conn,
                    model.domain_id,
                    model.table_name,
                    model.source_id,
                    model.schema_name,
                    alias,
                )
                if _conflict:
                    raise TableSaveRefused(MutationResult(success=False, message=_conflict))
                _owner_conflict = await _dataset_ownership_conflict(
                    _conn, model.source_id, model.table_name, model.domain_id
                )
                if _owner_conflict:
                    raise TableSaveRefused(MutationResult(success=False, message=_owner_conflict))
                if input.source_id == DERIVED_SOURCE_ID:
                    from provisa.api.app import state

                    await _conn.upsert(
                        sources,
                        {
                            "id": DERIVED_SOURCE_ID,
                            "type": state.federation_engine.name,
                            "description": "Provisa-managed virtual views — cross-source SQL views defined and published by the data team as governed data products",
                        },
                        index_elements=["id"],
                        update_columns=[],
                    )
                # REQ-1670/REQ-1683: a query-API table's query is checked before the row lands and
                # persisted after.
                from provisa.api.admin._query_api_registration import persist_query_api_registration

                if not model.query_template:
                    _qa_precheck = await persist_query_api_registration(_conn, model)
                    if _qa_precheck is not None:
                        raise TableSaveRefused(_qa_precheck)
                from provisa.api.admin._query_api_registration import pipeline_table_refusal

                _pipeline_refusal = await pipeline_table_refusal(_conn, model)  # REQ-1980
                if _pipeline_refusal is not None:
                    raise TableSaveRefused(_pipeline_refusal)
                if carried(input.pagination) and input.pagination is not None:
                    # REQ-318: the paging the steward registers the table with (accepted or edited from
                    # what the source suggested), refused by name when the table cannot take it.
                    from provisa.api.admin._table_paging import declared_paging
                    from provisa.api.app import state as _paging_state

                    _source_type = (
                        await _conn.execute_core(
                            select(sources.c.type).where(sources.c.id == input.source_id)
                        )
                    ).scalar_one()
                    _paging = declared_paging(
                        _paging_state,
                        _source_type,
                        input.source_id,
                        input.table_name,
                        input.pagination,
                    )
                    if isinstance(_paging, MutationResult):
                        raise TableSaveRefused(_paging)
                    model.pagination = _paging
                from provisa.api.admin._hiding_guard import table_hiding_refusal

                _hiding_refusal = await table_hiding_refusal(  # REQ-1943, REQ-1944
                    info, _conn, model, editor=True
                )
                if _hiding_refusal is not None:
                    raise TableSaveRefused(_hiding_refusal)
                try:
                    _carried_grants = model.write_grants  # REQ-2074, REQ-2109
                    model = await table_repo.keep_unedited(_conn, model)  # REQ-1919
                    if carried(input.write_grants):
                        model = model.model_copy(update={"write_grants": _carried_grants})
                    table_id = await table_repo.upsert(_conn, model)
                except glossary_repo.TermIsClass as _class:
                    # REQ-2141: a column named for a class term is not claimed by it.
                    raise TableSaveRefused(
                        MutationResult(
                            success=False,
                            message=str(_class),
                            code="glossary.term_is_class",
                            params={"term": _class.term, "tables": ", ".join(_class.tables)},
                        )
                    )
                except table_repo.ViewLoopRefused as _loop:
                    # REQ-1918: a view that would read itself through other views is refused at save.
                    raise TableSaveRefused(
                        MutationResult(
                            success=False,
                            message=str(_loop),
                            code="schema.view_reads_itself",
                            params={"view": _loop.loop[0], "loop": _loop.loop},
                        )
                    )
                except table_repo.NumericSizeRefused as _size:
                    # REQ-1961: a size no numeric can have is not saved; the column is named.
                    raise TableSaveRefused(
                        MutationResult(
                            success=False,
                            message=str(_size),
                            code="schema.numeric_size_invalid",
                            params={
                                "table": _size.table_name,
                                "column": _size.column,
                                "declared": _size.declared,
                            },
                        )
                    )
                except table_repo.ColumnDropRefused as _drop:
                    # REQ-1918: a column something still refers to is not dropped; each is named,
                    # with only the dependents the caller may read (REQ-2152).
                    from provisa.api.admin.read_rights import (
                        column_drop_text,
                        reader_for,
                        readable_columns_report,
                    )

                    _report = await readable_columns_report(_conn, reader_for(info), _drop.columns)
                    raise TableSaveRefused(
                        MutationResult(
                            success=False,
                            message=column_drop_text(_drop.table_name, _report),
                            code="schema.column_has_dependents",
                            params={"table": _drop.table_name, "columns": _report},
                        )
                    )
                except SqlAddressTaken as _taken:
                    # REQ-1933: one SQL address per table in a domain; the table that holds it is named.
                    raise TableSaveRefused(
                        MutationResult(
                            success=False,
                            message=str(_taken),
                            code="schema.sql_address_taken",
                            params={
                                "domain": _taken.domain_id,
                                "address": _taken.address,
                                "holder": _taken.holder,
                            },
                        )
                    )
                assert table_id is not None  # the upsert returns the id it wrote
                await save_carried_after_store(_conn, input, model, table_id)  # REQ-2109
                if model.query_template:
                    _qa_err = await persist_query_api_registration(_conn, model)
                    if _qa_err is not None:
                        raise TableSaveRefused(_qa_err)
                # REQ-316/REQ-318: an OpenAPI table's endpoint row, derived from its registration.
                from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
                from provisa.api.app import state as _app_state

                _oa_err = await persist_openapi_endpoint(_app_state, _conn, model)
                if _oa_err is not None:
                    raise TableSaveRefused(_oa_err)
                _sres = await _conn.execute_core(
                    select(sources.c.type, sources.c.path).where(sources.c.id == input.source_id)
                )
                _srow = _sres.fetchone()
                src_row = dict(_srow._mapping) if _srow is not None else None
                if input.domain_id != "meta":
                    _mres = await _conn.execute_core(
                        select(registered_tables.c.id).where(
                            registered_tables.c.source_id == "provisa-admin",
                            registered_tables.c.domain_id == "meta",
                            registered_tables.c.table_name == "registered_tables",
                        )
                    )
                    meta_rt_id = _mres.scalar()
                    if meta_rt_id:
                        await _conn.upsert(
                            table_meta_links,
                            {"source_table_id": table_id, "target_table_id": meta_rt_id},
                            index_elements=["source_table_id"],
                            update_columns=[],
                        )

                from provisa.core import deployment_settings as _deployment_settings

                if _deployment_settings.auto_track_fk():
                    from provisa.discovery.fk_introspect import auto_register_fk_relationships
                    from provisa.api.app import state as _state

                    source_type = (src_row["type"] if src_row else None) or ""
                    _naming_cfg = getattr(getattr(_state, "config", None), "naming", None)
                    _v2_style = bool(getattr(_naming_cfg, "hasura_v2_relationship_style", False))
                    fk_count = await auto_register_fk_relationships(
                        _state.source_pools,
                        source_type,
                        input.source_id,
                        input.schema_name,
                        input.table_name,
                        _conn,
                        hasura_v2_relationship_style=_v2_style,
                    )
                    if fk_count:
                        import logging as _logging

                        _logging.getLogger(__name__).info(
                            "Auto-tracked %d FK relationship(s) for %s.%s",
                            fk_count,
                            input.schema_name,
                            input.table_name,
                        )
                # REQ-1934, REQ-2109: the row rules the registration carries (a profiler result
                # table's), saved with the table.
                await save_row_rules(info, _conn, input, model)
    except TableSaveRefused as _refused:
        # The transaction rolled back: nothing of the save is stored.
        return _refused.result

    if _effective_view_sql and input.materialize:
        await _sync_view_mv(
            input.table_name,
            _effective_view_sql,
            input.mv_refresh_interval,
            input.change_signal,
            consistency=input.mv_consistency,  # REQ-879
            preprocess=input.mv_preprocess,  # REQ-957
            business_key=list(input.business_key),  # REQ-2109
            valid_time=input.valid_time,  # REQ-2109
            recorded_time=input.recorded_time,  # REQ-2109
            bitemporal_columns=[c.name for c in model.columns],
            persist=input.mv_persist,  # REQ-965
            primary_key=list(input.mv_primary_key),  # REQ-970
            incremental=input.mv_incremental,  # REQ-969
            calendar=input.mv_calendar,  # REQ-962
            grain=input.mv_grain,  # REQ-962/1168
            allowed_lateness=input.mv_allowed_lateness,  # REQ-961
            expected_events=input.mv_expected_events,  # REQ-961
            business_day_grain=input.mv_business_day_grain,  # REQ-962
            draft=model.draft,  # REQ-1921
            region=model.region,  # REQ-1921
        )

    await _rebuild_schemas()
    # Schema-currency reconcile (REQ-846/932): a UI (re)registration re-enters the convergent
    # reconcile — the new/changed table's landing schema is created (or recreated on a column
    # drift) and its read view attached. Same primitive as the boot pass. Best-effort: a store
    # hiccup logs but does not fail the registration.
    try:
        from provisa.api.app import state as _rc_state

        await _rc_state.federation_engine.reconcile_landed_tables()
    except Exception:
        logging.getLogger(__name__).exception("landed-table reconcile after registration failed")

    # REQ-1730: a mapping-DSL source (redis) registered under an engine that is ALREADY primary
    # from a cold start (no prior DuckDB registration to replay from) has its Trino catalog
    # provisioned at create_source time, BEFORE this table existed — create_source's own
    # `_synthesize_mapping_dsl_tables` synthesizes an entry from `registered_tables`, which had no
    # rows yet, so the catalog's `redis.table-names` was written empty and stayed that way forever
    # (nothing else ever re-provisions it). The existing DuckDB-first→Trino-replay flow
    # (`reprovisionSourceOnEngine`) never hits this because register_table always runs before that
    # replay's own create_source call. Verified live (REQ-1730 engine-swap harness, 2026-09-20):
    # registering redis directly against a Trino-primary process raised `SCHEMA_NOT_FOUND: Schema
    # 'default' does not exist` on every subsequent query. Re-run the same synthesis +
    # engine-provisioning call create_source itself makes, now that this table exists to
    # synthesize from — a no-op for every non-redis type (the synthesis function's own gate) and
    # for a native engine (register_source has nothing catalog-like to provision). Best-effort,
    # same posture as the reconcile above.
    try:
        from provisa.api.app import state as _mapping_state

        await reprovision_source_after_registration(pool, _mapping_state, input.source_id)
    except Exception:
        logging.getLogger(__name__).exception(
            "mapping-DSL catalog refresh after registration failed for source %r",
            input.source_id,
        )

    # A newly-created materialized view is materialized immediately and its refresh job registered, so
    # it lands FRESH instead of STALE-until-restart (the event loop otherwise wires only at boot).
    if _effective_view_sql and input.materialize and not model.draft:
        from provisa.api.admin.schema_common import activate_view_mv

        await activate_view_mv(input.table_name)
    from provisa.dq.scan import reschedule_org_checkers

    await reschedule_org_checkers()  # REQ-1443: a checker table scans from now on
    return await _registered_result(input, table_id)


async def deploy_view_to_db(info: StrawberryInfo, table_id: int) -> MutationResult:
    """Promote a virtual Provisa view to a real database view on its underlying native source."""
    from provisa.api.admin.capabilities import require_capability

    require_capability(info, "table_registration")

    from provisa.api.app import state
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.transpiler.transpile import transpile

    pool = await _get_pool()
    async with pool.acquire() as conn:
        _res = await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.source_id,
                registered_tables.c.domain_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
                registered_tables.c.view_sql,
            ).where(registered_tables.c.id == table_id)
        )
        row = _res.fetchone()
    if not row:
        return MutationResult(
            success=False,
            message=f"Table {table_id} not found",
            code="schema.table_not_found",
            params={"table": table_id},
        )
    if row.source_id != DERIVED_SOURCE_ID:
        return MutationResult(
            success=False,
            message="Table is not a virtual Provisa view (source_id != '__derived__')",
            code="schema.not_virtual_view",
        )
    if not row.view_sql:
        return MutationResult(
            success=False, message="Table has no view_sql", code="schema.no_view_sql"
        )

    view_sql = row.view_sql
    view_name = row.alias or row.table_name

    # Fetch all non-provisa registered tables with domain_id, source info
    async with pool.acquire() as conn:
        _ares = await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.source_id,
                registered_tables.c.domain_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
                sources.c.type.label("source_type"),
            )
            .select_from(
                registered_tables.join(sources, sources.c.id == registered_tables.c.source_id)
            )
            .where(registered_tables.c.source_id != DERIVED_SOURCE_ID)
        )
        all_tables = [dict(r._mapping) for r in _ares.fetchall()]

    # Build replacement dict: virtual ref → physical ref, tracking source_ids hit
    # Sort by length descending so longest match wins
    replacements: list[
        tuple[str, str, str, str]
    ] = []  # (virtual_ref, physical_ref, source_id, schema_name)
    for t in all_tables:
        domain_sql = domain_to_sql_name(t["domain_id"])
        alias_or_name = t["alias"] or t["table_name"]
        virtual_ref = f'"{domain_sql}"."{alias_or_name}"'
        physical_ref = f'"{t["schema_name"]}"."{t["table_name"]}"'
        replacements.append((virtual_ref, physical_ref, t["source_id"], t["schema_name"]))

    # Apply replacements (longest virtual_ref first), track which sources are hit
    physical_sql = view_sql
    hit_sources: dict[str, str] = {}  # source_id → schema_name
    for virtual_ref, physical_ref, source_id, schema_name in sorted(
        replacements, key=lambda x: len(x[0]), reverse=True
    ):
        if virtual_ref in physical_sql:
            physical_sql = physical_sql.replace(virtual_ref, physical_ref)
            hit_sources[source_id] = schema_name

    if not hit_sources:
        return MutationResult(
            success=False, message="no recognized table references", code="schema.no_table_refs"
        )
    if len(hit_sources) > 1:
        return MutationResult(
            success=False,
            message=f"view spans multiple sources: {', '.join(sorted(hit_sources))}",
            code="schema.view_spans_sources",
            params={"sources": ", ".join(sorted(hit_sources))},
        )

    target_source_id = next(iter(hit_sources))
    target_schema = hit_sources[target_source_id]

    if not state.source_pools.has(target_source_id):
        return MutationResult(
            success=False,
            message=f"source {target_source_id!r} has no active connection",
            code="schema.source_no_connection",
            params={"source": target_source_id},
        )

    dialect = state.source_pools.dialect_for(target_source_id) or "postgres"
    native_sql = transpile(physical_sql, dialect)

    ddl = f'CREATE OR REPLACE VIEW "{target_schema}"."{view_name}" AS {native_sql}'
    await state.source_pools.execute_ddl(target_source_id, ddl)

    async with pool.acquire() as conn:
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == table_id)
            .values(source_id=target_source_id, schema_name=target_schema, view_sql=None)
        )

    await _rebuild_schemas()
    return MutationResult(
        success=True,
        message=f"View '{view_name}' deployed to {target_source_id!r} schema '{target_schema}'",
        code="schema.view_deployed",
        params={"view": view_name, "source": target_source_id, "schema": target_schema},
    )


async def invalidate_file_source(table_id: int) -> MutationResult:
    """Force a sqlite file-connector table's next access to re-sync from disk.

    Deletes the table's ``file_source_mtimes`` row (source_mtime/synced_at, REQ-252)
    so the mtime comparison that would otherwise skip a re-sync sees no prior record.
    Only meaningful for sqlite sources — csv/parquet are ATTACHed live by Trino/DuckDB
    (no cached snapshot to invalidate) and every other source type is not a file
    connector at all.
    """
    pool = await _get_pool()
    async with pool.acquire() as conn:
        _res = await conn.execute_core(
            select(registered_tables.c.id, registered_tables.c.source_id).where(
                registered_tables.c.id == table_id
            )
        )
        row = _res.fetchone()
    if not row:
        return MutationResult(
            success=False,
            message=f"Table {table_id} not found",
            code="schema.table_not_found",
            params={"table": table_id},
        )

    async with pool.acquire() as conn:
        _sres = await conn.execute_core(select(sources.c.type).where(sources.c.id == row.source_id))
        source_row = _sres.fetchone()
    source_type = source_row.type if source_row else ""
    if source_type != "sqlite":
        return MutationResult(
            success=False,
            message=f"Source type {source_type!r} is not sqlite",
            code="schema.not_sqlite_source",
            params={"source_type": source_type},
        )

    async with pool.acquire() as conn:
        await conn.execute_core(
            delete(file_source_mtimes).where(file_source_mtimes.c.table_id == table_id)
        )
    return MutationResult(
        success=True,
        message=f"Invalidated file-source cache for table {table_id}",
        code="schema.file_source_invalidated",
        params={"table": table_id},
    )


async def reschedule_org_triggers() -> None:  # REQ-1003
    """Reschedule the bound org's triggers from its model, if this process runs a scheduler.

    No-op when none runs (unit tests, a process that does no background work); the scheduler's
    holder picks the change up when the org's runtime is next built."""
    from provisa.api.app import state
    from provisa.core.request_context import current_env, require_current_org
    from provisa.scheduler.jobs import register_org_triggers

    scheduler = state._scheduler
    if scheduler is None:
        return
    # An edit in an environment other than prod schedules nothing (register_org_triggers).
    await register_org_triggers(scheduler, require_current_org(), current_env.get())


async def create_scheduled_task_op(  # REQ-1003, REQ-1004
    request: Request,
    id: str,
    name: str,
    cron: str | None,
    kind: str,
    webhook_name: str | None,
    args_json: str | None,
    sql: str | None,
    role: str | None = None,
    *,
    on_table: str | None = None,
    on_event: str | None = None,
    command: str | None = None,
    replace: bool = False,
) -> MutationResult:
    """Create a scheduled trigger (webhook or SQL) in the CALLER's org and schedule it (REQ-1003).
    With ``replace``, the org's trigger of that id is given this definition instead (REQ-2054: the
    Triggers page edits what is declared), checked as a new one is; whether it is enabled stays.

    The trigger is stored in the org's model store (REQ-1919) -- never the shared config file --
    and runs bound to that org. url/sql are mutually exclusive — supplying both fails loudly.
    ``request`` is the caller's: a SQL trigger runs as its role, so saving one needs that role
    held or ``cross_org``."""
    import json as _json

    from sqlalchemy import select

    from provisa.api.admin._table_ops import _get_pool
    from provisa.core.models import ScheduledTrigger
    from provisa.core.repositories import scheduled_trigger as trigger_repo
    from provisa.core.schema_org import tracked_webhooks

    kind = kind.strip().lower()
    if kind not in ("webhook", "sql", "command"):
        return MutationResult(
            success=False,
            message=f"Unknown trigger kind {kind!r}",
            code="schema.unknown_trigger_kind",
            params={"kind": kind},
        )
    cron = cron.strip() if cron else None
    if not id.strip() or not name.strip() or not (cron or on_table):
        return MutationResult(
            success=False,
            message="id, name, and a cron schedule or a table to fire on are required",
            code="schema.trigger_fields_required",
        )

    trigger: dict = {"id": id.strip(), "name": name.strip(), "cron": cron, "enabled": True}
    if on_table:  # REQ-2054: fired by the table's change or event-loop event, not a schedule
        trigger["on_table"] = on_table.strip()
        trigger["on_event"] = (on_event or "change").strip()
    pool = await _get_pool()

    if kind == "webhook":
        if not webhook_name:
            return MutationResult(
                success=False,
                message="webhook_name is required for a webhook trigger",
                code="schema.webhook_name_required",
            )
        if sql:  # fail loud on url/sql collision (REQ-1003)
            return MutationResult(
                success=False,
                message="url and sql are mutually exclusive",
                code="schema.url_sql_exclusive",
            )
        async with pool.acquire() as conn:
            res = await conn.execute_core(
                select(tracked_webhooks.c.url).where(tracked_webhooks.c.name == webhook_name)
            )
            row = res.fetchone()
        if row is None:
            return MutationResult(
                success=False,
                message=f"Webhook {webhook_name!r} not found",
                code="schema.webhook_not_found",
                params={"webhook": webhook_name},
            )
        # The webhook's URL is resolved in the org when the trigger fires, not copied here.
        trigger["webhook_name"] = webhook_name
        if args_json:
            trigger["args"] = _json.loads(args_json)
    elif kind == "command":  # REQ-2054
        from provisa.api.app import state as _state

        if not command or not role or role not in (_state.roles or {}):
            return MutationResult(
                success=False,
                message=f"Trigger {id.strip()!r}: a command trigger names a command and a role "
                "of this org it runs as",
                code="schema.trigger_role_required",
                params={"trigger": id.strip()},
            )
        from provisa.api.admin.capabilities import require_trigger_role

        require_trigger_role(request, role)
        trigger["command"] = command
        trigger["role"] = role
        if args_json:
            trigger["args"] = _json.loads(args_json)
    else:  # sql
        if not sql or not sql.strip():
            return MutationResult(
                success=False,
                message="sql is required for a SQL trigger",
                code="schema.sql_required",
            )
        from datetime import datetime, timezone

        from provisa.api.app import state as _state
        from provisa.scheduler.trigger_sql import TriggerSqlRefused, checked_trigger_sql

        if not role or role not in (_state.roles or {}):
            return MutationResult(
                success=False,
                message=f"Trigger {id.strip()!r}: a SQL trigger runs as a role of this org",
                code="schema.trigger_role_required",
                params={"trigger": id.strip()},
            )
        from provisa.api.admin.capabilities import require_trigger_role

        require_trigger_role(request, role)
        try:
            checked_trigger_sql(sql.strip(), id.strip(), datetime.now(timezone.utc))
        except TriggerSqlRefused as exc:
            return MutationResult(
                success=False,
                message=str(exc),
                code="schema.trigger_sql_refused",
                params={"trigger": id.strip(), "reason": str(exc)},
            )
        trigger["sql"] = sql.strip()
        trigger["role"] = role

    try:
        model = ScheduledTrigger(**trigger)
    except ValueError as invalid:
        return MutationResult(
            success=False,
            message=str(invalid),
            code="schema.trigger_refused",
            params={"trigger": trigger["id"], "reason": str(invalid)},
        )
    from provisa.api.app import state as _held
    from provisa.core.models import trigger_refusal
    from provisa.security.rights import SYSTEM_ROLE_IDS

    refused = trigger_refusal(  # REQ-2054
        model,
        commands={
            fn["name"]: [a["name"] for a in fn["arguments"]]
            for fn in _held.tracked_functions.values()
        },
        tables={f"{t['source_id']}.{t['schema_name']}.{t['table_name']}" for t in _held.tables},
        roles=set(_held.roles) | set(SYSTEM_ROLE_IDS),
    )
    if refused is not None:
        return MutationResult(
            success=False,
            message=refused,
            code="schema.trigger_refused",
            params={"trigger": model.id, "reason": refused},
        )
    webhook = trigger.get("webhook_name")
    async with pool.acquire() as conn:
        if replace:
            if not await trigger_repo.replace(conn, model, webhook_name=webhook):
                return MutationResult(
                    success=False,
                    message=f"Task {model.id!r} not found",
                    code="schema.task_not_found",
                    params={"task": model.id},
                )
        else:
            if await trigger_repo.get(conn, model.id) is not None:
                return MutationResult(
                    success=False,
                    message=f"Trigger {model.id!r} already exists",
                    code="schema.trigger_exists",
                    params={"trigger": model.id},
                )
            await trigger_repo.create(conn, model)

    await reschedule_org_triggers()
    if replace:
        return MutationResult(
            success=True,
            message=f"Scheduled task {trigger['id']!r} updated",
            code="schema.scheduled_task_updated",
            params={"task": trigger["id"]},
        )
    return MutationResult(
        success=True,
        message=f"Scheduled task {trigger['id']!r} created",
        code="schema.scheduled_task_created",
        params={"task": trigger["id"]},
    )


async def delete_scheduled_task_op(task_id: str) -> MutationResult:  # REQ-1003
    """Remove one of the CALLER's org's triggers from its model and unschedule it. Another org's
    trigger of the same id is a different trigger and is not touched."""
    from provisa.api.admin._table_ops import _get_pool
    from provisa.core.repositories import scheduled_trigger as trigger_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        removed = await trigger_repo.delete_one(conn, task_id)
    if not removed:
        return MutationResult(
            success=False,
            message=f"Task {task_id!r} not found",
            code="schema.task_not_found",
            params={"task": task_id},
        )
    await reschedule_org_triggers()
    return MutationResult(
        success=True,
        message=f"Task {task_id!r} deleted",
        code="schema.task_deleted",
        params={"task": task_id},
    )
