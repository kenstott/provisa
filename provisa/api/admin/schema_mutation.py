# Copyright (c) 2026 Kenneth Stott
# Canary: 6ce0dc34-587c-4adf-bd5e-60e767a084c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""Admin GraphQL Mutation type — write-side resolvers for all config entities."""

from __future__ import annotations


import logging
import os
from typing import TYPE_CHECKING, Any, Optional, cast

from provisa.api.admin.engine_auth import run_admin_catalog_sql

import strawberry
from sqlalchemy import select, update
from strawberry.types.info import Info as StrawberryInfo

from provisa.core.schema_org import (
    registered_tables,
    relationship_candidates,
    relationships,
    roles,
    sources,
    tracked_webhooks,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

from provisa.compiler.sql_types import key_list
from provisa.core.paging import stored_paging
from provisa.security.sensitive import SENSITIVE_DATA
from provisa.core.repositories import rls as rls_repo
from provisa.api.admin.capabilities import require_capability, require_right_in_domains
from provisa.security.rights import ALL_DOMAINS
from provisa.api.admin.types import (
    CalendarInput,
    ColumnAliasType,
    CompileQueryInput,
    CompileQueryResult,
    DataProductInput,
    DomainInput,
    DqDryRunCheckType,
    DqDryRunType,
    EnforcementType,
    EntityInput,
    FactInput,
    GrantKind,
    KaggleStageResultType,
    MetricInput,
    MutationResult,
    RelationshipInput,
    RLSRuleInput,
    RoleInput,
    PagingInput,
    RoleTtlInput,
    SourceInput,
    TableInput,
    TagAssignmentInput,
    TagInput,
    TagParamValueInput,
)

from provisa.api.admin.schema_helpers import (
    _dataset_ownership_conflict,
    _domain_table_conflict,
    _get_pool,
    _rebuild_schemas,
)
from provisa.api.admin._live_mappers import table_model_from_input as _table_model_from_input
from provisa.api.admin._landing_ttl import (  # REQ-1907, REQ-826
    SourceTtl,
    TableTtl,
    landing_ttl_refusal,
    replicate_contradiction_refusal,
)
from provisa.api.admin._table_ops import _build_columns_for_input
from provisa.api.admin._fake_guard import FakeRefusedSave as _FakeRefusedSave
from provisa.api.admin import schema_mutation_ops as _ops


from provisa.api.admin._row_mappers import (  # noqa: E402
    _federation_hints_from_input,
    _parse_mapping_json,
    _cdc_model_from_input,
)
from provisa.api.admin.schema_common import (  # noqa: E402
    _add_source_pool,
    _analyze_source_on_engine,
    _configure_govdata_env,
    _fire_catalog_indexing,
    _drop_source_on_engine,
    _prime_govdata_cache,
    _queue_creation_request,
    _rebuild_relationship_input,
    _rebuild_source_input,
    _rebuild_table_input,
    _resolve_admin_context,
    _register_source_on_engine,
    _remove_view_mv,
    _stage_kaggle_if_needed,
    _sync_view_mv,
    _cache_prometheus_label_columns,
    _synthesize_mapping_dsl_tables,
    _upsert_source_with_domains,
    _validate_govdata_api_key,
    forget_source_password,
    persist_source_password,
)


# The port pgwire is served on when nothing says otherwise: what provisa-install.yaml's own
# dq-checker/dq-soda entries dial.
_DQ_STANDARD_PGWIRE_PORT = 5439


def _dq_pgwire_port() -> str:
    """The pgwire port a data-quality check dials on ``PROVISA_DQ_HOST``.

    This deployment's own pgwire listener when it runs one — the operator setting
    ``server.pgwire_port`` (REQ-1913). When the listener is off here (port 0), the check is aimed
    at a host that serves pgwire itself, on the standard port; the default is that design's, the
    same one the generated dq-checker/dq-soda entries use, not a guess at a missing value.
    """
    from provisa.core import settings_registry

    port = settings_registry.value("server.pgwire_port")
    return str(port if port != 0 else _DQ_STANDARD_PGWIRE_PORT)


async def _upsert_relationship_impl(
    info: StrawberryInfo, input: RelationshipInput
) -> MutationResult:  # REQ-019, REQ-020, REQ-366, REQ-434
    """Shared body of the upsertRelationship mutation. Module-level so register_fact can
    create its dimension links directly — strawberry invokes root mutations with self=None,
    so a self.upsert_relationship call never works from inside another resolver."""
    from provisa.api.admin.capabilities import has_capability

    # REQ-434/366: a user lacking create_relationship queues a request instead of erroring.
    if not has_capability(info, "create_relationship"):
        return await _queue_creation_request(info, "relationship", "create_relationship", input)
    from provisa.core.models import Relationship as RelModel, Cardinality
    from provisa.core.repositories import relationship as rel_repo
    from provisa.api.admin.capabilities import _identity_from_info

    pool = await _get_pool()
    try:
        Cardinality(input.cardinality)
    except ValueError:
        return MutationResult(
            success=False,
            message=f"Invalid cardinality: {input.cardinality!r}",
            code="schema.invalid_cardinality",
            params={"cardinality": input.cardinality},
        )
    # REQ-1531: A RELATIONSHIP IS OWNED BY ITS SOURCE. The row is source -> target and the unique
    # constraint is (source_table_id, alias), so the edge hangs off the source side and the source's
    # domain is the one being changed. The target is referenced, not altered — its own RLS and
    # masking still apply when the join is traversed. So the gate asks about the SOURCE domain, and
    # an edge whose target lives elsewhere is recorded but flagged for the other domain to review.
    from provisa.api.admin.capabilities import require_domain
    from provisa.core.repositories import table as table_repo

    _pool_g = await _get_pool()
    async with _pool_g.acquire() as _gconn:
        _src_row = await table_repo.find_by_table_name(
            cast("Connection", _gconn), input.source_table_id
        )
        _tgt_row = (
            await table_repo.find_by_table_name(cast("Connection", _gconn), input.target_table_id)
            if input.target_table_id
            else None
        )
    if _src_row is None:
        return MutationResult(
            success=False,
            message=f"Source table {input.source_table_id!r} is not registered",
            code="schema.relationship_unknown_source",
            params={"table": input.source_table_id},
        )
    try:
        require_domain(info, _src_row["domain_id"])
    except PermissionError:
        # REQ-434/1531: out of domain queues a request, the same answer a missing right gets.
        return await _queue_creation_request(info, "relationship", "create_relationship", input)
    _cross_domain = _tgt_row is not None and _tgt_row["domain_id"] != _src_row["domain_id"]

    # REQ-1586: a junction end is an ordered column list paired positionally against the
    # relationship's own key, so the two lists must be the same length. Saving a mismatch would
    # produce an edge the compiler can only reject at query time.
    if input.via_table:
        _via_src = key_list(input.via_source_column)
        _via_tgt = key_list(input.via_target_column)
        if len(_via_src) != len(key_list(input.source_column)) or len(_via_tgt) != len(
            key_list(input.target_column or "")
        ):
            return MutationResult(
                success=False,
                message=(
                    "Junction key lists must pair positionally with the relationship's own "
                    "source and target keys"
                ),
                code="schema.relationship_junction_key_mismatch",
                params={"relationship": input.id},
            )

    # REQ-020: record the defining steward as owner.
    _identity = _identity_from_info(info)
    _owner = getattr(_identity, "user_id", None) if _identity is not None else None
    model = RelModel(
        id=input.id,
        source_table_id=input.source_table_id,
        target_table_id=input.target_table_id or "",
        source_column=input.source_column,
        target_column=input.target_column or "",
        cardinality=Cardinality(input.cardinality),
        materialize=input.materialize,
        refresh_interval=input.refresh_interval,
        target_function_name=input.target_function_name or None,
        function_arg=input.function_arg or None,
        alias=input.alias or None,
        graphql_alias=getattr(input, "graphql_alias", None) or None,
        disable_cypher=getattr(input, "disable_cypher", False),
        # REQ-1586: the junction declaration travels with the edge on save.
        via_table=input.via_table,
        via_source_column=input.via_source_column,
        via_target_column=input.via_target_column,
        via_type_column=input.via_type_column or None,
        via_type_value=input.via_type_value or None,
        via_label_source=input.via_label_source,
        owner=_owner,
    )
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        from provisa.api.admin._fake_guard import relationship_fake_refusal

        try:
            async with _conn.transaction():
                await rel_repo.upsert(_conn, model)
                # REQ-1494: the edge's two columns, both faked, must declare one fake; the save
                # is undone when they do not.
                await relationship_fake_refusal(_conn, input.id)
        except _FakeRefusedSave as refused:
            return refused.result
        if _cross_domain:
            # REQ-1531: re-assert AFTER the upsert. rel_repo.upsert clears needs_review on conflict
            # (REQ-020 treats a save as an explicit re-review), and a cross-domain edge is not the
            # source steward's to clear — the flag is the other domain's notice that its tables are
            # now reachable through an edge it did not define.
            await _conn.execute_core(
                update(relationships)
                .where(relationships.c.id == input.id)
                .values(needs_review=True)
            )
        if input.record_candidate and not input.target_function_name:
            _rres = await _conn.execute_core(
                select(relationships.c.source_table_id, relationships.c.target_table_id).where(
                    relationships.c.id == input.id
                )
            )
            rel_row = _rres.fetchone()
            if rel_row and rel_row.target_table_id is not None:
                # DO UPDATE sets the same literal values it inserts (accepted / 1.0 /
                # 'SQL modeling (admin)'), so an EXCLUDED-column upsert is equivalent.
                await _conn.upsert(
                    relationship_candidates,
                    {
                        "source_table_id": rel_row.source_table_id,
                        "target_table_id": rel_row.target_table_id,
                        "source_column": input.source_column,
                        "target_column": input.target_column or None,
                        "cardinality": input.cardinality,
                        "confidence": 1.0,
                        "reasoning": "SQL modeling (admin)",
                        "suggested_name": input.id,
                        "scope": "admin",
                        "status": "accepted",
                    },
                    index_elements=[
                        "source_table_id",
                        "source_column",
                        "target_table_id",
                        "target_column",
                    ],
                    update_columns=["status", "confidence", "reasoning"],
                )
    await _rebuild_schemas()
    return MutationResult(
        success=True,
        message=f"Relationship {input.id!r} saved",
        code="schema.relationship_saved",
        params={"relationship": input.id},
    )


def _assignment_target_problem(model) -> "MutationResult | None":  # REQ-1377
    """Reject an assignment whose typed target fields don't match its object_type."""
    from provisa.core.models import TAG_OBJECT_TYPES

    required = {
        "source": model.source_id,
        "table": model.table_id,
        "column": model.table_id is not None and model.column_name,
        "relationship": model.relationship_id,
        "command": model.command_name,
    }
    if model.object_type not in TAG_OBJECT_TYPES:
        return MutationResult(
            success=False,
            message=f"object_type must be one of {list(TAG_OBJECT_TYPES)}",
            code="schema.tag_bad_object_type",
            params={"objectType": model.object_type},
        )
    if not required[model.object_type]:
        return MutationResult(
            success=False,
            message=f"Missing target identifier for a {model.object_type!r} tag assignment",
            code="schema.tag_bad_target",
            params={"objectType": model.object_type},
        )
    return None


def _require_tag_editor(info: StrawberryInfo) -> None:  # REQ-1944
    """The surface gate for tag definitions and assignments: a table editor, or a holder of
    sensitive_data (whose reach the per-tag checks then narrow). Admits the caller to ask; what
    the edit then needs is decided once the tag is known."""
    from provisa.api.admin.capabilities import has_capability

    if not has_capability(info, SENSITIVE_DATA):
        require_capability(info, "table_registration")


def _require_sensitive_tag_definer(info: StrawberryInfo) -> None:  # REQ-1944
    """Editing a sensitive tag's definition (not its option): a table editor as before, or a
    holder of sensitive_data reaching every domain -- the tag hides columns in all of them."""
    from provisa.api.admin.capabilities import has_capability, holds_right_in_domains

    if holds_right_in_domains(info, SENSITIVE_DATA, {ALL_DOMAINS}):
        return
    if not has_capability(info, "table_registration"):
        require_right_in_domains(info, SENSITIVE_DATA, {ALL_DOMAINS})


async def _require_tag_assignment_right(  # REQ-1943, REQ-1944
    info: StrawberryInfo,
    conn: "Connection",
    tag_row: dict,
    object_type: str,
    table_id: int | None,
) -> None:
    """A sensitive tag on a column decides how its values are hidden: sensitive_data in the
    domain of the column's table (REQ-1944). Any other assignment is a table editor's act."""
    if not (tag_row["sensitive"] and object_type == "column"):
        require_capability(info, "table_registration")
        return
    from provisa.api.admin.domain_guard import table_domain

    assert table_id is not None  # _assignment_target_problem: a column names its table
    require_right_in_domains(info, SENSITIVE_DATA, {await table_domain(conn, table_id)})


async def _refresh_config_tags() -> None:  # REQ-1373/1377
    """The DB is the source of truth for tags; mirror it into state.config for consumers
    (metadata export builder) that read the in-memory config."""
    from provisa.api.app import state
    from provisa.core.models import Tag as TagModel, TagAssignment as TagAssignmentModel
    from provisa.core.repositories import tag as tag_repo

    if state.config is None:
        return
    pool = await _get_pool()
    async with pool.acquire() as conn:
        tag_rows = await tag_repo.list_all(cast("Connection", conn))
        assignment_rows = await tag_repo.list_assignments(cast("Connection", conn))
    state.config.tags = [
        TagModel(
            id=r["id"],
            description=r["description"],
            applies_to=list(r["applies_to"] or []),
            is_system=bool(r["is_system"]),
            derived=bool(r["derived"]),
            reason_policy=r["reason_policy"],
            expires_policy=r["expires_policy"],
            param_policy=r["param_policy"],  # REQ-1467
        )
        for r in tag_rows
    ]
    state.config.tag_assignments = [
        TagAssignmentModel(
            tag_id=r["tag_id"],
            object_type=r["object_type"],
            source_id=r["source_id"],
            table_id=r["table_id"],
            column_name=r["column_name"],
            relationship_id=r["relationship_id"],
            command_name=r["command_name"],
            table_ref=r["table_ref"],
            reason=r["reason"],
            expires_on=r["expires_on"],
        )
        for r in assignment_rows
    ]


def _validate_load_protection(
    load_protected: bool | None,
    off_peak_window: str | None,
    cache_ttl: int | None,
    change_signal: str | None,
    who: str,
) -> "MutationResult | None":  # REQ-1141
    """Enforce the REQ-1141 ≥1-gate rule for a load-protected target. Returns a failing
    MutationResult when load protection is on but no gate is armed, else None."""
    if not load_protected:
        return None
    armed = (
        bool(off_peak_window) or cache_ttl is not None or (change_signal in ("probe", "ttl_probe"))
    )
    if not armed:
        return MutationResult(
            success=False,
            message=(
                f"{who}: load protection requires at least one refresh gate — set an off-peak "
                "window, a cache_ttl cadence, or a probing change_signal (probe/ttl_probe) (REQ-1141)"
            ),
            code="schema.load_protection_gate_required",
            params={"who": who},
        )
    return None


def _validate_source_load_management(input: SourceInput) -> "MutationResult | None":  # REQ-1909
    """The "Load Management and Timeliness" panel's source settings, validated at write time by the
    same rules the config loader and the query path apply: a known change_signal (REQ-929), a
    non-negative cache_ttl, a live cap of at least 1 (REQ-1909), a sentinel URL with a supported
    scheme (REQ-1148), a freshness gate the source's signal can build (REQ-860), a parseable
    off-peak window (REQ-1141), and at least one refresh gate when load protected (REQ-1141).
    Returns a failing MutationResult naming the rule, else None."""
    from provisa.core.change_signal import resolve as _resolve_signal

    def _fail(code: str, message: str) -> MutationResult:
        return MutationResult(
            success=False,
            message=f"Source {input.id!r}: {message}",
            code=code,
            params={"source": input.id},
        )

    try:
        _resolve_signal(None, input.change_signal)
    except ValueError as e:
        return _fail("schema.invalid_change_signal", str(e))
    if input.cache_ttl is not None and input.cache_ttl < 0:
        return _fail("schema.invalid_cache_ttl", "cache_ttl must be 0 or more seconds")
    if input.max_live_concurrency is not None and input.max_live_concurrency < 1:
        return _fail(
            "schema.max_live_concurrency_invalid",
            "max_live_concurrency must be 1 or more (leave it empty for no cap; to stop live "
            "reads use replicate 0 (always) or load_protected)",
        )
    if input.sentinel_path:
        from provisa.events.sentinel_probe import build_sentinel_probe

        try:
            build_sentinel_probe(input.sentinel_path)
        except ValueError as e:
            return _fail("schema.invalid_sentinel_path", str(e))
    if input.freshness_gate:
        from types import SimpleNamespace

        from provisa.freshness.source_gate import source_strategy

        try:
            source_strategy(
                cast(
                    Any,
                    SimpleNamespace(
                        id=input.id, change_signal=input.change_signal, cache_ttl=input.cache_ttl
                    ),
                )
            )
        except ValueError as e:
            return _fail("schema.invalid_freshness_gate", str(e))
    window_err = _parsed_off_peak(input.off_peak_window, input.off_peak_tz)
    if window_err is not None:
        return window_err
    return _validate_load_protection(
        input.load_protected, input.off_peak_window, input.cache_ttl, input.change_signal, input.id
    )


def _snapshot_load_protection_conflict(
    load_protected: bool | None,
    off_peak_window: str | None,
    mv_bitemporal_mode: str | None,
) -> bool:  # REQ-1170
    """True when load protection (off-peak window) AND snapshotting are both configured on one
    table — their timing can fight (a snapshot boundary may fall outside the off-peak window), so
    the caller emits a WARNING (not a block)."""
    return bool((load_protected or off_peak_window) and mv_bitemporal_mode)


def _parsed_off_peak(off_peak_window: str | None, tz: str) -> "MutationResult | None":  # REQ-1141
    """Validate an off-peak window spec at write time; returns a failing MutationResult on a
    malformed spec/zone, else None (no silent default window)."""
    if off_peak_window is None:
        return None
    from provisa.federation.scheduled_refresh import parse_off_peak_window

    try:
        parse_off_peak_window(off_peak_window, tz)
    except (ValueError, KeyError) as e:  # ZoneInfoNotFoundError subclasses KeyError
        return MutationResult(
            success=False,
            message=f"invalid off-peak window: {e}",
            code="schema.invalid_off_peak_window",
            params={"error": str(e)},
        )
    return None


def _refuse_invalid_profiler(
    source_id: str, source_type: str, mapping: dict
) -> MutationResult | None:  # REQ-1934
    """A Data Profiler source's settings must describe a schedule and a run default."""
    if source_type != "data_profiler":
        return None
    from provisa.profiler.source import profiler_settings

    try:
        profiler_settings(source_id, mapping)
    except ValueError as exc:
        return MutationResult(
            success=False,
            message=str(exc),
            code="schema.profiler_invalid",
            params={"source": source_id},
        )
    return None


async def _refuse_over_source_limit(source_id: str) -> MutationResult | None:  # REQ-1513
    """Refuse a NEW source the org's plan has no room for, or None when there is room.

    The ceiling is the one the Billing page prints on the plan card, so the number an administrator
    chose the plan for is the number enforced here. Only a source the org does not already hold is
    tested: ``create_source`` is an upsert, and editing the connection details of a source that is
    already registered adds nothing to the count.

    None on a self-hosted deployment — there is no subscription, so there is no ceiling (REQ-1513).
    """
    from provisa.api.app import state
    from provisa.core.request_context import require_current_org
    from provisa.core.commerce import source_limit_for_org
    from provisa.core.repositories import source as source_repo

    org_id = require_current_org()
    limit = await source_limit_for_org(state, org_id)
    if limit is None:
        return None
    max_sources, plan = limit
    pool = await _get_pool()
    async with pool.acquire() as conn:
        if await source_repo.get(conn, source_id) is not None:
            return None
        held = await source_repo.count_billable(conn)
    if held < max_sources:
        return None
    return MutationResult(
        success=False,
        message=(
            f"This organization holds {held} of the {max_sources} data sources its {plan} plan "
            f"admits. Change the plan on the Billing page, or remove a source."
        ),
        code="schema.source_limit_reached",
        params={"source": source_id, "held": str(held), "limit": str(max_sources), "plan": plan},
    )


def _refuse_soda_on_hosted_plane(source_type: str) -> MutationResult | None:  # REQ-1725
    """Refuse a ``soda`` source on the operator-hosted plane, or None elsewhere.

    soda-core is Elastic License 2.0, which prohibits offering it to third parties as a hosted or
    managed service (config/capabilities.yaml's ``cloud_eligible: false`` on the ``soda`` option
    documents this, but nothing read that field — the Sources form never lists ``soda`` as an
    option, so it was unreachable there, but ``create_source`` itself took the type from any
    caller, API clients included, with no check at all). ``commerce.enabled()`` is the same
    self-hosted-vs-hosted signal REQ-1469's ``/auth/me`` billing flag and REQ-1513's source-limit
    gate both use — the commercial plugin is mounted only on the plane this license bars soda
    from. A self-hosted or desktop install, where the plugin is absent, is unaffected.
    """
    if source_type != "soda":
        return None
    from provisa.core.commerce import enabled as commerce_enabled

    if not commerce_enabled():
        return None
    return MutationResult(
        success=False,
        message=(
            "Soda (external data quality) is not offered on this hosted platform — its "
            "Elastic License 2.0 terms prohibit offering it as a hosted or managed service. "
            "Great Expectations (Apache 2.0) covers the same checker pattern."
        ),
        code="schema.source_type_not_hosted",
        params={"type": source_type},
    )


def _invalid_replicate(replicate: int | None) -> MutationResult | None:  # REQ-826
    """The refusal for a ``replicate`` value that is not one the setting has, else None."""
    from provisa.core.replicate import check_replicate

    try:
        check_replicate(replicate)
    except ValueError as exc:
        return MutationResult(
            success=False,
            message=str(exc),
            code="schema.replicate_invalid",
            params={"value": str(replicate)},
        )
    return None


@strawberry.type
class Mutation:  # REQ-012, REQ-013, REQ-016, REQ-042
    @strawberry.mutation
    async def rebuild_schemas(self, info: StrawberryInfo) -> MutationResult:
        """Rebuild in-memory schema from DB state. Useful after external DB changes."""
        require_capability(info, "org_settings")
        await _rebuild_schemas()
        return MutationResult(
            success=True, message="Schemas rebuilt", code="schema.schemas_rebuilt"
        )

    @strawberry.mutation
    async def dry_run_dq_contract(  # REQ-1443 clause 7
        self, info: StrawberryInfo, source_id: str, contract_text: str
    ) -> DqDryRunType:
        """Run a contract against the live table and report the outcomes, landing none.

        A mutation rather than a query because it costs a real scan — a client that refetched it on
        cache invalidation would re-scan the table — but it writes nothing: the checker's rows go
        into the response instead of into the results table. What it proves is the thing a syntax
        check cannot: which governed table the dataset identifier actually resolved to."""
        require_capability(info, "query_development")
        from provisa.api.admin._dq_resolvers import dry_run_contract

        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await dry_run_contract(
                cast("Connection", conn), source_id=source_id, contract_text=contract_text
            )
        if not result["success"]:
            return DqDryRunType(success=False, message=result["message"])
        return DqDryRunType(
            success=True,
            message=result["message"],
            checker_version=result["checker_version"],
            checks=[DqDryRunCheckType(**c) for c in result["checks"]],
        )

    @strawberry.mutation
    async def run_dq_check_now(  # REQ-1443: "run now and retain" from the DQ check detail
        self,
        info: StrawberryInfo,
        table_id: int | None = None,
        schema_name: str | None = None,
        table_name: str | None = None,
    ) -> MutationResult:
        """Fire a checker table's poll job immediately instead of waiting for its cadence. The
        table is named by its registered id, or by schema and table name (refused when more than
        one source registers that name).

        Reuses the same registered poll job the event loop already runs on cadence (REQ-941) — this
        does not re-scan into the response like the dry run; it lands the scan's rows the normal way,
        so results persist and the DQ check detail's history shows the new scan."""
        require_capability(info, "table_registration")
        from provisa.api.app import state
        from provisa.api.admin._dq_resolvers import run_dq_check_now as _run_now
        from provisa.core.request_context import require_current_org

        # REQ-1266: the scheduler namespaces a poll job's id by the org bound when it was
        # registered (register_poll_job/register_runtime read `current_org`), and the boot and
        # every request bind theirs, so the bound org names the job this request registered.
        org_id = require_current_org()
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await _run_now(
                cast("Connection", conn),
                scheduler=state._scheduler,
                org_id=org_id,
                table_id=table_id,
                schema_name=schema_name,
                table_name=table_name,
            )
        return MutationResult(
            success=result["success"],
            message=result["message"],
            code="dq.run_now" if result["success"] else "dq.run_now_failed",
        )

    @strawberry.mutation
    async def create_calendar(
        self, info: StrawberryInfo, input: "CalendarInput"
    ) -> MutationResult:  # REQ-962
        """Create/replace a versioned snapshot-boundary calendar (REQ-962). Validated by constructing
        the in-memory Calendar (fails loud on a bad base_system/tz/anchor) before it is persisted; a
        rebuild reloads the registry so a periodic MV can resolve it."""
        require_capability(info, "table_registration")
        from datetime import date

        from provisa.core.repositories import calendar as calendar_repo
        from provisa.events.calendars import BaseSystem, Calendar

        try:
            Calendar(  # validation only — raises on an unknown base_system / bad tz
                name=input.name,
                version=input.version,
                base_system=BaseSystem(input.base_system),
                tz=input.tz,
                fiscal_anchor=(input.fiscal_anchor_month, input.fiscal_anchor_day),
                retail_anchor=date.fromisoformat(input.retail_anchor)
                if input.retail_anchor
                else None,
                week_start=input.week_start,
                holidays=frozenset(date.fromisoformat(d) for d in input.holidays),
                weekend=frozenset(input.weekend),
            )
        except (ValueError, KeyError) as e:
            return MutationResult(
                success=False,
                message=f"invalid calendar: {e}",
                code="schema.invalid_calendar",
                params={"error": str(e)},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await calendar_repo.upsert(
                cast("Connection", conn),
                {
                    "name": input.name,
                    "version": input.version,
                    "base_system": input.base_system,
                    "tz": input.tz,
                    "fiscal_anchor_month": input.fiscal_anchor_month,
                    "fiscal_anchor_day": input.fiscal_anchor_day,
                    "retail_anchor": date.fromisoformat(input.retail_anchor)
                    if input.retail_anchor
                    else None,
                    "week_start": input.week_start,
                    "holidays": input.holidays,
                    "weekend": input.weekend,
                },
            )
        return MutationResult(
            success=True,
            message=f"calendar {input.name!r} v{input.version} saved",
            code="schema.calendar_saved",
            params={"calendar": input.name, "version": input.version},
        )

    @strawberry.mutation
    async def delete_calendar(self, info: StrawberryInfo, name: str) -> MutationResult:  # REQ-962
        """Delete a snapshot-boundary calendar (all versions) — ONLY when no MV references it. A
        calendar in use MUST NOT be removed (its snapshots would lose their boundary source), so this
        fails loud with the usage count rather than orphaning a periodic MV."""
        require_capability(info, "table_registration")
        from provisa.core.repositories import calendar as calendar_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            try:
                removed = await calendar_repo.delete(_conn, name)
            except calendar_repo.CalendarDeleteRefused as refused:
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code="schema.calendar_in_use",
                    params={
                        "calendar": name,
                        "count": len(refused.dependents),
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if removed == 0:
            return MutationResult(
                success=False,
                message=f"calendar {name!r} not found",
                code="schema.calendar_not_found",
                params={"calendar": name},
            )
        return MutationResult(
            success=True,
            message=f"calendar {name!r} deleted",
            code="schema.calendar_deleted",
            params={"calendar": name},
        )

    @strawberry.mutation
    async def create_source(
        self, info: StrawberryInfo, input: SourceInput
    ) -> MutationResult:  # REQ-012, REQ-013
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "source_registration")
        from provisa.core.models import Source as SourceModel, SourceType as SourceTypeEnum

        # REQ-1907: a ttl / ttl_probe signal needs a cache_ttl on the source or on each table
        # already registered under this id.
        async with (await _get_pool()).acquire() as _ttl_conn:
            _ttl_refusal = await landing_ttl_refusal(
                _ttl_conn,
                input.id,
                source=SourceTtl(
                    input.change_signal,
                    input.cache_ttl,
                    input.replicate,
                    input.load_protected,
                ),
            )
        if _ttl_refusal is not None:
            return _ttl_refusal

        _limit_refusal = await _refuse_over_source_limit(input.id)
        if _limit_refusal is not None:
            return _limit_refusal

        _load_mgmt_refusal = _validate_source_load_management(input)  # REQ-1909
        if _load_mgmt_refusal is not None:
            return _load_mgmt_refusal

        _soda_refusal = _refuse_soda_on_hosted_plane(input.type)
        if _soda_refusal is not None:
            return _soda_refusal

        if input.type == "govdata":
            _err = await _validate_govdata_api_key(input)
            if _err is not None:
                return _err

        pool = await _get_pool()
        from provisa.api.app import state
        from provisa.core.secrets_store import bound_to_request_org

        # REQ-012: validate the direct connection before persisting; reject on failure
        # rather than leaving a half-registered source behind a swallowed error.
        # REQ-1695: inside the org's vault, because the form may carry a ``${secret:NAME}`` the
        # operator wrote rather than a literal, and the pool resolves what it was given.
        try:
            async with bound_to_request_org():
                # REQ-1819: BEFORE connection validation — a Kaggle-hinted source's files must
                # exist on disk before _add_source_pool can validate a connection to them. Runs
                # for every path that reaches create_source (this direct call, create_source_now,
                # and a queued propose_source request executed on approval), overwriting
                # input.path with the real staged directory regardless of what the caller sent.
                _kaggle_refusal = await _stage_kaggle_if_needed(input)
                if _kaggle_refusal is not None:
                    return _kaggle_refusal
                await _add_source_pool(state, input)
        except Exception as _conn_err:
            logging.getLogger(__name__).exception(
                "create_source: connection validation failed for %r", input.id
            )
            return MutationResult(
                success=False,
                message=f"Source {input.id!r}: connection validation failed: {_conn_err}",
                code="schema.source_connection_failed",
                params={"source": input.id, "error": str(_conn_err)},
            )

        from provisa.api.admin._change_feed import source_change_feed_refusal

        async with (await _get_pool()).acquire() as _feed_conn:
            _feed_refusal = await source_change_feed_refusal(_feed_conn, input)  # REQ-1861
        if _feed_refusal is not None:
            return _feed_refusal

        # REQ-1695: the connection answered, so this credential is worth keeping. A literal goes
        # into the org vault and the row keeps the reference that names it; the row never holds a
        # credential. Done after the validation so a rejected source leaves no vault entry behind.
        password_ref = await persist_source_password(info, input.id, input.password)
        _mapping = _parse_mapping_json(input.mapping_json)
        _profiler_refusal = _refuse_invalid_profiler(input.id, input.type, _mapping)  # REQ-1934
        if _profiler_refusal is not None:
            return _profiler_refusal
        if not _mapping:
            from provisa.dq.registration import is_checker_source_type

            if is_checker_source_type(input.type):
                # REQ-1742 gap: a checker source (soda/great_expectations) has NO connection
                # fields in the Sources form (NO_CONNECTION_TYPES, constants.ts) — the checker
                # doesn't connect to a remote system, it scans an already-registered table
                # through PROVISA'S OWN pgwire endpoint (dq.runner.run_contract's `connection`
                # arg). The shipped dq-checker/dq-soda demo sources (config/provisa-install.yaml)
                # hardcode this mapping by hand; a source created THROUGH THE UI never got it at
                # all, so its mapping stayed permanently empty and every dry-run/scan against it
                # raised a raw KeyError('host') the first time run_contract indexed into it.
                # Same env-var defaults provisa-install.yaml's own dq-checker/dq-soda entries use.
                _mapping = {
                    "host": os.environ.get("PROVISA_DQ_HOST", "localhost"),
                    "port": _dq_pgwire_port(),
                    "database": "provisa",
                    "user": os.environ.get("PROVISA_DQ_USER", "org_admin"),
                    "password": os.environ.get("PROVISA_DQ_PASSWORD", "provisa"),
                }
        model = SourceModel(
            id=input.id,
            type=SourceTypeEnum(input.type),
            host=input.host,
            port=input.port,
            database=input.database,
            username=input.username,
            password=password_ref,
            path=input.path,
            description=input.description,
            mapping=_mapping,
            federation_hints=_federation_hints_from_input(input),
            change_signal=input.change_signal,
            load_protected=input.load_protected,  # REQ-1141
            off_peak_window=input.off_peak_window,  # REQ-1141
            off_peak_tz=input.off_peak_tz,  # REQ-1141
            cache_enabled=input.cache_enabled,
            cache_ttl=input.cache_ttl,
            replicate=input.replicate,  # REQ-826
            max_live_concurrency=input.max_live_concurrency,  # REQ-1909
            sentinel_path=input.sentinel_path,  # REQ-1148
            freshness_gate=input.freshness_gate,  # REQ-860
            cdc=_cdc_model_from_input(input),
        )

        # REQ-1531: a source with no allowed list is open to every domain, so opening it to a
        # domain — or to all of them — needs the caller to reach that domain.
        from provisa.api.admin.capabilities import require_reach_of_added_domains
        from provisa.core.repositories import source as _source_repo

        async with pool.acquire() as _held_conn:
            _held = await _source_repo.get(cast("Connection", _held_conn), input.id)
        _was = None if _held is None else _held["allowed_domains"] or []
        # The list the source will hold: the one given, or — when none is given — the one it
        # already holds (``_upsert_source_with_domains`` writes only a list that names a domain).
        _named = [d for d in (input.allowed_domains or []) if d.strip()]
        require_reach_of_added_domains(info, _was, _named or _was or [], empty_is_all=True)
        await _upsert_source_with_domains(pool, model, input)

        if input.type == "govdata" and input.username:
            _configure_govdata_env(input)

        _domains = [d for d in (input.allowed_domains or []) if d.strip()]
        if _domains:
            state.source_allowed_domains[input.id] = _domains
        state.source_types[input.id] = input.type
        # REQ-1757: was hardcoded "" for every dynamically-registered source regardless of type —
        # decide_route's `dialect = decision.dialect or "postgres"` (pgwire/_pipeline.py) then
        # silently fell back to postgres dialect for the DIRECT route of ANY source created here
        # (mariadb/tidb included), compiling ANSI-double-quoted SQL for a MySQL-wire server that
        # rejects it. model.dialect (core/models.py's Source.dialect) is the SAME
        # SOURCE_TO_DIALECT-backed property app_loaders.py's config-load path already uses.
        state.source_dialects[input.id] = model.dialect or ""
        if model.federation_hints:
            # Mirrors _populate_source_catalog_names in app_loaders.py: the config path publishes
            # the hints to runtime state, so the dynamic path must too.
            state.source_federation_hints[input.id] = dict(model.federation_hints)

        # Populate the org-scoped catalog name so catalog_for() resolves this source
        # after dynamic creation (mirrors _populate_source_catalog_names in app_loaders.py).
        from provisa.api.app_loaders import catalog_name_for_source

        # The physical catalog is derived from the source id and nothing else — create_catalog
        # (provisa/core/catalog.py:116) names it `_to_catalog_name(source.id)`, and native engines
        # attach by source id too. `input.database` is the *remote* database/tenant the connector
        # talks to, never a catalog name: a SharePoint source puts its Azure tenant GUID there, so
        # deriving the catalog from it recorded `"5d2609cc-…"` for a catalog physically created as
        # `e2e_sharepoint`, and every engine query for the source died on CATALOG_NOT_FOUND.
        # Exceptions: a fixed-catalog warehouse engine (BigQuery/Fabric/Synapse) pins every source
        # to the one warehouse catalog instead; an adapter-fetched source under Trino resolves
        # through Trino's own materialize-store catalog (REQ-1730) — see catalog_name_for_source.
        state.source_catalogs[input.id] = catalog_name_for_source(state, input.type, input.id)

        # Provision on the bound engine (the engine makes a catalog; native engines no-op / attach lazily).
        # REQ-1695: under the org's vault -- the password it registers is now a reference into it.
        async with bound_to_request_org():
            await _synthesize_mapping_dsl_tables(pool, model)
            await _cache_prometheus_label_columns(pool, state, model)
            _register_source_on_engine(state, model, input)
        await _analyze_source_on_engine(state, pool, model, input)

        if input.type == "govdata" and input.database and input.username:
            _prime_govdata_cache(input)

        _fire_catalog_indexing(state, pool, input)

        # REQ-1730: createSource is an upsert (REQ-1266's own doc on _upsert_source_with_domains) —
        # replaying it against a DIFFERENT engine than the one active at original registration
        # (reprovisioning a source under a newly-swapped-to engine) leaves that engine's own
        # ctx/schema_build_cache never rebuilt, so any table ALREADY registered for this source
        # (in the shared control plane, from its original registration) compiles against whatever
        # catalog_name this source resolved to at the LAST rebuild — before this replay corrected
        # state.source_catalogs[input.id] moments ago. update_table/register_table already rebuild
        # on every call; create_source alone never did, because a brand-new source has no
        # registered tables yet to rebuild for. An upserted one can.
        await _rebuild_schemas()
        # Same gap update_table's own reconcile call (below, this file) already closed: a
        # materialize-only table replayed onto a new engine here has an EXISTING registered_tables
        # row (from its original registration) but no landed replica on THIS engine yet — nothing
        # else creates it. Reproduced live: airport/firebird SCHEMA_NOT_FOUND on Trino after a
        # swap replay, because reconcile_landed_tables() never ran for this engine's process
        # against the just-reprovisioned source.
        try:
            await state.federation_engine.reconcile_landed_tables()
        except Exception:
            logging.getLogger(__name__).warning(
                "Landed-table reconcile failed after create_source", exc_info=True
            )

        if input.type == "data_profiler":  # REQ-1934: its schedule fires on the org's scheduler
            from provisa.api.admin.schema_mutation_ops import reschedule_org_triggers

            await reschedule_org_triggers()
        return MutationResult(
            success=True,
            message=f"Source {input.id!r} created",
            code="schema.source_created",
            params={"source": input.id},
        )

    @strawberry.mutation
    async def stage_kaggle_dataset(  # REQ-1780, REQ-1781, REQ-1782
        self, info: StrawberryInfo, token: str, owner: str, ref: str, id_prefix: str
    ) -> KaggleStageResultType:
        """Download+unzip a Kaggle dataset bundle onto local disk (REQ-1780 v1 scope: SQLite
        bundles are rejected whole, not partially staged) and hand back the staged directory.

        (Amended 2026-09-19, one `files` Source per dataset:) registration is deliberately NOT
        done here, same as before, but the caller now creates exactly ONE Source — type `files`,
        `path` = the staged directory — instead of one plain csv/parquet Source per bundle file.
        The `files`/pgwire-file connector already discovers every file in a directory as its own
        table (recursively, REQ-1690), single- or multi-file alike, so there is nothing left for
        this mutation to enumerate: no per-file crawl, no per-column type/name sanitization (the
        naming authority, REQ-471, already guarantees valid identifiers for whatever the Register
        Table form discovers live). [SUPERSEDED by this amendment: the previous version crawled
        the directory here and returned one KaggleStagedFileType per bundle file, each meant to
        become its own plain csv/parquet Source — kept for history; do not implement against it.]
        """
        import re

        from provisa.api.admin.capabilities import require_capability
        from provisa.core.models import _SAFE_ID_PATTERN
        from provisa.kaggle.downloader import UnsupportedKaggleDataset, stage_dataset

        require_capability(info, "source_registration")

        try:
            staged_root = await stage_dataset(token, owner, ref)
        except UnsupportedKaggleDataset as exc:
            return KaggleStageResultType(
                success=False, message=str(exc), directory="", suggested_source_id=""
            )

        suggested_id = id_prefix
        if not _SAFE_ID_PATTERN.match(suggested_id):
            suggested_id = "s_" + re.sub(r"[^a-zA-Z0-9_-]", "_", suggested_id)
        return KaggleStageResultType(
            success=True,
            message=f"staged {owner}/{ref}",
            directory=str(staged_root),
            suggested_source_id=suggested_id,
        )

    @strawberry.mutation
    async def refresh_kaggle_source(  # REQ-1780/1781/1782/1783
        self, info: StrawberryInfo, source_id: str, token: str
    ) -> MutationResult:
        """Re-fetch a Kaggle-derived source's dataset in place. stage_dataset's own contract
        (downloader.py) is idempotent -- re-running it overwrites each file at the SAME directory
        registerSource already points at.

        (Amended 2026-09-19, one `files` Source per dataset:) unlike the pre-amendment plain
        csv/parquet SCAN sources (which re-read the path fresh on every query, no cache to
        invalidate), a `files` source is ATTACHed live through a per-source-id pgwire-file JVM
        server that is started once and cached for the life of the process (REQ-1690). Re-staging
        the directory on disk without also evicting that cache would silently keep serving
        whatever schema the server saw at its first attach — the exact bug `stop_endpoint`
        (pgwire_replica.py) was added THIS SESSION to close for delete+recreate; a refresh needs
        the identical fix. The Kaggle token is deliberately never persisted server-side (REQ-1783)
        -- the caller re-enters it for this call, same as the original staging step."""
        from datetime import datetime, timezone
        from pathlib import Path

        from provisa.api.admin.capabilities import require_capability
        from provisa.core.repositories import source as source_repo
        from provisa.federation.pgwire_replica import stop_endpoint
        from provisa.kaggle.client import get_dataset_last_updated
        from provisa.kaggle.downloader import UnsupportedKaggleDataset, stage_dataset, staged_mtime

        require_capability(info, "source_registration")

        pool = await _get_pool()
        async with pool.acquire() as conn:
            row = await source_repo.get(cast("Connection", conn), source_id)
        if row is None:
            return MutationResult(
                success=False,
                message=f"Source {source_id!r} not found",
                code="schema.source_not_found",
                params={"source": source_id},
            )
        hints = row.get("federation_hints") or {}
        owner = hints.get("kaggle_owner")
        ref = hints.get("kaggle_ref")
        if not owner or not ref:
            return MutationResult(
                success=False,
                message=f"Source {source_id!r} was not created from a Kaggle dataset "
                "(no kaggle_owner/kaggle_ref recorded)",
                code="schema.not_a_kaggle_source",
                params={"source": source_id},
            )
        # REQ-1787: skip the re-download (and the pgwire endpoint teardown it would otherwise
        # force) when Kaggle has nothing newer than what's already on disk -- the staged files'
        # own mtime IS the "last refreshed at" record; no separate timestamp needs to be stored.
        staging_root = Path(row["path"]) if row.get("path") else None
        local_mtime = staged_mtime(staging_root) if staging_root else None
        if local_mtime is not None:
            try:
                remote_last_updated = await get_dataset_last_updated(token, owner, ref)
                remote_dt = datetime.fromisoformat(remote_last_updated.replace("Z", "+00:00"))
                local_dt = datetime.fromtimestamp(local_mtime, tz=timezone.utc)
                if remote_dt <= local_dt:
                    return MutationResult(
                        success=True,
                        message=f"{owner}/{ref} already up to date for source {source_id!r}",
                        code="schema.kaggle_source_already_current",
                        params={"source": source_id},
                    )
            except Exception as _check_err:
                logging.getLogger(__name__).warning(
                    "Kaggle lastUpdated check for %r failed, refreshing unconditionally: %s",
                    source_id,
                    _check_err,
                )
        try:
            await stage_dataset(token, owner, ref)
        except UnsupportedKaggleDataset as exc:
            return MutationResult(
                success=False,
                message=str(exc),
                code="schema.kaggle_refresh_failed",
                params={"source": source_id},
            )
        try:
            stop_endpoint(source_id)
        except Exception as _pgwire_err:
            logging.getLogger(__name__).warning(
                "pgwire endpoint teardown for %r failed during Kaggle refresh: %s",
                source_id,
                _pgwire_err,
            )
        return MutationResult(
            success=True,
            message=f"Refreshed {owner}/{ref} for source {source_id!r}",
            code="schema.kaggle_source_refreshed",
            params={"source": source_id},
        )

    @strawberry.mutation
    async def update_source(
        self, info: StrawberryInfo, input: SourceInput
    ) -> MutationResult:  # REQ-012
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "source_registration")
        from provisa.core.models import Source as SourceModel, SourceType as SourceTypeEnum
        from provisa.core.repositories import source as source_repo

        # REQ-1725: create_source's gate is not enough on its own — update_source takes a fresh
        # type on every call too, so a source created as something else could retype to soda here.
        _soda_refusal = _refuse_soda_on_hosted_plane(input.type)
        if _soda_refusal is not None:
            return _soda_refusal
        _load_mgmt_refusal = _validate_source_load_management(input)  # REQ-1909
        if _load_mgmt_refusal is not None:
            return _load_mgmt_refusal

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            existing = await source_repo.get(_conn, input.id)
            if existing is None:
                return MutationResult(
                    success=False,
                    message=f"Source {input.id!r} not found",
                    code="schema.source_not_found",
                    params={"source": input.id},
                )
            _ttl_refusal = await landing_ttl_refusal(  # REQ-1907
                _conn,
                input.id,
                source=SourceTtl(
                    input.change_signal,
                    input.cache_ttl,
                    input.replicate,
                    input.load_protected,
                ),
            )
            if _ttl_refusal is not None:
                return _ttl_refusal
            from provisa.api.admin._change_feed import source_change_feed_refusal

            _feed_refusal = await source_change_feed_refusal(_conn, input)  # REQ-1861
            if _feed_refusal is not None:
                return _feed_refusal
            # REQ-1695: the literal a person retyped into the form replaces the vault entry under
            # the same name -- a rotation, not a second secret -- and the row keeps the reference.
            password_ref = await persist_source_password(info, input.id, input.password)
            _profiler_refusal = _refuse_invalid_profiler(  # REQ-1934
                input.id, input.type, _parse_mapping_json(input.mapping_json)
            )
            if _profiler_refusal is not None:
                return _profiler_refusal
            model = SourceModel(
                id=input.id,
                type=SourceTypeEnum(input.type),
                host=input.host,
                port=input.port,
                database=input.database,
                username=input.username,
                password=password_ref,
                path=input.path,
                description=input.description,
                mapping=_parse_mapping_json(input.mapping_json),
                federation_hints=_federation_hints_from_input(input),
                change_signal=input.change_signal,
                load_protected=input.load_protected,  # REQ-1141
                off_peak_window=input.off_peak_window,  # REQ-1141
                off_peak_tz=input.off_peak_tz,  # REQ-1141
                cache_enabled=input.cache_enabled,
                cache_ttl=input.cache_ttl,
                replicate=input.replicate,  # REQ-826
                # REQ-1921: kept; the form carries no region, and writing NULL over the stored one
                # changed it without anyone choosing to.
                region=existing["region"],
                max_live_concurrency=input.max_live_concurrency,  # REQ-1909
                sentinel_path=input.sentinel_path,  # REQ-1148
                freshness_gate=input.freshness_gate,  # REQ-860
                cdc=_cdc_model_from_input(input),
                # REQ-1919: the settings the form does not carry are kept as the store holds them;
                # writing a default over one would change it without anyone choosing to.
                **{name: existing[name] for name in source_repo.KEPT_ON_FORM_EDIT},
            )
            if input.allowed_domains is not None:
                from provisa.api.admin.capabilities import require_reach_of_added_domains

                require_reach_of_added_domains(  # REQ-1531: see create_source
                    info,
                    existing["allowed_domains"] or [],
                    input.allowed_domains,
                    empty_is_all=True,
                )
            await source_repo.upsert(_conn, model)
            if input.allowed_domains is not None:
                await conn.execute_core(
                    update(sources)
                    .where(sources.c.id == input.id)
                    .values(allowed_domains=input.allowed_domains)
                )

        # REQ-1690: a pgwire-replica source (files/sharepoint/splunk) keeps its Calcite server
        # running in pgwire_replica._ENDPOINTS, keyed by this same id, for the life of this
        # process — delete_source already stops it (stop_endpoint), but an in-place edit (this
        # mutation) never did, so changing a `files` source's path/mapping in place kept serving
        # whatever schema the FIRST attach saw. Confirmed live: a `files` source's path edited
        # from an empty/wrong directory to a real one still resolved zero tables afterward, same
        # symptom as the delete+recreate bug this session already fixed, just reached by editing
        # instead of deleting. No-ops when no server was ever started for this id.
        from provisa.federation.pgwire_replica import stop_endpoint

        try:
            stop_endpoint(input.id)
        except Exception as _pgwire_err:
            logging.getLogger(__name__).warning(
                "pgwire endpoint teardown for %r failed during update_source: %s",
                input.id,
                _pgwire_err,
            )

        if input.type == "govdata" and input.username:
            import os as _os
            from provisa.core.secrets import resolve_secrets as _rs

            _os.environ["AWS_ACCESS_KEY_ID"] = _rs(input.username)
            if input.password:
                _os.environ["AWS_SECRET_ACCESS_KEY"] = _rs(input.password)
            if input.host:
                _os.environ["AWS_ENDPOINT_OVERRIDE"] = _rs(input.host)

        from provisa.api.app import state
        from provisa.executor.drivers.registry import has_driver
        from provisa.core.secrets import resolve_secrets
        from provisa.core.secrets_store import bound_to_request_org

        if has_driver(input.type):
            await state.source_pools.remove(input.id)
            try:
                # REQ-1695: the password the pool dials with is the persisted REFERENCE, resolved
                # inside the org's vault -- the same value every other reader of this source gets.
                async with bound_to_request_org():
                    await state.source_pools.add(
                        source_id=input.id,
                        source_type=input.type,
                        host=resolve_secrets(input.host) if input.host else "localhost",
                        port=input.port,
                        database=input.database,
                        user=input.username,
                        password=resolve_secrets(password_ref),
                    )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Direct pool for %r failed — the engine-routed queries still work.",
                    input.id,
                )
        state.source_types[input.id] = input.type
        # REQ-1757: see create_source's identical fix above — was hardcoded "" regardless of type.
        from provisa.core.source_registry import SOURCE_TO_DIALECT

        state.source_dialects[input.id] = SOURCE_TO_DIALECT.get(input.type, "")
        if input.allowed_domains is not None:
            state.source_allowed_domains[input.id] = list(input.allowed_domains)

        # Keep catalog name in sync with the (possibly renamed) source config.
        from provisa.api.app_loaders import catalog_name_for_source

        # Source id only — see the same derivation in create_source above for why `input.database`
        # (the remote database/tenant) is not a catalog name, the fixed-catalog exception, and the
        # adapter-fetched-under-Trino exception (REQ-1730).
        state.source_catalogs[input.id] = catalog_name_for_source(state, input.type, input.id)

        # Invalidate and re-index catalog cache (REQ-464)
        from provisa.discovery.catalog_cache import (
            invalidate_source as _invalidate,
            index_source as _index_source,
        )

        async def _reindex():
            await _invalidate(pool, input.id)
            await _index_source(
                input.id,
                pool,
                state.federation_engine,
                state.source_pools,
                state.source_types,
                state,
            )

        # REQ-1882: background worker, not the request's own loop (which cancels leftover tasks
        # when the request ends).
        from provisa.core.connection_loop import spawn_background

        spawn_background(_reindex(), name=f"catalog-reindex:{input.id}")

        if input.type == "data_profiler":  # REQ-1934: a changed schedule reschedules
            from provisa.api.admin.schema_mutation_ops import reschedule_org_triggers

            await reschedule_org_triggers()
        return MutationResult(
            success=True,
            message=f"Source {input.id!r} updated",
            code="schema.source_updated",
            params={"source": input.id},
        )

    @strawberry.mutation
    async def rename_source(self, info: StrawberryInfo, old_id: str, new_id: str) -> MutationResult:
        require_capability(info, "source_registration")
        from provisa.core.repositories import source as source_repo

        if not new_id.strip():
            return MutationResult(
                success=False, message="New ID must not be empty", code="schema.new_id_empty"
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            renamed = await source_repo.rename(cast("Connection", conn), old_id, new_id)
        if renamed:
            return MutationResult(
                success=True,
                message=f"Source renamed {old_id!r} → {new_id!r}",
                code="schema.source_renamed",
                params={"old": old_id, "new": new_id},
            )
        return MutationResult(
            success=False,
            message=f"Source {old_id!r} not found",
            code="schema.source_not_found",
            params={"source": old_id},
        )

    @strawberry.mutation
    async def delete_source(self, info: StrawberryInfo, id: str) -> MutationResult:
        require_capability(info, "source_registration")
        from provisa.core.repositories import source as source_repo
        from provisa.api.app import state

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            # REQ-1695: read the reference before the row goes, so the vault entry it names can be
            # removed with it. A source whose credential outlived it is a credential nothing owns.
            _existing = await source_repo.get(_conn, id)
            try:
                deleted = await source_repo.delete(_conn, id)
            except source_repo.SourceDeleteRefused as refused:
                # REQ-1918: nothing is removed; every dependent is named.
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code=(
                        "schema.source_has_dependents"
                        if refused.reason == "dependents"
                        else "schema.source_is_system"
                    ),
                    params={
                        "source": id,
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if deleted:
            assert _existing is not None  # delete reported a row, so get found one
            await forget_source_password(id, _existing["password_ref"])
            state.graphql_remote_sources.pop(id, None)
            # REQ-1730: was never called here at all — see _drop_source_on_engine's own comment for
            # the exact orphaned-catalog accumulation this closes.
            _drop_source_on_engine(state, id)
            state.source_catalogs.pop(id, None)
            await _rebuild_schemas()
            if _existing["type"] == "data_profiler":  # REQ-1934: its schedule goes with it
                from provisa.api.admin.schema_mutation_ops import reschedule_org_triggers

                await reschedule_org_triggers()
            return MutationResult(
                success=True,
                message=f"Source {id!r} deleted",
                code="schema.source_deleted",
                params={"source": id},
            )
        return MutationResult(
            success=False,
            message=f"Source {id!r} not found",
            code="schema.source_not_found",
            params={"source": id},
        )

    @strawberry.mutation
    async def create_domain(
        self, info: StrawberryInfo, input: DomainInput
    ) -> MutationResult:  # REQ-021, REQ-1531
        # REQ-1531: the domain vocabulary itself is the org_admin's. A member scoped to sales cannot
        # answer their own scoping by minting a domain, so this is gated by the right that owns the
        # org's domains and NOT by domain membership — there is no domain to be a member of yet.
        from provisa.api.admin.capabilities import require_capability
        from provisa.api.metadata_export.refs import RESERVED_KIND_KEYWORDS

        require_capability(info, "org_settings")
        from provisa.core.models import Domain as DomainModel
        from provisa.core.repositories import domain as domain_repo

        # REQ-1385: kind keywords are reserved URI path segments; a domain with one of these
        # names would make its semantic URIs unparseable.
        #
        # REQ-1526: EVERY SEGMENT, not the whole id. A hierarchical id is split on "/" into path
        # segments both by the URI builder and by the serialized directory tree, so "a/tables" plants
        # a domain directory exactly where the kind directory goes even though the id as a whole
        # matches no reserved word. Checking the id entire only ever caught the one-segment case.
        # REQ-1592: "*" already means "every domain" on a role's domain_access and on a glossary
        # term's scope. A domain literally named "*" would collide with both readings — a term
        # scoped to it would read as enterprise-wide, and a role granted it as unlimited.
        from provisa.core.glossary import ENTERPRISE_DOMAIN

        if ENTERPRISE_DOMAIN in input.id.split("/"):
            return MutationResult(
                success=False,
                message=f"Domain id {input.id!r} uses the reserved word {ENTERPRISE_DOMAIN!r}",
                code="schema.domain_reserved_word",
                params={"domain": input.id, "reserved": ENTERPRISE_DOMAIN},
            )
        reserved = [seg for seg in input.id.split("/") if seg in RESERVED_KIND_KEYWORDS]
        if reserved:
            return MutationResult(
                success=False,
                message=f"Domain id {input.id!r} uses the reserved word {reserved[0]!r}",
                code="schema.domain_reserved_word",
                params={"domain": input.id, "reserved": reserved[0]},
            )
        pool = await _get_pool()
        model = DomainModel(
            id=input.id,
            description=input.description,
            steward=input.steward or None,  # REQ-609
            graphql_alias=input.graphql_alias or None,
        )
        async with pool.acquire() as conn:
            await domain_repo.upsert(cast("Connection", conn), model)
        return MutationResult(
            success=True,
            message=f"Domain {input.id!r} created",
            code="schema.domain_created",
            params={"domain": input.id},
        )

    @strawberry.mutation
    async def delete_domain(self, info: StrawberryInfo, id: str) -> MutationResult:  # REQ-1531
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.repositories import domain as domain_repo

        require_capability(info, "org_settings")  # REQ-1531: see create_domain

        pool = await _get_pool()
        async with pool.acquire() as conn:
            try:
                deleted = await domain_repo.delete(cast("Connection", conn), id)
            except domain_repo.DomainDeleteRefused as refused:
                # REQ-1917: a domain is deleted only when nothing refers to it; every
                # dependent is named and nothing is removed.
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code=(
                        "schema.domain_has_dependents"
                        if refused.reason == "dependents"
                        else "schema.domain_is_system"
                    ),
                    params={
                        "domain": id,
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if deleted:
            # The domain's catalog entry and alias must not outlive it in the built schemas.
            await _rebuild_schemas()
            return MutationResult(
                success=True,
                message=f"Domain {id!r} deleted",
                code="schema.domain_deleted",
                params={"domain": id},
            )
        return MutationResult(
            success=False,
            message=f"Domain {id!r} not found",
            code="schema.domain_not_found",
            params={"domain": id},
        )

    @strawberry.mutation
    async def create_data_product(
        self, info: StrawberryInfo, input: DataProductInput
    ) -> MutationResult:  # REQ-1634
        # REQ-1634: creating/deleting a data product is catalog curation, gated on its own
        # write right rather than ORG_SETTINGS — see Capability.DATA_PRODUCT_RW.
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.models import DataProduct as DataProductModel
        from provisa.core.repositories import data_product as data_product_repo
        from provisa.core.repositories import domain as domain_repo

        from provisa.api.admin.capabilities import require_right_in_domains

        require_capability(info, "data_product_rw")

        pool = await _get_pool()
        async with pool.acquire() as conn:
            conn = cast("Connection", conn)
            # REQ-1944: the product's domain, and the one it is moved out of when it exists.
            stored = await data_product_repo.get(conn, input.id)
            require_right_in_domains(
                info,
                "data_product_rw",
                {input.domain_id} | ({stored["domain_id"]} if stored is not None else set()),
            )
            if await domain_repo.get(conn, input.domain_id) is None:
                return MutationResult(
                    success=False,
                    message=f"Domain {input.domain_id!r} not found",
                    code="schema.domain_not_found",
                    params={"domain": input.domain_id},
                )
            model = DataProductModel(
                id=input.id,
                domain_id=input.domain_id,
                name=input.name,
                owner_role=input.owner_role or None,
                team_role=input.team_role or None,
                purpose=input.purpose,
                limitations=input.limitations,
                usage=input.usage,
                version=input.version,
                status=input.status,
                sla=input.sla,
                support=input.support,
                custom_properties=input.custom_properties,
            )
            await data_product_repo.upsert(conn, model)
        return MutationResult(
            success=True,
            message=f"Data product {input.id!r} created",
            code="schema.data_product_created",
            params={"data_product": input.id},
        )

    @strawberry.mutation
    async def delete_data_product(
        self, info: StrawberryInfo, id: str
    ) -> MutationResult:  # REQ-1634
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.repositories import data_product as data_product_repo

        from provisa.api.admin.capabilities import require_right_in_domains

        require_capability(info, "data_product_rw")  # REQ-1634: see create_data_product

        pool = await _get_pool()
        async with pool.acquire() as conn:
            stored = await data_product_repo.get(cast("Connection", conn), id)
            if stored is not None:  # REQ-1944: an absent product is the not-found below
                require_right_in_domains(info, "data_product_rw", {stored["domain_id"]})
            try:
                deleted = await data_product_repo.delete(cast("Connection", conn), id)
            except data_product_repo.DataProductDeleteRefused as refused:
                # REQ-1918: a data product is blocked by its members; each is named.
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code="schema.data_product_has_dependents",
                    params={
                        "data_product": id,
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if deleted:
            return MutationResult(
                success=True,
                message=f"Data product {id!r} deleted",
                code="schema.data_product_deleted",
                params={"data_product": id},
            )
        return MutationResult(
            success=False,
            message=f"Data product {id!r} not found",
            code="schema.data_product_not_found",
            params={"data_product": id},
        )

    @strawberry.mutation
    async def upsert_tag(
        self, info: StrawberryInfo, input: TagInput
    ) -> MutationResult:  # REQ-1373, REQ-1375, REQ-1944
        _require_tag_editor(info)
        from provisa.core.models import (
            DERIVED_TAG_IDS,
            SYSTEM_TAG_IDS,
            TAG_FIELD_POLICIES,
            TAG_OBJECT_TYPES,
            TAG_PARAM_POLICIES,
            TAG_PARAM_SEPARATOR,
            Tag as TagModel,
        )
        from provisa.core.repositories import tag as tag_repo

        # REQ-1467: a registry id is a base id. Accepting "entity:customer" here would define a
        # tag whose id parses as the system `entity` tag carrying a parameter, and every base-id
        # comparison downstream would then resolve the user tag to the intrinsic.
        if TAG_PARAM_SEPARATOR in input.id:
            return MutationResult(
                success=False,
                message=(
                    f"Tag id {input.id!r} may not contain {TAG_PARAM_SEPARATOR!r} — "
                    "the separator introduces a parameter value on an assignment"
                ),
                code="schema.tag_id_has_separator",
                params={"tag": input.id},
            )
        # REQ-1443: a derived tag is code-defined like a system tag, so it is reserved the same way.
        if input.id in SYSTEM_TAG_IDS + DERIVED_TAG_IDS:
            return MutationResult(
                success=False,
                message=f"Tag {input.id!r} is a system tag and cannot be redefined",
                code="schema.tag_system_immutable",
                params={"tag": input.id},
            )
        bad_scopes = [s for s in input.applies_to if s not in TAG_OBJECT_TYPES]
        if bad_scopes or not input.applies_to:
            return MutationResult(
                success=False,
                message=f"applies_to must be a non-empty subset of {list(TAG_OBJECT_TYPES)}",
                code="schema.tag_bad_scope",
                params={"tag": input.id, "scopes": ",".join(bad_scopes)},
            )
        for policy in (input.reason_policy, input.expires_policy):
            if policy not in TAG_FIELD_POLICIES:
                return MutationResult(
                    success=False,
                    message=f"Field policy must be one of {list(TAG_FIELD_POLICIES)}",
                    code="schema.tag_bad_policy",
                    params={"tag": input.id, "policy": policy},
                )
        if input.param_policy not in TAG_PARAM_POLICIES:  # REQ-1467
            return MutationResult(
                success=False,
                message=f"Parameter policy must be one of {list(TAG_PARAM_POLICIES)}",
                code="schema.tag_bad_param_policy",
                params={"tag": input.id, "policy": input.param_policy},
            )
        model = TagModel(
            id=input.id,
            description=input.description,
            applies_to=list(input.applies_to),
            reason_policy=input.reason_policy,
            expires_policy=input.expires_policy,
            param_policy=input.param_policy,
            sensitive=input.sensitive,
        )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            existing = await tag_repo.get(cast("Connection", conn), input.id)
            was_sensitive = bool(existing is not None and existing["sensitive"])
            if input.sensitive != was_sensitive:
                # REQ-1943: setting or clearing the Sensitive data option reveals or hides every
                # column carrying the tag -- in every domain, so the right must reach them all
                # (REQ-1944).
                require_right_in_domains(info, SENSITIVE_DATA, {ALL_DOMAINS})
            elif was_sensitive:
                # REQ-1944: a sensitive tag's definition is maintained by whoever governs
                # sensitive columns across the org, or by a table editor as before.
                _require_sensitive_tag_definer(info)
            else:
                require_capability(info, "table_registration")
            await tag_repo.upsert(cast("Connection", conn), model)
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {input.id!r} saved",
            code="schema.tag_saved",
            params={"tag": input.id},
        )

    @strawberry.mutation
    async def delete_tag(
        self, info: StrawberryInfo, id: str
    ) -> MutationResult:  # REQ-1373, REQ-1375, REQ-1944
        _require_tag_editor(info)
        from provisa.core.models import DERIVED_TAG_IDS, SYSTEM_TAG_IDS, base_tag_id
        from provisa.core.repositories import tag as tag_repo

        # REQ-1467: on the base id, so "entity:customer" is refused as the system tag it names
        # rather than looked up as a user tag, found missing, and reported as not found.
        if base_tag_id(id) in SYSTEM_TAG_IDS + DERIVED_TAG_IDS:
            return MutationResult(
                success=False,
                message=f"Tag {id!r} is a system tag and cannot be deleted",
                code="schema.tag_system_immutable",
                params={"tag": id},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            existing = await tag_repo.get(cast("Connection", conn), id)
            if existing is not None and existing["sensitive"]:
                # REQ-1943: deleting a sensitive tag removes it from every column carrying it,
                # in every domain (REQ-1944).
                require_right_in_domains(info, SENSITIVE_DATA, {ALL_DOMAINS})
            else:
                require_capability(info, "table_registration")
            deleted = await tag_repo.delete(cast("Connection", conn), id)
        if not deleted:
            return MutationResult(
                success=False,
                message=f"Tag {id!r} not found",
                code="schema.tag_not_found",
                params={"tag": id},
            )
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {id!r} deleted",
            code="schema.tag_deleted",
            params={"tag": id},
        )

    @strawberry.mutation
    async def assign_tag(
        self, info: StrawberryInfo, input: TagAssignmentInput
    ) -> MutationResult:  # REQ-1376/1377, REQ-1944
        _require_tag_editor(info)
        from provisa.core.models import TagAssignment as TagAssignmentModel
        from provisa.core.repositories import tag as tag_repo

        model = TagAssignmentModel(
            tag_id=input.tag_id,
            object_type=input.object_type,
            source_id=input.source_id,
            table_id=input.table_id,
            column_name=input.column_name,
            relationship_id=input.relationship_id,
            command_name=input.command_name,
            reason=(input.reason or "").strip() or None,
            expires_on=(input.expires_on or "").strip() or None,
        )
        problem = _assignment_target_problem(model)
        if problem is not None:
            return problem
        if model.expires_on is not None:
            import datetime as _dt

            try:
                _dt.date.fromisoformat(model.expires_on)
            except ValueError:
                return MutationResult(
                    success=False,
                    message=f"expires_on must be an ISO date, got {model.expires_on!r}",
                    code="schema.tag_bad_expires_on",
                    params={"expiresOn": model.expires_on},
                )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            tag_row = await tag_repo.get(cast("Connection", conn), input.tag_id)
            if tag_row is None:
                return MutationResult(
                    success=False,
                    message=f"Tag {input.tag_id!r} not found",
                    code="schema.tag_not_found",
                    params={"tag": input.tag_id},
                )
            await _require_tag_assignment_right(
                info, cast("Connection", conn), tag_row, input.object_type, input.table_id
            )
            # REQ-1443: a derived tag reports state the table already carries, so assigning it
            # would either duplicate that state or contradict it — the registration is the only
            # way to change it.
            if tag_row["derived"]:
                return MutationResult(
                    success=False,
                    message=(
                        f"Tag {input.tag_id!r} is derived from the object's own registration "
                        "and cannot be assigned"
                    ),
                    code="schema.tag_derived_immutable",
                    params={"tag": input.tag_id},
                )
            # REQ-1375: the registry's per-tag field policy governs the assignment fields —
            # a required field refuses absence, a hidden field refuses presence.
            for field_name, value, policy in (
                ("reason", model.reason, tag_row["reason_policy"]),
                ("expires_on", model.expires_on, tag_row["expires_policy"]),
            ):
                if policy == "required" and value is None:
                    return MutationResult(
                        success=False,
                        message=f"Tag {input.tag_id!r} requires {field_name}",
                        code="schema.tag_reason_required"
                        if field_name == "reason"
                        else "schema.tag_expires_required",
                        params={"tag": input.tag_id, "field": field_name},
                    )
                if policy == "hidden" and value is not None:
                    return MutationResult(
                        success=False,
                        message=f"Tag {input.tag_id!r} does not take {field_name}",
                        code="schema.tag_field_hidden",
                        params={"tag": input.tag_id, "field": field_name},
                    )
            # REQ-1467: the parameter is part of the assignment, and the permitted values are a
            # closed maintainer-owned list. An unlisted value is refused rather than stored: a
            # misspelt "entity:custmoer" indexes the column's values under a type nothing
            # queries, and the empty result reads as absence rather than as the typo it is.
            param = model.tag_param()
            if tag_row["param_policy"] == "required":
                if param is None:
                    return MutationResult(
                        success=False,
                        message=(
                            f"Tag {input.tag_id!r} must be assigned with a value, "
                            f"as {input.tag_id}:<value>"
                        ),
                        code="schema.tag_param_required",
                        params={"tag": input.tag_id},
                    )
                permitted = {
                    p["value"]
                    for p in await tag_repo.list_param_values(
                        cast("Connection", conn), input.tag_id
                    )
                }
                if param not in permitted:
                    return MutationResult(
                        success=False,
                        message=(
                            f"{param!r} is not a permitted value for tag "
                            f"{model.base_tag_id()!r} — choose one of {sorted(permitted)} "
                            "or add it to the tag's value list"
                        ),
                        code="schema.tag_param_unknown",
                        params={"tag": model.base_tag_id(), "value": param},
                    )
            elif param is not None:
                return MutationResult(
                    success=False,
                    message=f"Tag {model.base_tag_id()!r} does not take a value",
                    code="schema.tag_param_not_allowed",
                    params={"tag": model.base_tag_id(), "value": param},
                )
            if input.object_type not in list(tag_row["applies_to"] or []):
                return MutationResult(
                    success=False,
                    message=(
                        f"Tag {input.tag_id!r} does not apply to {input.object_type!r} objects"
                    ),
                    code="schema.tag_scope_mismatch",
                    params={"tag": input.tag_id, "objectType": input.object_type},
                )
            await tag_repo.assign(cast("Connection", conn), model)
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {input.tag_id!r} assigned",
            code="schema.tag_assigned",
            params={"tag": input.tag_id, "objectKey": model.object_key()},
        )

    @strawberry.mutation
    async def unassign_tag(
        self, info: StrawberryInfo, input: TagAssignmentInput
    ) -> MutationResult:  # REQ-1377, REQ-1944
        _require_tag_editor(info)
        from provisa.core.models import TagAssignment as TagAssignmentModel
        from provisa.core.repositories import tag as tag_repo

        model = TagAssignmentModel(
            tag_id=input.tag_id,
            object_type=input.object_type,
            source_id=input.source_id,
            table_id=input.table_id,
            column_name=input.column_name,
            relationship_id=input.relationship_id,
            command_name=input.command_name,
        )
        problem = _assignment_target_problem(model)
        if problem is not None:
            return problem
        pool = await _get_pool()
        async with pool.acquire() as conn:
            tag_row = await tag_repo.get(cast("Connection", conn), input.tag_id)
            if tag_row is None:
                require_capability(info, "table_registration")
            else:
                # REQ-1943: removing a sensitive tag from a column stops it being sensitive.
                await _require_tag_assignment_right(
                    info, cast("Connection", conn), tag_row, input.object_type, input.table_id
                )
            removed = await tag_repo.unassign(
                cast("Connection", conn), input.tag_id, model.object_key()
            )
        if not removed:
            return MutationResult(
                success=False,
                message=f"Tag {input.tag_id!r} is not assigned to that object",
                code="schema.tag_assignment_not_found",
                params={"tag": input.tag_id, "objectKey": model.object_key()},
            )
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {input.tag_id!r} unassigned",
            code="schema.tag_unassigned",
            params={"tag": input.tag_id, "objectKey": model.object_key()},
        )

    @strawberry.mutation
    async def upsert_tag_param_value(
        self, info: StrawberryInfo, input: TagParamValueInput
    ) -> MutationResult:  # REQ-1467
        """Add or re-describe a permitted parameter value for a parameterized tag.

        The value list is data, not definition — it is editable on a system tag, whose definition
        is not. An org that trades in vessels adds ``entity:vessel`` here; nothing in code has to
        know the word.
        """
        require_capability(info, "table_registration")
        from provisa.core.models import TAG_PARAM_SEPARATOR, TagParamValue
        from provisa.core.repositories import tag as tag_repo

        value = input.value.strip()
        if not value or TAG_PARAM_SEPARATOR in value:
            return MutationResult(
                success=False,
                message=(
                    f"Parameter value {input.value!r} must be non-empty and may not contain "
                    f"{TAG_PARAM_SEPARATOR!r}"
                ),
                code="schema.tag_param_value_invalid",
                params={"tag": input.tag_id, "value": input.value},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            tag_row = await tag_repo.get(cast("Connection", conn), input.tag_id)
            if tag_row is None:
                return MutationResult(
                    success=False,
                    message=f"Tag {input.tag_id!r} not found",
                    code="schema.tag_not_found",
                    params={"tag": input.tag_id},
                )
            if tag_row["param_policy"] == "none":
                return MutationResult(
                    success=False,
                    message=f"Tag {input.tag_id!r} does not take values",
                    code="schema.tag_param_not_allowed",
                    params={"tag": input.tag_id, "value": value},
                )
            await tag_repo.upsert_param_value(
                cast("Connection", conn),
                TagParamValue(
                    tag_id=input.tag_id, value=value, description=input.description.strip()
                ),
            )
        return MutationResult(
            success=True,
            message=f"Value {value!r} saved for tag {input.tag_id!r}",
            code="schema.tag_param_value_saved",
            params={"tag": input.tag_id, "value": value},
        )

    @strawberry.mutation
    async def delete_tag_param_value(
        self, info: StrawberryInfo, tag_id: str, value: str
    ) -> MutationResult:  # REQ-1467
        """Remove a permitted value, refusing while any assignment still carries it.

        Deleting a value in use would leave those assignments naming a type the list no longer
        admits — legal in the database, unreachable from the picker, and silently unfixable.
        """
        require_capability(info, "table_registration")
        from provisa.core.repositories import tag as tag_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            in_use = await tag_repo.param_value_assignment_count(
                cast("Connection", conn), tag_id, value
            )
            if in_use:
                return MutationResult(
                    success=False,
                    message=(
                        f"Value {value!r} is carried by {in_use} assignment(s) — "
                        "unassign them first"
                    ),
                    code="schema.tag_param_value_in_use",
                    params={"tag": tag_id, "value": value, "count": str(in_use)},
                )
            deleted = await tag_repo.delete_param_value(cast("Connection", conn), tag_id, value)
        if not deleted:
            return MutationResult(
                success=False,
                message=f"Tag {tag_id!r} has no value {value!r}",
                code="schema.tag_param_value_not_found",
                params={"tag": tag_id, "value": value},
            )
        return MutationResult(
            success=True,
            message=f"Value {value!r} removed from tag {tag_id!r}",
            code="schema.tag_param_value_deleted",
            params={"tag": tag_id, "value": value},
        )

    @strawberry.mutation
    async def create_role(
        self, info: StrawberryInfo, input: RoleInput
    ) -> MutationResult:  # REQ-042, REQ-059, REQ-060, REQ-215, REQ-1531
        # REQ-1531: a role carries both capabilities and domain_access (REQ-1530), so minting one is
        # how a member would widen their own scope. It is the org_admin's act, gated by the right
        # that administers members.
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.models import Role as RoleModel

        require_capability(info, "user_management")
        from provisa.core.models import RoleRateLimit
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        # REQ-1174: carry the per-role rate + query-complexity limits through to the model/DB.
        rl = input.rate_limit
        rate_limit = (
            RoleRateLimit(
                requests_per_second=rl.requests_per_second,
                max_query_complexity=rl.max_query_complexity,
                max_query_time_ms=rl.max_query_time_ms,
            )
            if rl is not None
            else None
        )
        # REQ-1677: the parent must exist, be another role, and not close a cycle.
        from provisa.security.inheritance import parent_map, parent_problem

        parent_id = input.parent_role_id or None
        async with pool.acquire() as conn:
            existing = await role_repo.list_all(cast("Connection", conn))
        problem = parent_problem(input.id, parent_id, parent_map(existing))
        if problem is not None:
            return MutationResult(
                success=False,
                message=problem,
                code="schema.role_parent_invalid",
                params={"role": input.id, "parent": parent_id, "reason": problem},
            )
        # REQ-042/REQ-1337: the same two refusals the REST surface makes — an unknown capability,
        # and a platform right (the role's own or its parent chain's) defined by a caller who is
        # not a platform administrator.
        from provisa.api.admin._platform_guard import role_definition_problem
        from provisa.security.inheritance import effective_capabilities, effective_domain_access

        inherited = effective_capabilities(parent_id, existing) if parent_id is not None else []
        inherited_domains = (
            effective_domain_access(parent_id, existing) if parent_id is not None else []
        )
        definition_problem = role_definition_problem(
            info.context["request"],
            input.capabilities,
            inherited,
            role_id=input.id,
            domain_access=input.domain_access,
            inherited_domain_access=inherited_domains,
        )
        if definition_problem is not None:
            return MutationResult(
                success=False,
                message=str(definition_problem.detail),
                code=definition_problem.code,
                params={"role": input.id, **definition_problem.params},
            )
        # REQ-1531: a role hands out reach; whoever defines it must hold every domain the
        # definition adds to what the role reaches — listed or inherited.
        from provisa.api.admin.capabilities import require_reach_of_added_domains

        held = next((r for r in existing if r["id"] == input.id), None)
        require_reach_of_added_domains(
            info,
            None if held is None else effective_domain_access(input.id, existing),
            [*input.domain_access, *inherited_domains],
        )
        model = RoleModel(
            id=input.id,
            capabilities=input.capabilities,
            domain_access=input.domain_access,
            rate_limit=rate_limit,
            parent_role_id=parent_id,
            # REQ-1919: what the form does not carry is kept as the store holds it.
            **({} if held is None else {name: held[name] for name in role_repo.KEPT_ON_FORM_EDIT}),
        )
        async with pool.acquire() as conn:
            await role_repo.upsert(
                cast("Connection", conn),
                model,
                org_id=_resolve_admin_context(info),
            )
        # A new role has no state.schemas[role_id]/state.contexts[role_id] until some rebuild
        # runs; without this, the role is unusable until an unrelated mutation happens to trigger
        # one, and any prepared-plan cache keyed on schema_version would never see this role exist.
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Role {input.id!r} created",
            code="schema.role_created",
            params={"role": input.id},
        )

    @strawberry.mutation
    async def register_table(
        self, info: StrawberryInfo, input: TableInput
    ) -> MutationResult:  # REQ-013, REQ-016, REQ-252, REQ-366, REQ-413, REQ-432, REQ-433, REQ-434
        require_capability(info, "table_registration", domain_id=input.domain_id)
        return await _ops.register_table(info, input)

    @strawberry.mutation
    async def register_entity(self, info: StrawberryInfo, input: "EntityInput") -> MutationResult:
        """REQ-1164: entity sugar → lower to a (bitemporal, when historized) MV and register it."""
        require_capability(info, "table_registration")
        from provisa.api.admin.modeling_register import entity_table_input

        return await _ops.register_table(info, entity_table_input(input))

    @strawberry.mutation
    async def register_fact(self, info: StrawberryInfo, input: "FactInput") -> MutationResult:
        """REQ-1164: fact sugar → lower to an aggregate MV + dimension relationships and register."""
        from provisa.api.admin.modeling_register import fact_table_input

        from provisa.core.repositories import metric as metric_repo

        ti, rels, fact_metrics = fact_table_input(input)
        res = await _ops.register_table(info, ti)
        if not res.success:
            return res
        # Relationships resolve tables by their VIRTUAL name (alias when set, else
        # table_name — find_by_table_name). Registration may auto-alias under the org
        # naming convention (dim_pet → dimPet), so resolve each side before linking.
        pool = await _get_pool()

        async def _virtual_name(name: str) -> str:
            async with pool.acquire() as conn:
                row = (
                    await conn.execute_core(
                        select(registered_tables.c.alias).where(
                            registered_tables.c.table_name == name
                        )
                    )
                ).fetchone()
            if row is None:
                raise ValueError(f"table {name!r} is not registered")
            return row.alias or name

        for rel in rels:
            rel.source_table_id = await _virtual_name(rel.source_table_id)
            rel.target_table_id = await _virtual_name(rel.target_table_id)
            rr = await _upsert_relationship_impl(info, rel)
            if not rr.success:
                return rr
        # REQ-1320: each fact measure auto-registers as a governed metric (upsert by name).
        async with pool.acquire() as conn:
            for m in fact_metrics:
                await metric_repo.upsert(cast("Connection", conn), m)
        if fact_metrics:
            await _rebuild_schemas()  # republish state.metrics + schema metric blocks
        return MutationResult(
            success=True,
            message=(
                f"Fact {input.name!r} registered with {len(rels)} dimension link(s) "
                f"and {len(fact_metrics)} metric(s)"
            ),
            code="schema.fact_registered",
            params={"fact": input.name, "links": len(rels), "metrics": len(fact_metrics)},
        )

    @strawberry.mutation
    async def upsert_metric(
        self, info: StrawberryInfo, input: MetricInput
    ) -> MutationResult:  # REQ-1317
        """Create or replace a governed metric definition (REQ-1317). The expression must parse
        under sqlglot and contain at least one aggregate function — hard error otherwise."""
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.models import Metric as MetricModel
        from provisa.core.repositories import metric as metric_repo

        require_capability(info, "table_registration")
        try:
            model = MetricModel(
                name=input.name,
                expression=input.expression,
                datatype=input.datatype,
                description=input.description,
                ai_context=input.ai_context,
                visible_to=list(input.visible_to),
            )
        except ValueError as e:  # pydantic name validation (snake_case)
            return MutationResult(success=False, message=str(e))
        pool = await _get_pool()
        try:
            async with pool.acquire() as conn:
                await metric_repo.upsert(cast("Connection", conn), model)
                # REQ-1318: every registered view whose view_metrics spec references this
                # metric regenerates its stored view_sql against the UPDATED definition.
                # Free-hand view_sql born from inline metric() calls carries no stored
                # provenance and is not regenerated (config-path views regenerate on reload).
                from provisa.api.admin._metric_views import regenerate_metric_views

                regenerated = await regenerate_metric_views(cast("Connection", conn), input.name)
        except ValueError as e:  # REQ-1317/1318: invalid expression / spec no longer compiles
            return MutationResult(success=False, message=str(e))
        # Always rebuild: state.metrics (raw-SQL `metrics.<name>` expansion) and the
        # per-role schema metric blocks republish from the DB registry on rebuild.
        await _rebuild_schemas()
        if regenerated:
            return MutationResult(
                success=True,
                message=(
                    f"Metric {input.name!r} saved; regenerated view(s): "
                    + ", ".join(sorted(regenerated))
                ),
                code="schema.metric_saved_regenerated",
                params={"metric": input.name, "views": ", ".join(sorted(regenerated))},
            )
        return MutationResult(
            success=True,
            message=f"Metric {input.name!r} saved",
            code="schema.metric_saved",
            params={"metric": input.name},
        )

    @strawberry.mutation
    async def delete_metric(self, info: StrawberryInfo, name: str) -> MutationResult:  # REQ-1317
        """Delete a governed metric definition by name (REQ-1317)."""
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.repositories import metric as metric_repo

        require_capability(info, "table_registration")
        from provisa.api.admin.domain_guard import metric_domains, require_domains

        pool = await _get_pool()
        async with pool.acquire() as conn:
            # REQ-1531: a metric is an object of every domain its expression reads from.
            domains = await metric_domains(cast("Connection", conn), name)
            if domains is not None:
                require_domains(info, domains)
            try:
                deleted = await metric_repo.delete(cast("Connection", conn), name)
            except metric_repo.MetricDeleteRefused as refused:
                # REQ-1918: a view uses it; nothing is removed, each is named.
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code="schema.metric_has_dependents",
                    params={
                        "metric": name,
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if deleted:
            await _rebuild_schemas()  # republish state.metrics + schema metric blocks
            return MutationResult(
                success=True,
                message=f"Metric {name!r} deleted",
                code="schema.metric_deleted",
                params={"metric": name},
            )
        return MutationResult(
            success=False,
            message=f"Metric {name!r} not found",
            code="schema.metric_not_found",
            params={"metric": name},
        )

    @strawberry.mutation
    async def update_table(
        self, info: StrawberryInfo, input: TableInput
    ) -> MutationResult:  # REQ-016, REQ-020, REQ-155, REQ-156
        """Update an existing table's alias, description, and column metadata."""
        from provisa.api.admin._hiding_guard import require_table_save

        _editor = await require_table_save(info, input)  # REQ-1944
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        columns, _col_err = await _build_columns_for_input(pool, input)
        if _col_err is not None:
            return _col_err
        from provisa.core.models import ColumnPreset as ColumnPresetModel

        presets = [
            ColumnPresetModel(
                column=cp.column,
                source=cp.source,
                name=cp.name,
                value=cp.value,
                data_type=cp.data_type,
            )
            for cp in input.column_presets
        ]
        # REQ-957/964: reject a non-deterministic / unsafe preprocess hook at registration.
        from provisa.mv.preprocess import validate_preprocess

        try:
            validate_preprocess(input.mv_preprocess)
        except ValueError as _pp_err:
            return MutationResult(success=False, message=str(_pp_err))
        model = _table_model_from_input(input, columns, presets, input.alias)
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            # REQ-1443: same contract-driven derivation the register path and the YAML loader run.
            from provisa.api.admin._dq_registration import apply_dq_registration

            try:
                await apply_dq_registration(_conn, model)
                # REQ-1934: a profile result table's derivation, and a table's profiler membership.
                from provisa.profiler.registration import apply_registration

                await apply_registration(_conn, model)
            except ValueError as _dq_err:
                return MutationResult(success=False, message=str(_dq_err))
            from provisa.api.admin.region_defaults import kept_region

            model.region = await kept_region(_conn, model)  # REQ-1921
            _conflict = await _domain_table_conflict(
                _conn,
                model.domain_id,
                model.table_name,
                model.source_id,
                model.schema_name,
                input.alias,
            )
            if _conflict:
                return MutationResult(success=False, message=_conflict)
            _owner_conflict = await _dataset_ownership_conflict(
                _conn, model.source_id, model.table_name, model.domain_id
            )
            if _owner_conflict:
                return MutationResult(success=False, message=_owner_conflict)
            # REQ-1907/REQ-318: TableInput carries no cache_ttl / role_ttl / row_materialize /
            # pagination -- they are saved through updateTableCache / updateTableRoleTtl /
            # updateTablePaging -- so the upsert keeps the stored values instead of resetting them.
            _kept = await _conn.execute_core(
                select(
                    registered_tables.c.cache_ttl,
                    registered_tables.c.role_ttl,
                    registered_tables.c.row_materialize,
                    registered_tables.c.replicate,
                    registered_tables.c.pagination,
                ).where(
                    (registered_tables.c.source_id == model.source_id)
                    & (registered_tables.c.schema_name == model.schema_name)
                    & (registered_tables.c.table_name == model.table_name)
                )
            )
            _kept_row = _kept.fetchone()
            if _kept_row is not None:
                model.cache_ttl = _kept_row.cache_ttl
                model.role_ttl = dict(_kept_row.role_ttl)
                model.pagination = stored_paging(_kept_row.pagination)
                model.row_materialize = bool(_kept_row.row_materialize)
                # replicate is saved through updateTableReplicate, never by this upsert: the
                # stored value is kept (and is the one the checks below judge).
                model.replicate = _kept_row.replicate
            _ttl_refusal = await landing_ttl_refusal(
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
                return _ttl_refusal
            from provisa.api.admin._change_feed import table_change_feed_refusal

            _feed_refusal = await table_change_feed_refusal(  # REQ-1861
                _conn, model.source_id, model.change_signal
            )
            if _feed_refusal is not None:
                return _feed_refusal
            from provisa.api.admin._file_glob import table_file_glob_refusal

            _glob_refusal = await table_file_glob_refusal(_conn, model)  # REQ-788
            if _glob_refusal is not None:
                return _glob_refusal
            from provisa.api.admin._fake_guard import table_fake_refusal

            _fake_refusal = await table_fake_refusal(_conn, model)  # REQ-1494
            if _fake_refusal is not None:
                return _fake_refusal
            from provisa.api.admin._delta_guard import table_delta_refusal

            _delta_refusal = table_delta_refusal(model)  # REQ-874
            if _delta_refusal is not None:
                return _delta_refusal
            from provisa.api.admin._hiding_guard import table_hiding_refusal

            _hiding_refusal = await table_hiding_refusal(  # REQ-1943, REQ-1944
                info, _conn, model, editor=_editor
            )
            if _hiding_refusal is not None:
                return _hiding_refusal
            try:
                model = await table_repo.keep_unedited(_conn, model)  # REQ-1919
                table_id = await table_repo.upsert(_conn, model)
            except table_repo.ViewLoopRefused as _loop:
                # REQ-1918: a view that would read itself through other views is refused at save.
                return MutationResult(
                    success=False,
                    message=str(_loop),
                    code="schema.view_reads_itself",
                    params={"view": _loop.loop[0], "loop": _loop.loop},
                )
            except table_repo.ColumnDropRefused as _drop:
                # REQ-1918: a column something still refers to is not dropped; each is named.
                return MutationResult(
                    success=False,
                    message=str(_drop),
                    code="schema.column_has_dependents",
                    params={"table": _drop.table_name, "columns": _drop.report()},
                )
            if model.query_template:
                # REQ-1670/REQ-1683: an edited query re-persists the endpoint the table serves from.
                from provisa.api.admin._query_api_registration import persist_query_api_registration

                _neo_err = await persist_query_api_registration(_conn, model)
                if _neo_err is not None:
                    return _neo_err
            # REQ-316/REQ-318: an OpenAPI table's endpoint row follows its registration.
            from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
            from provisa.api.app import state as _app_state

            _oa_err = await persist_openapi_endpoint(_app_state, _conn, model)
            if _oa_err is not None:
                return _oa_err
            if table_id is not None:
                await _conn.execute_core(
                    update(registered_tables)
                    .where(registered_tables.c.id == table_id)
                    .values(
                        enable_aggregates=input.enable_aggregates,
                        enable_group_by=input.enable_group_by,
                    )
                )
            # REQ-020: a column change may invalidate a relationship's join field — flag
            # any relationship whose join column on this table is no longer present.
            from provisa.core.repositories import relationship as _rel_repo

            if table_id is not None:
                await _rel_repo.mark_relationships_for_review(
                    _conn, table_id, [c.name for c in model.columns]
                )
        if input.view_sql and input.materialize:
            try:
                await _sync_view_mv(
                    input.table_name,
                    input.view_sql,
                    input.mv_refresh_interval,
                    input.change_signal,
                    debounce_quiet=input.mv_debounce_quiet,  # REQ-963
                    debounce_max_delay=input.mv_debounce_max_delay,  # REQ-963
                    consistency=input.mv_consistency,  # REQ-879
                    preprocess=input.mv_preprocess,  # REQ-957
                    bitemporal_mode=input.mv_bitemporal_mode,  # REQ-1162
                    bitemporal_key=list(input.mv_bitemporal_key),  # REQ-1162
                    persist=input.mv_persist,  # REQ-965
                    primary_key=list(input.mv_primary_key),  # REQ-970
                    incremental=input.mv_incremental,  # REQ-969
                    calendar=input.mv_calendar,  # REQ-962
                    grain=input.mv_grain,  # REQ-962/1168
                    allowed_lateness=input.mv_allowed_lateness,  # REQ-961
                    expected_events=input.mv_expected_events,  # REQ-961
                    business_day_grain=input.mv_business_day_grain,  # REQ-962
                )
            except ValueError as _det_err:  # REQ-964: reject non-deterministic MV SQL
                return MutationResult(success=False, message=str(_det_err))
        elif not input.materialize:
            _remove_view_mv(input.table_name)
        await _rebuild_schemas()
        # REQ-1742: graphql_remote_router.py's/grpc_remote_router.py's own one-shot registration
        # flows call reconcile_landed_tables() right after _rebuild_schemas() — the pass that
        # actually creates a replicated table's replica in the store (REQ-846/932). update_table
        # (this mutation) never did, so a replica that didn't converge for any reason at
        # registration time (e.g. a transient failure, or state not yet fully committed) had no
        # other path to ever catch up — every later grant/edit through this mutation left it
        # permanently stuck. Idempotent (a matching replica is kept), so calling it
        # unconditionally here is safe for every table type.
        try:
            from provisa.api.app import state as _state

            await _state.federation_engine.reconcile_landed_tables()
        except Exception:
            logging.getLogger(__name__).warning(
                "Landed-table reconcile failed after update_table", exc_info=True
            )
        # Materialize + wire a (re)materialized view immediately — FRESH now, not STALE-until-restart.
        if input.view_sql and input.materialize:
            from provisa.api.admin.schema_common import activate_view_mv

            await activate_view_mv(input.table_name)
        return MutationResult(
            success=True,
            message=f"Table {input.table_name!r} updated (id={table_id})",
            code="schema.table_updated",
            params={"table": input.table_name, "id": table_id},
        )

    @strawberry.mutation
    async def delete_table(self, info: StrawberryInfo, id: int) -> MutationResult:  # REQ-1531
        # REQ-1531: deleting a registration is a change to the domain that holds it, so it asks the
        # same question register/update ask. The id names the object, so the domain is looked up.
        require_capability(info, "table_registration")
        from provisa.api.admin.domain_guard import DomainLookupError, require_table_domain
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            try:
                await require_table_domain(info, cast("Connection", conn), id)
            except DomainLookupError:
                return MutationResult(
                    success=False,
                    message=f"Table {id} not found",
                    code="schema.table_not_found",
                    params={"table": id},
                )
            try:
                deleted = await table_repo.delete(cast("Connection", conn), id)
            except table_repo.TableDeleteRefused as refused:
                # REQ-1918: nothing is removed; every dependent is named.
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code="schema.table_has_dependents",
                    params={
                        "table": id,
                        "name": refused.name,
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if deleted:
            # Data kept for the table outside the control plane. What exists today is removed
            # here: the response-cache entries indexed under it and its hot-tier rows. Its
            # replica, row-level rows and a view's storage relation are not removed yet — that
            # waits for the replica state's own removal path.
            from provisa.api.app import state
            from provisa.cache.tenancy import invalidate_tables

            await invalidate_tables(state, [id])
            if state.hot_manager is not None:
                await state.hot_manager.invalidate(id)
            await _rebuild_schemas()
            return MutationResult(
                success=True,
                message=f"Table {id} deleted",
                code="schema.table_deleted",
                params={"table": id},
            )
        return MutationResult(
            success=False,
            message=f"Table {id} not found",
            code="schema.table_not_found",
            params={"table": id},
        )

    @strawberry.mutation
    async def delete_role(self, info: StrawberryInfo, id: str) -> MutationResult:  # REQ-1531
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.repositories import role as role_repo

        require_capability(info, "user_management")  # REQ-1531: see create_role
        pool = await _get_pool()
        async with pool.acquire() as conn:
            try:
                deleted = await role_repo.delete(cast("Connection", conn), id)
            except role_repo.RoleDeleteRefused as refused:
                # REQ-1918: refused while anything depends on the role, naming each dependent;
                # and for a role the deployment defines.
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code=(
                        "schema.role_has_dependents"
                        if refused.reason == "dependents"
                        else "schema.role_is_system"
                    ),
                    params={
                        "role": id,
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if deleted:
            # state.contexts/schemas[role_id] must not survive a deleted role — see create_role's
            # matching rebuild for why this can't wait for an unrelated mutation.
            await _rebuild_schemas()
            return MutationResult(
                success=True,
                message=f"Role {id!r} deleted",
                code="schema.role_deleted",
                params={"role": id},
            )
        return MutationResult(
            success=False,
            message=f"Role {id!r} not found",
            code="schema.role_not_found",
            params={"role": id},
        )

    @strawberry.mutation
    async def revoke_role_from_table(
        self, info: StrawberryInfo, role_id: str, table_id: int
    ) -> MutationResult:  # REQ-1918
        """Take a role off every column grant of one table (read, write, unmasked), so the role
        can be deleted. A role the table does not grant is a success that changed nothing."""
        from provisa.api.admin.capabilities import require_capability
        from provisa.api.admin.domain_guard import require_table_domain
        from provisa.core.repositories import grants

        require_capability(info, "table_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            c = cast("Connection", conn)
            held = (
                await c.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.id == table_id)
                )
            ).fetchone()
            if held is None:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_id_not_found",
                    params={"id": table_id},
                )
            await require_table_domain(info, c, table_id)
            changed = await grants.revoke_from_table(c, table_id, role_id)
        if changed:
            await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Role {role_id!r} removed from the grants of table {table_id}",
            code="schema.role_revoked_from_table",
            params={"role": role_id, "id": table_id},
        )

    @strawberry.mutation
    async def revoke_role_from_object(
        self, info: StrawberryInfo, role_id: str, kind: GrantKind, name: str
    ) -> MutationResult:  # REQ-1918
        """Take a role off a metric's, command's or webhook's assigned roles, so the role can be
        deleted. A role the object does not grant is a success that changed nothing."""
        from provisa.api.admin.capabilities import require_capability
        from provisa.api.admin.domain_guard import metric_domains, require_domains
        from provisa.core.repositories import grants
        from provisa.core.schema_org import metrics, tracked_functions, tracked_webhooks

        require_capability(info, "table_registration")
        table = {
            GrantKind.METRIC: metrics,
            GrantKind.COMMAND: tracked_functions,
            GrantKind.WEBHOOK: tracked_webhooks,
        }[kind]
        object_kind = kind.value
        pool = await _get_pool()
        async with pool.acquire() as conn:
            c = cast("Connection", conn)
            row = (
                await c.execute_core(
                    select(
                        *[
                            table.c.name,
                            *([table.c.domain_id] if kind != GrantKind.METRIC else []),
                        ]
                    ).where(table.c.name == name)
                )
            ).fetchone()
            if row is None:
                return MutationResult(
                    success=False,
                    message=f"No {object_kind} named {name!r}",
                    code="schema.grant_object_not_found",
                    params={"kind": object_kind, "name": name},
                )
            if kind == GrantKind.METRIC:
                domains = await metric_domains(c, name)
                if domains is not None:
                    require_domains(info, domains)
            else:
                require_domains(info, [row.domain_id])
            changed = await grants.revoke_from_object(c, object_kind, name, role_id)
        if changed:
            await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Role {role_id!r} removed from {object_kind} {name!r}",
            code="schema.role_revoked_from_object",
            params={"role": role_id, "kind": object_kind, "name": name},
        )

    @strawberry.mutation
    async def upsert_rls_rule(
        self, info: StrawberryInfo, input: RLSRuleInput
    ) -> MutationResult:  # REQ-041, REQ-402, REQ-1531, REQ-1676
        # REQ-1531: an RLS rule decides who sees which rows of a domain's tables. Writing one is the
        # masking surface, and it lands in a domain — named directly for a domain-level rule, or the
        # table's own for a table-level one.
        from provisa.api.admin.capabilities import require_capability
        from provisa.api.admin.domain_guard import table_domain_by_name
        from provisa.core.models import RLSRule as RLSRuleModel

        require_capability(info, "masking_config")
        pool = await _get_pool()
        if input.action_name:  # REQ-1679: the target is a tracked function or webhook
            return await _upsert_action_rls_rule(info, input)
        if input.domain_id:
            require_right_in_domains(info, "masking_config", {input.domain_id})
        elif input.table_id:
            async with pool.acquire() as _gconn:
                _dom = await table_domain_by_name(cast("Connection", _gconn), input.table_id)
            if _dom is None:
                return MutationResult(
                    success=False,
                    message=f"Table not registered: {input.table_id}",
                    code="schema.table_not_found",
                    params={"table": input.table_id},
                )
            require_right_in_domains(info, "masking_config", {_dom})
        # REQ-1676: the predicate is parsed and resolved against the model here, at save, so a
        # rule the administrator cannot query with is refused with the reason instead of failing
        # closed for the role at its first query.
        from provisa.compiler.rls_validate import validate_rls_predicate
        from provisa.core.repositories import table as table_repo

        async with pool.acquire() as _vconn:
            registry = await table_repo.list_all(cast("Connection", _vconn))
        if input.domain_id:
            targets = [t for t in registry if t["domain_id"] == input.domain_id]
        else:
            targets = [t for t in registry if (t.get("alias") or t["table_name"]) == input.table_id]
        problem = validate_rls_predicate(input.filter_expr, targets, registry)
        if problem is not None:
            return MutationResult(
                success=False,
                message=problem,
                code="schema.rls_rule_invalid",
                params={"reason": problem},
            )
        model = RLSRuleModel(
            table_id=input.table_id or None,
            domain_id=input.domain_id or None,
            role_id=input.role_id,
            filter=input.filter_expr,
        )
        try:
            async with pool.acquire() as conn:
                await rls_repo.upsert(cast("Connection", conn), model)
        except ValueError as e:
            return MutationResult(success=False, message=str(e))
        # state.rls_contexts[role_id] is only ever populated by _rebuild_schemas's own
        # build_rls_context call — without this, a saved rule has no effect until an unrelated
        # mutation happens to trigger a rebuild first.
        await _rebuild_schemas()
        target = f"domain {input.domain_id!r}" if input.domain_id else f"table {input.table_id!r}"
        return MutationResult(
            success=True,
            message=f"RLS rule for {target} / role {input.role_id!r} saved",
            code="schema.rls_rule_saved_domain"
            if input.domain_id
            else "schema.rls_rule_saved_table",
            params=(
                {"domain": input.domain_id, "role": input.role_id}
                if input.domain_id
                else {"table": input.table_id, "role": input.role_id}
            ),
        )

    @strawberry.mutation
    async def delete_rls_rule(
        self,
        info: StrawberryInfo,
        role_id: str,
        table_id: Optional[int] = None,
        domain_id: Optional[str] = None,
        action_name: Optional[str] = None,
    ) -> MutationResult:  # REQ-1531, REQ-1679
        from provisa.api.admin.capabilities import require_capability
        from provisa.api.admin.domain_guard import table_domain

        require_capability(info, "masking_config")  # REQ-1531: see upsert_rls_rule
        pool = await _get_pool()
        async with pool.acquire() as conn:
            if action_name:  # REQ-1679: gated on the action's domain
                from provisa.api.app import state

                action = state.tracked_functions.get(action_name) or (
                    getattr(state, "tracked_webhooks", None) or {}
                ).get(action_name)
                if action is not None and action.get("domain_id"):
                    require_right_in_domains(info, "masking_config", {action["domain_id"]})
            elif domain_id:
                require_right_in_domains(info, "masking_config", {domain_id})
            elif table_id is not None:
                require_right_in_domains(
                    info, "masking_config", {await table_domain(cast("Connection", conn), table_id)}
                )
            deleted = await rls_repo.delete(
                cast("Connection", conn),
                role_id,
                table_id=table_id,
                domain_id=domain_id,
                action_name=action_name,
            )
        if deleted:
            # See upsert_rls_rule's matching rebuild — a deleted rule must stop being enforced
            # immediately, not wait for an unrelated mutation to refresh state.rls_contexts.
            await _rebuild_schemas()
            return MutationResult(
                success=True, message="RLS rule deleted", code="schema.rls_rule_deleted"
            )
        return MutationResult(
            success=False, message="RLS rule not found", code="schema.rls_rule_not_found"
        )

    @strawberry.mutation
    async def execute_creation_request(  # REQ-434, REQ-063
        self, info: StrawberryInfo, request_id: int
    ) -> MutationResult:
        """REQ-434: a rights-holder executes a queued creation request."""
        from provisa.api.admin.capabilities import _identity_from_info, require_capability
        from provisa.core.repositories import creation_request as cr_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            req = await cr_repo.get(cast("Connection", conn), request_id)
        if req is None or req["status"] != "pending":
            return MutationResult(
                success=False,
                message="Request not found or already resolved",
                code="schema.request_not_pending",
            )
        try:
            require_capability(info, req["capability"])
        except PermissionError as e:
            return MutationResult(success=False, message=str(e))

        if req["request_type"] == "relationship":
            # Same strawberry-decorator signature limitation as above.
            result = await self.upsert_relationship(  # pyright: ignore[reportCallIssue]
                info,
                _rebuild_relationship_input(req["payload"]),  # pyright: ignore[reportCallIssue]
            )
        elif req["request_type"] in ("view", "table"):  # REQ-1792: "table" is the MCP-proposal kind
            result = await self.register_table(info, _rebuild_table_input(req["payload"]))  # pyright: ignore[reportCallIssue]
        elif req["request_type"] == "source":  # REQ-1792
            result = await self.create_source(info, _rebuild_source_input(req["payload"]))  # pyright: ignore[reportCallIssue]
        elif req["request_type"] == "webhook":
            # REQ-209: approving a webhook only requires marking this request executed (done
            # below) — the schema-build gate then exposes the webhook whose latest request is
            # executed. Verify the webhook still exists, then rebuild.
            wh_name = req["payload"]["name"]
            async with pool.acquire() as conn:
                _ex = await conn.execute_core(
                    select(tracked_webhooks.c.id).where(tracked_webhooks.c.name == wh_name)
                )
                exists = _ex.scalar()
            if not exists:
                return MutationResult(
                    success=False,
                    message=f"Webhook {wh_name!r} not found",
                    code="schema.webhook_not_found",
                    params={"webhook": wh_name},
                )
            from provisa.api.app import _rebuild_schemas

            await _rebuild_schemas()
            result = MutationResult(
                success=True,
                message=f"Approved webhook {wh_name!r}",
                code="schema.webhook_approved",
                params={"webhook": wh_name},
            )
        else:
            return MutationResult(
                success=False,
                message=f"Unknown request type {req['request_type']!r}",
                code="schema.unknown_request_type",
                params={"type": req["request_type"]},
            )
        if not result.success:
            return result

        identity = _identity_from_info(info)
        resolved_by = getattr(identity, "user_id", None) if identity is not None else None
        async with pool.acquire() as conn:
            await cr_repo.mark_executed(cast("Connection", conn), request_id, resolved_by)
        return MutationResult(
            success=True,
            message=f"Executed creation request #{request_id}",
            code="schema.request_executed",
            params={"id": request_id},
        )

    @strawberry.mutation
    async def reject_creation_request(  # REQ-434, REQ-063
        self, info: StrawberryInfo, request_id: int, reason: str
    ) -> MutationResult:
        """REQ-434/063: a rights-holder rejects a queued request with an actionable reason."""
        from provisa.api.admin.capabilities import _identity_from_info, require_capability
        from provisa.core.repositories import creation_request as cr_repo

        if not reason or not reason.strip():
            return MutationResult(
                success=False,
                message="A rejection reason is required",
                code="schema.rejection_reason_required",
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            req = await cr_repo.get(cast("Connection", conn), request_id)
            if req is None or req["status"] != "pending":
                return MutationResult(
                    success=False,
                    message="Request not found or already resolved",
                    code="schema.request_not_pending",
                )
            try:
                require_capability(info, req["capability"])
            except PermissionError as e:
                return MutationResult(success=False, message=str(e))
            identity = _identity_from_info(info)
            resolved_by = getattr(identity, "user_id", None) if identity is not None else None
            await cr_repo.mark_rejected(
                cast("Connection", conn), request_id, reason.strip(), resolved_by
            )
        return MutationResult(
            success=True,
            message=f"Rejected creation request #{request_id}",
            code="schema.request_rejected",
            params={"id": request_id},
        )

    @strawberry.mutation
    async def upsert_relationship(  # REQ-019, REQ-020, REQ-366, REQ-434
        self, info: StrawberryInfo, input: RelationshipInput
    ) -> MutationResult:
        return await _upsert_relationship_impl(info, input)

    @strawberry.mutation
    async def delete_relationship(self, info: StrawberryInfo, id: str) -> MutationResult:
        require_capability(info, "create_relationship")
        from provisa.api.admin.domain_guard import relationship_domains, require_domains
        from provisa.core.repositories import relationship as rel_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            # REQ-1531: a relationship belongs to the domains of both tables it joins.
            domains = await relationship_domains(cast("Connection", conn), id)
            if domains is not None:
                require_domains(info, domains)
            try:
                deleted = await rel_repo.delete(cast("Connection", conn), id)
            except rel_repo.RelationshipDeleteRefused as refused:
                # REQ-1918: a published view relies on it; nothing is removed, each is named.
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code="schema.relationship_has_dependents",
                    params={
                        "relationship": id,
                        "dependents": [d.as_dict() for d in refused.dependents],
                    },
                )
        if deleted:
            await _rebuild_schemas()
            return MutationResult(
                success=True,
                message=f"Relationship {id!r} deleted",
                code="schema.relationship_deleted",
                params={"relationship": id},
            )
        return MutationResult(
            success=False,
            message=f"Relationship {id!r} not found",
            code="schema.relationship_not_found",
            params={"relationship": id},
        )

    # ── Admin: Cache Configuration ──

    @strawberry.mutation
    async def update_source_cache(
        self,
        info: StrawberryInfo,
        source_id: str,
        cache_enabled: bool,
        cache_ttl: int | None = None,
    ) -> MutationResult:
        """Update cache settings for a source."""
        require_capability(info, "source_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _sig = await conn.execute_core(
                select(
                    sources.c.change_signal, sources.c.replicate, sources.c.load_protected
                ).where(sources.c.id == source_id)
            )
            _sig_row = _sig.fetchone()
            if _sig_row is not None:
                _ttl_refusal = await landing_ttl_refusal(  # REQ-1907
                    conn,
                    source_id,
                    source=SourceTtl(
                        _sig_row.change_signal,
                        cache_ttl,
                        _sig_row.replicate,
                        _sig_row.load_protected,
                    ),
                )
                if _ttl_refusal is not None:
                    return _ttl_refusal
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(
                    success=False,
                    message=f"Source {source_id!r} not found",
                    code="schema.source_not_found",
                    params={"source": source_id},
                )
        return MutationResult(
            success=True,
            message=f"Cache settings updated for source {source_id!r}",
            code="schema.source_cache_updated",
            params={"source": source_id},
        )

    @strawberry.mutation
    async def update_table_cache(
        self, info: StrawberryInfo, table_id: int, cache_ttl: int | None = None
    ) -> MutationResult:
        """Update cache TTL for a registered table."""
        require_capability(info, "table_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _t = await conn.execute_core(
                select(
                    registered_tables.c.source_id,
                    registered_tables.c.schema_name,
                    registered_tables.c.table_name,
                    registered_tables.c.change_signal,
                    registered_tables.c.materialize,
                    registered_tables.c.row_materialize,
                    registered_tables.c.replicate,
                    registered_tables.c.load_protected,
                ).where(registered_tables.c.id == table_id)
            )
            _t_row = _t.fetchone()
            if _t_row is not None:
                _ttl_refusal = await landing_ttl_refusal(  # REQ-1907
                    conn,
                    _t_row.source_id,
                    table=TableTtl(
                        _t_row.schema_name,
                        _t_row.table_name,
                        _t_row.change_signal,
                        cache_ttl,
                        _t_row.materialize,
                        _t_row.row_materialize,
                        _t_row.replicate,
                        _t_row.load_protected,
                    ),
                )
                if _ttl_refusal is not None:
                    return _ttl_refusal
            result = await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(cache_ttl=cache_ttl)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_not_found",
                    params={"table": table_id},
                )
        return MutationResult(
            success=True,
            message=f"Cache TTL updated for table {table_id}",
            code="schema.table_cache_updated",
            params={"table": table_id},
        )

    @strawberry.mutation
    async def update_table_role_ttl(
        self, info: StrawberryInfo, table_id: int, role_ttl: list[RoleTtlInput]
    ) -> MutationResult:  # REQ-1907
        """Full-replace a table's role -> TTL list. Every role must exist, appear once, and carry a
        non-negative TTL; any violation rejects the whole list."""
        require_capability(info, "table_registration")
        seen: set[str] = set()
        for entry in role_ttl:
            if entry.role in seen:
                return MutationResult(
                    success=False,
                    message=f"Role {entry.role!r} appears more than once",
                    code="schema.role_ttl_duplicate_role",
                    params={"role": entry.role},
                )
            seen.add(entry.role)
            if entry.ttl < 0:
                return MutationResult(
                    success=False,
                    message=f"TTL for role {entry.role!r} must be >= 0, got {entry.ttl}",
                    code="schema.role_ttl_negative",
                    params={"role": entry.role, "ttl": entry.ttl},
                )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            known = {
                r[0]
                for r in (
                    await conn.execute_core(select(roles.c.id).where(roles.c.id.in_(seen)))
                ).fetchall()
            }
            for entry in role_ttl:
                if entry.role not in known:
                    return MutationResult(
                        success=False,
                        message=f"Role {entry.role!r} does not exist",
                        code="schema.role_ttl_unknown_role",
                        params={"role": entry.role},
                    )
            result = await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(role_ttl={e.role: e.ttl for e in role_ttl})
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_not_found",
                    params={"table": table_id},
                )
        return MutationResult(
            success=True,
            message=f"Role TTLs updated for table {table_id}",
            code="schema.table_role_ttl_updated",
            params={"table": table_id},
        )

    @strawberry.mutation
    async def update_table_paging(
        self, info: StrawberryInfo, table_id: int, paging: PagingInput | None = None
    ) -> MutationResult:  # REQ-318
        """Replace a table's paging (null clears it): a paged REST endpoint's type and parameters,
        or a connection table's max_rows, which may only lower graphql_remote.max_rows."""
        from provisa.api.admin._table_paging import save_table_paging
        from provisa.api.app import state

        require_capability(info, "table_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            return await save_table_paging(state, conn, table_id, paging)

    @strawberry.mutation
    async def update_source_replicate(
        self, info: StrawberryInfo, source_id: str, replicate: int | None = None
    ) -> MutationResult:  # REQ-826
        """Set when a source's tables are served from their replicas — the default a table with
        no value of its own inherits. None = Default (the global threshold), -1 = never, N > 0 =
        once a table passes N governed statements per interval, 0 = always."""
        require_capability(info, "source_registration")
        invalid = _invalid_replicate(replicate)
        if invalid is not None:
            return invalid
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _row = await conn.execute_core(
                select(
                    sources.c.change_signal, sources.c.cache_ttl, sources.c.load_protected
                ).where(sources.c.id == source_id)
            )
            row = _row.fetchone()
            if row is None:
                return MutationResult(
                    success=False,
                    message=f"Source {source_id!r} not found",
                    code="schema.source_not_found",
                    params={"source": source_id},
                )
            # REQ-826 / REQ-1907: judged with the value being saved — load_protected with never
            # is contradictory, and a table this now says is replicated needs its refresh clock.
            # A save that passed and then failed every read is refused here instead.
            _refusal = await landing_ttl_refusal(
                conn,
                source_id,
                source=SourceTtl(row.change_signal, row.cache_ttl, replicate, row.load_protected),
            )
            if _refusal is not None:
                return _refusal
            await conn.execute_core(
                update(sources).where(sources.c.id == source_id).values(replicate=replicate)
            )
        # The operator floor routing reads (REQ-030) is keyed on the schema generation; bump it so
        # the next read routes under the new setting instead of a cached decision.
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"replicate set for source {source_id!r}",
            code="schema.source_replicate_set",
            params={"source": source_id},
        )

    @strawberry.mutation
    async def update_table_replicate(
        self, info: StrawberryInfo, table_id: int, replicate: int | None = None
    ) -> MutationResult:  # REQ-826
        """Set when one table is served from its replica; None = inherit its source's value.
        -1 = never, N > 0 = once it passes N governed statements per interval, 0 = always."""
        require_capability(info, "table_registration")
        invalid = _invalid_replicate(replicate)
        if invalid is not None:
            return invalid
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _row = await conn.execute_core(
                select(
                    registered_tables.c.source_id,
                    registered_tables.c.schema_name,
                    registered_tables.c.table_name,
                    registered_tables.c.change_signal,
                    registered_tables.c.cache_ttl,
                    registered_tables.c.materialize,
                    registered_tables.c.row_materialize,
                    registered_tables.c.load_protected,
                ).where(registered_tables.c.id == table_id)
            )
            row = _row.fetchone()
            if row is None:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_not_found",
                    params={"table": table_id},
                )
            # REQ-826 / REQ-1907: judged with the value being saved (see update_source_replicate).
            _refusal = await landing_ttl_refusal(
                conn,
                row.source_id,
                table=TableTtl(
                    row.schema_name,
                    row.table_name,
                    row.change_signal,
                    row.cache_ttl,
                    row.materialize,
                    row.row_materialize,
                    replicate,
                    row.load_protected,
                ),
            )
            if _refusal is not None:
                return _refusal
            await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(replicate=replicate)
            )
        # A saved value must route: the floored tables and the replica routes are published by
        # the schema build.
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"replicate set for table {table_id}",
            code="schema.table_replicate_set",
            params={"table": table_id},
        )

    @strawberry.mutation
    async def update_source_load_protection(
        self,
        info: StrawberryInfo,
        source_id: str,
        load_protected: bool,
        off_peak_window: str | None = None,
        off_peak_tz: str = "UTC",
    ) -> MutationResult:  # REQ-1141
        """Mark a source load-protected (scheduled-refresh-only) and set its off-peak window.

        Enforces the REQ-1141 rule that a load-protected source MUST arm at least one refresh gate
        (off-peak window, a cache_ttl cadence, or a probing change_signal); a validation failure is a
        governed error, never a silently-accepted no-gate config."""
        require_capability(info, "source_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(sources.c.cache_ttl, sources.c.change_signal, sources.c.replicate).where(
                    sources.c.id == source_id
                )
            )
            row = _res.fetchone()
            if row is None:
                return MutationResult(
                    success=False,
                    message=f"Source {source_id!r} not found",
                    code="schema.source_not_found",
                    params={"source": source_id},
                )
            err = _validate_load_protection(
                load_protected, off_peak_window, row.cache_ttl, row.change_signal, source_id
            )
            if err is not None:
                return err
            _window = _parsed_off_peak(off_peak_window, off_peak_tz)
            if isinstance(_window, MutationResult):
                return _window
            # REQ-826: load_protected contradicts replicate -1 (never), on the source or on a
            # table of it that inherits the value being saved.
            _contradicted = await replicate_contradiction_refusal(
                conn,
                source_id,
                source=SourceTtl(row.change_signal, row.cache_ttl, row.replicate, load_protected),
            )
            if _contradicted is not None:
                return _contradicted
            await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(
                    load_protected=load_protected,
                    off_peak_window=off_peak_window,
                    off_peak_tz=off_peak_tz,
                )
            )
        # The operator floor routing reads (REQ-030) is keyed on the schema generation; bump it so
        # the next read routes under the new setting instead of a cached decision.
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"load protection set for source {source_id!r}",
            code="schema.source_load_protection_set",
            params={"source": source_id},
        )

    @strawberry.mutation
    async def update_table_load_protection(
        self,
        info: StrawberryInfo,
        table_id: int,
        load_protected: bool | None = None,
        off_peak_window: str | None = None,
        off_peak_tz: str | None = None,
    ) -> MutationResult:  # REQ-1141
        """Override load protection for one table; None load_protected = inherit the source default.

        When the EFFECTIVE load_protected is True, enforces the REQ-1141 ≥1-gate rule over the
        effective (table→source) window/cadence/probe."""
        require_capability(info, "table_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(
                    registered_tables.c.source_id,
                    registered_tables.c.cache_ttl,
                    registered_tables.c.change_signal,
                    registered_tables.c.off_peak_window,
                    registered_tables.c.mv_bitemporal_mode,
                    registered_tables.c.replicate,
                ).where(registered_tables.c.id == table_id)
            )
            row = _res.fetchone()
            if row is None:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_not_found",
                    params={"table": table_id},
                )
            # REQ-1141/1162: load protection (WHEN a source may be hit) and snapshotting (WHAT
            # point-in-time the data represents) are different axes, but on ONE table their timing can
            # fight — a snapshot boundary can fall outside the off-peak window, so the snapshot never
            # captures the intended instant. Warn (not block): they compose only when the snapshot
            # deadline can wait for the off-peak refresh (allowed_lateness).
            if _snapshot_load_protection_conflict(
                load_protected, off_peak_window, row.mv_bitemporal_mode
            ):
                logging.getLogger(__name__).warning(
                    "table %s: load protection (off-peak window) AND %r snapshotting are both set — "
                    "verify the snapshot boundary falls inside the off-peak refresh window (or that "
                    "allowed_lateness covers the lag), else snapshots may miss their intended instant "
                    "(REQ-1141/1162)",
                    table_id,
                    row.mv_bitemporal_mode,
                )
            _sres = await conn.execute_core(
                select(
                    sources.c.load_protected,
                    sources.c.cache_ttl,
                    sources.c.change_signal,
                    sources.c.off_peak_window,
                    sources.c.replicate,
                ).where(sources.c.id == row.source_id)
            )
            src = _sres.fetchone()
            effective_lp = src.load_protected if load_protected is None else load_protected
            eff_window = off_peak_window or (src.off_peak_window if src else None)
            eff_ttl = (
                row.cache_ttl if row.cache_ttl is not None else (src.cache_ttl if src else None)
            )
            eff_sig = row.change_signal or (src.change_signal if src else None)
            err = _validate_load_protection(
                effective_lp, eff_window, eff_ttl, eff_sig, f"table {table_id}"
            )
            if err is not None:
                return err
            _window = _parsed_off_peak(off_peak_window, off_peak_tz or "UTC")
            if isinstance(_window, MutationResult):
                return _window
            # REQ-826: load_protected contradicts a resolved replicate of -1 (never).
            from provisa.core.replicate import contradiction

            _contradicted = contradiction(
                row.replicate if row.replicate is not None else src.replicate, bool(effective_lp)
            )
            if _contradicted is not None:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id}: {_contradicted}",
                    code="schema.replicate_contradicts_load_protected",
                    params={"table": str(table_id)},
                )
            await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(
                    load_protected=load_protected,
                    off_peak_window=off_peak_window,
                    off_peak_tz=off_peak_tz,
                )
            )
        # A saved value must route: the floored tables and the replica routes are published by
        # the schema build.
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"load protection set for table {table_id}",
            code="schema.table_load_protection_set",
            params={"table": table_id},
        )

    # ── Admin: Naming Convention ──

    @strawberry.mutation
    async def update_gql_naming_convention(
        self, info: StrawberryInfo, convention: str
    ) -> MutationResult:  # REQ-253, REQ-416
        """Set the global naming convention and rebuild schemas for all roles."""
        require_capability(info, "table_registration")
        from provisa.api.app import state

        from provisa.compiler import naming as _naming

        # REQ-416: reject free-form conventions; only the presets (and their aliases) are valid.
        err = _naming.validation_error_for_convention(convention)
        if err:
            return MutationResult(success=False, message=err)

        state.global_gql_naming_convention = convention
        _naming.configure(gql=convention, sql=state.global_sql_naming_convention)
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Naming convention set to {convention!r}",
            code="schema.naming_convention_set",
            params={"convention": convention},
        )

    @strawberry.mutation
    async def update_source_naming(
        self, info: StrawberryInfo, source_id: str, gql_naming_convention: Optional[str] = None
    ) -> MutationResult:
        """Update naming convention for a source."""
        require_capability(info, "source_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(gql_naming_convention=gql_naming_convention)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(
                    success=False,
                    message=f"Source {source_id!r} not found",
                    code="schema.source_not_found",
                    params={"source": source_id},
                )
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Naming convention updated for source {source_id!r}",
            code="schema.source_naming_updated",
            params={"source": source_id},
        )

    @strawberry.mutation
    async def update_source_allowed_domains(
        self, info: StrawberryInfo, source_id: str, allowed_domains: list[str]
    ) -> MutationResult:  # REQ-1531
        """Set the allowed domain list for a source (empty list = unrestricted)."""
        # REQ-1531: this decides which domains may reach a source at all. It belongs to whoever
        # registers sources, and opening the source to a domain — or, with an empty list, to
        # every domain — needs the caller to reach that domain. Closing it to one needs no reach.
        from provisa.api.admin.capabilities import (
            require_capability,
            require_reach_of_added_domains,
        )
        from provisa.core.repositories import source as source_repo

        require_capability(info, "source_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            held = await source_repo.get(cast("Connection", conn), source_id)
            if held is not None:
                require_reach_of_added_domains(
                    info, held["allowed_domains"] or [], allowed_domains, empty_is_all=True
                )
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(allowed_domains=allowed_domains)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(
                    success=False,
                    message=f"Source {source_id!r} not found",
                    code="schema.source_not_found",
                    params={"source": source_id},
                )
        from provisa.api.app import state

        if allowed_domains:
            state.source_allowed_domains[source_id] = list(allowed_domains)
        else:
            state.source_allowed_domains.pop(source_id, None)
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Allowed domains updated for source {source_id!r}",
            code="schema.allowed_domains_updated",
            params={"source": source_id},
        )

    @strawberry.mutation
    async def update_table_naming(
        self, info: StrawberryInfo, table_id: int, gql_naming_convention: Optional[str] = None
    ) -> MutationResult:
        """Update naming convention for a registered table."""
        require_capability(info, "table_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(gql_naming_convention=gql_naming_convention)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_not_found",
                    params={"table": table_id},
                )
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Naming convention updated for table {table_id}",
            code="schema.table_naming_updated",
            params={"table": table_id},
        )

    # ── Admin: Forced Regen ──

    @strawberry.mutation
    async def force_regen(
        self, info: StrawberryInfo, table_id: int, reason: str
    ) -> MutationResult:  # REQ-968
        """Recompute one table's landed rows ON DEMAND, bypassing the REQ-958/981 change gate.

        THE SCOPE IS DERIVED, never asked of the operator: a derived view recomputes from its own SQL
        (``node``) while a landed source re-lands and cascades to its dependents (``source``) — the
        operator knows they want this table rebuilt, not which kind of node the event loop thinks it
        is. A table that federates LIVE has no landed rows to regenerate, so it is refused rather than
        given an event nothing will ever claim. The reason is REQ-968's audit why-tag and rides on the
        posted event.
        """
        require_capability(info, "org_settings")
        from provisa.api.app import state
        from provisa.api.admin._refresh_summary import _resolve_engine
        from provisa.events import injector
        from provisa.federation.engine import UnreachableSource
        from provisa.federation.strategy import Strategy, federate

        pool = await _get_pool()
        async with pool.acquire() as conn:
            row = (
                await conn.execute_core(
                    select(
                        registered_tables.c.schema_name,
                        registered_tables.c.table_name,
                        registered_tables.c.source_id,
                    ).where(registered_tables.c.id == table_id)
                )
            ).fetchone()
            if row is None:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_not_found",
                    params={"table": table_id},
                )
            schema_name, table_name, source_id = row[0], row[1], row[2]
            from provisa.events.nodes import source_node, view_node

            view = state.mv_registry.get(f"view-{table_name}")
            node = (
                view_node(view)
                if view is not None
                else source_node(source_id, schema_name, table_name)
            )
            if view is not None:
                scope = "node"  # a derived view: recompute its SQL without re-landing its inputs
            else:
                scope = "source"
                engine = _resolve_engine()
                if engine is None:
                    return MutationResult(
                        success=False,
                        message="Federation engine is not ready",
                        code="schema.engine_not_ready",
                        params={"table": node},
                    )
                # The live config's own Source model — the same object the event loop classifies from,
                # so this answer and the DAG's cannot disagree.
                src = next(
                    (
                        s
                        for s in (state.config.sources if state.config else [])
                        if s.id == source_id
                    ),
                    None,
                )
                if src is None:
                    return MutationResult(
                        success=False,
                        message=f"Source {source_id!r} not found",
                        code="schema.source_not_found",
                        params={"source": source_id},
                    )
                try:
                    landed = federate(src, engine) is Strategy.MATERIALIZED
                except UnreachableSource:
                    # The source is down. Its replica is a frozen snapshot (REQ-1143), and a forced
                    # re-land is exactly how an operator retries the load once it is back.
                    landed = True
                if not landed:
                    return MutationResult(
                        success=False,
                        message=f"{node} federates live — it has no landed rows to regenerate",
                        code="schema.table_not_landed",
                        params={"table": node},
                    )
            try:
                event_id = await injector.force_regen(conn, scope=scope, node=node, reason=reason)
            except ValueError as exc:  # REQ-968 refuses a missing reason / unknown scope, loudly
                return MutationResult(
                    success=False,
                    message=str(exc),
                    code="schema.regen_refused",
                    params={"table": node},
                )
        return MutationResult(
            success=True,
            message=f"Regen queued for {node}",
            code="schema.regen_queued",
            params={"table": node, "scope": scope, "event": event_id},
        )

    # ── Admin: MV Management ──

    @strawberry.mutation
    async def refresh_mv(
        self, info: StrawberryInfo, mv_id: str
    ) -> MutationResult:  # REQ-133, REQ-158
        """Trigger a manual refresh of a materialized view."""
        require_capability(info, "table_registration")
        from provisa.api.app import state

        mv = state.mv_registry.get(mv_id)
        if mv is None:
            return MutationResult(
                success=False,
                message=f"MV {mv_id!r} not found",
                code="schema.mv_not_found",
                params={"mv": mv_id},
            )
        try:
            from provisa.mv.refresh import refresh_failure, refresh_mv

            assert state.federation_engine is not None
            # REQ-879: coordinate the refresh across the fleet via the shared control-plane catalog.
            await refresh_mv(
                state.federation_engine,
                mv,
                state.mv_registry,
                store=state.model_db,
                ledger=state.tenant_db,
            )
            # refresh_mv records a failed refresh on the view and returns (it runs on the
            # scheduler too, where there is nobody to raise to). The mutation answers the
            # caller who asked: a failed refresh is reported as one, with the view's own error
            # — the text the view list shows as its last error.
            failure = refresh_failure(mv)
            if failure is not None:
                return MutationResult(success=False, message=failure)
            return MutationResult(
                success=True,
                message=f"MV {mv_id!r} refreshed",
                code="schema.mv_refreshed",
                params={"mv": mv_id},
            )
        except Exception as e:
            logging.getLogger(__name__).exception("refresh_mv %r failed", mv_id)
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def toggle_mv(self, info: StrawberryInfo, mv_id: str, enabled: bool) -> MutationResult:
        """Enable or disable a materialized view."""
        require_capability(info, "table_registration")
        from provisa.api.app import state
        from provisa.mv.models import MVStatus

        mv = state.mv_registry.get(mv_id)
        if mv is None:
            return MutationResult(
                success=False,
                message=f"MV {mv_id!r} not found",
                code="schema.mv_not_found",
                params={"mv": mv_id},
            )
        mv.enabled = enabled
        if not enabled:
            mv.status = MVStatus.DISABLED
        elif mv.status == MVStatus.DISABLED:
            mv.status = MVStatus.STALE
        return MutationResult(
            success=True,
            message=f"MV {mv_id!r} {'enabled' if enabled else 'disabled'}",
            code="schema.mv_enabled" if enabled else "schema.mv_disabled",
            params={"mv": mv_id},
        )

    # ── Admin: Cache Management ──

    @strawberry.mutation
    async def purge_cache(self, info: StrawberryInfo) -> MutationResult:
        """Purge the cached query results of the org and environment the caller is acting in."""
        require_capability(info, "org_settings")
        from provisa.api.app import state
        from provisa.cache import tenancy

        try:
            # REQ-595: an org administrator's purge reaches that org's entries, never another's.
            count = await tenancy.purge_acting_place(state)
            return MutationResult(
                success=True,
                message=f"Purged {count} cache entries",
                code="schema.cache_purged",
                params={"count": count},
            )
        except Exception as e:
            logging.getLogger(__name__).exception("purge_cache failed")
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def purge_cache_by_table(self, info: StrawberryInfo, table_id: int) -> MutationResult:
        """Purge cached results for a specific table."""
        require_capability(info, "org_settings")
        from provisa.api.app import state

        try:
            from provisa.cache.tenancy import invalidate_tables

            # REQ-595: the acting org's entries — the tenant they were written under.
            count = await invalidate_tables(state, [table_id])
            return MutationResult(
                success=True,
                message=f"Purged {count} cache entries for table {table_id}",
                code="schema.cache_purged_table",
                params={"count": count, "table": table_id},
            )
        except Exception as e:
            logging.getLogger(__name__).exception("purge_cache_by_table %s failed", table_id)
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def invalidate_file_source(self, info: StrawberryInfo, table_id: int) -> MutationResult:
        """Force a sqlite file-connector table's next access to re-sync from disk."""
        require_capability(info, "source_registration")
        return await _ops.invalidate_file_source(table_id)

    # ── Admin: Scheduled Task Management ──

    @strawberry.mutation
    async def toggle_scheduled_task(
        self, info: StrawberryInfo, task_id: str, enabled: bool
    ) -> MutationResult:
        """Enable or disable one of the caller's org's scheduled triggers (REQ-1003)."""
        require_capability(info, "org_settings")
        from provisa.api.admin._table_ops import _get_pool
        from provisa.core.repositories import scheduled_trigger as trigger_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            held = await trigger_repo.get(conn, task_id)
            if held is None:
                return MutationResult(
                    success=False,
                    message=f"Task {task_id!r} not found",
                    code="schema.task_not_found",
                    params={"task": task_id},
                )
            if enabled and held["kind"] == "sql":
                from provisa.api.admin.capabilities import require_trigger_role

                # Enabling a SQL trigger sets it running as its role, so it is the same act
                # as saving one. Disabling one runs nothing and needs no role.
                require_trigger_role(info.context["request"], held["role"])
            await trigger_repo.set_enabled(conn, task_id, enabled)
        await _ops.reschedule_org_triggers()

        return MutationResult(
            success=True,
            message=f"Task {task_id!r} {'enabled' if enabled else 'disabled'}",
            code="schema.task_enabled" if enabled else "schema.task_disabled",
            params={"task": task_id},
        )

    @strawberry.mutation
    async def create_scheduled_task(  # REQ-1003, REQ-1004
        self,
        info: StrawberryInfo,
        id: str,
        name: str,
        cron: str,
        kind: str,
        webhook_name: Optional[str] = None,
        args_json: Optional[str] = None,
        sql: Optional[str] = None,
        role: Optional[str] = None,
    ) -> MutationResult:
        """Create a scheduled trigger (webhook or SQL) and register it live (REQ-1003/1004)."""
        require_capability(info, "org_settings")
        return await _ops.create_scheduled_task_op(
            info.context["request"], id, name, cron, kind, webhook_name, args_json, sql, role
        )

    @strawberry.mutation
    async def delete_scheduled_task(
        self, info: StrawberryInfo, task_id: str
    ) -> MutationResult:  # REQ-1003
        """Remove a scheduled trigger from config and the live scheduler."""
        require_capability(info, "org_settings")
        return await _ops.delete_scheduled_task_op(task_id)

    @strawberry.mutation
    async def refresh_source_statistics(
        self, info: StrawberryInfo, source_id: str
    ) -> MutationResult:  # REQ-276
        """Run ANALYZE on all registered tables for a source (Phase AL).

        Triggers the engine to collect fresh table statistics, which improves the
        quality of join-order and broadcast decisions for federated queries.
        """
        require_capability(info, "source_registration")
        from provisa.api.app import state

        if state.federation_engine is None:
            return MutationResult(
                success=False,
                message="Query engine not available",
                code="schema.query_engine_unavailable",
            )

        pool = await _get_pool()
        if pool is None:
            return MutationResult(
                success=False,
                message="Database pool not available",
                code="schema.db_pool_unavailable",
            )

        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(registered_tables.c.schema_name, registered_tables.c.table_name).where(
                    registered_tables.c.source_id == source_id
                )
            )
            rows = _res.fetchall()

        if not rows:
            return MutationResult(
                success=False,
                message=f"No tables registered for source {source_id!r}",
                code="schema.no_tables_for_source",
                params={"source": source_id},
            )

        analyzed: list[str] = []
        errors: list[str] = []
        source_catalog = state.catalog_for(source_id)

        engine = state.federation_engine
        for row in rows:
            full_name = f"{source_catalog}.{row.schema_name}.{row.table_name}"
            try:
                # REQ-1912: statistics are collected where the engine reads the table. A table
                # served from its replica is analyzed at its replica's address, in the store
                # (its source has no live attach to analyze); a live table at its registered name.
                registered = (source_catalog, row.schema_name, row.table_name)
                read = engine.read_address(*registered)
                if read != registered:
                    r_catalog, r_schema, r_table = read
                    await engine.analyze_landed_table(
                        catalog=r_catalog, schema=r_schema, table=r_table
                    )
                else:
                    await run_admin_catalog_sql(
                        state, engine, f"ANALYZE {full_name}", "source statistics"
                    )
                analyzed.append(full_name)
            except Exception as exc:
                logging.getLogger(__name__).exception("ANALYZE %s failed", full_name)
                errors.append(f"{full_name}: {exc}")

        if errors:
            return MutationResult(
                success=False,
                message=f"ANALYZE completed with errors. OK={len(analyzed)} errors={errors}",
                code="schema.analyze_errors",
                params={"ok": len(analyzed), "errors": str(errors)},
            )
        return MutationResult(
            success=True,
            message=f"ANALYZE completed for {len(analyzed)} table(s) on source {source_id!r}",
            code="schema.analyze_completed",
            params={"count": len(analyzed), "source": source_id},
        )

    @strawberry.mutation
    async def compile_query(
        self, info: StrawberryInfo, input: CompileQueryInput
    ) -> list[CompileQueryResult]:  # REQ-161
        require_capability(info, "query_development")
        from provisa.api.admin import dev_queries
        from provisa.api.admin.capabilities import require_inspectable_role

        # REQ-1620: the role may be a comma-separated set (the explorer under "Role: All"); each
        # named role must be one the caller may inspect, and several compile as their meta-role --
        # the role a request naming that set is served as.
        from provisa.api.app import state as _state
        from provisa.security.meta_role import ensure_meta_role, refuse_named_meta_role

        named = sorted({r.strip() for r in input.role.split(",") if r.strip()})
        refuse_named_meta_role(named)
        for role_name in named:
            require_inspectable_role(info, role_name)
        role_id = named[0] if len(named) == 1 else ensure_meta_role(_state, named)
        variables = cast(dict, input.variables) if input.variables else None
        results = await dev_queries.compile_query(
            role_id,
            input.query,
            variables,
            flat_sql=input.flat_sql,
            flat_cypher=input.flat_cypher,
            node_only_cypher=input.node_only_cypher,
        )
        out = []
        for r in results:
            enf = r["enforcement"]
            out.append(
                CompileQueryResult(
                    sql=r["sql"],
                    semantic_sql=r["semantic_sql"],
                    engine_sql=r.get("engine_sql"),
                    direct_sql=r.get("direct_sql"),
                    route=r["route"],
                    route_reason=r["route_reason"],
                    sources=r["sources"],
                    root_field=r["root_field"],
                    canonical_field=r["canonical_field"],
                    column_aliases=[
                        ColumnAliasType(field_name=a["field_name"], column=a["column"])
                        for a in r["column_aliases"]
                    ],
                    enforcement=EnforcementType(
                        rls_filters_applied=enf.rls_filters_applied,
                        columns_excluded=enf.columns_excluded,
                        schema_scope=enf.schema_scope,
                        masking_applied=enf.masking_applied,
                        ceiling_applied=enf.ceiling_applied,
                        route=enf.route,
                    ),
                    optimizations=r["optimizations"],
                    warnings=r["warnings"],
                    compiled_cypher=r.get("compiled_cypher"),
                    cypher_error=r.get("cypher_error"),
                )
            )
        return out

    @strawberry.mutation
    async def deploy_view_to_db(self, info: StrawberryInfo, table_id: int) -> MutationResult:
        """Promote a virtual Provisa view to a real database view on its underlying native source."""
        require_capability(info, "table_registration")
        return await _ops.deploy_view_to_db(info, table_id)


async def _upsert_action_rls_rule(
    info: StrawberryInfo, input: RLSRuleInput
) -> MutationResult:  # REQ-1679
    """An RLS rule over an action's response contract: validated against the contract the way
    a table rule is validated against the table (REQ-1676), gated on the action's domain."""
    from provisa.api.app import state
    from provisa.api.data.action_governance import contract_columns
    from provisa.compiler.rls_validate import validate_rls_predicate
    from provisa.core.models import RLSRule as RLSRuleModel

    name = input.action_name or ""
    action = state.tracked_functions.get(name) or (
        getattr(state, "tracked_webhooks", None) or {}
    ).get(name)
    if action is None:
        return MutationResult(
            success=False,
            message=f"Action not registered: {name}",
            code="schema.action_not_found",
            params={"action": name},
        )
    if action.get("domain_id"):
        require_right_in_domains(info, "masking_config", {action["domain_id"]})
    cols = contract_columns(action)
    if cols is None:
        return MutationResult(
            success=False,
            message=f"Action {name!r} declares no output columns; a row filter has nothing to bind to",
            code="schema.rls_rule_invalid",
            params={"reason": "no output contract"},
        )
    target = {
        "table_name": name,
        "alias": None,
        "domain_id": action.get("domain_id") or "",
        "columns": [
            {"column_name": c["name"], "data_type": c.get("type"), "alias": None} for c in cols
        ],
    }
    problem = validate_rls_predicate(input.filter_expr, [target], [target])
    if problem is not None:
        return MutationResult(
            success=False,
            message=problem,
            code="schema.rls_rule_invalid",
            params={"reason": problem},
        )
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await rls_repo.upsert(
            cast("Connection", conn),
            RLSRuleModel(action_name=name, role_id=input.role_id, filter=input.filter_expr),
        )
    # See upsert_rls_rule's own matching rebuild — this action-RLS path bypasses that function
    # entirely (early-returns before it), so it needs the identical rebuild call itself.
    await _rebuild_schemas()
    return MutationResult(
        success=True,
        message=f"RLS rule for action {name!r} / role {input.role_id!r} saved",
        code="schema.rls_rule_saved_action",
        params={"action": name, "role": input.role_id},
    )
