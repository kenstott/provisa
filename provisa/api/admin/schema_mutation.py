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
from datetime import date
from provisa.compiler.naming import SqlAddressTaken
from typing import TYPE_CHECKING, Any, Optional, cast

from provisa.api.admin.engine_auth import run_admin_catalog_sql

import strawberry
from sqlalchemy import select, update
from strawberry.types.info import Info as StrawberryInfo

from provisa.core.schema_org import (
    registered_tables,
    relationship_candidates,
    relationships,
    sources,
    tracked_webhooks,
)

if TYPE_CHECKING:
    from provisa.api.admin.read_rights import DependentsReader
    from provisa.core.database import Connection

from provisa.compiler.sql_types import key_list
from provisa.core.paging import stored_paging
from provisa.security.sensitive import SENSITIVE_DATA
from provisa.core.repositories import glossary as glossary_repo, rls as rls_repo
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
    WriteGrantInput,
    RoleInput,
    PagingInput,
    RoleTtlInput,
    SourceInput,
    TableInput,
    ColumnInput,
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
from provisa.api.admin._table_settings import (
    TableSaveRefused,
    carried,
    load_protection_refusal,
    save_carried_paging,
    save_carried_region,
    save_row_rules,
    invalid_replicate as _invalid_replicate,
    naming_refusal,
    role_ttl_refusal,
    row_rule_refusal,
    save_carried_after_store,
    save_naming,
    with_carried_settings,
    write_grant_invalid,
    write_grant_models,
    write_grant_predicates_refusal,
    write_grant_roles_refusal,
)
from provisa.api.admin._landing_ttl import (  # REQ-1907, REQ-826
    SourceTtl,
    TableTtl,
    landing_ttl_refusal,
    replicate_contradiction_refusal,
    row_materialize_save_refusal,
)
from provisa.api.admin._table_ops import _build_columns_for_input, column_field_refusal
from provisa.api.admin._fake_guard import FakeRefusedSave as _FakeRefusedSave
from provisa.api.admin import schema_mutation_ops as _ops


from provisa.api.admin._cdc_registry import (  # noqa: E402
    refuse_registry_settings,
    stored_cdc,
)
from provisa.api.admin._row_mappers import (  # noqa: E402
    _federation_hints_from_input,
    _parse_mapping_json,
)
from provisa.api.admin.schema_common import (  # noqa: E402
    check_view_mv,
    _add_source_pool,
    _analyze_source_on_engine,
    _fire_catalog_indexing,
    _drop_source_on_engine,
    _queue_creation_request,
    _rebuild_relationship_input,
    _rebuild_source_input,
    _rebuild_table_input,
    _resolve_admin_context,
    _register_source_on_engine,
    restart_source_server,
    _remove_view_mv,
    _expand_wikipedia_if_needed,
    google_workspace_refusal,
    logs_refusal,
    sandbox_destination_refusal,
    download_source_file,
    file_source_path,
    google_workspace_organisation_refusal,
    microsoft_365_organisation_refusal,
    microsoft_365_refusal,
    _stage_kaggle_if_needed,
    _sync_view_mv,
    _cache_prometheus_label_columns,
    _synthesize_mapping_dsl_tables,
    _upsert_source_with_domains,
    _another_govdata_source,
    _validate_govdata_api_key,
)


# The port pgwire is served on when nothing says otherwise: what provisa-install.yaml's own
# dq-checker/dq-soda entries dial.
_DQ_STANDARD_PGWIRE_PORT = 5439


def _secured() -> bool:
    """Whether an auth provider secures this deployment: pgwire then checks a password; with
    none it is in trust mode and asks for none (pgwire/server.py)."""
    from provisa.api.app import state

    return state.auth_middleware_active


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


def checker_mapping() -> dict[str, str]:
    """The connection a checker source made in the Sources form scans through: this deployment's
    own pgwire endpoint, with the env-var defaults provisa-install.yaml's dq-checker and dq-soda
    entries use (REQ-1742, REQ-1443)."""
    return {
        "host": os.environ.get("PROVISA_DQ_HOST", "localhost"),
        "port": _dq_pgwire_port(),
        "database": "provisa",
        "user": os.environ.get("PROVISA_DQ_USER", "org_admin"),
        # Under an auth provider, a reference resolved at each scan (dq.scan): unset, the scan
        # fails naming it. No account's password is assumed. With none, pgwire is in trust mode
        # and asks for no password, as the shipped dq-checker and dq-soda sources write it.
        # reference-convention: REQ-1443 -- docs/configuration.md's checker source
        "password": "${env:PROVISA_DQ_PASSWORD}" if _secured() else "",
    }


async def _table_unread(info: StrawberryInfo, table_id: int) -> MutationResult | None:
    """A refusal for a table the caller is served no column of, answered as one that does not
    exist (REQ-2152): the right that changes a table's settings does not reach a table its role
    cannot read. None when the caller reads it."""
    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.admin.read_rights import served_columns_of

    scope = catalog_scope(info)
    if scope.whole:
        return None
    pool = await _get_pool()
    async with pool.acquire() as conn:
        served = await served_columns_of(cast("Connection", conn), scope, table_id)
    if served != set():
        return None
    return MutationResult(
        success=False,
        message=f"Table {table_id} not found",
        code="schema.table_not_found",
        params={"table": table_id},
    )


async def _rule_beyond_reads(
    info: StrawberryInfo, input: Any, targets: list[dict]
) -> MutationResult | None:
    """Why a row rule over ``targets`` is refused to a caller who does not read what it names
    (REQ-2152), or None."""
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ParseError

    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.admin.read_rights import served_columns_of

    scope = catalog_scope(info)
    if scope.whole:
        return None
    try:
        parsed = sqlglot.parse_one(input.filter_expr)
    except ParseError:
        # A predicate that does not parse names no column to judge: the save's own check, which
        # runs next (validate_rls_predicate), refuses it with the parse reason (REQ-1676).
        return None
    named = {c.name for c in parsed.find_all(exp.Column)}
    pool = await _get_pool()
    async with pool.acquire() as conn:
        for target in targets:
            served = await served_columns_of(cast("Connection", conn), scope, target["id"])
            if served is None:
                continue
            if not served and not input.domain_id:
                return MutationResult(
                    success=False,
                    message=f"Table not registered: {input.table_id}",
                    code="schema.table_not_found",
                    params={"table": input.table_id},
                )
            stored = {c["column_name"] for c in target["columns"]}
            if not (named & stored) <= served:
                reason = "the filter names a column the acting role cannot read"
                return MutationResult(
                    success=False,
                    message=reason,
                    code="schema.rls_rule_invalid",
                    params={"reason": reason},
                )
    return None


async def _save_relationship_as_caller(
    info: StrawberryInfo, input: RelationshipInput, *, creating: bool
) -> MutationResult:  # REQ-019, REQ-020, REQ-366, REQ-434
    """Shared body of the createRelationship and updateRelationship mutations. Module-level so
    register_fact can create its dimension links directly — strawberry invokes root mutations
    with self=None, so a self.create_relationship call never works from inside another
    resolver."""
    from provisa.api.admin.capabilities import has_capability, require_capability

    # REQ-434/366: a user lacking create_relationship queues a request instead of erroring. A
    # request is for a new relationship; changing one needs the right itself.
    if not has_capability(info, "create_relationship"):
        if not creating:
            require_capability(info, "create_relationship")
        return await _queue_creation_request(info, "relationship", "create_relationship", input)
    from provisa.api.admin.capabilities import _identity_from_info

    _invalid = _cardinality_problem(input)
    if _invalid is not None:
        return _invalid
    # REQ-1531: A RELATIONSHIP IS OWNED BY ITS SOURCE. The row is source -> target and the unique
    # constraint is (source_table_id, alias), so the edge hangs off the source side and the source's
    # domain is the one being changed. The target is referenced, not altered — its own RLS and
    # masking still apply when the join is traversed. So the gate asks about the SOURCE domain, and
    # an edge whose target lives elsewhere is recorded but flagged for the other domain to review.
    from provisa.api.admin.capabilities import require_domain

    _pool_g = await _get_pool()
    async with _pool_g.acquire() as _gconn:
        _refusal, _src_row, _tgt_row = await _relationship_tables(cast("Connection", _gconn), input)
    if _refusal is not None:
        return _refusal
    assert _src_row is not None  # _relationship_tables refuses an unresolved source
    _beyond = await _relationship_beyond_reads(info, input, _src_row, _tgt_row)  # REQ-2152
    if _beyond is not None:
        return _beyond
    try:
        require_domain(info, _src_row["domain_id"])
    except PermissionError:
        if not creating:
            raise
        # REQ-434/1531: out of domain queues a request, the same answer a missing right gets.
        return await _queue_creation_request(info, "relationship", "create_relationship", input)
    _cross_domain = _tgt_row is not None and _tgt_row["domain_id"] != _src_row["domain_id"]
    # REQ-020: record the defining steward as owner.
    _identity = _identity_from_info(info)
    _owner = getattr(_identity, "user_id", None) if _identity is not None else None
    return await save_relationship(
        input, owner=_owner, needs_review=_cross_domain, creating=creating
    )


async def _relationship_beyond_reads(
    info: StrawberryInfo, input: RelationshipInput, source: dict, target: dict | None
) -> MutationResult | None:
    """Why a relationship is refused to a caller who does not read what it joins (REQ-2152), or
    None: an end table it is served no column of is answered as unregistered, and a join column
    it is not served -- whether or not the table has it -- is refused naming none."""
    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.admin.read_rights import served_columns_of

    scope = catalog_scope(info)
    if scope.whole:
        return None
    ends = [("source", input.source_table_id, source, input.source_column)]
    if target is not None:
        ends.append(("target", input.target_table_id, target, input.target_column or ""))
    pool = await _get_pool()
    async with pool.acquire() as conn:
        for end, name, row, columns in ends:
            served = await served_columns_of(cast("Connection", conn), scope, row["id"])
            if served is None:
                continue
            if not served:
                return MutationResult(
                    success=False,
                    message=f"{end.capitalize()} table {name!r} is not registered",
                    code=f"schema.relationship_unknown_{end}",
                    params={"table": name},
                )
            if not set(key_list(columns)) <= served:
                return MutationResult(
                    success=False,
                    message="a relationship joins only on columns the acting role can read",
                    code="schema.relationship_column_beyond_reads",
                )
    return None


async def _relationship_tables(
    conn: "Connection", input: RelationshipInput
) -> "tuple[MutationResult | None, dict | None, dict | None]":
    """The relationship's source and target tables, by the virtual names it gives them, or the
    refusal naming the one that resolves to no table or to more than one. Checked before any
    write: a refused save stores nothing."""
    from provisa.core.repositories import table as table_repo

    ends = [("source", input.source_table_id)]
    if not input.target_function_name and input.target_table_id:
        ends.append(("target", input.target_table_id))
    if input.via_table:
        ends.append(("junction", input.via_table))
    rows: dict[str, dict] = {}
    for end, name in ends:
        try:
            row = await table_repo.find_by_table_name(conn, name)
        except table_repo.AmbiguousTableName as amb:
            return (
                MutationResult(
                    success=False,
                    message=str(amb),
                    code="schema.relationship_ambiguous_table",
                    params={"end": end, "table": name, "sources": ", ".join(amb.sources)},
                ),
                None,
                None,
            )
        if row is None:
            return (
                MutationResult(
                    success=False,
                    message=f"{end.capitalize()} table {name!r} is not registered",
                    code=f"schema.relationship_unknown_{end}",
                    params={"table": name},
                ),
                None,
                None,
            )
        rows[end] = row
    return None, rows["source"], rows.get("target")


async def _relationship_id_refusal(
    conn: "Connection", input: RelationshipInput, *, creating: bool
) -> "MutationResult | None":
    """A create makes a new relationship and an update changes one: an id another relationship
    holds, or one no relationship holds, is refused by name."""
    held = (
        await conn.execute_core(select(relationships.c.id).where(relationships.c.id == input.id))
    ).fetchone()
    if creating and held is not None:
        return MutationResult(
            success=False,
            message=f"Relationship {input.id!r} already exists",
            code="schema.relationship_id_taken",
            params={"relationship": input.id},
        )
    if not creating and held is None:
        return MutationResult(
            success=False,
            message=f"Relationship {input.id!r} not found",
            code="schema.relationship_not_found",
            params={"relationship": input.id},
        )
    return None


async def _same_entity_refusal(
    conn: "Connection", input: RelationshipInput, source: dict, target: dict | None
) -> "MutationResult | None":
    """REQ-2141: a relationship marked same entity, refused by name when it does not join two
    tables one class term grounds, one-to-one on a key of each (repositories/identity.py)."""
    from provisa.core.repositories.identity import same_entity_refusal

    refused = await same_entity_refusal(
        conn,
        relationship=input.id,
        source_table_id=source["id"],
        target_table_id=None if target is None else target["id"],
        source_column=input.source_column,
        target_column=input.target_column or "",
        cardinality=input.cardinality,
        junction=bool(input.via_table) or bool(input.target_function_name),
    )
    if refused is None:
        return None
    return MutationResult(
        success=False, message=refused.message, code=refused.code, params=refused.params
    )


async def _relationship_alias_refusal(
    conn: "Connection", input: RelationshipInput, source_table_id: int
) -> "MutationResult | None":
    """An alias -- the Cypher type or the GraphQL field -- another relationship of the same
    source table already holds, refused by name with the relationship that holds it: one source
    table's relationships are told apart by each. A GraphQL field two relationships shared used
    to drop one of them from the schema."""
    graphql_alias = getattr(input, "graphql_alias", None)
    if graphql_alias:
        shared = (
            await conn.execute_core(
                select(relationships.c.id).where(
                    relationships.c.source_table_id == source_table_id,
                    relationships.c.graphql_alias == graphql_alias,
                    relationships.c.id != input.id,
                )
            )
        ).fetchone()
        if shared is not None:
            return MutationResult(
                success=False,
                message=(
                    f"GraphQL alias {graphql_alias!r} is already the alias of relationship "
                    f"{shared.id!r} from {input.source_table_id!r}"
                ),
                code="schema.relationship_graphql_alias_taken",
                params={
                    "alias": graphql_alias,
                    "table": input.source_table_id,
                    "relationship": shared.id,
                },
            )
    if not input.alias:
        return None
    held = (
        await conn.execute_core(
            select(relationships.c.id).where(
                relationships.c.source_table_id == source_table_id,
                relationships.c.alias == input.alias,
                relationships.c.id != input.id,
            )
        )
    ).fetchone()
    if held is None:
        return None
    return MutationResult(
        success=False,
        message=(
            f"Alias {input.alias!r} is already the alias of relationship {held.id!r} from "
            f"{input.source_table_id!r}"
        ),
        code="schema.relationship_alias_taken",
        params={"alias": input.alias, "table": input.source_table_id, "relationship": held.id},
    )


async def _save_data_product(
    info: StrawberryInfo, input: DataProductInput, *, creating: bool
) -> MutationResult:  # REQ-1634
    """A data product saved: a create makes a new one, refusing an id that exists; an update
    changes one, refusing an id that does not. Either refusal names the id and writes nothing."""
    # REQ-1634: creating/deleting a data product is catalog curation, gated on its own
    # write right rather than ORG_SETTINGS — see Capability.DATA_PRODUCT_RW. REQ-2155: a product's
    # owner (data_product_owned_rw) edits the one it owns.
    from provisa.api.admin.capabilities import _identity_from_info, data_product_edit_refusal
    from provisa.api.app import state as app_state
    from provisa.core.models import DataProduct as DataProductModel
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.core.repositories import domain as domain_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        conn = cast("Connection", conn)
        # REQ-1944: the product's domain, and the one it is moved out of when it exists.
        stored = await data_product_repo.get(conn, input.id)
        refusal = data_product_edit_refusal(
            _identity_from_info(info),
            app_state,
            stored,
            {input.domain_id} | ({stored["domain_id"]} if stored is not None else set()),
        )
        if refusal is not None:
            raise PermissionError(refusal)
        if creating and stored is not None:
            return MutationResult(
                success=False,
                message=f"Data product {input.id!r} already exists",
                code="schema.data_product_id_taken",
                params={"data_product": input.id},
            )
        if not creating and stored is None:
            return MutationResult(
                success=False,
                message=f"Data product {input.id!r} not found",
                code="schema.data_product_not_found",
                params={"data_product": input.id},
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
            # Absent or null is "no custom properties" (DataProductInput says why).
            custom_properties=({} if input.custom_properties is None else input.custom_properties),
        )
        await data_product_repo.upsert(conn, model)
    if creating:
        return MutationResult(
            success=True,
            message=f"Data product {input.id!r} created",
            code="schema.data_product_created",
            params={"data_product": input.id},
        )
    return MutationResult(
        success=True,
        message=f"Data product {input.id!r} updated",
        code="schema.data_product_updated",
        params={"data_product": input.id},
    )


async def _save_role(
    info: StrawberryInfo, input: RoleInput, *, creating: bool
) -> MutationResult:  # REQ-042, REQ-059, REQ-060, REQ-215, REQ-1531
    """A role saved: a create makes a new one, an update changes one that exists."""
    # REQ-1531: a role carries both capabilities and domain_access (REQ-1530), so minting one is
    # how a member would widen their own scope. It is the org_admin's act, gated by the right
    # that administers members.
    from provisa.api.admin.capabilities import require_capability
    from provisa.core.models import Role as RoleModel

    require_capability(info, "user_management")
    from provisa.core.db import STANDARD_ROLES
    from provisa.core.models import RoleRateLimit
    from provisa.core.repositories import role as role_repo

    if input.id in STANDARD_ROLES:  # REQ-2155
        return MutationResult(
            success=False,
            message=f"Role {input.id!r} is a standard role. Extend it with a role of your own "
            "that inherits from it.",
            code="schema.role_standard_locked",
            params={"role": input.id},
        )

    # REQ-1682: each constant session value is refused by name for what is wrong with it.
    from provisa.auth.attributes import normalize_name, session_constant_problem

    session_vars: dict[str, str] = {}
    for pair in input.session_vars:
        problem = session_constant_problem(pair.name)
        normalized = normalize_name(pair.name)
        named = pair.name
        if problem is None and normalized in session_vars:
            problem = ("name_repeated", f"Session value '{normalized}' is named twice")
            named = normalized
        if problem is not None:
            return MutationResult(
                success=False,
                message=problem[1],
                code=f"schema.role_session_var_{problem[0]}",
                params={"role": input.id, "name": named},
            )
        session_vars[normalized] = pair.value

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
    # REQ-2155: each parent must exist, be another role, be named once and not close a cycle.
    from provisa.security.inheritance import parent_map, parents_problem

    parents = list(input.parents)
    async with pool.acquire() as conn:
        existing = await role_repo.list_all(cast("Connection", conn))
    problem = parents_problem(input.id, parents, parent_map(existing))
    if problem is not None:
        return MutationResult(
            success=False,
            message=problem,
            code="schema.role_parent_invalid",
            params={"role": input.id, "parents": parents, "reason": problem},
        )
    # REQ-042/REQ-1337: the same two refusals the REST surface makes — an unknown capability,
    # and a platform right (the role's own or an ancestor's) defined by a caller who is not a
    # platform administrator.
    from provisa.api.admin._platform_guard import role_definition_problem
    from provisa.security.inheritance import effective_capabilities

    inherited = sorted({c for p in parents for c in effective_capabilities(p, existing)})
    definition_problem = role_definition_problem(
        info.context["request"],
        input.capabilities,
        inherited,
        role_id=input.id,
        domain_access=input.domain_access,
    )
    if definition_problem is not None:
        return MutationResult(
            success=False,
            message=str(definition_problem.detail),
            code=definition_problem.code,
            params={"role": input.id, **definition_problem.params},
        )
    # REQ-1531: a role hands out reach; whoever defines it must hold every domain the
    # definition adds to what the role reaches (its own list: reach is not inherited, REQ-2155).
    from provisa.api.admin.capabilities import require_reach_of_added_domains

    held = next((r for r in existing if r["id"] == input.id), None)
    # A create makes a new role and an update changes one: an id held by another role, or one no
    # role holds, is refused by name. A refused definition was refused above, for what it is.
    if creating and held is not None:
        return MutationResult(
            success=False,
            message=f"Role {input.id!r} already exists",
            code="schema.role_id_taken",
            params={"role": input.id},
        )
    if not creating and held is None:
        return MutationResult(
            success=False,
            message=f"Role {input.id!r} not found",
            code="schema.role_not_found",
            params={"role": input.id},
        )
    if not input.description.strip():  # REQ-2080
        return MutationResult(
            success=False,
            message=f"Role {input.id!r} needs a description of what it is for",
            code="schema.role_description_missing",
            params={"role": input.id},
        )
    if held is not None:
        # REQ-1531: redefining a role is an act in every domain it reaches now, not only in
        # the ones the change adds.
        require_right_in_domains(info, "user_management", held["domain_access"])
    require_reach_of_added_domains(
        info, None if held is None else held["domain_access"], input.domain_access
    )
    model = RoleModel(
        id=input.id,
        description=input.description,  # REQ-2080
        capabilities=input.capabilities,
        domain_access=input.domain_access,
        rate_limit=rate_limit,
        parents=parents,
        residency_values=input.residency_values,  # REQ-1921
        session_vars=session_vars,  # REQ-1682
        inactivity_days=input.inactivity_days,  # REQ-2077
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
    if creating:
        return MutationResult(
            success=True,
            message=f"Role {input.id!r} created",
            code="schema.role_created",
            params={"role": input.id},
        )
    return MutationResult(
        success=True,
        message=f"Role {input.id!r} updated",
        code="schema.role_updated",
        params={"role": input.id},
    )


async def _save_tag(
    info: StrawberryInfo, input: TagInput, *, creating: bool
) -> MutationResult:  # REQ-1373, REQ-1375, REQ-1944
    """A tag saved: a create makes a new one, an update changes one that exists."""
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
    # REQ-1415: the id is listed in the catalog description suffix, so it is a slug: it can
    # never hold a comma or bracket, and no surface needs an escaping rule.
    from provisa.core.modeling_tags import TAG_SLUG_CHARSET, is_tag_slug

    if not is_tag_slug(input.id):
        return MutationResult(
            success=False,
            message=f"Tag id {input.id!r} may only contain {TAG_SLUG_CHARSET}",
            code="schema.tag_id_not_slug",
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
        # A create makes a new tag and an update changes one: an id another tag holds, or one
        # no tag holds, is refused by name, and nothing is written.
        if creating and existing is not None:
            return MutationResult(
                success=False,
                message=f"Tag {input.id!r} already exists",
                code="schema.tag_id_taken",
                params={"tag": input.id},
            )
        if not creating and existing is None:
            return MutationResult(
                success=False,
                message=f"Tag {input.id!r} not found",
                code="schema.tag_not_found",
                params={"tag": input.id},
            )
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


async def _save_metric(
    info: StrawberryInfo, input: MetricInput, *, creating: bool
) -> MutationResult:  # REQ-1317
    """A governed metric definition saved (REQ-1317): a create makes a new one, an update
    replaces one that exists. The expression must parse under sqlglot and contain at least one
    aggregate function -- hard error otherwise."""
    from provisa.api.admin.capabilities import require_capability
    from provisa.core.models import Metric as MetricModel
    from provisa.core.repositories import metric as metric_repo

    require_capability(info, "table_registration")
    from provisa.api.admin.capabilities import acting_role_of
    from provisa.security.default_grant import default_read_grant

    # REQ-1317: a metric created naming no role is read by the acting role and Viewer; an update
    # naming none keeps the roles it has (filled in below, from the stored definition).
    named = None if input.visible_to is None else list(input.visible_to)
    try:
        model = MetricModel(
            name=input.name,
            expression=input.expression,
            datatype=input.datatype,
            description=input.description,
            ai_context=input.ai_context,
            visible_to=named
            if named is not None
            else (default_read_grant(acting_role_of(info)) if creating else []),
        )
    except ValueError as e:  # pydantic name validation (snake_case)
        return MutationResult(success=False, message=str(e))
    from provisa.api.admin.domain_guard import (
        expression_domains,
        metric_domains,
        require_domains,
    )

    pool = await _get_pool()
    try:
        async with pool.acquire() as conn:
            # REQ-1531: a metric is an object of every domain its expression reads. Saving one
            # needs the domains the NEW expression reads, and those of the definition it
            # replaces -- replacing it is an act on that one too.
            reads = await expression_domains(cast("Connection", conn), input.name, input.expression)
            replaced = await metric_domains(cast("Connection", conn), input.name)
            # A create makes a new metric and an update changes one: a name another metric
            # holds, or one no metric holds, is refused by name, and nothing is written.
            if creating and replaced is not None:
                return MutationResult(
                    success=False,
                    message=f"Metric {input.name!r} already exists",
                    code="schema.metric_name_taken",
                    params={"metric": input.name},
                )
            if not creating and replaced is None:
                return MutationResult(
                    success=False,
                    message=f"Metric {input.name!r} not found",
                    code="schema.metric_not_found",
                    params={"metric": input.name},
                )
            require_domains(info, reads | (replaced or set()))
            if named is None and not creating:
                stored = await metric_repo.get(cast("Connection", conn), input.name)
                assert stored is not None  # replaced is not None: the metric exists
                model = model.model_copy(update={"visible_to": list(stored["visible_to"])})
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


def _cardinality_problem(input: RelationshipInput) -> "MutationResult | None":
    from provisa.core.models import Cardinality

    try:
        Cardinality(input.cardinality)
    except ValueError:
        return MutationResult(
            success=False,
            message=f"Invalid cardinality: {input.cardinality!r}",
            code="schema.invalid_cardinality",
            params={"cardinality": input.cardinality},
        )
    return None


async def save_relationship(
    input: RelationshipInput, *, owner: str | None, needs_review: bool, creating: bool
) -> MutationResult:  # REQ-019, REQ-020, REQ-1948
    """Store a relationship whose author has already been authorized: by holding the right in
    the source table's domain (the mutation above), or by the approvals of the domains the
    relationship touches (REQ-1948). ``needs_review`` flags an edge the target's domain has not
    seen (REQ-1531). ``creating``: a new relationship, whose id must be free; else one that
    exists, changed."""
    from provisa.core.models import Relationship as RelModel, Cardinality
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.term_tags import (
        TagLossRefused,
        TermTagRefused,
        inherited_snapshot,
        tag_changes_of_moves,
    )

    _invalid = _cardinality_problem(input)
    if _invalid is not None:
        return _invalid
    pool = await _get_pool()
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
        same_entity=input.same_entity,  # REQ-2141
        # REQ-1586: the junction declaration travels with the edge on save.
        via_table=input.via_table,
        via_source_column=input.via_source_column,
        via_target_column=input.via_target_column,
        via_type_column=input.via_type_column or None,
        via_type_value=input.via_type_value or None,
        via_label_source=input.via_label_source,
        # REQ-2099: the parameters the edge feeds travel with it on save.
        parameters={p.parameter: p.source_column for p in input.parameters},
        owner=owner,
    )
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        from provisa.api.admin._fake_guard import relationship_fake_refusal

        _refusal, _src_row, _tgt_row = await _relationship_tables(_conn, input)
        if _refusal is None:
            _refusal = await _relationship_id_refusal(_conn, input, creating=creating)
        if _refusal is None:
            assert _src_row is not None  # _relationship_tables refuses an unresolved source
            _refusal = await _relationship_alias_refusal(_conn, input, _src_row["id"])
        if _refusal is None and input.same_entity:
            assert _src_row is not None
            _refusal = await _same_entity_refusal(_conn, input, _src_row, _tgt_row)
        if _refusal is not None:
            return _refusal
        try:
            async with _conn.transaction():
                tags_before = await inherited_snapshot(_conn)  # REQ-2145
                await rel_repo.upsert(_conn, model)
                # REQ-1494: the edge's two columns, both faked, must declare one fake; the save
                # is undone when they do not.
                await relationship_fake_refusal(_conn, input.id)
                # REQ-2145: key refs it re-homed off a tagged term; undone when unconfirmed.
                await tag_changes_of_moves(
                    _conn,
                    tags_before,
                    confirm_tag_loss=input.confirm_tag_loss,
                    carry_tags=input.carry_tags,
                )
        except _FakeRefusedSave as refused:
            return refused.result
        except (TagLossRefused, TermTagRefused) as refused:
            return _tag_move_refusal(refused)
        if needs_review:
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


def _rule_refusal(code: str, message: str, params: "dict | None" = None) -> Any:
    from provisa.api.admin.relationship_approvals import Refusal

    return Refusal(code, message, dict(params or {}))


async def _relationship_decision_refusal(
    info: StrawberryInfo, req: dict, *, executing: bool
) -> "MutationResult | None":  # REQ-1948
    """Why the caller may not reject (or, ``executing``, carry out) a relationship request.
    A refused attempt is written to the org's administrative trail."""
    from provisa.api.admin import relationship_approvals as rule

    involved, reach, user_id = await _request_decision_scope(info, req)
    refusal = rule.rejection_refusal(
        user_id=user_id,
        requested_by=req["requested_by"],
        involved=involved,
        reach=reach,
        tables=rule.tables_named(req["payload"]),
    )
    if refusal is None and executing:
        refusal = rule.incomplete_refusal(involved, req["approvals"], req["requested_by"])
    if refusal is None:
        return None
    await _record_request_decision(info, req, "execute" if executing else "reject", refusal=refusal)
    return MutationResult(
        success=False, message=refusal.message, code=refusal.code, params=refusal.params
    )


async def _execution_refusal(info: StrawberryInfo, req: dict) -> "MutationResult | None":
    """Why the caller may not carry out ``req``, for every request type: executing is open to the
    users who may decide the request, and only once it has had its approvals."""
    from provisa.api.admin import relationship_approvals as rule
    from provisa.api.admin.capabilities import _identity_from_info, require_capability

    if req["request_type"] == rule.REQUEST_TYPE:
        return await _relationship_decision_refusal(info, req, executing=True)
    try:
        require_capability(info, req["capability"])
    except PermissionError as e:
        return MutationResult(success=False, message=str(e))
    identity = _identity_from_info(info)
    refusal = rule.repeat_refusal(
        user_id=getattr(identity, "user_id", None), requested_by=req["requested_by"], approvals=[]
    ) or rule.count_refusal(req["approvals"], req["requested_by"], req["required_approvals"])
    if refusal is None:
        return None
    await _record_request_decision(info, req, "execute", refusal=refusal)
    return MutationResult(
        success=False, message=refusal.message, code=refusal.code, params=refusal.params
    )


async def _request_decision_scope(
    info: StrawberryInfo, req: dict
) -> "tuple[frozenset[str], frozenset[str] | None, str | None]":
    """The domains the request touches, the caller's reach of the right, and the caller. Only a
    relationship request is decided by domains (REQ-1948); any other touches none here."""
    from provisa.api.admin import relationship_approvals as rule
    from provisa.api.admin.capabilities import _identity_from_info, right_reach
    from provisa.api.app import state

    identity = _identity_from_info(info)
    user_id = getattr(identity, "user_id", None)
    if req["request_type"] != rule.REQUEST_TYPE:
        return frozenset(), None, user_id
    pool = await _get_pool()
    async with pool.acquire() as conn:
        involved = await rule.domains_involved(cast("Connection", conn), req["payload"])
    return involved, right_reach(identity, state, rule.RIGHT), user_id


async def _record_request_decision(
    info: StrawberryInfo, req: dict, action: str, *, refusal: Any = None
) -> None:  # REQ-1948
    from provisa.api.admin import relationship_approvals as rule

    involved, reach, user_id = await _request_decision_scope(info, req)
    await rule.record(
        await _get_pool(),
        action=action,
        request=req,
        actor=user_id,
        involved=involved,
        reach=reach,
        refusal=refusal,
    )


async def perform_request_creation(info: StrawberryInfo, req: dict) -> MutationResult:
    """Create what a creation request asks for, through the code the direct mutation uses.

    The one place a request of any type is carried out, for the REST queue and the GraphQL
    mutation alike. A relationship is stored on its approvals' authority (REQ-1948); a view,
    table or source is created as the caller, under the caller's own gates; a webhook is exposed
    by the schema rebuild once its request is marked executed (REQ-209). The caller marks the
    request executed when this succeeds.
    """
    from provisa.api.admin.capabilities import _identity_from_info

    kind = req["request_type"]
    if kind == "relationship":
        caller = _identity_from_info(info)
        return await save_relationship(
            _rebuild_relationship_input(req["payload"]),
            owner=getattr(caller, "user_id", None),
            needs_review=False,
            creating=True,  # a request is for a new relationship
        )
    if kind in ("view", "table"):  # REQ-1792: "table" is the MCP-proposal kind
        table = _rebuild_table_input(req["payload"])
        # Same strawberry-decorator signature limitation as the other in-module mutation calls.
        return await Mutation().register_table(info, table)  # pyright: ignore[reportCallIssue]
    if kind == "source":  # REQ-1792
        source = _rebuild_source_input(req["payload"])
        return await Mutation().create_source(info, source)  # pyright: ignore[reportCallIssue]
    if kind == "webhook":
        # REQ-209: approving a webhook only requires marking this request executed (the caller
        # does) — the schema-build gate then exposes the webhook whose latest request is
        # executed. Verify the webhook still exists; the rebuild follows the mark.
        wh_name = req["payload"]["name"]
        pool = await _get_pool()
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
        return MutationResult(
            success=True,
            message=f"Approved webhook {wh_name!r}",
            code="schema.webhook_approved",
            params={"webhook": wh_name},
        )
    return MutationResult(
        success=False,
        message=f"Unknown request type {kind!r}",
        code="schema.unknown_request_type",
        params={"type": kind},
    )


async def request_carried_out(req: dict) -> None:
    """What follows marking a request executed: a webhook is exposed by the schema rebuild that
    reads its latest request's status (REQ-209), so the rebuild comes after the mark."""
    if req["request_type"] == "webhook":
        await _rebuild_schemas()


async def _require_tag_target_domains(
    info: StrawberryInfo, conn: "Connection", model
) -> "MutationResult | None":  # REQ-1531
    """Gate a tag assignment on its target's domains. A target that is not there is the caller's
    not-found, answered as one; a domain the caller does not reach raises."""
    from provisa.api.admin.domain_guard import (
        DomainLookupError,
        require_domains,
        tag_target_domains,
    )

    try:
        domains = await tag_target_domains(conn, model)
    except DomainLookupError as missing:
        return MutationResult(
            success=False,
            message=f"The object this tag assignment names does not exist: {missing}",
            code="schema.tag_target_not_found",
            params={"objectType": model.object_type},
        )
    require_domains(info, domains)
    return None


def _assignment_target_problem(model) -> "MutationResult | None":  # REQ-1377
    """Reject an assignment whose typed target fields don't match its object_type."""
    from provisa.core.models import TAG_OBJECT_TYPES

    required = {
        "source": model.source_id,
        "table": model.table_id,
        "column": model.table_id is not None and model.column_name,
        "relationship": model.relationship_id,
        "command": model.command_name,
        "term": model.term_id,  # REQ-2145
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
    term_id: int | None = None,
) -> None:
    """A sensitive tag on a column decides how its values are hidden: sensitive_data in the
    domain of the column's table (REQ-1944), and on a term in the domains of the columns it holds
    (REQ-2145). Any other assignment is a table editor's act."""
    if object_type == "term":  # REQ-2145: what the term's columns are hidden by
        from provisa.core.repositories import glossary as glossary_repo

        assert term_id is not None  # _assignment_target_problem: a term assignment names it
        if tag_row["sensitive"]:
            domains = (await glossary_repo.term_domains(conn)).get(term_id, set())
            require_right_in_domains(info, SENSITIVE_DATA, domains)
        else:
            require_capability(info, "table_registration")
        return
    if not (tag_row["sensitive"] and object_type == "column"):
        require_capability(info, "table_registration")
        return
    from provisa.api.admin.domain_guard import table_domain

    assert table_id is not None  # _assignment_target_problem: a column names its table
    require_right_in_domains(info, SENSITIVE_DATA, {await table_domain(conn, table_id)})


async def _on_columns_term(  # REQ-2145
    conn: "Connection", model: Any
) -> "tuple[Any, MutationResult | None]":
    """A column assignment made ``also_term``, as the assignment of its tag on the column's term
    instead -- or why it may not be: the column is a key (its ref follows its table's Class), or
    it has no term."""
    from provisa.core.repositories import glossary as glossary_repo

    column = f"{model.table_id}.{model.column_name}"
    if await glossary_repo.is_key_column(conn, model.table_id, model.column_name):
        return model, MutationResult(
            success=False,
            message=(
                f"Column {column} is a key: its meaning is its table's class, so a tag for every "
                "column it means goes on no term. Tag the column itself."
            ),
            code="tag.key_column",
            params={"tag": model.tag_id, "column": model.column_name},
        )
    term = await glossary_repo.get_term_by_ref(conn, model.table_id, model.column_name)
    if term is None:
        return model, MutationResult(
            success=False,
            message=f"Column {column} has no glossary term to tag",
            code="tag.column_has_no_term",
            params={"tag": model.tag_id, "column": model.column_name},
        )
    return (
        model.model_copy(
            update={
                "object_type": "term",
                "term_id": term["id"],
                "table_id": None,
                "column_name": None,
            }
        ),
        None,
    )


async def _require_term_tagger(  # REQ-2145
    info: StrawberryInfo, conn: "Connection", term_id: int
) -> None:
    """A tag on a term is curation of the term: the glossary's curation rights, as the glossary's
    own surface asks them."""
    from provisa.api.admin.glossary_router import _require_glossary_rw, _require_term_curatable

    request = info.context["request"]
    _require_glossary_rw(request)
    await _require_term_curatable(conn, term_id, request)


async def _term_tag_refused(  # REQ-2145
    conn: "Connection", model: Any
) -> "MutationResult | None":
    """Why a term may not take the tag (provisa.core.term_tags.refuse_term_tag), or None."""
    from provisa.core.term_tags import TermTagRefused, refuse_term_tag

    try:
        await refuse_term_tag(conn, model.term_id, model.tag_id)
    except TermTagRefused as refused:
        return MutationResult(
            success=False, message=str(refused), code=refused.code, params=refused.params
        )
    return None


def _tag_move_refusal(refused: Exception) -> "MutationResult":  # REQ-2145
    """A move refused over the tags its columns would lose (tag.move_loses, with each column's
    change), or because a lost tag could not be carried to the term a column moved to."""
    from provisa.core.term_tags import TagLossRefused

    if isinstance(refused, TagLossRefused):
        return MutationResult(
            success=False,
            message=str(refused),
            code=refused.code,
            params={"columns": refused.columns},
        )
    return MutationResult(
        success=False,
        message=str(refused),
        code=getattr(refused, "code"),
        params=getattr(refused, "params"),
    )


async def _covers(conn: "Connection", model: Any) -> int | None:  # REQ-2145
    """For a column, how many other columns its term holds -- those a tag on the term would
    also cover; for a term, how many columns it holds."""
    from provisa.core.repositories import glossary as glossary_repo

    if model.object_type == "term":
        return await glossary_repo.term_column_count(conn, model.term_id)
    if model.object_type != "column":
        return None
    term = await glossary_repo.get_term_by_ref(conn, model.table_id, model.column_name)
    if term is None:
        return 0
    return await glossary_repo.term_column_count(conn, term["id"]) - 1


async def store_tag_assignment(  # REQ-1375, REQ-1377, REQ-1443, REQ-1467
    conn: "Connection", model: Any, tag_row: dict
) -> "MutationResult | None":
    """Put ``model``'s tag on its object, or say why it may not be put there: the tag's own
    rules (a derived tag, its reason, expiry and value policies, what it applies to), then the
    one write. THE writer of a tag assignment: ``assignTag`` calls it once the caller's right
    is established."""
    from provisa.core.repositories import tag as tag_repo

    # REQ-1443: a derived tag reports state the table already carries, so assigning it
    # would either duplicate that state or contradict it — the registration is the only
    # way to change it.
    if tag_row["derived"]:
        return MutationResult(
            success=False,
            message=(
                f"Tag {model.tag_id!r} is derived from the object's own registration "
                "and cannot be assigned"
            ),
            code="schema.tag_derived_immutable",
            params={"tag": model.tag_id},
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
                message=f"Tag {model.tag_id!r} requires {field_name}",
                code="schema.tag_reason_required"
                if field_name == "reason"
                else "schema.tag_expires_required",
                params={"tag": model.tag_id, "field": field_name},
            )
        if policy == "hidden" and value is not None:
            return MutationResult(
                success=False,
                message=f"Tag {model.tag_id!r} does not take {field_name}",
                code="schema.tag_field_hidden",
                params={"tag": model.tag_id, "field": field_name},
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
                    f"Tag {model.tag_id!r} must be assigned with a value, as {model.tag_id}:<value>"
                ),
                code="schema.tag_param_required",
                params={"tag": model.tag_id},
            )
        permitted = {
            p["value"]
            for p in await tag_repo.list_param_values(cast("Connection", conn), model.tag_id)
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
    if model.object_type not in list(tag_row["applies_to"] or []):
        return MutationResult(
            success=False,
            message=(f"Tag {model.tag_id!r} does not apply to {model.object_type!r} objects"),
            code="schema.tag_scope_mismatch",
            params={"tag": model.tag_id, "objectType": model.object_type},
        )
    await tag_repo.assign(cast("Connection", conn), model)
    return None


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
            term_id=r["term_id"],  # REQ-2145
            term=r["term"],
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
                "window, a cache_ttl cadence, or a probing change_signal (probe/ttl_probe)"
            ),
            code="schema.load_protection_gate_required",
            params={"who": who},
        )
    return None


def _engine_name() -> str:
    """The federation engine the acting org runs on."""
    from provisa.api.app import state

    return state.federation_engine.name


def _validate_source_load_management(input: SourceInput) -> "MutationResult | None":  # REQ-1909
    """The "Load Management and Timeliness" panel's source settings, validated at write time by the
    same rules the config loader and the query path apply: a known change_signal (REQ-929), a
    non-negative cache_ttl, a live cap of at least 1 (REQ-1909), a sentinel URL with a supported
    scheme (REQ-1148), a freshness gate the source's signal can build (REQ-860), a parseable
    off-peak window (REQ-1141), and at least one refresh gate when load protected (REQ-1141).
    Returns a failing MutationResult naming the rule, else None."""
    from provisa.core.change_signal import resolve as _resolve_signal
    from provisa.core.change_signal import source_change_signal

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
    # REQ-1907/REQ-929: a push source type cannot be polled — an explicit non-push change_signal is
    # refused by name (an unset one resolves to the type's push signal, applied at model build).
    try:
        source_change_signal(input.type, input.change_signal)
    except ValueError as e:
        return _fail("schema.change_signal_not_push", str(e))
    from provisa.core.change_signal import native_signal_refusal

    _no_receiver = native_signal_refusal(input.type, input.change_signal)
    if _no_receiver is not None:
        return MutationResult(
            success=False,
            message=f"Source {input.id!r}: {_no_receiver}",
            code="schema.change_signal_no_receiver",
            params={"source": input.id, "type": input.type, "signal": input.change_signal},
        )
    # A mail source's tables have no probe query of their own: a probing signal on the source
    # is honoured only by the kind's own check for changes, once that is wired
    # (probes.LOADER_PROBE_TYPES). Until then it is refused by name rather than saved and
    # never asked.
    from provisa.core import mail_platforms
    from provisa.events.probes import LOADER_PROBE_TYPES

    if (
        input.change_signal in ("probe", "ttl_probe")
        and input.type in mail_platforms.source_types()
        and input.type not in LOADER_PROBE_TYPES
    ):
        return MutationResult(
            success=False,
            message=(
                f"Source {input.id!r}: a {input.type} source cannot yet be checked for changes, "
                f"so change_signal {input.change_signal!r} would never be asked; use ttl"
            ),
            code="schema.change_signal_not_honoured",
            params={"source": input.id, "type": input.type, "signal": input.change_signal},
        )
    from provisa.core.cache_ttl import cache_ttl_refusal  # REQ-2031

    _ttl_err = cache_ttl_refusal(input.cache_ttl)
    if _ttl_err is not None:
        return _fail("schema.invalid_cache_ttl", _ttl_err)
    if input.max_live_concurrency is not None and input.max_live_concurrency < 1:
        return _fail(
            "schema.max_live_concurrency_invalid",
            "max_live_concurrency must be 1 or more (leave it empty for no cap; to stop live "
            "reads use replicate 0 (always) or load_protected)",
        )
    if input.type == "postgresql" and _engine_name() == "duckdb":
        # REQ-1909: the DuckDB engine divides the cap over the workers' connection pools, so a cap
        # below the worker count is refused here, at save, rather than at the next attach.
        from provisa.federation.connector_duckdb import PoolCapBelowWorkers, pg_pool_max_connections

        try:
            pg_pool_max_connections(input.id, input.max_live_concurrency)
        except PoolCapBelowWorkers as e:
            return _fail("schema.max_live_concurrency_below_workers", str(e))
    if input.sentinel_path:
        from provisa.events.sentinel_probe import applies_to, build_sentinel_probe

        if not applies_to(input.type):
            return MutationResult(
                success=False,
                message=(
                    f"Source {input.id!r}: a {input.type} source has no sentinel: nothing of "
                    "yours produces its data and could mark a change, or the source is not probed"
                ),
                code="schema.sentinel_path_not_applicable",
                params={"source": input.id, "type": input.type},
            )
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
    business_key: list[str] | None,
) -> bool:  # REQ-1170
    """True when load protection (off-peak window) AND snapshotting are both configured on one
    table — their timing can fight (a snapshot boundary may fall outside the off-peak window), so
    the caller emits a WARNING (not a block)."""
    return bool((load_protected or off_peak_window) and business_key)


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


def _residency_refusal(
    info, what: str, before: object, after: str | None, *, draft: bool = False
) -> MutationResult | None:  # REQ-1921
    """The refusal of a region change the caller's data_residency grant does not cover, naming
    the value; None when it may be made (``capabilities.require_residency_change``)."""
    from provisa.api.admin.capabilities import require_residency_change
    from provisa.security.residency import ResidencyRefused

    try:
        require_residency_change(info, what, before, after, draft=draft)
    except ResidencyRefused as refused:
        return MutationResult(
            success=False, message=str(refused), code=refused.code, params=refused.params
        )
    return None


async def _delete_source(id: str, reader: "DependentsReader") -> MutationResult:
    """Delete source ``id`` (the deleteSource mutation, its capability already checked); a
    refusal names only what ``reader`` may read (REQ-2152)."""
    from provisa.core.repositories import source as source_repo
    from provisa.api.app import state

    pool = await _get_pool()
    async with pool.acquire() as conn:
        _kind = await source_repo.get(cast("Connection", conn), id)
    if _kind is not None and _kind["type"] == "servicenow":
        # REQ-1954: its APIs' OpenAPI source goes first, and a dependent of that one stops
        # the ServiceNow source's deletion too.
        from provisa.openapi.servicenow import companion_id

        _companion = await _delete_source(companion_id(id), reader)
        if not _companion.success and _companion.code != "schema.source_not_found":
            return _companion
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        # REQ-1695: read the reference before the row goes, so the vault entry it names can be
        # removed with it. A source whose credential outlived it is a credential nothing owns.
        _existing = await source_repo.get(_conn, id)
        try:
            deleted = await source_repo.delete(_conn, id)
        except source_repo.SourceDeleteRefused as refused:
            from provisa.api.admin.read_rights import refusal

            # REQ-2152: only the dependents the caller may read, never how many.
            _said, _named = await refusal(_conn, reader, refused)
            # REQ-1918: nothing is removed; every dependent is named.
            return MutationResult(
                success=False,
                message=_said,
                code=(
                    "schema.source_has_dependents"
                    if refused.reason == "dependents"
                    else "schema.source_is_system"
                ),
                params={
                    "source": id,
                    "dependents": _named,
                },
            )
    if deleted:
        assert _existing is not None  # delete reported a row, so get found one
        state.graphql_remote_sources.pop(id, None)
        # REQ-1730: was never called here at all — see _drop_source_on_engine's own comment for
        # the exact orphaned-catalog accumulation this closes.
        _drop_source_on_engine(state, id)
        state.source_catalogs.pop(id, None)
        from provisa.core import source_uploads

        if _existing["type"] in source_uploads.UPLOAD_TYPES:  # REQ-1997: its upload goes too
            source_uploads.remove(id)
        await _rebuild_schemas()
        if _existing["type"] == "data_profiler":  # REQ-1934: its schedule goes with it
            from provisa.api.admin.schema_mutation_ops import reschedule_org_triggers

            await reschedule_org_triggers()
        if _existing["type"] in ("node", "python"):  # REQ-1984, REQ-1993: its workers go
            from provisa.codesource.source import forget

            forget(id)
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


async def _register_draft_table(
    info: StrawberryInfo, input: SourceInput
) -> MutationResult | None:  # REQ-2113
    """A single-file CSV or Parquet source's table, set up with the source as a draft (out
    of service until a steward takes it out of draft, REQ-1921): named after the source and
    typed from its file as Register Table proposes it, every role seeing every column. None
    when it is set up; else the source stands and why its table was not set up is said."""
    from provisa.api.admin.schema_query import resolve_available_columns_metadata
    from provisa.api.app import state
    from provisa.core.source_downloads import DOWNLOAD_TYPES

    def _without(reason: str) -> MutationResult:
        return MutationResult(
            success=True,
            message=f"Source {input.id!r} created; its table was not set up: {reason}",
            code="schema.source_created_without_table",
            params={"source": input.id, "reason": reason},
        )

    if input.type not in DOWNLOAD_TYPES:
        return _without(f"a {input.type} source has no single table to set up")
    if not input.table_domain_id:
        return _without("no domain was chosen for it")
    columns = await resolve_available_columns_metadata(input.id, "main", input.id)
    if not columns:
        return _without("its file has no columns")
    import re

    def _snake(name: str) -> str:
        # As the Register Table form names a column's alias (naming.ts toSnakeCase).
        name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()

    # What the Register Table form gives a new table by default: every role sees each column,
    # in domain scope (REQ-1959), unmasked, aliased snake_case where its name is not.
    roles = sorted(state.roles or {})
    table = TableInput(
        source_id=input.id,
        domain_id=input.table_domain_id,
        schema_name="main",
        table_name=input.id,
        columns=[
            ColumnInput(
                name=c.name,
                visible_to=roles,
                alias=_snake(c.name) if _snake(c.name) != c.name else None,
                data_type=c.data_type,
                description=c.comment,
            )
            for c in columns
        ],
    )
    result = await Mutation().register_table(info, table)  # pyright: ignore[reportCallIssue]
    return None if result.success else _without(result.message)


@strawberry.type
class Mutation:  # REQ-012, REQ-013, REQ-016, REQ-042
    @strawberry.mutation
    async def set_servicenow_api(
        self, info: StrawberryInfo, source_id: str, namespace: str, name: str, added: bool
    ) -> MutationResult:
        """Offer one of a ServiceNow instance's APIs through its source's companion OpenAPI
        source, or stop offering it (REQ-1954)."""
        require_capability(info, "source_registration")
        from provisa.api.admin._servicenow_companion import set_api

        try:
            await set_api(source_id, namespace, name, added)
        except LookupError as exc:
            return MutationResult(
                success=False,
                message=str(exc).strip("'\""),
                code="schema.servicenow_api_unknown",
                params={"source": source_id, "api": f"{namespace}/{name}"},
            )
        return MutationResult(
            success=True,
            message=f"{namespace}/{name} {'added to' if added else 'removed from'} {source_id!r}",
            code="schema.servicenow_api_set",
            params={"source": source_id, "api": f"{namespace}/{name}"},
        )

    @strawberry.mutation
    async def set_servicenow_import_set(
        self, info: StrawberryInfo, source_id: str, table_name: str, import_set: str, added: bool
    ) -> MutationResult:
        """Offer a typed insert into one of a ServiceNow instance's import sets through its
        source's companion OpenAPI source, or stop offering it (REQ-1954)."""
        require_capability(info, "source_registration")
        from provisa.api.admin._servicenow_companion import set_import_set

        try:
            await set_import_set(source_id, table_name, import_set, added)
        except LookupError as exc:
            return MutationResult(
                success=False,
                message=str(exc).strip("'\""),
                code="schema.servicenow_import_set_unknown",
                params={"source": source_id, "importSet": import_set},
            )
        return MutationResult(
            success=True,
            message=f"import set {import_set!r} {'added to' if added else 'removed from'} "
            f"{source_id!r}",
            code="schema.servicenow_import_set_set",
            params={"source": source_id, "importSet": import_set},
        )

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
        """Fire a checker table's scan job immediately instead of waiting for its interval. The
        table is named by its registered id, or by schema and table name (refused when more than
        one source registers that name).

        Reuses the scan job the org's scheduler runs every cache_ttl seconds — this does not
        re-scan into the response like the dry run; it appends the scan's rows to the table's
        history, so the DQ check detail shows the new scan beside the earlier ones."""
        require_capability(info, "table_registration")
        from provisa.api.app import state
        from provisa.api.admin._dq_resolvers import run_dq_check_now as _run_now
        from provisa.core.request_context import require_current_org

        # REQ-1266: the scheduler namespaces a scan job's id by the org bound when it was
        # registered (register_org_checkers), and the boot and every request bind theirs, so the
        # bound org names the job this request registered.
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
        from provisa.core.repositories import calendar as calendar_repo
        from provisa.events.calendars import InvalidCalendar, calendar_of

        fields = {
            "name": input.name,
            "version": input.version,
            "base_system": input.base_system,
            "tz": input.tz,
            "fiscal_anchor_month": input.fiscal_anchor_month,
            "fiscal_anchor_day": input.fiscal_anchor_day,
            "retail_anchor": input.retail_anchor,
            "week_start": input.week_start,
            "holidays": input.holidays,
            "weekend": input.weekend,
            "day_close": input.day_close or None,
        }
        try:
            # REQ-2120: the one validation every surface declaring a calendar applies.
            cal = calendar_of(fields)
        except InvalidCalendar as e:
            return MutationResult(
                success=False,
                message=str(e),
                code="schema.invalid_calendar",
                params={"error": e.reason},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await calendar_repo.upsert(
                cast("Connection", conn), {**fields, "retail_anchor": cal.retail_anchor}
            )
        return MutationResult(
            success=True,
            message=f"calendar {input.name!r} {input.version} saved",
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
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(_conn, reader_for(info), refused)
                return MutationResult(
                    success=False,
                    message=_said,
                    code="schema.calendar_in_use",
                    params={
                        "calendar": name,
                        "dependents": _named,
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
        from provisa.core.repositories import source as _source_repo

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
                    input.freshness_gate,
                ),
                source_type=input.type,
                federation_hints=_federation_hints_from_input(input),  # REQ-1867
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

        from provisa.security.residency import CREATED

        _residency = _residency_refusal(info, f"source {input.id}", CREATED, input.region)
        if _residency is not None:  # REQ-1921
            return _residency

        # A create makes a new source. A definition that is refused is refused for what it is
        # (above); a valid one whose id exists is refused by name, before anything is staged,
        # connected or written. Changing a source, and provisioning it again on another engine,
        # is updateSource.
        async with (await _get_pool()).acquire() as _taken_conn:
            if await _source_repo.get(cast("Connection", _taken_conn), input.id) is not None:
                return MutationResult(
                    success=False,
                    message=f"Source {input.id!r} already exists",
                    code="schema.source_id_taken",
                    params={"source": input.id},
                )

        pool = await _get_pool()
        if input.type == "govdata":
            _err = await _another_govdata_source(pool, input) or await _validate_govdata_api_key(
                input
            )
            if _err is not None:
                return _err

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
                # REQ-1960: a Wikipedia source is the files source that crawls it, from here on.
                _wikipedia_refusal = _expand_wikipedia_if_needed(input)
                if _wikipedia_refusal is not None:
                    return _wikipedia_refusal
                # REQ-1923: a Google Workspace source's settings are checked before it exists.
                _workspace_refusal = google_workspace_refusal(
                    input
                ) or await google_workspace_organisation_refusal(input)
                if _workspace_refusal is not None:
                    return _workspace_refusal
                _m365_refusal = microsoft_365_refusal(
                    input
                ) or await microsoft_365_organisation_refusal(input)  # REQ-1923
                if _m365_refusal is not None:
                    return _m365_refusal
                # REQ-2113: a CSV or Parquet source given its file by download has it copied
                # into the org's folder first, so its path names the copy when it is checked.
                _logs_refusal = (
                    await download_source_file(input)
                    or logs_refusal(input)
                    or file_source_path(input)
                )  # REQ-1996/1997/2113
                if _logs_refusal is not None:
                    return _logs_refusal
                # REQ-2006: a sandbox org's source is checked BEFORE the connection that validates
                # it is opened -- validating it would already reach where it points.
                _egress_refusal = sandbox_destination_refusal(input)
                if _egress_refusal is not None:
                    return _egress_refusal
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

        # Each credential is kept as the operator entered it (a value, or a reference they
        # typed) and resolved where it is used.
        _registry_refusal = refuse_registry_settings(input)  # REQ-1951
        if _registry_refusal is not None:
            return _registry_refusal
        password_ref = input.password or ""
        _mapping = _parse_mapping_json(input.mapping_json)
        _profiler_refusal = _refuse_invalid_profiler(input.id, input.type, _mapping)  # REQ-1934
        if _profiler_refusal is not None:
            return _profiler_refusal
        from provisa.api.admin._code_source_registration import record_code

        _code_refusal = await record_code(input.id, input.type, _mapping)  # REQ-1984, REQ-1993
        if _code_refusal is not None:
            return _code_refusal
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
                _mapping = checker_mapping()
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
            region=input.region,  # REQ-1921: its new tables' starting region
            max_live_concurrency=input.max_live_concurrency,  # REQ-1909
            sentinel_path=input.sentinel_path,  # REQ-1148
            download_from=input.download_from,  # REQ-2113
            freshness_gate=input.freshness_gate,  # REQ-860
            cdc=await stored_cdc(info, input),  # REQ-1951
        )

        # REQ-1531: a source with no allowed list is open to every domain, so opening it to a
        # domain — or to all of them — needs the caller to reach that domain.
        from provisa.api.admin.capabilities import require_reach_of_added_domains

        _named = [d for d in (input.allowed_domains or []) if d.strip()]
        require_reach_of_added_domains(info, None, _named, empty_is_all=True)
        try:
            await _upsert_source_with_domains(pool, model, input, create=True)
        except _source_repo.SourceIdTaken as taken:
            # Its id was free when judged above and was taken since, by another create of the
            # same source (two devices of one person at once): refused by name, never overwritten.
            await state.source_pools.remove(input.id)
            return MutationResult(
                success=False,
                message=str(taken),
                code="schema.source_id_taken",
                params={"source": input.id},
            )

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
            # REQ-1946: a SharePoint or Salesforce source's pgwire server takes its writes on
            # every engine, so it is started here rather than by an engine's attach.
            from provisa.api.data.pgwire_write import start_write_server

            start_write_server(model)
            # An AskAmerica server takes minutes to mount its schemas: started at save too.
            from provisa.federation.pgwire_replica import start_when_saved

            start_when_saved(model)
        await _analyze_source_on_engine(state, pool, model, input)

        _fire_catalog_indexing(state, pool, input)

        if input.type == "servicenow":  # REQ-1954: its APIs' OpenAPI source, kept beside it
            from provisa.api.admin._servicenow_companion import sync_companion

            async with pool.acquire() as _companion_conn:
                await sync_companion(cast("Connection", _companion_conn), input.id)
        await _rebuild_schemas()

        if input.type == "data_profiler":  # REQ-1934: its schedule fires on the org's scheduler
            from provisa.api.admin.schema_mutation_ops import reschedule_org_triggers

            await reschedule_org_triggers()
        if input.register_table_draft:  # REQ-2113
            refused = await _register_draft_table(info, input)
            if refused is not None:
                return refused
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
                    input.freshness_gate,
                ),
                federation_hints=_federation_hints_from_input(input),  # REQ-1867
            )
            if _ttl_refusal is not None:
                return _ttl_refusal
            from provisa.api.admin._change_feed import source_change_feed_refusal

            _feed_refusal = await source_change_feed_refusal(_conn, input)  # REQ-1861
            if _feed_refusal is not None:
                return _feed_refusal
            _registry_refusal = refuse_registry_settings(input)  # REQ-1951
            if _registry_refusal is not None:
                return _registry_refusal
            # REQ-1960: an edited Wikipedia source's crawl is made again from what was chosen.
            _wikipedia_refusal = _expand_wikipedia_if_needed(input)
            if _wikipedia_refusal is not None:
                return _wikipedia_refusal
            _workspace_refusal = google_workspace_refusal(
                input
            ) or await google_workspace_organisation_refusal(input)  # REQ-1923
            if _workspace_refusal is not None:
                return _workspace_refusal
            _m365_refusal = microsoft_365_refusal(
                input
            ) or await microsoft_365_organisation_refusal(input)  # REQ-1923
            if _m365_refusal is not None:
                return _m365_refusal
            # REQ-2113: a new address is downloaded again; the same one keeps the copy it made
            # (its TTL fetches it again), so a save that changes nothing else copies nothing.
            _fetch = input.download_from and input.download_from != existing.get("download_from")
            _logs_refusal = (
                (await download_source_file(input) if _fetch else None)
                or logs_refusal(input)
                or file_source_path(input)
            )  # REQ-1996/1997/2113
            if _logs_refusal is not None:
                return _logs_refusal
            # As entered: a value, or a reference the operator typed.
            password_ref = input.password or ""
            _stored_mapping = _parse_mapping_json(input.mapping_json)
            _profiler_refusal = _refuse_invalid_profiler(  # REQ-1934
                input.id, input.type, _stored_mapping
            )
            if _profiler_refusal is not None:
                return _profiler_refusal
            from provisa.api.admin._code_source_registration import record_code

            _code_refusal = await record_code(  # REQ-1984, REQ-1993
                input.id, input.type, _stored_mapping
            )
            if _code_refusal is not None:
                return _code_refusal
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
                mapping=_stored_mapping,
                federation_hints=_federation_hints_from_input(input),
                change_signal=input.change_signal,
                load_protected=input.load_protected,  # REQ-1141
                off_peak_window=input.off_peak_window,  # REQ-1141
                off_peak_tz=input.off_peak_tz,  # REQ-1141
                cache_enabled=input.cache_enabled,
                cache_ttl=input.cache_ttl,
                replicate=input.replicate,  # REQ-826
                # REQ-1921: kept; a source's region changes only through setSourceRegion.
                region=existing["region"],
                max_live_concurrency=input.max_live_concurrency,  # REQ-1909
                sentinel_path=input.sentinel_path,  # REQ-1148
                download_from=input.download_from,  # REQ-2113
                freshness_gate=input.freshness_gate,  # REQ-860
                cdc=await stored_cdc(info, input),  # REQ-1951
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

        await restart_source_server(model)

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
                        # The connection extras the standard arguments cannot carry (an Exasol
                        # certificate pin, a warehouse's account/http_path), as create_source's
                        # pool and the boot pool pass them: without them an edited source dialled
                        # unpinned and its discovery failed (REQ-1731, REQ-986/987/988).
                        extra={k: resolve_secrets(v) for k, v in model.federation_hints.items()},
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

        # REQ-1730: the source is provisioned again on the engine bound now -- the one it was saved
        # under, or another one swapped to since -- with the settings just saved.
        async with bound_to_request_org():
            await _synthesize_mapping_dsl_tables(pool, model)
            await _cache_prometheus_label_columns(pool, state, model)
            _register_source_on_engine(state, model, input)
        # Its tables already registered compile against the catalog name just recorded, and a
        # table whose rows the engine lands gets its replica on this engine (a swap leaves none:
        # airport/firebird SCHEMA_NOT_FOUND on Trino, reproduced live).
        await _rebuild_schemas()
        try:
            await state.federation_engine.reconcile_landed_tables()
        except Exception:
            logging.getLogger(__name__).warning(
                "Landed-table reconcile failed after update_source", exc_info=True
            )

        # Invalidate and re-index catalog cache (REQ-464)
        from provisa.discovery.catalog_cache import (
            invalidate_source as _invalidate,
            index_source as _index_source,
        )

        async def _reindex():
            await _invalidate(state.tenant_db, input.id)
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
        if (
            input.type == "servicenow"
        ):  # REQ-1954: its APIs' source follows its address and credential
            from provisa.api.admin._servicenow_companion import sync_companion

            async with (await _get_pool()).acquire() as _companion_conn:
                await sync_companion(cast("Connection", _companion_conn), input.id)
            await _rebuild_schemas()
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
            try:
                renamed = await source_repo.rename(cast("Connection", conn), old_id, new_id)
            except source_repo.SourceIdTaken as taken:
                return MutationResult(
                    success=False,
                    message=str(taken),
                    code="schema.source_id_taken",
                    params={"source": taken.source_id},
                )
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
        from provisa.api.admin.read_rights import reader_for

        return await _delete_source(id, reader_for(info))

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
            parent_domain_id=input.parent_domain_id or None,  # REQ-1363
        )
        from provisa.security.domain_tree import parent_map, parent_problem

        async with pool.acquire() as conn:
            # A create makes a new domain: an existing id is refused by name, nothing written.
            if await domain_repo.get(cast("Connection", conn), input.id) is not None:
                return MutationResult(
                    success=False,
                    message=f"Domain {input.id!r} already exists",
                    code="schema.domain_id_taken",
                    params={"domain": input.id},
                )
            # REQ-1363: the parent is a domain that exists. Refused by name, before the write.
            held = parent_map(await domain_repo.list_all(cast("Connection", conn)))
            problem = parent_problem(input.id, model.parent_domain_id, held)
            if problem is not None:
                return MutationResult(
                    success=False,
                    message=problem,
                    code="schema.domain_parent_invalid",
                    params={"domain": input.id, "parent": model.parent_domain_id},
                )
            await domain_repo.upsert(cast("Connection", conn), model)
        if model.parent_domain_id is not None:
            # REQ-1363: a role that lists the parent reaches the new domain now -- a member scoped
            # to the business unit registers into its new organization without a role edit.
            await _rebuild_schemas()
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
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(cast("Connection", conn), reader_for(info), refused)
                # REQ-1917: a domain is deleted only when nothing refers to it; every
                # dependent is named and nothing is removed.
                return MutationResult(
                    success=False,
                    message=_said,
                    code=(
                        "schema.domain_has_dependents"
                        if refused.reason == "dependents"
                        else "schema.domain_is_system"
                    ),
                    params={
                        "domain": id,
                        "dependents": _named,
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
        return await _save_data_product(info, input, creating=True)

    @strawberry.mutation
    async def update_data_product(
        self, info: StrawberryInfo, input: DataProductInput
    ) -> MutationResult:  # REQ-1634
        return await _save_data_product(info, input, creating=False)

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
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(cast("Connection", conn), reader_for(info), refused)
                # REQ-1918: a data product is blocked by its members; each is named.
                return MutationResult(
                    success=False,
                    message=_said,
                    code="schema.data_product_has_dependents",
                    params={
                        "data_product": id,
                        "dependents": _named,
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
    async def create_tag(
        self, info: StrawberryInfo, input: TagInput
    ) -> MutationResult:  # REQ-1373, REQ-1375, REQ-1944
        return await _save_tag(info, input, creating=True)

    @strawberry.mutation
    async def update_tag(
        self, info: StrawberryInfo, input: TagInput
    ) -> MutationResult:  # REQ-1373, REQ-1375, REQ-1944
        return await _save_tag(info, input, creating=False)

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
            try:
                deleted = await tag_repo.delete(cast("Connection", conn), id)
            except tag_repo.TagDeleteRefused as refused:
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(cast("Connection", conn), reader_for(info), refused)
                # REQ-1384: a tag that carries a row filter is not deleted; the filter is named
                # and nothing is removed.
                return MutationResult(
                    success=False,
                    message=_said,
                    code="schema.tag_has_dependents",
                    params={
                        "tag": id,
                        "dependents": _named,
                    },
                )
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
            term_id=input.term_id,  # REQ-2145
            reason=(input.reason or "").strip() or None,
            expires_on=(input.expires_on or "").strip() or None,
            why=input.why,  # REQ-2094
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
        if input.also_term and model.object_type != "column":
            return MutationResult(
                success=False,
                message="alsoTerm puts a column's tag on the column's term; it takes a column",
                code="tag.also_term_needs_column",
                params={"tag": input.tag_id, "objectType": model.object_type},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            refused = await _require_tag_target_domains(info, cast("Connection", conn), model)
            if refused is not None:
                return refused
            tag_row = await tag_repo.get(cast("Connection", conn), input.tag_id)
            if tag_row is None:
                return MutationResult(
                    success=False,
                    message=f"Tag {input.tag_id!r} not found",
                    code="schema.tag_not_found",
                    params={"tag": input.tag_id},
                )
            if input.also_term:  # REQ-2145
                model, refused = await _on_columns_term(cast("Connection", conn), model)
                if refused is not None:
                    return refused
            if model.object_type == "term":  # REQ-2145
                assert model.term_id is not None
                await _require_term_tagger(info, cast("Connection", conn), model.term_id)
                refused = await _term_tag_refused(cast("Connection", conn), model)
                if refused is not None:
                    return refused
            if model.object_type == "table" and model.table_id is not None:  # REQ-1384
                misfit = await _tag_filter_problem(
                    cast("Connection", conn), model.tag_id, model.table_id
                )
                if misfit is not None:
                    return MutationResult(
                        success=False,
                        message=f"Tag {input.tag_id!r} cannot go on this table: {misfit}",
                        code="schema.tag_filter_misfit",
                        params={"tag": input.tag_id, "reason": misfit},
                    )
            await _require_tag_assignment_right(
                info,
                cast("Connection", conn),
                tag_row,
                model.object_type,
                model.table_id,
                model.term_id,
            )
            refused = await store_tag_assignment(cast("Connection", conn), model, tag_row)
            if refused is not None:
                return refused
            covers = await _covers(cast("Connection", conn), model)  # REQ-2145
        await _refresh_config_tags()
        if model.object_type == "table":
            await _rebuild_if_tag_filtered(model.tag_id)
        return MutationResult(
            success=True,
            message=f"Tag {input.tag_id!r} assigned",
            code="schema.tag_assigned",
            params={"tag": input.tag_id, "objectKey": model.object_key(), "covers": covers},
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
            term_id=input.term_id,  # REQ-2145
        )
        problem = _assignment_target_problem(model)
        if problem is not None:
            return problem
        pool = await _get_pool()
        async with pool.acquire() as conn:
            refused = await _require_tag_target_domains(info, cast("Connection", conn), model)
            if refused is not None:
                return refused
            tag_row = await tag_repo.get(cast("Connection", conn), input.tag_id)
            if model.object_type == "term":  # REQ-2145
                assert model.term_id is not None
                await _require_term_tagger(info, cast("Connection", conn), model.term_id)
            if tag_row is None:
                require_capability(info, "table_registration")
            else:
                # REQ-1943: removing a sensitive tag from a column stops it being sensitive.
                await _require_tag_assignment_right(
                    info,
                    cast("Connection", conn),
                    tag_row,
                    input.object_type,
                    input.table_id,
                    input.term_id,
                )
            if model.object_type == "column":  # REQ-2145
                from provisa.core.term_tags import inherited_from

                assert model.table_id is not None and model.column_name is not None
                through = await inherited_from(
                    cast("Connection", conn),
                    model.table_id,
                    model.column_name,
                    model.base_tag_id(),
                )
                if through is not None:
                    return MutationResult(
                        success=False,
                        message=(
                            f"This column is PII because it means {through['term']}. Unmark "
                            "that meaning, or move the column if it means something else."
                        ),
                        code="tag.inherited_from_term",
                        params={
                            "tag": input.tag_id,
                            "column": model.column_name,
                            "term": through["term"],
                        },
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
        if model.object_type == "table":
            await _rebuild_if_tag_filtered(model.tag_id)
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
        # REQ-1415: the value rides in the catalog description suffix with the tag id.
        from provisa.core.modeling_tags import TAG_SLUG_CHARSET, is_tag_slug

        if not is_tag_slug(value):
            return MutationResult(
                success=False,
                message=f"Parameter value {input.value!r} may only contain {TAG_SLUG_CHARSET}",
                code="schema.tag_param_value_not_slug",
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
        return await _save_role(info, input, creating=True)

    @strawberry.mutation
    async def update_role(
        self, info: StrawberryInfo, input: RoleInput
    ) -> MutationResult:  # REQ-042, REQ-059, REQ-060, REQ-215, REQ-1531
        return await _save_role(info, input, creating=False)

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
            rr = await _save_relationship_as_caller(info, rel, creating=True)
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
    async def create_metric(
        self, info: StrawberryInfo, input: MetricInput
    ) -> MutationResult:  # REQ-1317
        return await _save_metric(info, input, creating=True)

    @strawberry.mutation
    async def update_metric(
        self, info: StrawberryInfo, input: MetricInput
    ) -> MutationResult:  # REQ-1317
        return await _save_metric(info, input, creating=False)

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
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(cast("Connection", conn), reader_for(info), refused)
                # REQ-1918: a view uses it; nothing is removed, each is named.
                return MutationResult(
                    success=False,
                    message=_said,
                    code="schema.metric_has_dependents",
                    params={
                        "metric": name,
                        "dependents": _named,
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
        from provisa.api.admin._hiding_guard import (
            governance_sets,
            refuse_governance_sets,
            require_table_save,
        )

        _governance_sets = governance_sets(info, input)  # REQ-1958: of the input as sent
        _editor, input = await require_table_save(info, input)  # REQ-1944
        from provisa.api.admin._table_ops import land_transform_view

        land_transform_view(input)  # REQ-2131: a view applying a column transform is landed
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        columns, _col_err = await _build_columns_for_input(pool, input)
        if _col_err is not None:
            return _col_err
        _field_refusal = await column_field_refusal(pool, input, columns)
        if _field_refusal is not None:
            return _field_refusal
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
        window_err = _parsed_off_peak(input.off_peak_window or None, input.off_peak_tz or "UTC")
        if window_err is not None:
            return window_err
        model = _table_model_from_input(input, columns, input.alias)
        try:
            async with pool.acquire() as conn:
                _conn = cast("Connection", conn)
                async with _conn.transaction():
                    from provisa.api.admin.domain_guard import require_current_domain

                    await require_current_domain(info, _conn, model)  # REQ-1531
                    from provisa.api.admin._parameter_guard import source_parameter_refusal

                    _param_refusal = await source_parameter_refusal(_conn, model)  # REQ-1986
                    if _param_refusal is not None:
                        raise TableSaveRefused(
                            MutationResult(
                                success=False,
                                message=_param_refusal,
                                code="schema.parameter_set_by_source",
                                params={"table": model.table_name},
                            )
                        )
                    from provisa.api.admin._query_api_registration import pipeline_table_refusal

                    _pipeline_refusal = await pipeline_table_refusal(_conn, model)  # REQ-1980
                    if _pipeline_refusal is not None:
                        raise TableSaveRefused(_pipeline_refusal)
                    # REQ-1443: same contract-driven derivation the register path and the YAML loader run.
                    from provisa.api.admin._dq_registration import apply_dq_registration

                    try:
                        await apply_dq_registration(_conn, model)
                        # REQ-1934: a profile result table's derivation, and a table's profiler membership.
                        from provisa.profiler.registration import (
                            apply_registration,
                            profile_reach_refusal,
                        )
                        from provisa.api.admin.catalog_scope import catalog_scope
                        from provisa.api.admin.read_rights import serves_every_column

                        _scope = catalog_scope(info)

                        async def _serves_all(table_id: int) -> bool:
                            return await serves_every_column(_conn, _scope, table_id)

                        # REQ-2152 (Q6a): profiling a table takes reading all of it.
                        _reach = await profile_reach_refusal(_conn, model, _serves_all)
                        if _reach is not None:
                            raise ValueError(_reach)
                        await apply_registration(_conn, model)
                    except ValueError as _dq_err:
                        raise TableSaveRefused(MutationResult(success=False, message=str(_dq_err)))
                    from provisa.api.admin.region_defaults import kept_placement

                    model.region, model.draft = await kept_placement(_conn, model)  # REQ-1921
                    _stored_region, _stored_draft = model.region, model.draft
                    _conflict = await _domain_table_conflict(
                        _conn,
                        model.domain_id,
                        model.table_name,
                        model.source_id,
                        model.schema_name,
                        input.alias,
                    )
                    if _conflict:
                        raise TableSaveRefused(MutationResult(success=False, message=_conflict))
                    _owner_conflict = await _dataset_ownership_conflict(
                        _conn, model.source_id, model.table_name, model.domain_id
                    )
                    if _owner_conflict:
                        raise TableSaveRefused(
                            MutationResult(success=False, message=_owner_conflict)
                        )
                    # REQ-1907/REQ-318: an update does not take cache_ttl / row_materialize / pagination
                    # -- they are saved through updateTableCache / updateTablePaging -- so the upsert
                    # keeps the stored values; role_ttl and replicate likewise unless the input
                    # carries them (REQ-2109, applied just below).
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
                    # REQ-2109: the settings the update carries replace the stored ones, checked as
                    # their own mutations check them; one refused refuses the whole update.
                    model = await with_carried_settings(info, _conn, input, model)
                    _lp_refusal = await load_protection_refusal(_conn, model)  # REQ-1141
                    if _lp_refusal is not None:
                        raise TableSaveRefused(_lp_refusal)
                    if input.view_sql and input.materialize:
                        # REQ-964, REQ-2109: the view's definition is judged before the save commits, so a
                        # refused one leaves nothing saved.
                        try:
                            check_view_mv(
                                input.table_name,
                                input.view_sql,
                                consistency=input.mv_consistency,
                                preprocess=input.mv_preprocess,
                                business_key=list(input.business_key),
                                persist=input.mv_persist,
                            )
                        except ValueError as _mv_err:
                            raise TableSaveRefused(
                                MutationResult(success=False, message=str(_mv_err))
                            )
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
                        raise TableSaveRefused(_ttl_refusal)
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
                        kafka_topic=model.live.kafka.topic
                        if model.live and model.live.kafka
                        else None,
                        table_name=model.table_name,
                    )
                    if _feed_refusal is not None:
                        raise TableSaveRefused(_feed_refusal)
                    from provisa.api.admin._file_glob import table_file_glob_refusal

                    _glob_refusal = await table_file_glob_refusal(_conn, model)  # REQ-788
                    if _glob_refusal is not None:
                        raise TableSaveRefused(_glob_refusal)
                    from provisa.api.app import state as _app_state

                    if _app_state.source_types.get(model.source_id) == "athena":
                        # REQ-1981: its format, from the source, decides the writes it takes.
                        from provisa.api.admin.introspect import athena_table_format

                        model.table_format = await athena_table_format(
                            model.source_id, model.schema_name, model.table_name
                        )
                    if _app_state.source_types.get(model.source_id) == "jira":
                        # REQ-2069: a form's project and issue type, and each column's field, from the
                        # account's form and field lists -- the steward picks a form and its columns.
                        from provisa.api.admin.introspect import complete_jira_form

                        try:
                            await complete_jira_form(model)
                        except ValueError as exc:
                            raise TableSaveRefused(
                                MutationResult(
                                    success=False,
                                    message=str(exc),
                                    code="schema.jira_form_unresolved",
                                    params={"table": model.table_name, "source": model.source_id},
                                )
                            )

                    from provisa.api.admin._import_set import table_import_set_refusal

                    _import_set_refusal = await table_import_set_refusal(_conn, model)  # REQ-1954
                    if _import_set_refusal is not None:
                        raise TableSaveRefused(_import_set_refusal)
                    from provisa.api.admin._import_set import table_arguments_refusal

                    _arguments_refusal = await table_arguments_refusal(_conn, model)  # REQ-2061
                    if _arguments_refusal is not None:
                        raise TableSaveRefused(_arguments_refusal)
                    from provisa.api.admin._fake_guard import table_fake_refusal

                    _fake_refusal = await table_fake_refusal(_conn, model)  # REQ-1494
                    if _fake_refusal is not None:
                        raise TableSaveRefused(_fake_refusal)
                    from provisa.api.admin._delta_guard import table_delta_refusal

                    _delta_refusal = await table_delta_refusal(_conn, model)  # REQ-874
                    if _delta_refusal is not None:
                        raise TableSaveRefused(_delta_refusal)
                    from provisa.api.admin._hiding_guard import table_hiding_refusal

                    _hiding_refusal = await table_hiding_refusal(  # REQ-1943, REQ-1944
                        info, _conn, model, editor=_editor
                    )
                    if _hiding_refusal is not None:
                        raise TableSaveRefused(_hiding_refusal)
                    refuse_governance_sets(_governance_sets)  # REQ-1958
                    try:
                        _carried_grants = model.write_grants  # REQ-2074, REQ-2109
                        model = await table_repo.keep_unedited(_conn, model)  # REQ-1919
                        if carried(input.write_grants):
                            model = model.model_copy(update={"write_grants": _carried_grants})
                        table_id = await table_repo.upsert(_conn, model)
                    except glossary_repo.TermIsClass as _class:
                        # REQ-2141: a column named for a class term is not claimed by it; REQ-2152: only
                        # the tables the caller reads are named.
                        from provisa.api.admin.read_rights import reader_for, told_glossary_refusal

                        _said, _tables = await told_glossary_refusal(
                            _conn, reader_for(info), _class
                        )
                        raise TableSaveRefused(
                            MutationResult(
                                success=False,
                                message=_said,
                                code="glossary.term_is_class",
                                params={"term": _class.term, "tables": _tables},
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
                        # REQ-1918: a column something still refers to is not dropped; each is
                        # named, with only the dependents the caller may read (REQ-2152).
                        from provisa.api.admin.read_rights import (
                            column_drop_text,
                            reader_for,
                            readable_columns_report,
                        )

                        _report = await readable_columns_report(
                            _conn, reader_for(info), _drop.columns
                        )
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
                        # REQ-1670/REQ-1683: an edited query re-persists the endpoint the table serves from.
                        from provisa.api.admin._query_api_registration import (
                            persist_query_api_registration,
                        )

                        _neo_err = await persist_query_api_registration(_conn, model)
                        if _neo_err is not None:
                            raise TableSaveRefused(_neo_err)
                    # REQ-316/REQ-318: an OpenAPI table's endpoint row follows its registration.
                    from provisa.api.admin._openapi_table_registration import (
                        persist_openapi_endpoint,
                    )
                    from provisa.api.app import state as _app_state

                    _oa_err = await persist_openapi_endpoint(_app_state, _conn, model)
                    if _oa_err is not None:
                        raise TableSaveRefused(_oa_err)
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
                    # REQ-2109: the region, row rules and paging the update carries; paging last,
                    # as the endpoint this process serves the table from takes it too.
                    await save_carried_region(
                        info,
                        _conn,
                        input,
                        table_id,
                        stored_region=_stored_region,
                        stored_draft=_stored_draft,
                        draft=model.draft,
                    )
                    await save_row_rules(info, _conn, input, model)
                    await save_carried_paging(_conn, input, table_id)
        except TableSaveRefused as _refused:
            # The transaction rolled back: nothing of the save is stored.
            return _refused.result
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
        if input.view_sql and input.materialize and not model.draft:
            from provisa.api.admin.schema_common import activate_view_mv

            await activate_view_mv(input.table_name)
        from provisa.dq.scan import reschedule_org_checkers

        await reschedule_org_checkers()  # REQ-1443: a checker table's job follows its edit
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
        _unread = await _table_unread(info, id)  # REQ-2152
        if _unread is not None:
            return _unread
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
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(cast("Connection", conn), reader_for(info), refused)
                # REQ-1918: nothing is removed; every dependent is named.
                return MutationResult(
                    success=False,
                    message=_said,
                    code="schema.table_has_dependents",
                    params={
                        "table": id,
                        "name": refused.name,
                        "dependents": _named,
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
            from provisa.dq.scan import reschedule_org_checkers

            await reschedule_org_checkers()  # REQ-1443: a deleted checker table stops scanning
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
        from provisa.core.db import STANDARD_ROLES

        if id in STANDARD_ROLES:  # REQ-2155
            return MutationResult(
                success=False,
                message=f"Role {id!r} is a standard role and cannot be deleted",
                code="schema.role_standard_locked",
                params={"role": id},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            existing = await role_repo.list_all(cast("Connection", conn))
            for r in existing:
                if r["id"] == id:
                    # REQ-1531: removing a role is an act in every domain it reaches.
                    require_right_in_domains(info, "user_management", r["domain_access"])
            try:
                deleted = await role_repo.delete(cast("Connection", conn), id)
            except role_repo.RoleDeleteRefused as refused:
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(cast("Connection", conn), reader_for(info), refused)
                # REQ-1918: refused while anything depends on the role, naming each dependent;
                # and for a role the deployment defines.
                return MutationResult(
                    success=False,
                    message=_said,
                    code=(
                        "schema.role_has_dependents"
                        if refused.reason == "dependents"
                        else "schema.role_is_system"
                    ),
                    params={
                        "role": id,
                        "dependents": _named,
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
            # REQ-2154: neither the acting role's own read nor a column's last reader is taken.
            from types import SimpleNamespace

            from provisa.api.admin._hiding_guard import grant_edit_refusal
            from provisa.api.admin.capabilities import acting_role_of
            from provisa.core.repositories.table import load_columns

            stored_columns = {col["column_name"]: col for col in await load_columns(c, table_id)}
            left = SimpleNamespace(
                columns=[
                    SimpleNamespace(
                        name=name,
                        visible_to=[r for r in col.get("visible_to") or () if r != role_id],
                    )
                    for name, col in stored_columns.items()
                ]
            )
            kept = grant_edit_refusal(stored_columns, left, acting_role_of(info))
            if kept is not None:
                return MutationResult(success=False, message=kept, code="schema.read_grant_kept")
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
    async def set_table_write_grants(
        self, info: StrawberryInfo, table_id: int, grants: list[WriteGrantInput]
    ) -> MutationResult:  # REQ-2074, REQ-1676
        """Make ``grants`` the table's whole set of write grants. Who may write which rows is
        governance as a row filter is, so it is gated the same way (masking_config in the
        table's domain), and each grant is checked against the table before anything is stored:
        its roles exist, its columns are the table's, one grant per role and operation, and its
        rows and check resolve against the table as a row filter must (REQ-1676)."""
        from pydantic import ValidationError

        from provisa.api.admin.capabilities import require_capability
        from provisa.core import model_change
        from provisa.core.models import Column as ColumnModel
        from provisa.core.models import Table as TableModel
        from provisa.core.repositories import table as table_repo
        from provisa.core.repositories import write_grant as write_grant_repo

        require_capability(info, "masking_config")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            c = cast("Connection", conn)
            row = await table_repo.get(c, table_id)
            # REQ-2152: a table the caller is served no column of is answered as missing.
            if row is None or await _table_unread(info, table_id) is not None:
                return MutationResult(
                    success=False,
                    message=f"Table {table_id} not found",
                    code="schema.table_id_not_found",
                    params={"id": table_id},
                )
            require_right_in_domains(info, "masking_config", {row["domain_id"]})
            # REQ-2152: a write grant names only columns the caller is served -- what it cannot
            # read it cannot grant. Refused naming no column.
            from provisa.api.admin._table_settings import write_grant_names
            from provisa.api.admin.catalog_scope import catalog_scope
            from provisa.api.admin.read_rights import served_columns_of

            served = await served_columns_of(c, catalog_scope(info), table_id)
            if served is not None and any(not write_grant_names(g) <= served for g in grants):
                return write_grant_invalid(
                    "a write grant names a column the acting role cannot read"
                )
            refusal = await write_grant_roles_refusal(c, grants)
            if refusal is not None:
                return refusal
            columns = await table_repo.load_columns(c, table_id)
            registry = await table_repo.list_all(c)
            unheld = await table_repo._control_plane_role_ids(c)
        try:
            models = write_grant_models(grants)
            # The table's own rules for its grants: one per role and operation, its columns.
            TableModel(
                source_id=row["source_id"],
                domain_id=row["domain_id"],
                schema_name=row["schema_name"],
                table_name=row["table_name"],
                columns=[ColumnModel(name=col["column_name"], visible_to=[]) for col in columns],
                write_grants=models,
            )
        except ValidationError as refused:
            return write_grant_invalid("; ".join(e["msg"] for e in refused.errors()))
        refusal = write_grant_predicates_refusal(table_id, models, registry)
        if refusal is not None:
            return refusal
        model_change.name("set", "write grants", f"on table {table_id}")
        async with pool.acquire() as conn:
            await write_grant_repo.replace_for_table(
                cast("Connection", conn), table_id, models, unheld=unheld
            )
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Write grants of table {table_id} saved",
            code="schema.write_grants_saved",
            params={"id": table_id, "count": len(models)},
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
        # A rule binds to a registered role: one naming no role, or a role that does not exist,
        # is refused with the reason here rather than by the role_id foreign key at insert.
        async with pool.acquire() as _rconn:
            _rule_refusal = await row_rule_refusal(
                cast("Connection", _rconn), input.role_id, input.description
            )
        if _rule_refusal is not None:
            return _rule_refusal
        if input.action_name:  # REQ-1679: the target is a tracked function or webhook
            return await _upsert_action_rls_rule(info, input)
        if input.tag_id:  # REQ-1384: the target is every table carrying a tag
            return await _upsert_tag_rls_rule(info, input)
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
        # REQ-2152: a row rule is written only over what the caller reads -- a table it is served
        # no column of is answered as unregistered, and a filter naming a column it is not served
        # is refused naming none.
        _unread = await _rule_beyond_reads(info, input, targets)
        if _unread is not None:
            return _unread
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
            description=input.description,
            why=input.why,  # REQ-2094
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
        tag_id: Optional[str] = None,
    ) -> MutationResult:  # REQ-1531, REQ-1679, REQ-1384
        from provisa.api.admin.capabilities import require_capability
        from provisa.api.admin.domain_guard import table_domain

        require_capability(info, "masking_config")  # REQ-1531: see upsert_rls_rule
        pool = await _get_pool()
        async with pool.acquire() as conn:
            if tag_id:  # REQ-1384: gated on the domain of every table the tag is on
                require_right_in_domains(
                    info,
                    "masking_config",
                    {
                        t["domain_id"]
                        for t in await _tables_carrying(cast("Connection", conn), tag_id)
                    },
                )
            elif action_name:  # REQ-1679: gated on the action's domain
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
        # REQ-2152: a rule on a table the caller is served no column of is answered as missing.
        if table_id is not None and await _table_unread(info, table_id) is not None:
            return MutationResult(
                success=False, message="RLS rule not found", code="schema.rls_rule_not_found"
            )
        async with pool.acquire() as conn:
            deleted = await rls_repo.delete(
                cast("Connection", conn),
                role_id,
                table_id=table_id,
                domain_id=domain_id,
                action_name=action_name,
                tag_id=tag_id,
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
    async def execute_creation_request(  # REQ-434, REQ-063, REQ-1948
        self, info: StrawberryInfo, request_id: int
    ) -> MutationResult:
        """REQ-434: a rights-holder carries out a creation request that has had its approvals.

        Refused until the request's required approvals are met; after that it serves to retry a
        creation that failed when the last approval was given."""
        from provisa.api.admin.capabilities import _identity_from_info
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
        refusal = await _execution_refusal(info, req)
        if refusal is not None:
            return refusal
        result = await perform_request_creation(info, req)
        if not result.success:
            if result.code is not None:
                await _record_request_decision(
                    info, req, "execute", refusal=_rule_refusal(result.code, result.message)
                )
            return result

        identity = _identity_from_info(info)
        resolved_by = getattr(identity, "user_id", None) if identity is not None else None
        async with pool.acquire() as conn:
            await cr_repo.mark_executed(cast("Connection", conn), request_id, resolved_by)
        await request_carried_out(req)
        await _record_request_decision(info, req, "execute")
        return MutationResult(
            success=True,
            message=f"Executed creation request #{request_id}",
            code="schema.request_executed",
            params={"id": request_id},
        )

    @strawberry.mutation
    async def approve_creation_request(  # REQ-434, REQ-063, REQ-1948
        self, info: StrawberryInfo, request_id: int
    ) -> MutationResult:
        """Approve a creation request: the REST queue's approve, asked over GraphQL.

        One implementation, so the rule, the refusals and the admin-trail entries are the same
        on both surfaces. The approval that completes the request's count creates what it asks
        for; ``params.status`` says whether this one did."""
        from fastapi import HTTPException

        from provisa.api.admin.creation_requests_router import approve_request
        from provisa.api.errors import ApiError

        try:
            row = await approve_request(request_id, info.context["request"])
        except ApiError as refused:
            return MutationResult(
                success=False,
                message=str(refused.detail),
                code=refused.code,
                params=refused.params,
            )
        except HTTPException as refused:
            if refused.status_code in (404, 409):
                return MutationResult(
                    success=False,
                    message="Request not found or already resolved",
                    code="schema.request_not_pending",
                )
            return MutationResult(success=False, message=str(refused.detail))
        return MutationResult(
            success=True,
            message=f"Approved creation request #{request_id}",
            code="schema.request_approved",
            params={
                "id": request_id,
                "status": row["status"],
                "waitingOn": row.get("waiting_on", []),
            },
        )

    @strawberry.mutation
    async def reject_creation_request(  # REQ-434, REQ-063, REQ-1948
        self, info: StrawberryInfo, request_id: int, reason: str
    ) -> MutationResult:
        """REQ-434/063: a rights-holder rejects a queued request with an actionable reason; the
        author of a request (any type but a relationship's) may take it back."""
        from provisa.api.admin import relationship_approvals as _rule
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
            if req["request_type"] == "relationship":
                # REQ-1948: a rejection comes from any user who could approve.
                refusal = await _relationship_decision_refusal(info, req, executing=False)
                if refusal is not None:
                    return refusal
            identity = _identity_from_info(info)
            resolved_by = getattr(identity, "user_id", None) if identity is not None else None
            # Its author may take a request back; anyone else needs the right it names.
            withdrawn = _rule.is_withdrawal(
                request_type=req["request_type"],
                user_id=resolved_by,
                requested_by=req["requested_by"],
            )
            if req["request_type"] != "relationship" and not withdrawn:
                try:
                    require_capability(info, req["capability"])
                except PermissionError as e:
                    return MutationResult(success=False, message=str(e))
            await cr_repo.mark_rejected(
                cast("Connection", conn), request_id, reason.strip(), resolved_by
            )
        await _record_request_decision(info, req, "withdraw" if withdrawn else "reject")
        return MutationResult(
            success=True,
            message=f"Rejected creation request #{request_id}",
            code="schema.request_rejected",
            params={"id": request_id},
        )

    @strawberry.mutation
    async def create_relationship(  # REQ-019, REQ-020, REQ-366, REQ-434
        self, info: StrawberryInfo, input: RelationshipInput
    ) -> MutationResult:
        return await _save_relationship_as_caller(info, input, creating=True)

    @strawberry.mutation
    async def update_relationship(  # REQ-019, REQ-020
        self, info: StrawberryInfo, input: RelationshipInput
    ) -> MutationResult:
        return await _save_relationship_as_caller(info, input, creating=False)

    @strawberry.mutation
    async def delete_relationship(
        self,
        info: StrawberryInfo,
        id: str,
        # REQ-2145: as on a save -- each tag a re-homed key ref loses, confirmed or carried.
        confirm_tag_loss: list[str] | None = None,
        carry_tags: bool = False,
    ) -> MutationResult:
        require_capability(info, "create_relationship")
        from provisa.api.admin.domain_guard import relationship_domains, require_domains
        from provisa.core.repositories import relationship as rel_repo
        from provisa.core.term_tags import (
            TagLossRefused,
            TermTagRefused,
            inherited_snapshot,
            tag_changes_of_moves,
        )

        pool = await _get_pool()
        from provisa.api.admin.read_rights import reader_for, relationship_readable

        # REQ-2152: a relationship joining a column the caller is not served is answered as
        # missing.
        async with pool.acquire() as conn:
            _readable = await relationship_readable(cast("Connection", conn), reader_for(info), id)
        if not _readable:
            return MutationResult(
                success=False,
                message=f"Relationship {id!r} not found",
                code="schema.relationship_not_found",
                params={"relationship": id},
            )
        async with pool.acquire() as conn:
            # REQ-1531: a relationship belongs to the domains of both tables it joins.
            domains = await relationship_domains(cast("Connection", conn), id)
            if domains is not None:
                require_domains(info, domains)
            _conn = cast("Connection", conn)
            try:
                async with _conn.transaction():
                    tags_before = await inherited_snapshot(_conn)
                    deleted = await rel_repo.delete(_conn, id)
                    await tag_changes_of_moves(
                        _conn,
                        tags_before,
                        confirm_tag_loss=confirm_tag_loss or (),
                        carry_tags=carry_tags,
                    )
            except (TagLossRefused, TermTagRefused) as refused:
                return _tag_move_refusal(refused)
            except rel_repo.RelationshipDeleteRefused as refused:
                from provisa.api.admin.read_rights import reader_for, refusal

                # REQ-2152: only the dependents the caller may read, never how many.
                _said, _named = await refusal(_conn, reader_for(info), refused)
                # REQ-1918: a published view relies on it; nothing is removed, each is named.
                return MutationResult(
                    success=False,
                    message=_said,
                    code="schema.relationship_has_dependents",
                    params={
                        "relationship": id,
                        "dependents": _named,
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
        from provisa.core.cache_ttl import cache_ttl_refusal  # REQ-2031

        _ttl_err = cache_ttl_refusal(cache_ttl)
        if _ttl_err is not None:
            return MutationResult(
                success=False,
                message=_ttl_err,
                code="schema.invalid_cache_ttl",
                params={"source": source_id},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _sig = await conn.execute_core(
                select(
                    sources.c.change_signal,
                    sources.c.replicate,
                    sources.c.load_protected,
                    sources.c.freshness_gate,
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
                        _sig_row.freshness_gate,
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
        _unread = await _table_unread(info, table_id)  # REQ-2152
        if _unread is not None:
            return _unread
        from provisa.core.cache_ttl import cache_ttl_refusal  # REQ-2031

        _ttl_err = cache_ttl_refusal(cache_ttl)
        if _ttl_err is not None:
            return MutationResult(
                success=False,
                message=_ttl_err,
                code="schema.invalid_cache_ttl",
                params={"table": str(table_id)},
            )
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
                from types import SimpleNamespace

                from provisa.api.app import state as _ttl_state
                from provisa.dq.registration import is_checker_source_type, require_scan_interval

                if is_checker_source_type(_ttl_state.source_types.get(_t_row.source_id)):
                    try:  # REQ-1443: a checker table's cache_ttl is the interval it scans on
                        require_scan_interval(
                            SimpleNamespace(table_name=_t_row.table_name, cache_ttl=cache_ttl)
                        )
                    except ValueError as _interval_err:
                        return MutationResult(
                            success=False,
                            message=str(_interval_err),
                            code="dq.scan_interval_required",
                            params={"table": _t_row.table_name},
                        )
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
        from provisa.dq.scan import reschedule_org_checkers

        await reschedule_org_checkers()  # REQ-1443: a checker table scans on its new interval
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
        _unread = await _table_unread(info, table_id)  # REQ-2152
        if _unread is not None:
            return _unread
        pool = await _get_pool()
        async with pool.acquire() as conn:
            refusal = await role_ttl_refusal(conn, role_ttl)
            if refusal is not None:
                return refusal
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

        _unread = await _table_unread(info, table_id)  # REQ-2152

        if _unread is not None:
            return _unread
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
                    sources.c.change_signal,
                    sources.c.cache_ttl,
                    sources.c.load_protected,
                    sources.c.freshness_gate,
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
                source=SourceTtl(
                    row.change_signal,
                    row.cache_ttl,
                    replicate,
                    row.load_protected,
                    row.freshness_gate,
                ),
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
        _unread = await _table_unread(info, table_id)  # REQ-2152
        if _unread is not None:
            return _unread
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
    async def set_table_region(
        self, info: StrawberryInfo, table_id: int, region: str | None = None
    ) -> MutationResult:  # REQ-1921
        """Set where one table's data lives (None = no region): the one place a registered
        table's region changes. Its copies follow — built in the new region, retired elsewhere."""
        from provisa.core.repositories import region as region_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _row = await conn.execute_core(
                select(
                    registered_tables.c.domain_id,
                    registered_tables.c.table_name,
                    registered_tables.c.region,
                    registered_tables.c.draft,
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
            require_capability(info, "table_registration", domain_id=row.domain_id)
            # A draft table is claimed: its destination alone is judged (REQ-1921).
            refused_here = _residency_refusal(
                info, f"table {row.table_name}", row.region, region, draft=row.draft
            )
            if refused_here is not None:
                return refused_here
            try:
                name = await region_repo.set_table_region(conn, table_id, region)
            except ValueError as refused:
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code="schema.region_refused",
                    params={"table": table_id, "region": region, "reason": str(refused)},
                )
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"region of table {name!r} set to {region or 'none'}",
            code="schema.table_region_set",
            params={"table": table_id, "region": region},
        )

    @strawberry.mutation
    async def set_table_draft(
        self, info: StrawberryInfo, table_id: int, draft: bool
    ) -> MutationResult:  # REQ-1921
        """Put a table or view out of service (draft) or release it. While draft it is offered
        to no reader: in no schema, refused to every statement. It keeps the replica it has, and
        its steward's preview may land one (admin/table_read.py). Its domain's owners set and
        clear it, and no other right is needed."""
        from sqlalchemy import update as _update

        pool = await _get_pool()
        async with pool.acquire() as conn:
            row = (
                await conn.execute_core(
                    select(
                        registered_tables.c.domain_id,
                        registered_tables.c.table_name,
                        registered_tables.c.materialize,
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
            require_capability(info, "table_registration", domain_id=row.domain_id)
            _unread = await _table_unread(info, table_id)  # REQ-2152
            if _unread is not None:
                return _unread
            from provisa.core import model_change

            model_change.name("update", "table draft", row.table_name)  # REQ-1524
            await conn.execute_core(
                _update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(draft=draft)
            )
        await _rebuild_schemas()
        if not draft and row.materialize:
            # Released: a materialized view is built now, as a newly saved one is.
            from provisa.api.admin.schema_common import activate_view_mv

            await activate_view_mv(row.table_name)
        return MutationResult(
            success=True,
            message=f"table {row.table_name!r} {'set as draft' if draft else 'released'}",
            code="schema.table_draft_set" if draft else "schema.table_released",
            params={"table": row.table_name},
        )

    @strawberry.mutation
    async def set_source_region(
        self, info: StrawberryInfo, source_id: str, region: str | None = None
    ) -> MutationResult:  # REQ-1921
        """Set the region the admin form starts this source's new tables in (None = the
        connected one). Moves no existing table."""
        require_capability(info, "source_registration")
        from provisa.core.repositories import region as region_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            stored = (
                await conn.execute_core(select(sources.c.region).where(sources.c.id == source_id))
            ).fetchone()
            if stored is not None:
                refused_here = _residency_refusal(
                    info, f"source {source_id}", stored.region, region
                )
                if refused_here is not None:
                    return refused_here
            try:
                await region_repo.set_source_region(conn, source_id, region)
            except LookupError:
                return MutationResult(
                    success=False,
                    message=f"Source {source_id!r} not found",
                    code="schema.source_not_found",
                    params={"source": source_id},
                )
            except ValueError as refused:
                return MutationResult(
                    success=False,
                    message=str(refused),
                    code="schema.region_refused",
                    params={"source": source_id, "region": region, "reason": str(refused)},
                )
        return MutationResult(
            success=True,
            message=f"region of source {source_id!r} set to {region or 'none'}",
            code="schema.source_region_set",
            params={"source": source_id, "region": region},
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
                select(
                    sources.c.cache_ttl,
                    sources.c.change_signal,
                    sources.c.replicate,
                    sources.c.freshness_gate,
                ).where(sources.c.id == source_id)
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
                source=SourceTtl(
                    row.change_signal,
                    row.cache_ttl,
                    row.replicate,
                    load_protected,
                    row.freshness_gate,
                ),
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
        _unread = await _table_unread(info, table_id)  # REQ-2152
        if _unread is not None:
            return _unread
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(
                    registered_tables.c.source_id,
                    registered_tables.c.cache_ttl,
                    registered_tables.c.change_signal,
                    registered_tables.c.off_peak_window,
                    registered_tables.c.business_key,
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
                load_protected, off_peak_window, row.business_key
            ):
                logging.getLogger(__name__).warning(
                    "table %s: load protection (off-peak window) AND history keyed on %r are both set — "
                    "verify the snapshot boundary falls inside the off-peak refresh window (or that "
                    "allowed_lateness covers the lag), else snapshots may miss their intended instant "
                    "(REQ-1141/1162)",
                    table_id,
                    row.business_key,
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

        convention = _naming.normalize_convention(convention)  # REQ-195: kept as its preset
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
        refusal = naming_refusal(gql_naming_convention)
        if refusal is not None:
            return refusal
        from provisa.compiler.naming import stored_convention

        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(gql_naming_convention=stored_convention(gql_naming_convention))
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
        _unread = await _table_unread(info, table_id)  # REQ-2152
        if _unread is not None:
            return _unread
        refusal = naming_refusal(gql_naming_convention)
        if refusal is not None:
            return refusal
        pool = await _get_pool()
        async with pool.acquire() as conn:
            if await save_naming(cast("Connection", conn), table_id, gql_naming_convention) == 0:
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
        self,
        info: StrawberryInfo,
        mv_id: str,
        full: bool = False,
        window: Optional[date] = None,
    ) -> MutationResult:  # REQ-133, REQ-158, REQ-2109, REQ-2120
        """Trigger a manual refresh of a materialized view. ``full`` rebuilds every valid date of
        a view inheriting its versions, ignoring its watermark (REQ-2109). ``window`` builds the
        view sealed at that business day's close, by the calendar it names (REQ-2120)."""
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
        from provisa.mv.refresh import (
            SealedBuildNotDone,
            SealedBuildRefused,
            refresh_failure,
            refresh_mv,
        )

        try:
            assert state.federation_engine is not None
            # REQ-879: coordinate the refresh across the fleet via the shared control-plane catalog.
            await refresh_mv(
                state.federation_engine,
                mv,
                state.mv_registry,
                store=state.model_db,
                ledger=state.tenant_db,
                full=full,
                window=window,
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
        except SealedBuildRefused as refused:  # REQ-2120
            return MutationResult(
                success=False,
                message=str(refused),
                code="schema.mv_window_needs_calendar",
                params={"mv": mv_id},
            )
        except SealedBuildNotDone as unsealed:  # REQ-2120
            return MutationResult(
                success=False,
                message=str(unsealed),
                code=(
                    "schema.mv_window_held"
                    if unsealed.reason == "held"
                    else "schema.mv_window_unchanged"
                ),
                params={"mv": mv_id, "day": unsealed.day.isoformat()},
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

    @strawberry.mutation
    async def update_version_log(
        self, info: StrawberryInfo, mv_id: str, refresh_interval: int, allowed_lateness: float
    ) -> MutationResult:  # REQ-2109
        """Set how often a table's version log is captured and how late a row it reads may be
        committed: kept on the table's own row, which the log is built from at every rebuild."""
        require_capability(info, "table_registration")
        from provisa.api.app import state
        from provisa.mv.bitemporal_capture import logged_table_id

        mv = state.mv_registry.get(mv_id)
        table_id = logged_table_id(mv_id)
        if mv is None or table_id is None:
            return MutationResult(
                success=False,
                message=f"No version log {mv_id!r}",
                code="schema.mv_not_found",
                params={"mv": mv_id},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(mv_refresh_interval=refresh_interval, mv_allowed_lateness=allowed_lateness)
            )
        mv.refresh_interval = refresh_interval
        mv.allowed_lateness = allowed_lateness
        return MutationResult(
            success=True,
            message=f"Version log {mv_id!r} updated",
            code="schema.version_log_updated",
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
        kind: str,
        cron: Optional[str] = None,
        webhook_name: Optional[str] = None,
        args_json: Optional[str] = None,
        sql: Optional[str] = None,
        role: Optional[str] = None,
        on_table: Optional[str] = None,
        on_event: Optional[str] = None,
        command: Optional[str] = None,
    ) -> MutationResult:
        """Create a trigger -- on a cron schedule, or (REQ-2054) on a table's change or event --
        running a webhook, a SQL statement or a command, and register it live (REQ-1003/1004)."""
        require_capability(info, "org_settings")
        return await _ops.create_scheduled_task_op(
            info.context["request"],
            id,
            name,
            cron,
            kind,
            webhook_name,
            args_json,
            sql,
            role,
            on_table=on_table,
            on_event=on_event,
            command=command,
        )

    @strawberry.mutation
    async def update_scheduled_task(  # REQ-2054
        self,
        info: StrawberryInfo,
        id: str,
        name: str,
        kind: str,
        cron: Optional[str] = None,
        webhook_name: Optional[str] = None,
        args_json: Optional[str] = None,
        sql: Optional[str] = None,
        role: Optional[str] = None,
        on_table: Optional[str] = None,
        on_event: Optional[str] = None,
        command: Optional[str] = None,
    ) -> MutationResult:
        """Give one of the org's triggers a new definition -- checked as a new one is -- and
        register it live; whether it is enabled stays as it was."""
        require_capability(info, "org_settings")
        return await _ops.create_scheduled_task_op(
            info.context["request"],
            id,
            name,
            cron,
            kind,
            webhook_name,
            args_json,
            sql,
            role,
            on_table=on_table,
            on_event=on_event,
            command=command,
            replace=True,
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


async def _tables_carrying(conn: "Connection", tag_id: str) -> list[dict]:  # REQ-1384
    """The registered-table rows (with ``columns``) that carry ``tag_id`` as a table-level tag,
    whatever parameter value it was assigned with."""
    from provisa.core.models import base_tag_id
    from provisa.core.repositories import table as table_repo
    from provisa.core.repositories import tag as tag_repo
    from provisa.security.row_filters import tagged_table_ids

    carrying = tagged_table_ids(await tag_repo.list_assignments(conn)).get(
        base_tag_id(tag_id), set()
    )
    return [t for t in await table_repo.list_all(conn) if t["id"] in carrying]


async def _tag_filter_problem(  # REQ-1384, REQ-1676
    conn: "Connection", tag_id: str, table_id: int
) -> str | None:
    """Why ``table_id`` cannot take ``tag_id``, or None when it can: each row filter the tag
    carries is resolved against the table, the same check a rule gets at save, so a tag is
    never put on a table its filter cannot be applied to."""
    from provisa.compiler.rls_validate import validate_rls_predicate
    from provisa.core.models import base_tag_id
    from provisa.core.repositories import table as table_repo

    filters = await rls_repo.list_for_tag(conn, base_tag_id(tag_id))
    if not filters:
        return None
    registry = await table_repo.list_all(conn)
    targets = [t for t in registry if t["id"] == table_id]
    for rule in filters:
        problem = validate_rls_predicate(rule["filter_expr"], targets, registry)
        if problem is not None:
            return f"the tag's row filter for role {rule['role_id']!r} does not fit: {problem}"
    return None


async def _upsert_tag_rls_rule(
    info: StrawberryInfo, input: RLSRuleInput
) -> MutationResult:  # REQ-1384, REQ-1531, REQ-1676
    """A row filter on a tag: it applies to every table carrying the tag, AND-ed with the
    table's own (or its domain's) rule. Only a user tag that applies to tables takes one — a
    system or derived tag has no registry row for the rule to belong to."""
    from provisa.compiler.rls_validate import validate_rls_predicate
    from provisa.core.models import DERIVED_TAG_IDS, SYSTEM_TAG_IDS, base_tag_id
    from provisa.core.models import RLSRule as RLSRuleModel
    from provisa.core.repositories import table as table_repo
    from provisa.core.repositories import tag as tag_repo

    tag_id = input.tag_id or ""
    if input.table_id or input.domain_id:
        return MutationResult(
            success=False,
            message="An RLS rule targets exactly one of a table, a domain, an action or a tag",
            code="schema.rls_rule_invalid",
            params={"reason": "more than one target"},
        )
    if tag_id != base_tag_id(tag_id) or tag_id in SYSTEM_TAG_IDS + DERIVED_TAG_IDS:
        return MutationResult(
            success=False,
            message=(
                f"Tag {tag_id!r} cannot carry a row filter: only a user tag, named without a "
                "parameter value, takes one"
            ),
            code="schema.rls_rule_tag_not_user",
            params={"tag": tag_id},
        )
    pool = await _get_pool()
    async with pool.acquire() as conn:
        tag_row = await tag_repo.get(cast("Connection", conn), tag_id)
        if tag_row is None:
            return MutationResult(
                success=False,
                message=f"Tag {tag_id!r} not found",
                code="schema.tag_not_found",
                params={"tag": tag_id},
            )
        if "table" not in list(tag_row["applies_to"] or []):
            return MutationResult(
                success=False,
                message=f"Tag {tag_id!r} does not apply to tables, so it cannot carry a row filter",
                code="schema.rls_rule_tag_not_table",
                params={"tag": tag_id},
            )
        targets = await _tables_carrying(cast("Connection", conn), tag_id)
        # REQ-1531: the rule lands in the domain of every table the tag is on.
        require_right_in_domains(info, "masking_config", {t["domain_id"] for t in targets})
        if targets:
            registry = await table_repo.list_all(cast("Connection", conn))
            problem = validate_rls_predicate(input.filter_expr, targets, registry)
        else:
            # No table carries the tag yet, so there is nothing to resolve a column against;
            # the predicate is checked for shape now and against each table as it is tagged
            # (_tag_filter_problem).
            problem = validate_rls_predicate(input.filter_expr, [], [])
        if problem is not None:
            return MutationResult(
                success=False,
                message=problem,
                code="schema.rls_rule_invalid",
                params={"reason": problem},
            )
        await rls_repo.upsert(
            cast("Connection", conn),
            RLSRuleModel(
                tag_id=tag_id,
                role_id=input.role_id,
                filter=input.filter_expr,
                description=input.description,
                why=input.why,  # REQ-2094
            ),
        )
    await _rebuild_schemas()
    return MutationResult(
        success=True,
        message=f"RLS rule for tag {tag_id!r} / role {input.role_id!r} saved",
        code="schema.rls_rule_saved_tag",
        params={"tag": tag_id, "role": input.role_id},
    )


async def _rebuild_if_tag_filtered(tag_id: str) -> None:  # REQ-1384
    """A tag that carries a row filter decides which rows its tables' readers see, so putting
    it on a table or taking it off rebuilds the role contexts; any other tag is annotation."""
    from provisa.core.models import base_tag_id

    pool = await _get_pool()
    async with pool.acquire() as conn:
        filtered = await rls_repo.tags_with_filters(cast("Connection", conn))
    if base_tag_id(tag_id) in filtered:
        await _rebuild_schemas()


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
            RLSRuleModel(
                action_name=name,
                role_id=input.role_id,
                filter=input.filter_expr,
                description=input.description,
                why=input.why,  # REQ-2094
            ),
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
