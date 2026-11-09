# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4b2c17-6a30-4f8d-b1c2-7d05e8a3f614
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dry run of an apply (REQ-2222, amended 2026-11-09): the whole model build, in a transaction
that is rolled back.

An apply has two halves. The first writes the model store, in one transaction; a dry run runs it
as it is and rolls it back (``config_loader.dry_run_config``). The second builds the runtime from
what was committed, and cannot be run here: it issues engine catalogs, opens pools, installs
triggers on the sources, writes the build's own tables and publishes what every request reads.
None of that can be taken back, so none of it runs. What that half REFUSES a model for can be
judged without it, and is, here: :func:`check_model_build` runs the build's own refusals over the
rolled-back transaction, each from the function the build itself calls, and publishes nothing.
"""

# Requirements: REQ-2222, REQ-194

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import select

if TYPE_CHECKING:
    from provisa.core.config_loader import DryRun
    from provisa.core.database import Connection
    from provisa.core.models import ProvisaConfig

#: What an apply does that a dry run does not, because it cannot be rolled back. The report names
#: them: a model that passes a dry run has not exercised these.
NOT_RUN: tuple[str, ...] = (
    "engine catalogs of the sources",
    "source connection pools and enum types",
    "primary keys read from the sources",
    "notify triggers on the sources",
    "landed-table reconcile and replica convergence",
    "per-role schemas, views and class tables",
    "lifecycle wiring, cache purges and the model-changed event",
)


class _TrialState:
    """The application state as the build's refusals read it, with what this config changes laid
    over it. Writes stay here; everything else reads through to the real state."""

    def __init__(self, base: Any, **laid_over: Any) -> None:
        self._base = base
        self.__dict__.update(laid_over)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


async def check_model_build(conn: "Connection", config: "ProvisaConfig") -> None:
    """Judge the store as ``conn``'s transaction holds it by the refusals of the schema build
    (``app._rebuild_schemas_impl``): effective names, one name per domain, the domain tree, faked
    columns, commands, the names table (REQ-194) and data-quality contracts. Raises what the
    build raises; publishes nothing."""
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.api._ontology_views import ONTOLOGY_TABLE_ALIAS
    from provisa.api.app import _load_build_domains, state
    from provisa.api.app_loaders import (
        _META_TABLE_ALIAS,
        _check_fakes,
        catalog_name_for_source,
        checker_contract_targets,
        read_tracked_commands,
        role_build_inputs,
        whole_model_context,
    )
    from provisa.api.app_schema_build import (
        _assert_domain_table_unique,
        _build_gql_object_columns,
        _filter_tables_by_schema_cfg,
        _inject_gql_required_args,
        _synthesize_column_metadata,
    )
    from provisa.compiler.introspect import introspect_tables
    from provisa.compiler.names_table import build_model_names, clash_policy
    from provisa.compiler.naming import source_to_catalog
    from provisa.core.models import Metric
    from provisa.core.relationship_names import store_effective_names
    from provisa.core.repositories import metric as metric_repo
    from provisa.core.schema_org import domains as domains_t
    from provisa.core.schema_org import naming_rules as naming_rules_t
    from provisa.core.schema_org import sources as sources_t
    from provisa.security import domain_tree
    from provisa.synthetic.datasets import as_generated, synthetic_sources

    await store_effective_names(conn)
    tables = as_generated(await fetch_tables(conn), await synthetic_sources(conn))
    draft_tables = await fetch_tables(conn, draft=True)
    try:
        _assert_domain_table_unique(tables)
    except RuntimeError as clash:  # the build's own type for it; a refusal of this model here
        raise ValueError(str(clash)) from clash
    relationships = await fetch_relationships(conn)
    parents = domain_tree.parent_map(
        dict(r._mapping)
        for r in (
            await conn.execute_core(select(domains_t.c.id, domains_t.c.parent_domain_id))
        ).fetchall()
    )
    problem = domain_tree.tree_problem(parents)
    if problem is not None:
        raise ValueError(problem)

    allowed = {**state.source_allowed_domains}
    allowed.update({s.id: list(s.allowed_domains) for s in config.sources if s.allowed_domains})
    tables = _filter_tables_by_schema_cfg(
        tables,
        (state.raw_config or {}).get("schema", {}),
        {src: domain_tree.expand(held, parents) for src, held in allowed.items()},
    )
    naming_rules = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(naming_rules_t.c.pattern, naming_rules_t.c.replacement).order_by(
                    naming_rules_t.c.id
                )
            )
        ).fetchall()
    ]
    domains = await _load_build_domains(conn)
    sources = {
        r._mapping["id"]: dict(r._mapping)
        for r in (await conn.execute_core(select(sources_t))).fetchall()
    }
    source_types = {sid: row["type"] for sid, row in sources.items() if row.get("type")}
    source_catalogs = {
        **state.source_catalogs,
        **{sid: catalog_name_for_source(state, kind, sid) for sid, kind in source_types.items()},
    }
    hints = {**state.source_federation_hints}
    hints.update({s.id: dict(s.federation_hints) for s in config.sources if s.federation_hints})
    for sid, row in sources.items():
        if row.get("type") == "postgresql":
            sources[sid] = {**row, "database": source_to_catalog(sid)}

    physical = getattr(state, "kafka_table_physical", {})
    column_types = introspect_tables(
        None, tables, sources, {**_META_TABLE_ALIAS, **ONTOLOGY_TABLE_ALIAS, **(physical or {})}
    )
    remote_sources = getattr(state, "graphql_remote_sources", {})
    _inject_gql_required_args(tables, remote_sources)
    object_columns = _build_gql_object_columns(remote_sources)
    _synthesize_column_metadata(tables, column_types, remote_sources)
    await _check_fakes(conn)
    commands = await read_tracked_commands(conn, tables)
    metrics = [
        Metric(
            name=r["name"],
            expression=r["expression"],
            datatype=r["datatype"],
            description=r["description"],
            ai_context=r["ai_context"],
            visible_to=list(r["visible_to"]),
            from_fact=r["from_fact"],
        ).model_dump()
        for r in await metric_repo.list_all(conn)
    ]
    # The prefix the last build ran with: a build reads it from the deployment's file and the
    # org's settings, never from the configuration being applied.
    domain_prefix = bool((state.schema_build_cache or {}).get("domain_prefix", False))
    trial: Any = _TrialState(
        state,
        source_types={**state.source_types, **source_types},
        source_catalogs=source_catalogs,
        source_federation_hints=hints,
        tables=tables,
        # What only a role's own surface reads is left empty: this build makes none.
        role_build_inputs=role_build_inputs(
            roles=[],
            tables=tables,
            relationships=relationships,
            col_types_converted=column_types,
            naming_rules=naming_rules,
            domains=domains,
            domain_prefix=domain_prefix,
            kafka_physical=physical,
            tracked_functions=commands.functions,
            tracked_webhooks=commands.webhooks,
            gql_object_cols=object_columns,
            rls_rules=[],
            metrics=metrics,
            field_numbers=None,
            draft_tables=draft_tables,
            remote_sources=remote_sources,
        ),
    )
    model_input, context = whole_model_context(trial, tables, metrics)
    build_model_names(context, model_input, clash_policy(state.settings_overrides))
    trial.view_context = context
    checker_contract_targets(trial, tables)


async def dry_run_configuration(config: "ProvisaConfig") -> "DryRun":
    """``app_loaders.apply_configuration``, as far as it can be taken back."""
    from provisa.api.app import state
    from provisa.core.config_loader import dry_run_config

    model_db = state.model_db
    assert model_db is not None, "the caller refuses a request with no org bound"

    async def check_build(conn: "Connection") -> None:
        await check_model_build(conn, config)

    async with model_db.acquire() as conn:
        return await dry_run_config(
            config, cast("Connection", conn), state.federation_engine, check_build
        )
