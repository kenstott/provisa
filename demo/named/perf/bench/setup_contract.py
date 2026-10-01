# Copyright (c) 2026 Kenneth Stott
# Canary: 883ab443-eb91-4c63-ae6d-fc59dc0ad5fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The benchmark setup data contract (REQ-1911): load, bind and inspect.

One declarative YAML file names the deployment under test (endpoint per transport, credentials
reference, role mix), the tables it may use by their registered identity with their columns and
join pairs, the replication setting the run is measured under, and every knob (distribution +
probability, per-transport overrides, source weights, route, cache, load, seed).
``setups/perf-stack.yaml`` is the perf stack's own contract, every knob at zero, equal to the
optimistic test.

What each transport calls the tables and columns is not in the contract: ``load_setup`` takes the
deployment's answers (``lookup.py``) and binds them.

Nothing is defaulted: a missing key, an unknown key, a malformed value, or a knob that is set but
not built is a ``SetupError`` naming the field. The model is ``contract_model``, the parsing
``contract_parse``, the checks ``contract_checks``, the join expansion ``contract_joins``."""

from __future__ import annotations

import os
from collections.abc import Collection, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

import lookup
import yaml
from contract_checks import (
    _check_implemented,
    _check_joins,
    _check_ranges,
    _check_route,
    _check_sources_reachable,
)
from contract_model import CONTRACT_VERSION, Join, Setup, SetupError, Source, Table
from contract_parse import (
    _credentials,
    _cross_source_join,
    _endpoints,
    _int,
    _knobs,
    _list,
    _load_section,
    _mapping,
    _roles,
    _same_source_join,
    _source,
    _str,
    _transports,
)

# --------------------------------------------------------------------------------------------
# Load
# --------------------------------------------------------------------------------------------


def load_setup(
    path: Path | str,
    *,
    environ: Mapping[str, str] | None = None,
    known_transports: Collection[str],
    deployment: lookup.RawLookup | lookup.Resolved | None = None,
    verify_replication: bool = True,
) -> Setup:
    """Load and validate a contract. ``known_transports`` is the set the generator can run.

    ``environ`` is the process environment unless a test passes another."""
    try:
        raw = yaml.safe_load(Path(path).read_text())
    except yaml.YAMLError as exc:
        raise SetupError(f"{path}: not valid YAML: {exc}") from exc
    return setup_from_dict(
        raw,
        environ=environ,
        known_transports=known_transports,
        deployment=deployment,
        verify_replication=verify_replication,
    )


def setup_from_dict(
    raw: Any,
    *,
    environ: Mapping[str, str] | None = None,
    known_transports: Collection[str],
    deployment: lookup.RawLookup | lookup.Resolved | None = None,
    verify_replication: bool = True,
) -> Setup:
    """Validate a contract that is already parsed (``load_setup`` without the file).

    ``deployment``: the deployment's answers (raw lookup responses, or the resolved names a
    previous run wrote). Given, each table's and column's spelling per transport is filled in and
    the checks that need them run; without it the setup is unbound (names empty) and only the
    contract's own consistency is checked.

    ``verify_replication``: with a deployment, refuse a contract whose declared replication
    (live | replica, TTL) is not what the deployment has. A run that applies the setting itself
    (``--apply-replication``) passes False."""
    env = os.environ if environ is None else environ
    _mapping(
        raw,
        "",
        required=(
            "version",
            "deployment",
            "sources",
            "base",
            "knobs",
            "transports",
            "load",
            "seed",
        ),
        optional=("cross_source_joins",),
    )
    if raw["version"] != CONTRACT_VERSION:
        raise SetupError(f"version: must be {CONTRACT_VERSION}, got {raw['version']!r}")
    section = _mapping(
        raw["deployment"], "deployment", required=("name", "endpoints", "credentials", "roles")
    )
    src_raw = raw["sources"]
    if not isinstance(src_raw, dict) or not src_raw:
        raise SetupError("sources: must be a non-empty mapping")
    sources: dict[str, Source] = {}
    for sid, body in src_raw.items():
        tables, joins_raw, replication, container = _source(body, f"sources.{sid}")
        by_name = {t.name: t for t in tables}
        joins = tuple(
            _same_source_join(j, f"sources.{sid}.joins[{i}]", sid, by_name)
            for i, j in enumerate(joins_raw)
        )
        sources[sid] = Source(tables, joins, replication, container)
    cross = tuple(
        _cross_source_join(j, f"cross_source_joins[{i}]", sources)
        for i, j in enumerate(_list(raw.get("cross_source_joins", []), "cross_source_joins"))
    )
    # What a request is when no knob applies: one column, no filter, no join (the optimistic
    # request) and this many rows.
    base_raw = _mapping(raw["base"], "base", required=("rows",))
    base = {"rows": _int(base_raw["rows"], "base.rows", minimum=1)}
    setup = Setup(
        name=_str(section["name"], "deployment.name"),
        endpoints=_endpoints(section["endpoints"], "deployment.endpoints", env),
        credentials=_credentials(section["credentials"], "deployment.credentials", env),
        roles=_roles(section["roles"], "deployment.roles"),
        sources=sources,
        cross_source_joins=cross,
        base=base,
        knobs=_knobs(raw["knobs"], "knobs", list(sources)),
        transports=_transports(
            raw["transports"], "transports", list(known_transports), list(sources)
        ),
        load=_load_section(raw["load"], "load"),
        seed=_int(raw["seed"], "seed"),
    )
    _check_implemented(setup)
    _check_route(setup)
    _check_ranges(setup)
    if deployment is None:
        return setup
    try:
        resolved = (
            deployment
            if isinstance(deployment, lookup.Resolved)
            else lookup.resolve(*identities(setup), deployment)
        )
    except lookup.LookupError_ as exc:
        raise SetupError(str(exc)) from exc
    return bind(setup, resolved, verify_replication=verify_replication)


def base_table_unbound(setup: Setup) -> Table:
    """The first table of the first source (for inspecting an unbound setup)."""
    return next(iter(setup.sources.values())).tables[0]


def table_identity(setup: Setup, source: str, table: Table) -> lookup.TableIdentity:
    return lookup.TableIdentity(
        source, table.schema, table.name, tuple(c.name for c in table.columns)
    )


def identities(setup: Setup) -> tuple[list[lookup.TableIdentity], list[lookup.JoinIdentity]]:
    """The tables and joins the contract names, by registered identity, for the lookup."""
    tables = {
        (sid, t.name): table_identity(setup, sid, t)
        for sid, src in setup.sources.items()
        for t in src.tables
    }
    joins = [
        lookup.JoinIdentity(
            tables[(j.left_source, j.left_table)],
            j.left_column,
            tables[(j.right_source, j.right_table)],
            j.right_column,
        )
        for j in _all_joins(setup)
    ]
    return list(tables.values()), joins


def _all_joins(setup: Setup) -> list[Join]:
    return [j for src in setup.sources.values() for j in src.joins] + list(setup.cross_source_joins)


def bind(setup: Setup, resolved: lookup.Resolved, *, verify_replication: bool = True) -> Setup:
    """Fill the setup's tables, columns and joins with the deployment's spellings, then check what
    needs them: every source with weight on a transport must be exposed on it, and every join
    must be traversable."""
    sources: dict[str, Source] = {}
    for sid, src in setup.sources.items():
        tables = []
        for t in src.tables:
            key = table_identity(setup, sid, t).key
            names = resolved.tables.get(key)
            if names is None:
                raise SetupError(f"resolved names have no table {key}")
            for c in t.columns:
                if c.name not in names.columns:
                    raise SetupError(f"resolved names have no column {c.name!r} of {key}")
            tables.append(
                replace(
                    t,
                    sql=names.sql,
                    cypher_label=names.cypher_label,
                    cypher_var=t.name[0] if names.cypher_label else None,
                    graphql_field=names.graphql_field,
                    grpc_type_name=names.grpc_type,
                    rest_path=names.rest_path,
                    jsonapi_path=names.jsonapi_path,
                    jsonapi_type=names.jsonapi_type,
                    columns=tuple(replace(c, names=dict(names.columns[c.name])) for c in t.columns),
                )
            )
        sources[sid] = Source(
            tuple(tables),
            src.joins,
            src.replication,
            src.container,
            kind=resolved.sources[sid]["type"],
        )
    ident = {
        (sid, t.name): table_identity(setup, sid, t)
        for sid, src in setup.sources.items()
        for t in src.tables
    }

    def oriented(j: Join) -> Join:
        key = lookup.JoinIdentity(
            ident[(j.left_source, j.left_table)],
            j.left_column,
            ident[(j.right_source, j.right_table)],
            j.right_column,
        ).key
        names = resolved.joins.get(key)
        if names is None:
            raise SetupError(f"resolved names have no join {key}")
        if names.forward:
            return replace(j, graphql_field=names.graphql_field, cypher_rel=names.cypher_rel)
        # the registered relationship runs right -> left: orient the join the way it is registered
        return Join(
            j.right_source,
            j.right_table,
            j.right_column,
            j.left_source,
            j.left_table,
            j.left_column,
            names.graphql_field,
            names.cypher_rel,
        )

    for sid, src in list(sources.items()):
        sources[sid] = replace(src, joins=tuple(oriented(j) for j in src.joins))
    bound = replace(
        setup,
        sources=sources,
        cross_source_joins=tuple(oriented(j) for j in setup.cross_source_joins),
    )
    _check_sources_reachable(bound)
    _check_joins(bound)
    if verify_replication:
        import replication  # imports this module: not at the top

        problems = replication.mismatches(bound, resolved)
        if problems:
            raise SetupError(
                "the deployment is not as the contract declares: "
                + "; ".join(problems)
                + " (set it, or let the run set it and restore it with --apply-replication)"
            )
    return bound


# --------------------------------------------------------------------------------------------
# The base request
# --------------------------------------------------------------------------------------------


def base_table(setup: Setup, transport: str) -> tuple[str, Table]:
    """(source, table) the request reads when no knob applies, on ``transport``: the first
    weighted table, in declaration order, of the first source that has weight on it."""
    for sid, src in setup.sources.items():
        if setup.source_weights(transport)[sid] > 0:
            for table in src.tables:
                if table.weight > 0:
                    return sid, table
    raise SetupError(f"transports.{transport}: no source has weight, so no table can be read")
