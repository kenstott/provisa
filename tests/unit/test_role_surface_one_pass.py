"""REQ-1882: a role's surface finds and names its tables once, and is the surface it was.

The warm-up after a rebuild builds every role's surface: its context, schema, path map and proto.
Each part found the role's tables and named them again from the same input, and every name of
every column asked the org's settings for its convention. Now the surface build finds and names
them once (schema_gen.role_tables) and holds the org's conventions for the build
(naming.conventions_held). Each part, built by the warm-up's own path (_RoleParts.surface), is
the one its builder makes on its own (the path every other caller takes): the printed schema, the
proto, the path map and the context, for roles that reach every domain, none, meta's default tier
alone, the governance columns, commands alone, and under an org convention of its own.
"""

# Requirements: REQ-1882

from __future__ import annotations

import copy
import functools
import importlib.util
import json
import re
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from graphql import print_schema

from provisa.api.app_loaders import _RoleParts, schema_input_for
from provisa.api.org_runtime import OrgRuntime, serving
from provisa.compiler import naming
from provisa.compiler.context import build_context
from provisa.compiler.schema_gen import build_table_path_map, generate_schema
from provisa.core import request_context
from provisa.grpc.proto_gen import generate_proto
from provisa.security.rights import reads_data

_REPO = Path(__file__).resolve().parents[2]
_META = _REPO / "tests" / "fixtures" / "graphql_golden" / "meta_model.yaml"
_E2E = _REPO / "config" / "provisa.yaml"

_ALL = ["*", "meta", "ontology", "ops"]
_ROLES = [
    {"id": "viewer", "domain_access": _ALL, "capabilities": []},
    # The meta catalog's governance columns are served with this capability alone.
    {"id": "viewer", "domain_access": _ALL, "capabilities": ["view_governance"]},
    # No meta grant: meta's default tier is read on SQL alone (schema_gen._visible_tables).
    {"id": "viewer", "domain_access": ["ontology"], "capabilities": []},
    {"id": "data_steward", "domain_access": _ALL, "capabilities": []},
    {
        "id": "pet_analyst",
        "domain_access": ["pet-store", "shelter"],
        "capabilities": ["query_development"],
    },
    {"id": "nobody", "domain_access": [], "capabilities": []},
    # REQ-870: commands alone: its schema is its commands.
    {"id": "viewer", "domain_access": _ALL, "capabilities": ["command_execution"]},
]
_MODELS = ["meta", "meta-published", "e2e"]


def _cypher_golden():
    spec = importlib.util.spec_from_file_location(
        "cypher_golden", _REPO / "scripts" / "cypher_golden.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["cypher_golden"] = module
    spec.loader.exec_module(module)
    return module


@functools.cache
def _model(name: str):
    """A model's schema input as the server's build is given it: the meta catalog (granted to
    viewer), the same with one column of each meta table published (REQ-1959: meta's default
    tier, read on SQL alone by a role with no meta grant), or the e2e model."""
    si, *_ = _cypher_golden().model_inputs(_E2E if name == "e2e" else _META, parameters=True)
    if name == "meta-published":
        tables = copy.deepcopy(si.tables)
        for table in tables:
            if table["domain_id"] == "meta":
                table["columns"][0]["scope"] = "public"
        si = replace(si, tables=tables)
    return si


class _State:
    """One org runtime holding a model's build inputs, as AppState shows it to a role's build."""

    source_types: dict = {}
    source_federation_hints: dict = {}
    federation_engine = None
    source_catalogs: dict = {}
    pg_enum_types: dict = {}
    masking_rules: dict = {}

    def __init__(self, si) -> None:
        self.runtime = OrgRuntime(org_id="default")
        self.runtime.role_build_inputs = {
            "tables": si.tables,
            "metrics": [],
            "relationships": si.relationships,
            "column_types": si.column_types,
            "naming_rules": si.naming_rules,
            "domains": si.domains,
            "domain_prefix": si.domain_prefix,
            "physical_table_map": si.physical_table_map,
            "functions": si.functions,
            "webhooks": si.webhooks,
            "gql_object_columns": si.gql_object_columns,
            "governed_gql_types": si.governed_gql_types,
            "gql_governed_object_cols": si.gql_governed_object_cols,
            "columns_outside_relation": si.columns_outside_relation,
            "field_numbers": None,
            "withdrawn_tables": [],
        }

    def _active_runtime(self) -> OrgRuntime:
        return self.runtime

    @property
    def role_build_inputs(self):
        return self.runtime.role_build_inputs


def _context_text(ctx) -> str:
    """The context printed: every attribute, sets in order, no object address."""
    printed = {
        name: re.sub(
            r" at 0x[0-9a-f]+",
            "",
            repr(sorted(map(repr, value)) if isinstance(value, (set, frozenset)) else value),
        )
        for name, value in vars(ctx).items()
    }
    return json.dumps(printed, sort_keys=True, indent=0)


def _printed(schema, ctx, path_map, proto) -> dict[str, str]:
    return {
        "schema": print_schema(schema),
        "context": _context_text(ctx),
        "path_map": json.dumps(path_map, sort_keys=True, default=str),
        "proto": proto,
    }


def _each_part_on_its_own(state: _State, role: dict) -> dict[str, str]:
    """Each part as its builder makes it on its own: every one finds and names its tables."""
    inputs = state.role_build_inputs
    with serving(state.runtime):
        reads = reads_data(role.get("capabilities"))
        si = schema_input_for(
            state,
            role,
            inputs["tables"] if reads else [],
            inputs["metrics"] if reads else [],
        )
        ctx = build_context(si)
        ctx.refusal = state.runtime.data_refusal
        ctx.kept = dict(state.runtime.kept)
        return _printed(
            generate_schema(si),
            ctx,
            build_table_path_map(si),
            generate_proto(si, field_numbers=inputs["field_numbers"]),
        )


def _warm_up_surface(state: _State, role: dict):
    """The role's surface as the warm-up builds it (warm_role_surfaces resolves _RoleParts)."""
    return _RoleParts(state, state.runtime, state.role_build_inputs, role, rls=None).surface()


def _as_the_warm_up_builds_it(state: _State, role: dict) -> dict[str, str]:
    surface = _warm_up_surface(state, role)
    return _printed(surface.schema, surface.context, surface.table_path_map, surface.proto)


@pytest.fixture
def org_convention(monkeypatch):
    """The org's settings as the server reads them for the org a build is bound to
    (api/app.py's provider): an org's own GraphQL convention, or none."""

    def _set(convention: str | None) -> None:
        overrides = {"naming": {"convention": convention}} if convention else {}
        monkeypatch.setattr(request_context, "_org_overrides_provider", lambda: overrides)

    return _set


@pytest.mark.parametrize("convention", [None, "hasura_graphql"])
@pytest.mark.parametrize("config", _MODELS)
@pytest.mark.parametrize(
    "role",
    _ROLES,
    ids=lambda r: f"{r['id']}:{','.join(r['domain_access'])}:{','.join(r['capabilities'])}",
)
def test_each_part_of_a_role_surface_is_the_one_its_builder_makes(
    config, role, convention, org_convention
) -> None:
    org_convention(convention)
    state = _State(_model(config))
    expected = _each_part_on_its_own(state, role)
    built = _as_the_warm_up_builds_it(state, role)
    for part in ("schema", "context", "path_map", "proto"):
        assert built[part] == expected[part], f"{part} differs"


def test_the_roles_differ_so_the_comparison_covers_each_case() -> None:
    """The roles above are not one surface seven times: on the published meta catalog a role with
    every domain, the governance columns too, meta's default tier alone (its tables on SQL alone)
    and none are four surfaces, and the default tier is the role's SQL alone."""
    from provisa.compiler.schema_gen import role_tables

    state = _State(_model("meta-published"))
    schemas = {print_schema(_warm_up_surface(state, _ROLES[i]).schema) for i in (0, 1, 2, 5)}
    assert len(schemas) == 4
    with serving(state.runtime):
        found = role_tables(
            schema_input_for(state, _ROLES[2], state.role_build_inputs["tables"], [])
        )
    assert found.sql_only and len(found.served()) < len(found.for_sql())


def test_held_conventions_are_the_bound_orgs_and_no_others(monkeypatch) -> None:
    """Held for a block, the conventions are the org's the block began bound to; work in the
    block bound to another org reads that org's own, and after the block the settings are read
    again."""
    conventions = {"acme": "hasura_graphql", "beta": "snake"}
    monkeypatch.setattr(
        request_context,
        "_org_overrides_provider",
        lambda: {"naming": {"convention": conventions[request_context.require_current_org()]}},
    )
    acme = request_context.set_current_org("acme")
    try:
        with naming.conventions_held():
            assert naming.active_gql_convention() == naming.normalize_convention("hasura_graphql")
            beta = request_context.set_current_org("beta")
            try:
                assert naming.active_gql_convention() == naming.normalize_convention("snake")
            finally:
                request_context.reset_current_org(beta)
            assert naming.active_gql_convention() == naming.normalize_convention("hasura_graphql")
        conventions["acme"] = "snake"
        assert naming.active_gql_convention() == naming.normalize_convention("snake")
    finally:
        request_context.reset_current_org(acme)
