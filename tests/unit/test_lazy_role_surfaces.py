# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f1b62-4e07-4a95-b2c8-6e1a9f0d7c54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882: a role's data surface is built the first time it is read, not at every rebuild.

A rebuild built every role's schema, context and proto -- the rebuild's longest phase, linear in the
number of roles -- for every change of the model, whether or not any request then asked for the
role. Now a rebuild registers each role at once and builds it when first read: once, however many
requests ask together, and held after. A role whose build fails holds that failure, logged as an
ERROR naming it, and every read of it raises it; it is never "no schema". A background warm-up
builds every role after a rebuild, and stops when a newer rebuild supersedes it.
"""

# Requirements: REQ-1882

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.api.role_surface import LazySurface, RoleSurfaceUnbuilt, role_surface


class _State:
    """One org runtime, as AppState shows it to the build and to a request."""

    def __init__(self) -> None:
        self._runtime = OrgRuntime(org_id="default")
        self._runtime.role_build_inputs = {"tables": [], "metrics": [], "field_numbers": {}}

    def _active_runtime(self) -> OrgRuntime:
        return self._runtime

    @property
    def role_build_inputs(self):
        return self._runtime.role_build_inputs

    def __getattr__(self, name: str):
        return getattr(self._runtime, name)


@pytest.fixture
def builds(monkeypatch):
    """Every part builder, counted per role."""
    from provisa.api import app_loaders
    from provisa.compiler import schema_gen
    from provisa.grpc import proto_gen

    counted: list[str] = []
    gate = threading.Event()
    gate.set()

    def _schema_input(_state, role, *_a):
        return role["id"]

    def _part(kind: str):
        def _build(si, **_k):
            gate.wait(5)
            if kind == "schema":
                counted.append(si)
                if si == "broken":
                    raise ValueError("column type 'geography' has no proto mapping")
            return SimpleNamespace(kind=kind, role=si, refusal=None, kept=None)

        return _build

    monkeypatch.setattr(app_loaders, "schema_input_for", _schema_input)
    monkeypatch.setattr(app_loaders, "generate_schema", _part("schema"))
    monkeypatch.setattr(app_loaders, "build_context", _part("context"))
    monkeypatch.setattr(schema_gen, "build_table_path_map", _part("path_map"))
    monkeypatch.setattr(proto_gen, "generate_proto", _part("proto"))
    return SimpleNamespace(counted=counted, gate=gate)


def _register(state, *role_ids: str) -> None:
    from provisa.api.app_loaders import register_role_surface

    for role_id in role_ids:
        register_role_surface(state, {"id": role_id}, rls=f"rls-{role_id}")


def test_registering_roles_builds_none_of_them(builds) -> None:
    state = _State()
    _register(state, "analyst", "steward", "auditor")
    assert builds.counted == []
    # Known at once, without a build: membership and listing read no part.
    assert "analyst" in state.contexts and sorted(state.contexts) == [
        "analyst",
        "auditor",
        "steward",
    ]
    assert builds.counted == []


def test_the_first_read_builds_that_role_alone_and_the_next_builds_nothing(builds) -> None:
    state = _State()
    _register(state, "analyst", "steward")
    surface = role_surface(state, "analyst")
    assert (surface.schema.role, surface.context.role, surface.rls) == (
        "analyst",
        "analyst",
        "rls-analyst",
    )
    assert role_surface(state, "analyst") is surface
    assert state.contexts["analyst"] is surface.context
    assert builds.counted == ["analyst"]


def test_requests_asking_together_build_the_role_once(builds) -> None:
    state = _State()
    _register(state, "analyst")
    builds.gate.clear()
    seen: list = []
    failures: list[BaseException] = []

    def _read() -> None:
        try:
            seen.append(role_surface(state, "analyst"))
        except BaseException as exc:  # recorded for the assertion below
            failures.append(exc)

    readers = [threading.Thread(target=_read) for _ in range(4)]
    for r in readers:
        r.start()
    builds.gate.set()
    for r in readers:
        r.join(5)
    assert failures == []
    assert builds.counted == ["analyst"]
    assert len(seen) == 4 and all(s is seen[0] for s in seen)


def test_a_role_that_cannot_be_built_raises_its_failure_on_every_read(builds, caplog) -> None:
    state = _State()
    _register(state, "broken", "analyst")
    with caplog.at_level("ERROR", logger="provisa.api.role_surface"):
        for _ in range(2):
            with pytest.raises(RoleSurfaceUnbuilt) as failed:
                role_surface(state, "broken")
    assert "'broken'" in str(failed.value) and "geography" in str(failed.value)
    # Logged once, naming the role; built once.
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1 and "'broken'" in errors[0].getMessage()
    assert builds.counted == ["broken"]
    # The other roles are untouched by it.
    assert role_surface(state, "analyst").schema.role == "analyst"


def test_a_build_cut_short_by_the_readers_deadline_is_not_held(monkeypatch) -> None:
    from provisa.core.request_deadline import DeadlinePassed

    attempts: list[int] = []

    def _build():
        attempts.append(1)
        if len(attempts) == 1:
            raise DeadlinePassed()
        return "surface"

    lazy = LazySurface("analyst", _build)
    with pytest.raises(DeadlinePassed):
        lazy.resolve()
    assert lazy.resolve() == "surface" and len(attempts) == 2


@pytest.mark.asyncio
async def test_the_warm_up_builds_every_role_and_stops_when_superseded(builds, monkeypatch) -> None:
    from provisa.api import app_loaders

    scheduled: list = []
    monkeypatch.setattr(
        "provisa.core.connection_loop.spawn_after",
        lambda _delay, coro, name: scheduled.append(coro),
    )
    monkeypatch.setattr(app_loaders, "_WARM_PAUSE_S", 0)
    state = _State()
    _register(state, "analyst", "steward", "broken")
    app_loaders.warm_role_surfaces(state)
    await scheduled.pop()
    assert sorted(builds.counted) == ["analyst", "broken", "steward"]

    # A newer rebuild (new build inputs) before the warm-up runs: it builds nothing.
    builds.counted.clear()
    _register(state, "analyst", "steward")
    app_loaders.warm_role_surfaces(state)
    state._runtime.role_build_inputs = {"tables": [], "metrics": [], "field_numbers": {}}
    await scheduled.pop()
    assert builds.counted == []


# --- nothing reads every role's surface -------------------------------------------------------------

_PARTS = {"surfaces", "contexts", "schemas", "rls_contexts", "table_path_maps", "proto_files"}
_PROVISA = __import__("pathlib").Path(__file__).resolve().parents[2] / "provisa"
# The warm-up builds every role on purpose; role_surface.py is the surface itself; a region purge
# plan's ``schemas`` is database schemas, not a role's part.
_ALLOWED = {
    "provisa/api/app_loaders.py::warm_role_surfaces",
    "provisa/api/app_loaders.py::_warm",
    "provisa/api/role_surface.py",
    "provisa/core/region_purge.py::_drop_schemas",
}


def _reads_every_role(node) -> bool:
    import ast

    def is_part(n) -> bool:
        return isinstance(n, ast.Attribute) and n.attr in _PARTS

    # Listing the roles (iterating, membership, len) builds none; reading every one's part does.
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"values", "items"}
        and is_part(node.func.value)
    )


def test_no_code_reads_every_roles_surface() -> None:
    """Each would build every role (LazySurface): a model-wide lookup reads state.view_context or
    state.view_path_map, and a role's own reads its own surface."""
    import ast

    offenders = []
    for path in sorted(_PROVISA.rglob("*.py")):
        rel = path.relative_to(_PROVISA.parent).as_posix()
        if rel in _ALLOWED:
            continue
        tree = ast.parse(path.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if f"{rel}::{fn.name}" in _ALLOWED:
                continue
            for node in ast.walk(fn):
                if _reads_every_role(node):
                    offenders.append(f"{rel}:{getattr(node, 'lineno', fn.lineno)} {fn.name}")
    assert sorted(set(offenders)) == [], "\n".join(sorted(set(offenders)))


def test_a_contract_and_a_field_name_resolve_with_no_role_built(builds, monkeypatch) -> None:
    from provisa.api import app as app_module
    from provisa.api.admin._graphql_field_name import resolve_graphql_field_name
    from provisa.dq.contract import model_contexts, resolve_contract_target

    state = _State()
    _register(state, "analyst", "steward")
    meta = SimpleNamespace(
        table_id=7,
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        catalog_name="pg",
        display_name="",
        field_name="sales__orders",
    )
    state._runtime.view_context = SimpleNamespace(tables={"sales__orders": meta})
    state._runtime.view_path_map = {
        "sales__orders": {"domain_id": "sales", "schema_name": "public", "table_name": "orders"}
    }
    monkeypatch.setattr(app_module, "state", state)
    monkeypatch.setattr("provisa.dq.contract.meta_dataset", lambda m: "provisa/sales/orders")
    assert resolve_contract_target("provisa/sales/orders", model_contexts(state)) is meta
    assert (
        resolve_graphql_field_name(domain_id="sales", schema_name="public", table_name="orders")
        == "sales__orders"
    )
    assert builds.counted == []


def test_a_lazily_built_role_serves_what_an_eager_build_served(monkeypatch) -> None:
    """The SDL and proto of a role built on first read are byte-equal to those built from the same
    generation's inputs at once, and the rebuild itself built no role."""
    import provisa.api.app as appmod
    from graphql import print_schema

    from provisa.api import app_loaders
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy
    from provisa.grpc.proto_gen import generate_proto

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    monkeypatch.setattr(app_loaders, "warm_role_surfaces", lambda _state: None)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    roles = [
        {
            "id": r,
            "description": "Test role.",
            "capabilities": ["usage"],
            "domain_access": ["sales"],
        }
        for r in ("analyst", "steward")
    ]
    tables = [
        {
            "id": 1,
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["analyst", "steward"]},
                {"column_name": "amount", "visible_to": ["steward"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
        }
    ]
    app_loaders._build_and_register_schemas(
        roles=roles,
        tables=tables,
        relationships=[],
        col_types_converted={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="amount", data_type="numeric", is_nullable=True),
            ]
        },
        naming_rules=[],
        domains=[{"id": "sales"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )
    held = state.surfaces
    assert {"analyst", "steward"} <= set(held)
    assert not any(isinstance(h, LazySurface) and h.built for h in held.values())

    inputs = state.role_build_inputs
    for role in roles:
        si = app_loaders.schema_input_for(state, role, inputs["tables"], inputs["metrics"])
        eager_sdl = print_schema(app_loaders.generate_schema(si))
        eager_proto = generate_proto(si, field_numbers=inputs["field_numbers"])
        surface = role_surface(state, role["id"])
        assert print_schema(surface.schema) == eager_sdl
        assert surface.proto == eager_proto
    assert print_schema(role_surface(state, "analyst").schema) != print_schema(
        role_surface(state, "steward").schema
    )


def test_putting_back_a_replaced_part_builds_no_role() -> None:
    """A teardown puts back the view it replaced, and builds none of the roles the test
    installed since. Reading through them built each, and one whose build failed failed the
    teardown, leaving the org the test ran in bound for the next test."""
    from provisa.api.role_surface import install_surface

    runtime = OrgRuntime(org_id="default")
    before = runtime.surfaces
    patch = pytest.MonkeyPatch()
    patch.setattr(runtime, "schemas", {})

    def _fails():
        raise ValueError("No tables visible to role 'analyst'")

    lazy = LazySurface("analyst", _fails)  # as the test's build installs it
    install_surface(runtime, "analyst", lazy)
    patch.undo()
    assert runtime.surfaces == before and not lazy.built
