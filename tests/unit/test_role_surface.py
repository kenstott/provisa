# Copyright (c) 2026 Kenneth Stott
# Canary: c45c982e-3348-4ce0-b5ca-2be490b81cee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's data surface is one object: built whole, installed by one assignment, read once (#213).

A role's schema, compilation context, row filters, REST path map and proto were five maps written
one after another, and a request on its own thread read them one after another. Between the
schema's install and the context's, a query for a just-registered table was validated against the
new schema and compiled against the old context: an unhandled ``ValueError: Unknown root query
field``, a 500 and a closed connection.

The first test is that gap: a reader looking in the middle of a build. The rest hold the rule —
one writer, no reader of two parts through the views — and that an unknown root field which does
reach the compiler is a client error by name.
"""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.api.role_surface import RoleSurface, SurfacePart, role_surface

_PROVISA = Path(__file__).resolve().parents[2] / "provisa"
_PARTS = {"schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"}


class _State:
    """What the build writes to and a request reads from: one org runtime, as AppState shows it."""

    def __init__(self) -> None:
        self._runtime = OrgRuntime(org_id="default")
        self.role_build_inputs = {"tables": [], "metrics": [], "field_numbers": {}}

    def _active_runtime(self) -> OrgRuntime:
        return self._runtime

    def __getattr__(self, name: str):
        if name == "surfaces" or name in _PARTS:
            return getattr(self._runtime, name)
        raise AttributeError(name)


def _build_stubs(monkeypatch, generation: str, looked: list):
    """The four builders of a surface, each returning an object tagged with ``generation`` — and
    each, when called, looking at the role's surface the way a request does at that moment."""
    from provisa.api import app_loaders
    from provisa.compiler import schema_gen
    from provisa.grpc import proto_gen

    def _part(kind: str):
        def _build(*_args, **_kwargs):
            look = looked[0]
            look()
            return SimpleNamespace(kind=kind, generation=generation, refusal=None, kept=None)

        return _build

    monkeypatch.setattr(app_loaders, "schema_input_for", lambda *_a, **_k: object())
    monkeypatch.setattr(app_loaders, "generate_schema", _part("schema"))
    monkeypatch.setattr(app_loaders, "build_context", _part("context"))
    monkeypatch.setattr(schema_gen, "build_table_path_map", _part("path_map"))
    monkeypatch.setattr(proto_gen, "generate_proto", _part("proto"))


def test_a_request_during_a_rebuild_never_sees_the_schema_of_one_build_and_the_context_of_another(
    monkeypatch,
):
    """THE DEFECT. A reader that looks while a role's surface is being rebuilt — after any one
    part is built, before the next — sees one build's parts, never a mix. (Before: the schema
    was installed, then the path map and context built, then the context installed; a reader in
    between got the new schema with the old context.)"""
    from provisa.api.app_loaders import register_role_surface

    state = _State()
    role = {"id": "analyst"}
    seen: list[tuple[str, str]] = []

    def _nothing() -> None:
        return None

    def _a_request_reads_the_pair() -> None:
        # As the GraphQL endpoint read them: the schema, then the context.
        seen.append((state.schemas["analyst"].generation, state.contexts["analyst"].generation))

    looked = [_nothing]
    _build_stubs(monkeypatch, "v1", looked)
    register_role_surface(state, role, rls="rls-v1")

    looked[0] = _a_request_reads_the_pair
    _build_stubs(monkeypatch, "v2", looked)
    register_role_surface(state, role, rls="rls-v2")

    assert len(seen) == 4  # once from inside each of the four builders
    assert set(seen) == {("v1", "v1")}, f"a request saw parts of two builds: {seen}"
    assert (state.schemas["analyst"].generation, state.contexts["analyst"].generation) == (
        "v2",
        "v2",
    )


def test_a_build_installs_every_part_in_one_object(monkeypatch):
    from provisa.api.app_loaders import register_role_surface

    state = _State()
    _build_stubs(monkeypatch, "v1", [lambda: None])
    register_role_surface(state, {"id": "analyst"}, rls="rls-v1")
    surface = role_surface(state, "analyst")
    assert isinstance(surface, RoleSurface)
    assert (surface.schema.kind, surface.context.kind) == ("schema", "context")
    assert (surface.table_path_map.kind, surface.proto.kind) == ("path_map", "proto")
    assert surface.rls == "rls-v1"
    # The five old names are views of that one object.
    assert state.schemas["analyst"] is surface.schema
    assert state.contexts["analyst"] is surface.context
    assert state.rls_contexts["analyst"] is surface.rls
    assert state.table_path_maps["analyst"] is surface.table_path_map
    assert state.proto_files["analyst"] is surface.proto


def test_a_snapshot_taken_before_a_rebuild_stays_whole_after_it(monkeypatch):
    """What a request holds is the object, not a way to look the parts up again."""
    from provisa.api.app_loaders import register_role_surface

    state = _State()
    _build_stubs(monkeypatch, "v1", [lambda: None])
    register_role_surface(state, {"id": "analyst"}, rls="rls-v1")
    held = role_surface(state, "analyst")
    _build_stubs(monkeypatch, "v2", [lambda: None])
    register_role_surface(state, {"id": "analyst"}, rls="rls-v2")
    assert (held.schema.generation, held.context.generation, held.rls) == ("v1", "v1", "rls-v1")
    assert role_surface(state, "analyst") is not held


def test_a_role_that_loses_its_data_surface_loses_all_of_it(monkeypatch):
    from provisa.api.app_loaders import _drop_data_surface, register_role_surface

    state = _State()
    _build_stubs(monkeypatch, "v1", [lambda: None])
    register_role_surface(state, {"id": "analyst"}, rls="rls-v1")
    _drop_data_surface(state, "analyst")
    assert role_surface(state, "analyst") is None
    for part in _PARTS:
        assert "analyst" not in getattr(state, part)
    _drop_data_surface(state, "analyst")  # a role that had none: nothing to do


def test_a_part_is_a_view_of_the_surfaces_and_a_write_installs_a_surface():
    holder = SimpleNamespace(surfaces={})
    contexts = SurfacePart(holder, "context")
    assert dict(contexts) == {} and "analyst" not in contexts
    contexts["analyst"] = "ctx"  # a test setting a state up part by part
    assert holder.surfaces["analyst"] == RoleSurface(context="ctx")
    assert contexts == {"analyst": "ctx"} and SurfacePart(holder, "context") == contexts
    assert "analyst" not in SurfacePart(holder, "schema")  # that part was never set
    with pytest.raises(KeyError):
        SurfacePart(holder, "schema")["analyst"]
    assert contexts.get("nobody", "default") == "default"
    assert contexts.pop("nobody", None) is None
    del contexts["analyst"]
    assert holder.surfaces == {} and dict(SurfacePart(holder, "context")) == {}


def test_surfaces_is_replaced_never_changed_in_place():
    """So a mapping taken earlier — by a request, or by a view — still shows what it showed."""
    from provisa.api.role_surface import install_surface

    holder = SimpleNamespace(surfaces={})
    install_surface(holder, "analyst", RoleSurface(context="ctx-1"))
    earlier = holder.surfaces
    view = SurfacePart(holder, "context")
    install_surface(holder, "analyst", RoleSurface(context="ctx-2"))
    install_surface(holder, "admin", RoleSurface(context="ctx-a"))
    assert holder.surfaces is not earlier
    assert earlier == {"analyst": RoleSurface(context="ctx-1")}
    assert view == {"analyst": "ctx-1"}
    assert SurfacePart(holder, "context") == {"analyst": "ctx-2", "admin": "ctx-a"}
    install_surface(holder, "analyst", None)
    install_surface(holder, "nobody", None)  # a role that had none: nothing to do
    assert set(holder.surfaces) == {"admin"}


def test_a_part_taken_before_a_change_is_what_puts_it_back():
    """``monkeypatch.setattr(state, "contexts", {...})`` reads the old value, sets the new one,
    and at teardown sets the old one again: the old value must still be the old content."""
    runtime = OrgRuntime(org_id="default")
    runtime.contexts = {"analyst": "ctx-1"}
    runtime.rls_contexts = {"analyst": "rls-1"}
    before = runtime.contexts
    runtime.contexts = {"admin": "ctx-2"}
    assert runtime.contexts == {"admin": "ctx-2"}
    assert runtime.rls_contexts == {"analyst": "rls-1"}  # the other parts are untouched
    runtime.contexts = before
    assert runtime.contexts == {"analyst": "ctx-1"}
    assert runtime.surfaces == {"analyst": RoleSurface(context="ctx-1", rls="rls-1")}
    assert before == {"analyst": "ctx-1"}  # the view taken first never moved


# --- the rule, over the product's source -----------------------------------------------------------


def _functions():
    for path in sorted(_PROVISA.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield path.relative_to(_PROVISA.parent), node


_STATE_NAMES = {"state", "_state", "app_state", "runtime", "rt"}


def _is_part(node: ast.AST) -> bool:
    """``<the state or a runtime>.<a part>`` — not some other object's attribute of that name —
    or the same read spelled ``getattr(<state>, "<a part>", ...)``."""
    if isinstance(node, ast.Call):
        return (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _PARTS
        )
    if not (isinstance(node, ast.Attribute) and node.attr in _PARTS):
        return False
    holder = node.value
    name = holder.id if isinstance(holder, ast.Name) else getattr(holder, "attr", None)
    return name in _STATE_NAMES


def _part_of(node: ast.AST) -> str:
    return node.args[1].value if isinstance(node, ast.Call) else node.attr  # type: ignore[attr-defined]


def test_no_function_reads_two_parts_of_a_role_through_the_views():
    """Two lookups are two moments: a rebuild between them hands the function parts of two
    builds. A function that needs more than one part takes the surface once."""
    offenders = []
    for path, fn in _functions():
        parts = {_part_of(n) for n in ast.walk(fn) if _is_part(n)}
        if len(parts) >= 2:
            offenders.append(f"{path}:{fn.lineno} {fn.name} reads {sorted(parts)}")
    assert offenders == [], "\n".join(offenders)


def test_nothing_in_the_product_writes_a_part_through_a_view():
    """The build is the one writer, and it writes whole surfaces. (A view accepts a write so a
    test can set a state up part by part; product code never uses that.)"""
    mutators = {"pop", "update", "clear", "setdefault", "popitem"}
    offenders = []
    for path in sorted(_PROVISA.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            where = f"{path.relative_to(_PROVISA.parent)}:{getattr(node, 'lineno', 0)}"
            targets: list[ast.AST] = []
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
                targets = [node.target]
            elif isinstance(node, ast.Delete):
                targets = list(node.targets)
            for target in targets:
                if isinstance(target, ast.Subscript) and _is_part(target.value):
                    offenders.append(f"{where} writes {_part_of(target.value)}[...]")
                elif _is_part(target):
                    offenders.append(f"{where} replaces .{_part_of(target)}")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in mutators
                and _is_part(node.func.value)
            ):
                offenders.append(f"{where} calls .{_part_of(node.func.value)}.{node.func.attr}()")
    assert offenders == [], "\n".join(offenders)


def test_the_build_is_the_only_writer_of_surfaces():
    """Surfaces are installed through one function, called by the build and by nothing else;
    nothing changes the mapping in place or assigns it."""
    installers, in_place = set(), []
    for path, fn in _functions():
        where = f"{path.as_posix()}::{fn.name}"
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                callee = node.func
                name = callee.id if isinstance(callee, ast.Name) else getattr(callee, "attr", "")
                if name == "install_surface":
                    installers.add(where)
                if (
                    isinstance(callee, ast.Attribute)
                    and callee.attr in {"pop", "update", "clear", "setdefault", "popitem"}
                    and isinstance(callee.value, ast.Attribute)
                    and callee.value.attr == "surfaces"
                ):
                    in_place.append(where)
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
                targets = [node.target]
            elif isinstance(node, ast.Delete):
                targets = node.targets
            for target in targets:
                inner = target.value if isinstance(target, ast.Subscript) else target
                if isinstance(inner, ast.Attribute) and inner.attr == "surfaces":
                    in_place.append(where)
    assert installers == {
        "provisa/api/app_loaders.py::register_role_surface",
        "provisa/api/app_loaders.py::_drop_data_surface",
        # A view's own write, which product code never reaches (the test above).
        "provisa/api/role_surface.py::__setitem__",
        "provisa/api/role_surface.py::__delitem__",
    }
    assert sorted(in_place) == [
        # A draft table's read (REQ-1921) holds a state of its own: its surfaces are a new
        # mapping, made once when it is built, and no other holder's changes.
        "provisa/api/admin/table_read.py::__init__",
        "provisa/api/role_surface.py::install_surface",
    ]


# --- an unknown root field that reaches the compiler -------------------------------------------------


def test_an_unknown_root_field_is_a_client_error_by_name():
    from graphql import parse

    from provisa.compiler.sql_gen import UnknownRootField, compile_query

    ctx = SimpleNamespace(tables={})
    with pytest.raises(UnknownRootField) as refused:
        compile_query(parse("{ s__customers { id } }"), ctx, None)
    assert refused.value.field_name == "s__customers"
    assert refused.value.code == "data.unknown_root_field"
    assert refused.value.params == {"field": "s__customers"}
    assert str(refused.value) == "Unknown root query field: 's__customers'"
    assert isinstance(refused.value, ValueError)  # surfaces that answer compiler refusals do


def test_over_http_it_is_a_400_with_its_code_never_an_unhandled_exception(monkeypatch):
    """It was a 500 and a closed connection. The handler the application registers, driven
    through a route that raises what the compiler raises."""
    from fastapi.testclient import TestClient

    import provisa.api.app as app_mod
    from provisa.api.app import create_app
    from provisa.compiler.sql_gen import UnknownRootField
    from provisa.core.config_loader import parse_config_dict

    # A served app always has its config: lifespan loads it before the first request.
    monkeypatch.setattr(
        app_mod.state,
        "config",
        parse_config_dict({"sources": [], "domains": [], "tables": [], "roles": []}),
    )

    app = create_app()

    async def _raises() -> None:
        raise UnknownRootField("s__customers")

    app.add_api_route("/health/_unknown_root_field", _raises)
    answered = TestClient(app, raise_server_exceptions=False).get("/health/_unknown_root_field")
    assert answered.status_code == 400
    assert answered.json() == {
        "detail": "Unknown root query field: 's__customers'",
        "code": "data.unknown_root_field",
        "params": {"field": "s__customers"},
    }


def test_the_ui_has_the_refusals_message_in_every_locale():
    import json

    locales = sorted((_PROVISA.parent / "provisa-ui/src/i18n/locales").glob("*/serverErrors.json"))
    assert len(locales) == 13
    for path in locales:
        text = json.dumps(json.loads(path.read_text(encoding="utf-8")), ensure_ascii=False)
        assert '"unknown_root_field"' in text and "{{field}}" in text, path


# --- a role's surface belongs to its organisation's runtime ------------------------------------------


def test_two_organisations_roles_of_the_same_name_each_have_their_own_surface(monkeypatch):
    """Role ids repeat across organisations (every one has ``org_admin``). Each organisation's
    runtime holds its own surface for the role — schema, context, row filters, path map and
    proto alike — and dropping the role's surface in one leaves the other's."""
    from provisa.api.app_loaders import _drop_data_surface, register_role_surface

    first, second = _State(), _State()
    _build_stubs(monkeypatch, "first-org", [lambda: None])
    register_role_surface(first, {"id": "org_admin"}, rls="first-rls")
    _build_stubs(monkeypatch, "second-org", [lambda: None])
    register_role_surface(second, {"id": "org_admin"}, rls="second-rls")

    for state, org in ((first, "first-org"), (second, "second-org")):
        surface = role_surface(state, "org_admin")
        assert {
            surface.schema.generation,
            surface.context.generation,
            surface.table_path_map.generation,
            surface.proto.generation,
        } == {org}
        assert state.proto_files["org_admin"].generation == org
        assert state.table_path_maps["org_admin"].generation == org
    assert role_surface(first, "org_admin") is not role_surface(second, "org_admin")

    _drop_data_surface(first, "org_admin")
    assert role_surface(first, "org_admin") is None
    assert "org_admin" not in first.proto_files and "org_admin" not in first.table_path_maps
    assert role_surface(second, "org_admin").proto.generation == "second-org"
    assert second.table_path_maps["org_admin"].generation == "second-org"


def test_each_organisations_api_documents_list_its_own_tables():
    """The OpenAPI document of the REST routes and the one of the JSON:API routes are built from
    the role's surface: an organisation's document lists that organisation's tables."""
    from graphql import build_schema

    from provisa.api.jsonapi.spec import generate_jsonapi_openapi_spec
    from provisa.api.rest.openapi_spec import generate_rest_openapi_spec
    from provisa.api.role_surface import install_surface

    def _organisation(table: str) -> _State:
        state = _State()
        schema = build_schema(f"type Query {{ {table}: [Row] }} type Row {{ id: Int }}")
        path_map = {table: {"domain_id": "sales", "table_name": table}}
        install_surface(
            state._active_runtime(),
            "org_admin",
            RoleSurface(schema=schema, context=object(), rls=None, table_path_map=path_map),
        )
        return state

    first, second = _organisation("orders"), _organisation("invoices")
    for build in (generate_rest_openapi_spec, generate_jsonapi_openapi_spec):
        ours, theirs = str(build(first, "org_admin")), str(build(second, "org_admin"))
        assert "orders" in ours and "invoices" not in ours, build.__name__
        assert "invoices" in theirs and "orders" not in theirs, build.__name__


def test_the_application_state_reads_every_part_from_the_active_organisations_runtime():
    """None of the five is held by the application state itself: each is read from the runtime
    of the organisation the request is bound to."""
    import inspect

    from provisa.api.app import AppState

    for part in sorted(_PARTS | {"surfaces"}):
        attribute = inspect.getattr_static(AppState, part)
        assert isinstance(attribute, property), part
        assert "_active_runtime()" in inspect.getsource(attribute.fget), part
