# Copyright (c) 2026 Kenneth Stott
# Canary: f1d6c0a4-46a4-4cfe-9f4b-1779495f6b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an organisation's build loads belongs to that organisation's runtime.

One process serves every organisation. The application state shows a request the runtime of the
organisation it is bound to (``AppState._active_runtime()``); anything a build loads from an
organisation's control plane — and keys by a role, a source, a table or a view, names that
repeat from one organisation to the next — is held on that runtime and read through a property
of the state. A collection declared on the state's class instead is one object for the whole
process.

The rule is held here: every collection the state's class declares is named in the list
below, with its reason. A new one fails this test until it is a property over the runtime, or
is put on the list by someone who has decided it is the process's.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from provisa.api.app import AppState
from provisa.api.org_runtime import OrgRuntime

_APP = Path(inspect.getfile(AppState))

# The process's own: one value whatever organisation a request is for.
_THE_PROCESSES = {
    "flight_clients": "connections to the engine's Flight endpoints, by shard",
    "apq_cache": "one store; its keys carry the organisation",
    "engine_session_hints": "the engine's session settings, from the deployment's configuration",
    "server_cfg": "the server section of the deployment's configuration file",
    "server_limits": "the deployment's resolved limits",
    "udf_egress_allowlist": "the deployment's allow-list",
}

_COLLECTION_CALLS = {"set", "dict", "list", "MVRegistry", "NoopAPQCache"}


def _state_class() -> ast.ClassDef:
    tree = ast.parse(_APP.read_text())
    (cls,) = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "AppState"]
    return cls


def _class_level_collections() -> set[str]:
    """Names the class binds to a collection in its body and no property replaces."""
    cls = _state_class()
    properties = {n.name for n in cls.body if isinstance(n, ast.FunctionDef)}
    found = set()
    for node in cls.body:
        if not (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)):
            continue
        value = node.value
        is_collection = isinstance(value, (ast.Dict, ast.List, ast.Set)) or (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id in _COLLECTION_CALLS
        )
        if is_collection and node.target.id not in properties:
            found.add(node.target.id)
    return found


def test_every_collection_the_state_class_declares_is_accounted_for():
    declared = _class_level_collections()
    listed = set(_THE_PROCESSES)
    assert declared - listed == set(), (
        "new collection(s) on AppState's class: make each a property over the organisation's "
        "runtime, or decide it is the process's and list it with its reason"
    )
    assert listed - declared == set(), "listed here but no longer declared on the class"


def _runtime_properties() -> set[str]:
    return {
        name
        for name, attribute in vars(AppState).items()
        if isinstance(attribute, property)
        and "_active_runtime()" in inspect.getsource(attribute.fget)
    }


@pytest.mark.parametrize(
    "name",
    ["api_sources", "api_endpoints", "surfaces", "contexts", "schemas", "rls_contexts", "roles"],
)
def test_what_a_build_loads_is_read_from_the_organisations_runtime(name):
    assert name in _runtime_properties()
    assert name not in _class_level_collections()


def test_nothing_binds_a_runtime_property_onto_the_class():
    """A property over the runtime stays one: nothing assigns ``AppState.<name> = …`` or
    ``setattr(AppState, …)``, which would put one object back for the whole process."""
    provisa = _APP.parents[1]
    offenders = []
    for path in sorted(provisa.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            targets = node.targets if isinstance(node, ast.Assign) else []
            for target in targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "AppState"
                ):
                    offenders.append(f"{path.name}:{node.lineno} AppState.{target.attr} = …")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "setattr"
                and node.args
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id == "AppState"
            ):
                offenders.append(f"{path.name}:{node.lineno} setattr(AppState, …)")
    assert offenders == []


# --- an organisation's API sources belong to its runtime -------------------------------------------


def test_two_organisations_api_sources_of_the_same_id_are_each_their_own():
    """Source ids repeat across organisations. Each runtime holds its own API sources and
    endpoints; loading or replacing one organisation's leaves the other's as they were."""
    first, second = OrgRuntime(org_id="first"), OrgRuntime(org_id="second")
    assert first.api_sources is not second.api_sources
    assert first.api_endpoints is not second.api_endpoints

    first.api_sources["crm"] = "the first organisation's crm"
    second.api_sources["crm"] = "the second organisation's crm"
    first.api_endpoints[("crm", "contacts")] = "first/contacts"
    second.api_endpoints[("crm", "contacts")] = "second/contacts"
    assert first.api_sources == {"crm": "the first organisation's crm"}
    assert second.api_endpoints == {("crm", "contacts"): "second/contacts"}

    # A build replaces the organisation's whole set (app.py: `state.api_endpoints,
    # state.api_sources = await load_api_sources(...)`): only that runtime's.
    second.api_sources, second.api_endpoints = {}, {}
    assert first.api_sources == {"crm": "the first organisation's crm"}
    assert first.api_endpoints == {("crm", "contacts"): "first/contacts"}


def test_the_state_shows_a_request_the_api_sources_of_the_organisation_it_is_bound_to(monkeypatch):
    """Through the application state itself: the same attribute, two bound organisations."""
    state = AppState.__new__(AppState)
    runtimes = {"first": OrgRuntime(org_id="first"), "second": OrgRuntime(org_id="second")}
    bound = ["first"]
    monkeypatch.setattr(AppState, "_active_runtime", lambda self: runtimes[bound[0]])

    state.api_sources = {"crm": "first"}
    state.api_endpoints = {("crm", "contacts"): "first"}
    bound[0] = "second"
    assert state.api_sources == {} and state.api_endpoints == {}
    state.api_sources = {"crm": "second"}
    bound[0] = "first"
    assert state.api_sources == {"crm": "first"}
    assert state.api_endpoints == {("crm", "contacts"): "first"}
