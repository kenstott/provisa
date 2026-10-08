# Copyright (c) 2026 Kenneth Stott
# Canary: 8e0b7c52-3f1d-4a6e-9b84-5d2c1a7f6e30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every /admin handler carries an authorization gate, or is on a named exemption list.

Walks the MOUNTED application (sub-apps and Mounts included) and the admin GraphQL schema, and for
each handler follows its call graph (plus FastAPI ``Depends`` callables) looking for a call to a
known gate. A new ungated /admin route fails this test; the fix is to gate it with an existing
helper, or to add it to the exemption dict below WITH the reason it needs none.

An exemption entry that no longer matches an ungated handler also fails, so the list cannot rot.
"""

# Requirements: REQ-1337, REQ-1349, REQ-1913

from __future__ import annotations

import ast
import inspect
import textwrap

import pytest
from starlette.routing import Mount

# The gates. A handler is gated when its call graph reaches a call to one of these names.
GATE_NAMES = frozenset(
    {
        # provisa.api.admin._platform_guard
        "require_deployment_settings",
        "require_platform_settings",
        "require_org_settings",
        "require_observability",
        "has_deployment_settings",
        "has_platform_settings",
        "has_right",
        "_require_right",
        # provisa.api.admin.capabilities
        "require_capability",
        "require_capability_request",
        "require_domain",
        "require_domain_request",
        # REQ-1944: a governance right paired with the domains of the roles carrying it
        "require_right_in_domains",
        "require_right_in_domains_request",
        "holds_right_in_domains",
        # provisa.api.admin._hiding_guard (REQ-1944)
        "require_table_save",
        "require_hiding_editor_request",
        # provisa.api.admin.domain_guard
        "require_table_domain",
        # capability resolution done in-handler
        "_resolved_capabilities",
        "has_capability",
        "has_capability_request",
        "role_definitions_visible",
        "check_capability",
        "can_act_cross_org",
        # router-local gates
        "_require_org_admin",
        "_require_platform_admin",
        "_administered_org_scope",
        "_org_guard",
        "_guard_within",
        "_require_capability",
        "_require_glossary_rw",
        "_require_glossary_read",
        "_require_term_curatable",
        "_require_term_readable",
        "_require_table_registration",
        "_require_user_management",
        "_member",
    }
)

_DEPTH = 5

# (method, path) of a REST handler, or ("graphql:<kind>", field) of an admin resolver, that is
# reachable without a gate on purpose, or is waiting on a decision. Each needs a reason.
EXEMPT: dict[tuple[str, str], str] = {
    # --- caller-scoped by design ----------------------------------------------------------------
    (
        "POST",
        "/admin/orgs/",
    ): "self-service org creation; any authenticated user, anonymous refused",
    ("POST", "/admin/orgs/{org_id}/leave"): "a member removes themself",
    ("GET", "/admin/orgs/{org_id}/my-secrets"): "caller's own vault; owner derived from identity",
    (
        "PUT",
        "/admin/orgs/{org_id}/my-secrets/{name}",
    ): "caller's own vault; owner derived from identity",
    (
        "DELETE",
        "/admin/orgs/{org_id}/my-secrets/{name}",
    ): "caller's own vault; owner derived from identity",
    ("POST", "/admin/creation-requests/"): "any member submits a request; approval is gated",
    (
        "GET",
        "/admin/creation-requests/rejection-reasons",
    ): "static vocabulary for the request form",
    ("GET", "/admin/platform/maintenance"): "REQ-1466: the notice banner every client renders",
    # --- the GraphQL endpoint itself; its resolvers are checked individually below --------------
    ("GET", "/admin/graphql"): "transport; per-resolver gates are checked in the GraphQL pass",
    ("POST", "/admin/graphql"): "transport; per-resolver gates are checked in the GraphQL pass",
    ("*", "/admin/graphql"): "transport; per-resolver gates are checked in the GraphQL pass",
}

# Model metadata read by every role's pages; holds no connection details or security rules. Each
# resolves through _get_pool(), the org-routed tenant pool (one schema per org), so it returns only
# the acting org's rows; the ones that take info also require an org bound to the request.
_MODEL_METADATA_READS = ("allRelationships metrics relationships domains tags").split()
for _name in _MODEL_METADATA_READS:
    EXEMPT[("graphql:query", _name)] = (
        "model metadata read by every role's pages; holds no connection details or security rules"
    )
# resolveOwners is answered per right (has_capability("user_management") decides whether the user id
# and e-mail come back), so it counts as gated and needs no entry. tagAssignments is trimmed to the
# caller's reachable domains (allowed_domains_request) rather than refused.
EXEMPT[("graphql:query", "tagAssignments")] = (
    "returns only assignments on objects in the caller's reachable domains (table/column by the "
    "table's domain, relationship by both its tables, command by its domain, source by "
    "allowed_domains); unrestricted scope sees all"
)
EXEMPT[("graphql:query", "schemaVersion")] = "identifier hash only; no model content"
# Pure text parse (parse_contract reads no source, credential or model row); called by DqRulesModal
# on /data-products, opened with data_product_read.
EXEMPT[("graphql:query", "dqContractParse")] = (
    "parses the contract text it is given; touches no source, credential or model row"
)


def _callees(fn) -> set[str]:
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    except (OSError, TypeError):
        return set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Name):
                names.add(f.id)
            elif isinstance(f, ast.Attribute):
                names.add(f.attr)
    return names


def _reaches_gate(fn, depth: int = _DEPTH, seen: set | None = None) -> bool:
    seen = set() if seen is None else seen
    # A decorated handler (``functools.wraps``) is read through to the function it wraps: its
    # source is the wrapped function's, so the names it calls resolve in THAT module's globals.
    fn = inspect.unwrap(fn)
    if fn in seen or depth < 0:
        return False
    seen.add(fn)
    names = _callees(fn)
    if names & GATE_NAMES:
        return True
    scope = getattr(fn, "__globals__", {})
    for name in names:
        callee = scope.get(name)
        if inspect.isfunction(callee) and (callee.__module__ or "").startswith("provisa"):
            if _reaches_gate(callee, depth - 1, seen):
                return True
    return False


def _dependency_gated(dependant) -> bool:
    for sub in dependant.dependencies:
        if sub.call is not None and (
            getattr(sub.call, "__name__", "") in GATE_NAMES or _reaches_gate(sub.call)
        ):
            return True
        if _dependency_gated(sub):
            return True
    return False


def _walk(routes, prefix=""):
    for r in routes:
        if isinstance(r, Mount):
            sub = getattr(r.app, "routes", None)
            assert sub is not None, (
                f"mounted app at {prefix + r.path} exposes no routes to audit; "
                "an opaque sub-app under /admin cannot be checked"
            )
            yield from _walk(sub, prefix + r.path)
            continue
        yield prefix + getattr(r, "path", ""), r


@pytest.fixture(scope="module")
def app():
    """The mounted application, built so that building it leaves nothing behind.

    ``create_app()`` binds the process-global settings registry to the config file
    (``settings_registry.bind_config``, called from ``setup_otel`` at otel_setup.py) and records the
    attached exporter (``otel_setup._attached``). Tests that read those afterwards (the OTEL
    endpoint tests) expect an unbound registry, so the values in force before the build are put
    back when the module's tests finish.
    """
    from provisa.api import otel_setup
    from provisa.api.app import create_app
    from provisa.core import settings_registry

    saved = pytest.MonkeyPatch()
    for module, name in (
        (settings_registry, "_config"),
        (settings_registry, "_frozen"),
        (otel_setup, "_attached"),
    ):
        saved.setattr(module, name, getattr(module, name))
    try:
        yield create_app()
    finally:
        saved.undo()


def _rest_handlers(app) -> dict[tuple[str, str], bool]:
    found: dict[tuple[str, str], bool] = {}
    for path, route in _walk(app.routes):
        if not path.startswith("/admin"):
            continue
        endpoint = route.endpoint
        gated = _reaches_gate(endpoint) or (
            hasattr(route, "dependant") and _dependency_gated(route.dependant)
        )
        for method in getattr(route, "methods", None) or {"*"}:
            if method == "HEAD":
                continue
            found[(method, path)] = gated
    return found


def _graphql_resolvers() -> dict[tuple[str, str], bool]:
    from provisa.api.admin.schema import admin_schema

    schema = admin_schema._schema
    found: dict[tuple[str, str], bool] = {}
    for kind, root in (
        ("query", schema.query_type),
        ("mutation", schema.mutation_type),
        ("subscription", schema.subscription_type),
    ):
        if root is None:
            continue
        for name, field in root.fields.items():
            definition = (getattr(field, "extensions", None) or {}).get("strawberry-definition")
            resolver = getattr(getattr(definition, "base_resolver", None), "wrapped_func", None)
            assert resolver is not None, f"{kind} {name} has no resolver function to inspect"
            found[(f"graphql:{kind}", name)] = _reaches_gate(resolver)
    return found


def test_every_admin_rest_handler_is_gated(app):
    found = _rest_handlers(app)
    assert found, "no /admin routes found; the walk is broken"
    ungated = sorted(k for k, gated in found.items() if not gated and k not in EXEMPT)
    assert not ungated, (
        "/admin handlers with no authorization gate (gate them with an existing helper, or add "
        f"to EXEMPT with the reason): {ungated}"
    )


def test_every_admin_graphql_resolver_is_gated():
    found = _graphql_resolvers()
    assert found, "no admin GraphQL resolvers found; the walk is broken"
    ungated = sorted(k for k, gated in found.items() if not gated and k not in EXEMPT)
    assert not ungated, (
        "admin GraphQL resolvers with no authorization gate (gate them with require_capability, "
        f"or add to EXEMPT with the reason): {ungated}"
    )


def test_exempt_entries_are_still_ungated_routes(app):
    found = {**_rest_handlers(app), **_graphql_resolvers()}
    stale = sorted(k for k in EXEMPT if k not in found)
    now_gated = sorted(k for k in EXEMPT if found.get(k) is True)
    assert not stale, f"EXEMPT entries naming a handler that does not exist: {stale}"
    assert not now_gated, f"EXEMPT entries that are now gated; remove them: {now_gated}"
    assert all(EXEMPT.values()), "every EXEMPT entry needs a reason"
