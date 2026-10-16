# Copyright (c) 2026 Kenneth Stott
# Canary: cde31c01-6cf1-4f52-b91a-eaedbd1e29da
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A stand-in state for a unit test, holding its roles' surfaces as the real state does.

The real state keeps ONE object per role (``provisa.api.role_surface.RoleSurface``) and shows
``schemas`` / ``contexts`` / ``rls_contexts`` / ``table_path_maps`` / ``proto_files`` as views of
it; a request takes the object once. ``RoleState`` is ``SimpleNamespace`` with exactly that: a
test still writes ``RoleState(contexts={...}, rls_contexts={...}, roles=...)`` and reads or
changes ``state.contexts["analyst"]``, and code under test that takes the role's surface gets
one built from those parts.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from provisa.api.role_surface import RoleSurface, SurfacePart, set_part

_PARTS = {
    "schemas": "schema",
    "contexts": "context",
    "rls_contexts": "rls",
    "table_path_maps": "table_path_map",
    "proto_files": "proto",
}


class RoleState(SimpleNamespace):
    def __init__(self, **attrs: Any) -> None:
        surfaces: dict[str, RoleSurface] = attrs.pop("surfaces", None) or {}
        parts = {name: attrs.pop(name) for name in list(attrs) if name in _PARTS}
        if "mv_registry" not in attrs:
            # As the real state: it always holds a registry, empty until a view is registered.
            from provisa.mv.registry import MVRegistry

            attrs["mv_registry"] = MVRegistry()
        # As the real state of an org that records no member attributes, declares no claim type
        # (its own empty list) and has named no pseudo-role (REQ-1382, REQ-2057).
        attrs.setdefault("user_attributes", {})
        attrs.setdefault("settings_overrides", {"claim_roles": []})
        attrs.setdefault("meta_qualifiers", {})
        # As the real state of a generation no request has acted as a set of roles in yet.
        attrs.setdefault("meta_roles", {})
        super().__init__(surfaces=surfaces, **attrs)
        for name, values in parts.items():
            set_part(self, _PARTS[name], values)

    def __getattr__(self, name: str) -> Any:
        if name in _PARTS:
            return SurfacePart(self, _PARTS[name])
        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in _PARTS:
            set_part(self, _PARTS[name], value)
        else:
            super().__setattr__(name, value)


def _surfaces_from(state: Any) -> dict[str, RoleSurface]:
    holder = SimpleNamespace(surfaces={})
    for name, part in _PARTS.items():
        values = getattr(state, name, None)
        if isinstance(values, dict):
            set_part(holder, part, values)
    return holder.surfaces


def with_surfaces(state: Any) -> Any:
    """For a stand-in built another way (a ``MagicMock``, a class with attributes): give it the
    ``surfaces`` its part maps describe. Returns ``state``.

    A mock gets them live — read from its part maps each time ``surfaces`` is asked for — so
    the usual ``state = MagicMock(); state.contexts = {...}`` works wherever this is called,
    and so does a part a test replaces later. Anything else gets them as they stand now.
    """
    from unittest.mock import Mock

    if isinstance(state, Mock):
        # Every mock instance has a class of its own, so this reaches no other mock.
        type(state).surfaces = property(_surfaces_from)
    else:
        state.surfaces = _surfaces_from(state)
    return state
