# Copyright (c) 2026 Kenneth Stott
# Canary: c5f0b71d-4a92-4e63-8b17-2f6d94a1e0c8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1270: the default org id has exactly one source — the control plane's org_id.

The tenant schema is named ``org_<org_id>``, so an auth plane that binds ``active_org_id``
to a *different* literal makes every org-runtime resolution look up a schema that was never
created: ``/auth/me`` then 500s and the UI reports the identity as having no access. These
tests pin the resolution order (explicit config override → control-plane org_id → raise)
and forbid a silent literal default on the config model.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.auth.wiring import _resolve_default_org_id


@pytest.fixture
def app_state(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "org_id", "acme", raising=False)
    return state


def test_falls_back_to_control_plane_org_id_when_config_unset(app_state):
    # The common case: no explicit default_org_id, so the org that names org_acme is the one
    # an authenticated user is bound to.
    assert _resolve_default_org_id(SimpleNamespace(default_org_id=None)) == "acme"


def test_no_config_object_still_uses_control_plane_org_id(app_state):
    assert _resolve_default_org_id(None) == "acme"


def test_explicit_config_value_wins(app_state):
    assert _resolve_default_org_id(SimpleNamespace(default_org_id="widget")) == "widget"


def test_raises_when_neither_source_is_available(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "org_id", None, raising=False)
    with pytest.raises(RuntimeError, match="default org id unresolved"):
        _resolve_default_org_id(SimpleNamespace(default_org_id=None))


def test_config_model_has_no_literal_default():
    # A literal default here silently reintroduces the divergence: the config would report an
    # org id the control plane never created a schema for.
    from provisa.core.models import ProvisaConfig

    assert ProvisaConfig.model_fields["default_org_id"].default is None
