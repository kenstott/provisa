# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e5a19-6b3d-4f08-a1e4-9d0b8c3f7a52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An authenticated caller holding named capabilities, for tests that call a gated admin handler.

The gate reads the caller's rights through the real resolution path (identity role claims ->
``state.roles``), so a test that needs to pass a gate states which right it holds instead of
relying on the unauthenticated dev-mode exemption.
"""

# Requirements: REQ-1337, REQ-1349

from __future__ import annotations

import types

import provisa.api.app as appmod


def grant(monkeypatch, *capabilities: str, state=None):
    """Install a role holding ``capabilities``; return ``(info, request)`` for that caller.

    ``state`` is the app state object the test has substituted, when it replaced the module's own.
    """
    monkeypatch.setattr(
        appmod.state if state is None else state,
        "roles",
        {"test_role": {"capabilities": list(capabilities), "domain_access": ["*"]}},
        raising=False,
    )
    identity = types.SimpleNamespace(user_id="test-user", roles=["test_role"])
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))
    info = types.SimpleNamespace(context={"request": request})
    return info, request
