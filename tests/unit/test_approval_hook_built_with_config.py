# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e7a52-0d41-4b8f-9e26-7f1a5c0b8d34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ABAC approval hook is built once the config is loaded, and again at each rebuild
(REQ-247, REQ-555, REQ-1924, REQ-209; issue #344).

It was built only in ``create_app``, before the lifespan loaded the config, so on a running
server ``auth.approval_hook`` was never read and every ``requires_approval`` command or webhook
was refused ``functions.approval_unavailable``. Each schema build (boot, reload, org build) now
builds it from that build's config, after the contexts its per-table scopes resolve through.
"""

# Requirements: REQ-247, REQ-555, REQ-1924, REQ-209

from __future__ import annotations

import ast
import inspect
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api import app as app_module
from provisa.api.app import AppState
from provisa.api.app_loaders import _setup_approval_hook
from provisa.auth.approval_hook import GrpcApprovalHook, WebhookApprovalHook


def _config(hook: dict | None, *, table_hook: bool = True, source_hook: bool = True):
    return SimpleNamespace(
        auth=SimpleNamespace(approval_hook=hook),
        sources=[SimpleNamespace(id="pg1", approval_hook=source_hook)],
        tables=[
            SimpleNamespace(
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                approval_hook=table_hook,
            )
        ],
    )


@pytest.fixture
def st() -> Any:
    state: Any = AppState()
    state.view_context = SimpleNamespace(
        tables={
            "orders": SimpleNamespace(
                domain_id="sales", schema_name="public", table_name="orders", table_id=7
            )
        }
    )
    return state


_GRPC = {"type": "grpc", "url": "127.0.0.1:50999"}


def _calls(fn) -> list[str]:
    tree = ast.parse(inspect.getsource(fn).lstrip())
    return [
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]


def _line_of_call(fn, name: str) -> int:
    tree = ast.parse(inspect.getsource(fn).lstrip())
    lines = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name
    ]
    assert len(lines) == 1, f"{fn.__name__} calls {name} {len(lines)} times"
    return lines[0]


def test_app_creation_does_not_build_the_hook_before_the_config_loads() -> None:
    assert "_setup_approval_hook" not in _calls(app_module.create_app)


def test_every_schema_build_builds_the_hook_after_its_contexts() -> None:
    build = app_module._rebuild_schemas_impl
    assert _line_of_call(build, "_setup_approval_hook") > _line_of_call(
        build, "_build_and_register_schemas"
    )


def test_hook_is_built_from_the_loaded_config(st) -> None:
    st.config = _config(_GRPC)
    _setup_approval_hook(st)
    assert isinstance(st.approval_hook, GrpcApprovalHook)
    assert st.approval_hook_config.url == "127.0.0.1:50999"
    assert st.source_approval_hooks == {"pg1": True}
    assert st.table_approval_hooks == {7: True}


def test_a_rebuild_with_the_same_hook_keeps_it(st) -> None:
    st.config = _config(_GRPC)
    _setup_approval_hook(st)
    first = st.approval_hook
    st.config = _config(dict(_GRPC), table_hook=False)
    _setup_approval_hook(st)
    assert st.approval_hook is first
    assert st.table_approval_hooks == {}


def test_a_rebuild_with_a_changed_hook_replaces_it(st) -> None:
    st.config = _config(_GRPC)
    _setup_approval_hook(st)
    st.config = _config({"type": "webhook", "url": "http://policy/approve"})
    _setup_approval_hook(st)
    assert isinstance(st.approval_hook, WebhookApprovalHook)
    assert st.approval_hook_config.url == "http://policy/approve"


def test_a_rebuild_without_the_hook_disables_it(st) -> None:
    st.config = _config(_GRPC)
    _setup_approval_hook(st)
    st.config = _config(None)
    _setup_approval_hook(st)
    assert st.approval_hook is None
    assert st.approval_hook_config is None
    assert st.source_approval_hooks == {}
    assert st.table_approval_hooks == {}


def test_setup_without_a_loaded_config_is_an_error(st) -> None:
    with pytest.raises(RuntimeError, match="loaded config"):
        _setup_approval_hook(st)
