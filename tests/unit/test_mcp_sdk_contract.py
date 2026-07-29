# Copyright (c) 2026 Kenneth Stott
# Canary: 92f1e3c7-0a2f-48cb-b410-9d8dc0222a30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deferred third-party imports on a startup path must still be checked at test time.

The class of defect: an import is written inside a function so the dependency stays optional, which
also means importing the module proves nothing and no test ever executes the import statement. A
dependency that moves the symbol then breaks only in the deployed container, at boot. That is what
happened to ``from mcp.server.fastmcp import Context, FastMCP`` (provisa/api/mcp/server.py:126):
``mcp`` was pinned only ``>=1.2.0``, the image resolved 2.0.0, which dropped ``mcp.server.fastmcp``,
and the MCP endpoint failed to start with ModuleNotFoundError while every test stayed green.

These tests execute those deferred imports against the SDK actually installed, so a resolver that
picks an incompatible release fails here instead of in production.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

import provisa.api.mcp.server as mcp_server

_SRC = Path(mcp_server.__file__)


def _deferred_mcp_imports() -> list[tuple[str, tuple[str, ...]]]:
    """Every ``from mcp.... import a, b`` in the module, at any nesting depth."""
    tree = ast.parse(_SRC.read_text())
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "mcp":
            found.append((node.module, tuple(a.name for a in node.names)))
    return found


def test_the_module_actually_defers_some_mcp_imports():
    """Guards the guard: if the imports move to module scope this file must be revisited rather
    than silently passing on an empty list."""
    assert _deferred_mcp_imports(), f"no `from mcp...` imports found in {_SRC}"


@pytest.mark.parametrize(
    ("module", "names"), _deferred_mcp_imports(), ids=lambda v: v if isinstance(v, str) else ""
)
def test_deferred_mcp_import_resolves_against_the_installed_sdk(module, names):
    mod = importlib.import_module(module)
    missing = [n for n in names if not hasattr(mod, n)]
    assert not missing, f"{module} is installed but does not export {missing}"


def test_the_fastmcp_decorator_api_is_present():
    """The server is written against the FastMCP class specifically — an `mcp` release that keeps
    the module but drops the class is the same outage."""
    from mcp.server.fastmcp import Context, FastMCP

    assert callable(FastMCP)
    assert Context is not None
