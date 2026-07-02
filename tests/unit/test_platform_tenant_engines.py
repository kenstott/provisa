# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-837: platform and tenant control planes use two independent SQLAlchemy
engines/pools; the platform DB is configured by PLATFORM_DATABASE_URL and is
required at startup with no fallback.

Runs against SQLite in-memory (no Docker).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from provisa.core.database import create_engine_from_url

_REPO = Path(__file__).resolve().parents[2]


def test_engines_are_independent():
    # Two URLs -> two distinct AsyncEngine instances with distinct pools.
    # (URLs are not connected; engine construction is lazy.)
    admin = create_engine_from_url("postgresql+asyncpg://u:p@h/admin")
    tenant = create_engine_from_url("postgresql+asyncpg://u:p@h/tenant")
    assert admin is not tenant
    assert admin.pool is not tenant.pool


def test_any_async_backend_accepted():
    # PLATFORM_DATABASE_URL supports any async SQLAlchemy URI, not just asyncpg.
    for url, backend in (
        ("postgresql+asyncpg://u:p@h/db", "postgresql"),
        ("sqlite+aiosqlite:////tmp/provisa_req837.db", "sqlite"),
    ):
        engine = create_engine_from_url(url)
        assert engine.url.get_backend_name() == backend


def test_platform_url_required_with_no_fallback():
    # app.py must read PLATFORM_DATABASE_URL via subscript (fail-loud), never
    # os.environ.get(...) with a default that would silently mis-route.
    src = (_REPO / "provisa" / "api" / "app.py").read_text()
    tree = ast.parse(src)
    subscript_reads = 0
    for node in ast.walk(tree):
        # os.environ.get("PLATFORM_DATABASE_URL", ...) -> forbidden fallback
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "get":
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and arg.value == "PLATFORM_DATABASE_URL":
                        pytest.fail("PLATFORM_DATABASE_URL must not use .get() fallback")
        # os.environ["PLATFORM_DATABASE_URL"] -> required, fail-loud
        if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            if node.slice.value == "PLATFORM_DATABASE_URL":
                subscript_reads += 1
    assert subscript_reads >= 1, "PLATFORM_DATABASE_URL must be read as a required env var"
