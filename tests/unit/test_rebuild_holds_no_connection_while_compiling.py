# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2d17-6f93-4a50-9c21-e7a5d3f0b689
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882, REQ-1900: a schema rebuild holds no control-plane connection while it compiles.

The per-role compile (_build_and_register_schemas: every role's schema, context and proto) is the
rebuild's longest phase and needs no database. Run inside the rebuild's connection block it kept
a control-plane connection checked out for all of it, so concurrent requests and the health probe
waited at the connection gate. The rebuild reads what it needs, gives its connection back,
compiles, and checks one out again only for the writes after.
"""

# Requirements: REQ-1882, REQ-1900

from __future__ import annotations

import ast
import inspect

from provisa.api import app


def _calls_inside_acquire(fn) -> list[str]:
    """Names of the calls made inside an ``async with ...acquire()`` block of ``fn``."""
    tree = ast.parse(inspect.getsource(fn))
    found: list[str] = []

    def is_acquire(item: ast.withitem) -> bool:
        call = item.context_expr
        return (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "acquire"
        )

    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncWith) and any(is_acquire(i) for i in node.items):
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Name):
                    found.append(inner.func.id)
    return found


def test_the_per_role_compile_runs_with_no_connection_checked_out() -> None:
    assert "_build_and_register_schemas" not in _calls_inside_acquire(app._rebuild_schemas_impl)


def test_the_writes_after_the_compile_still_have_a_connection() -> None:
    inside = _calls_inside_acquire(app._rebuild_schemas_impl)
    assert {"persist_field_numbers", "write_lineage_edges", "write_checker_targets"} <= set(inside)
