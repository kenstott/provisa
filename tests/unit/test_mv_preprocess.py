# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e9a10-4d6f-4b83-9c15-8e0a3f5d7b26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-957 / REQ-964: preprocess-hook purity gate + compilation.

A node's ``preprocess(rows, ctx)`` hook must be deterministic (its output feeds the
content hash gating the re-post). The gate rejects non-determinism and sandbox-escape
at registration; the compiler runs the survivor in a restricted namespace.
"""

from __future__ import annotations

import pytest

from provisa.mv.preprocess import (
    PreprocessValidationError,
    check_preprocess_purity,
    compile_preprocess,
    validate_preprocess,
)

_VALID = "def preprocess(rows, ctx):\n    return [r for r in rows if r['qty'] > 0]"


@pytest.mark.parametrize(
    "source",
    [
        _VALID,
        # positional-only + a constant helper is fine
        "TAX = 1.2\ndef rate(x):\n    return x * TAX\ndef preprocess(rows, ctx):\n    return [{'p': rate(r['p'])} for r in rows]",
        # ctx.warn is a permitted (non-dunder) attribute call
        "def preprocess(rows, ctx):\n    ctx.warn('checked')\n    return rows",
        # raising to reject is allowed
        "def preprocess(rows, ctx):\n    if not rows:\n        raise ValueError('empty')\n    return rows",
    ],
)
def test_pure_hooks_accepted(source: str) -> None:
    ok, reason = check_preprocess_purity(source)
    assert ok, reason
    validate_preprocess(source)  # does not raise


@pytest.mark.parametrize(
    "source,needle",
    [
        ("import random\ndef preprocess(rows, ctx):\n    return rows", "module level"),
        (
            "def preprocess(rows, ctx):\n    import time\n    return rows",
            "not allowed",
        ),
        (
            "def preprocess(rows, ctx):\n    return rows.__class__.__bases__",
            "dunder",
        ),
        ("def preprocess(rows, ctx):\n    return eval('1')", "forbidden name"),
        ("def preprocess(rows, ctx):\n    return open('/etc/passwd')", "forbidden name"),
        ("def preprocess(rows, ctx):\n    return [id(r) for r in rows]", "forbidden name"),
        ("def preprocess(rows, ctx):\n    return [hash(r['k']) for r in rows]", "forbidden name"),
        ("def other(rows, ctx):\n    return rows", "must define"),
        ("def preprocess(rows):\n    return rows", "exactly two"),
        ("def preprocess(rows, ctx: bad syntax", "cannot be parsed"),
    ],
)
def test_impure_or_unsafe_hooks_rejected(source: str, needle: str) -> None:
    ok, reason = check_preprocess_purity(source)
    assert not ok
    assert needle in reason
    with pytest.raises(PreprocessValidationError):
        validate_preprocess(source)


def test_blank_hook_is_identity() -> None:
    assert compile_preprocess(None) is None
    assert compile_preprocess("   ") is None
    validate_preprocess(None)  # no raise


def test_compiled_hook_runs_in_restricted_namespace() -> None:
    fn = compile_preprocess(_VALID)
    assert fn is not None
    assert fn([{"qty": 1}, {"qty": 0}, {"qty": 3}], None) == [{"qty": 1}, {"qty": 3}]


def test_compiled_hook_has_no_dangerous_builtins() -> None:
    # ``__import__`` is absent from the sandbox namespace, so a hook that slipped a
    # dynamic import past the static check (it cannot, but defense in depth) still fails.
    fn = compile_preprocess("def preprocess(rows, ctx):\n    return sorted(rows, key=lambda r: r['k'])")
    assert fn is not None
    assert fn([{"k": 2}, {"k": 1}], None) == [{"k": 1}, {"k": 2}]
    globals_ = fn.__globals__
    assert "open" not in globals_["__builtins__"]
    assert "__import__" not in globals_["__builtins__"]
    assert "eval" not in globals_["__builtins__"]


def test_deterministic_output_is_stable_across_calls() -> None:
    fn = compile_preprocess(_VALID)
    assert fn is not None
    rows = [{"qty": 5}, {"qty": -1}, {"qty": 2}]
    assert fn(rows, None) == fn(rows, None)


def test_build_processors_threads_hook_into_processor() -> None:
    """REQ-957 wiring: a NodeSpec's compiled preprocess reaches the processor's ``_preprocess``."""

    async def _handle(pending, **_kw):  # noqa: ANN001, ANN003 — test double
        return None

    from provisa.events.boot import NodeSpec, build_processors

    hook = compile_preprocess(_VALID)
    spec = NodeSpec(
        node="mat.mv_orders",
        kind="mv",
        change_signal="ttl",
        watermark_column=None,
        handle=_handle,
        preprocess=hook,
    )
    procs = build_processors([spec], db=None, dependents_of=lambda _n: [])
    assert len(procs) == 1
    assert procs[0]._preprocess is hook
