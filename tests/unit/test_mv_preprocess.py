# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e9a10-4d6f-4b83-9c15-8e0a3f5d7b26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-957 / REQ-964 / REQ-1165: preflight-check purity gate + compilation.

A node's ``preflight(rows, ctx)`` check must be deterministic (a SQL-expressible check is
pushed down and its Python fallback must agree; a replay must reach the same verdict). The
gate rejects non-determinism and sandbox-escape at registration; the compiler runs the
survivor in a restricted namespace. The compiled callable returns a VERDICT, never rows.
"""

from __future__ import annotations

import pytest

from provisa.mv.preflight import CONTINUE, Decision, Verdict, to_verdict
from provisa.mv.preprocess import (
    PreprocessValidationError,
    check_preprocess_purity,
    compile_preprocess,
    validate_preprocess,
)

_VALID = (
    "def preflight(rows, ctx):\n"
    "    if any(r['qty'] < 0 for r in rows):\n"
    "        return ctx.abort('negative')\n"
    "    return ctx.ok()"
)


class _Ctx:
    """A minimal verdict-constructing context for exercising a compiled check in isolation."""

    def ok(self) -> Verdict:
        return CONTINUE

    def abort(self, reason: str | None = None) -> Verdict:
        return Verdict(Decision.ABORT, reason)

    def quarantine(self, reason: str | None = None) -> Verdict:
        return Verdict(Decision.QUARANTINE, reason)

    def warn(self, _reason) -> None:
        pass


@pytest.mark.parametrize(
    "source",
    [
        _VALID,
        # positional-only + a constant helper is fine
        "LIMIT = 100\ndef over(x):\n    return x > LIMIT\n"
        "def preflight(rows, ctx):\n    if any(over(r['p']) for r in rows):\n"
        "        return ctx.quarantine('too big')\n    return ctx.ok()",
        # ctx.warn is a permitted (non-dunder) attribute call
        "def preflight(rows, ctx):\n    ctx.warn('checked')\n    return ctx.ok()",
        # raising to reject is allowed
        "def preflight(rows, ctx):\n    if not rows:\n        raise ValueError('empty')\n    return ctx.ok()",
    ],
)
def test_pure_checks_accepted(source: str) -> None:
    ok, reason = check_preprocess_purity(source)
    assert ok, reason
    validate_preprocess(source)  # does not raise


@pytest.mark.parametrize(
    "source,needle",
    [
        ("import random\ndef preflight(rows, ctx):\n    return ctx.ok()", "module level"),
        (
            "def preflight(rows, ctx):\n    import time\n    return ctx.ok()",
            "not allowed",
        ),
        (
            "def preflight(rows, ctx):\n    return rows.__class__.__bases__",
            "dunder",
        ),
        ("def preflight(rows, ctx):\n    return eval('1')", "forbidden name"),
        ("def preflight(rows, ctx):\n    return open('/etc/passwd')", "forbidden name"),
        ("def preflight(rows, ctx):\n    return [id(r) for r in rows]", "forbidden name"),
        ("def preflight(rows, ctx):\n    return [hash(r['k']) for r in rows]", "forbidden name"),
        # REQ-1165: the required function is now ``preflight``; the old ``preprocess`` name is rejected
        ("def preprocess(rows, ctx):\n    return rows", "must define"),
        ("def preflight(rows):\n    return None", "exactly two"),
        ("def preflight(rows, ctx: bad syntax", "cannot be parsed"),
    ],
)
def test_impure_or_unsafe_checks_rejected(source: str, needle: str) -> None:
    ok, reason = check_preprocess_purity(source)
    assert not ok
    assert needle in reason
    with pytest.raises(PreprocessValidationError):
        validate_preprocess(source)


#: A check that walks from a generator's frame to its caller's frame, and from there to a real
#: module's globals and the real builtins. The payload is benign: it only says what it reached.
_FRAME_WALK = (
    "def preflight(streams, ctx):\n"
    "    def gen():\n"
    "        yield g.gi_frame.f_back\n"
    "    g = gen()\n"
    "    mine = next(g)\n"
    "    reached = mine.f_back.f_globals['__builtins__']\n"
    "    if 'open' in reached:\n"
    "        return ctx.abort('reached the real builtins')\n"
    "    return ctx.ok()"
)


def test_frame_walk_to_the_real_builtins_is_refused() -> None:
    """REQ-2133 (amended 2026-11-16): the hook the audit showed reaching the server's globals."""
    ok, reason = check_preprocess_purity(_FRAME_WALK)
    assert not ok
    assert "gi_frame" in reason and "f_back" in reason and "f_globals" in reason
    with pytest.raises(PreprocessValidationError):
        compile_preprocess(_FRAME_WALK)


@pytest.mark.parametrize(
    "attr",
    [
        "gi_frame",
        "gi_code",
        "gi_yieldfrom",
        "cr_frame",
        "cr_code",
        "cr_await",
        "ag_frame",
        "ag_code",
        "ag_await",
        "f_back",
        "f_globals",
        "f_locals",
        "f_builtins",
        "f_code",
        "tb_frame",
        "tb_next",
    ],
)
def test_frame_generator_and_traceback_attributes_refused(attr: str) -> None:
    source = f"def preflight(streams, ctx):\n    x = streams.{attr}\n    return ctx.ok()"
    ok, reason = check_preprocess_purity(source)
    assert not ok
    assert repr(attr) in reason
    with pytest.raises(PreprocessValidationError):
        validate_preprocess(source)


@pytest.mark.parametrize(
    "subscript",
    ["['__builtins__']", "['__globals__']", "['__import__']", "['__class__']"],
)
def test_string_subscript_naming_a_dunder_refused(subscript: str) -> None:
    source = f"def preflight(streams, ctx):\n    x = streams{subscript}\n    return ctx.ok()"
    ok, reason = check_preprocess_purity(source)
    assert not ok
    assert "subscript" in reason
    with pytest.raises(PreprocessValidationError):
        validate_preprocess(source)


def test_ordinary_string_subscripts_still_accepted() -> None:
    source = (
        "def preflight(streams, ctx):\n"
        "    if any(r['f_back'] is None or r['__x'] for r in streams['orders']):\n"
        "        return ctx.abort('missing')\n"
        "    return ctx.ok()"
    )
    ok, reason = check_preprocess_purity(source)
    assert ok, reason


def test_blank_check_is_absent() -> None:
    assert compile_preprocess(None) is None
    assert compile_preprocess("   ") is None
    validate_preprocess(None)  # no raise


def test_compiled_check_returns_a_verdict_not_rows() -> None:
    fn = compile_preprocess(_VALID)
    assert fn is not None
    ctx = _Ctx()
    assert to_verdict(fn([{"qty": 1}, {"qty": 3}], ctx)).decision is Decision.CONTINUE
    v = to_verdict(fn([{"qty": 1}, {"qty": -2}], ctx))
    assert v.decision is Decision.ABORT and v.reason == "negative"


def test_compiled_check_has_no_dangerous_builtins() -> None:
    fn = compile_preprocess(_VALID)
    assert fn is not None
    builtins_ = fn.__globals__["__builtins__"]
    assert "open" not in builtins_
    assert "__import__" not in builtins_
    assert "eval" not in builtins_


def test_deterministic_verdict_is_stable_across_calls() -> None:
    fn = compile_preprocess(_VALID)
    assert fn is not None
    rows = [{"qty": 5}, {"qty": -1}, {"qty": 2}]
    ctx = _Ctx()
    assert to_verdict(fn(rows, ctx)).decision == to_verdict(fn(rows, ctx)).decision


def test_build_processors_threads_check_into_processor() -> None:
    """REQ-957/REQ-1165 wiring: a NodeSpec's compiled preflight reaches the processor's ``_preprocess``."""

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
    procs = build_processors([spec], db=None, dependents_of=lambda _n: [], on_event=lambda *_: None)
    assert len(procs) == 1
    assert procs[0]._preprocess is hook
