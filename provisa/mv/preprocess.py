# Copyright (c) 2026 Kenneth Stott
# Canary: 1f4b9d02-8c6e-4a37-9b21-6d0f5e8a4c19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""User preflight-check validation + compilation (REQ-957, REQ-964, REQ-1165).

REQ-1165 rescopes the REQ-957 hook from a row transform to a PREFLIGHT CHECK —
``preflight(streams, ctx) -> verdict`` — run after produce and before land, where ``streams`` is a
``dict[str, Iterable[dict]]`` keyed by input node (one lazy Arrow stream per input). Authors express
custom prechecks, validation, and rejection here; the hook returns a verdict (continue /
abort / quarantine, see :mod:`provisa.mv.preflight`) and NEVER a mutated dataset. Transforms
belong in SQL (engine pushdown) or an external processor (REQ-940).

The hook must be DETERMINISTIC: a SQL-expressible check is translated to an engine-side probe
(:mod:`provisa.mv.preflight_sql`) and its Python fallback must agree, and a replayed refresh
must reach the same verdict (REQ-964). This module enforces that at REGISTRATION with a static
AST purity check, then compiles the source into a callable that executes in a restricted
namespace with no dangerous builtins.

The check is fail-closed: source that cannot be parsed, that imports anything, that reaches
wall-clock / randomness / process identity, or that touches dunder-escape attributes is
rejected. There is no partial trust — a rejected script never runs.
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from typing import Any

# Builtins the hook may reference. Everything here is a pure, deterministic value
# transform. NOTHING that reaches wall-clock, randomness, process identity, IO,
# reflection, or code execution is exposed — those are the non-determinism and
# sandbox-escape vectors REQ-964 forbids.
_SAFE_BUILTINS: dict[str, Any] = {
    b.__name__: b
    for b in (
        abs,
        all,
        any,
        bool,
        dict,
        divmod,
        enumerate,
        filter,
        float,
        frozenset,
        int,
        len,
        list,
        map,
        max,
        min,
        range,
        reversed,
        round,
        set,
        sorted,
        str,
        sum,
        tuple,
        zip,
        bytes,
        complex,
        ord,
        chr,
        format,
    )
}
# Constants + exceptions the hook may need (raising is REQ-957's fatal-reject path).
_SAFE_BUILTINS.update(
    {
        "True": True,
        "False": False,
        "None": None,
        "Exception": Exception,
        "ValueError": ValueError,
        "KeyError": KeyError,
        "TypeError": TypeError,
    }
)

# Bare names the hook may NOT reference — code execution, IO, reflection, and the
# process-varying builtins (``id`` = address, ``hash`` = per-process-salted for str).
_FORBIDDEN_NAMES = frozenset(
    {
        "eval",
        "exec",
        "compile",
        "open",
        "input",
        "__import__",
        "globals",
        "locals",
        "vars",
        "dir",
        "getattr",
        "setattr",
        "delattr",
        "hasattr",
        "help",
        "exit",
        "quit",
        "breakpoint",
        "object",
        "super",
        "type",
        "memoryview",
        "classmethod",
        "staticmethod",
        "property",
        "id",
        "hash",
        "repr",
        "print",
    }
)

REQUIRED_FUNC = "preflight"

#: The one module a preflight may import (REQ-957, REQ-1165 amended 2026-10-21): Provisa's services,
#: in-process. A preflight that imports it is evaluated by the Python path only, never translated to
#: an SQL probe, and is excluded from replay determinism (REQ-964): its verdict is the state its
#: service calls find at refresh time.
SERVICES_MODULE = "provisa_services"


class PreprocessValidationError(ValueError):
    """REQ-964: a preflight script failed the purity gate — rejected at registration."""


class _PurityVisitor(ast.NodeVisitor):
    """Walk the hook's AST and collect every purity/safety violation."""

    def __init__(self) -> None:
        self.errors: list[str] = []

    def visit_Import(self, node: ast.Import) -> None:
        others = [a.name for a in node.names if a.name != SERVICES_MODULE]
        if others:
            self.errors.append(
                f"import statements are not allowed (non-deterministic / unsafe), but "
                f"`import {SERVICES_MODULE}`: {', '.join(others)}"
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        del node
        self.errors.append(
            f"import statements are not allowed (non-deterministic / unsafe), but "
            f"`import {SERVICES_MODULE}`; a from-import is not"
        )

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # Dunder attribute access (__globals__, __class__, __subclasses__, …) is the
        # classic sandbox-escape vector — reject it wholesale.
        if node.attr.startswith("__") and node.attr.endswith("__"):
            self.errors.append(f"dunder attribute access is not allowed: {node.attr!r}")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load) and node.id in _FORBIDDEN_NAMES:
            self.errors.append(f"forbidden name: {node.id!r}")
        self.generic_visit(node)


def check_preprocess_purity(source: str) -> tuple[bool, str]:
    """Return (pure, reason). ``reason`` is empty when the hook is safe + deterministic.

    Fail-closed: unparseable source, a missing ``preflight(streams, ctx)`` definition,
    any import, any forbidden name, or any dunder attribute access is rejected.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return False, f"cannot be parsed: {exc}"

    # The module must DEFINE preflight(streams, ctx). Predicate helpers (other defs,
    # constant assignments) are allowed alongside it; top-level side effects are not.
    func: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if stmt.name == REQUIRED_FUNC:
                func = stmt
        elif isinstance(stmt, (ast.Assign, ast.AnnAssign, ast.Expr, ast.Pass)):
            continue  # module-level constants / docstrings are harmless
        elif isinstance(stmt, ast.Import) and all(a.name == SERVICES_MODULE for a in stmt.names):
            continue  # REQ-1165: Provisa's services, the one import a check may make
        else:
            return False, (
                f"only function definitions and constant assignments are allowed at "
                f"module level, found {type(stmt).__name__}"
            )

    if func is None:
        return False, f"must define a {REQUIRED_FUNC}(streams, ctx) function"
    positional = func.args.posonlyargs + func.args.args
    if len(positional) != 2:
        return False, f"{REQUIRED_FUNC} must take exactly two parameters (streams, ctx)"

    visitor = _PurityVisitor()
    visitor.visit(tree)
    if visitor.errors:
        return False, "; ".join(dict.fromkeys(visitor.errors))
    return True, ""


def uses_services(source: str | None) -> bool:
    """Whether the preflight ``source`` imports :data:`SERVICES_MODULE` (REQ-1165): such a check runs
    with Provisa's services bound, on the Python path only."""
    if source is None or not source.strip():
        return False
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False
    return any(
        isinstance(node, ast.Import) and any(a.name == SERVICES_MODULE for a in node.names)
        for node in ast.walk(tree)
    )


def validate_preprocess(source: str | None) -> None:
    """Enforce the preflight purity gate at registration (REQ-957 / REQ-964 / REQ-1165).

    None / blank → no hook (always-continue), nothing to check. A non-empty script that
    fails the purity check raises :class:`PreprocessValidationError` so it is rejected
    loudly — a non-deterministic gate would reach different verdicts on replay (REQ-964).
    """
    if source is None or not source.strip():
        return
    ok, reason = check_preprocess_purity(source)
    if not ok:
        raise PreprocessValidationError(f"invalid preprocess hook: {reason}")


def _services_import(name: str, *args: Any, **kwargs: Any) -> Any:
    """The hook's ``__import__``: :data:`SERVICES_MODULE` and nothing else (the registration check
    has already refused every other import; this refuses one that slipped past it)."""
    del args, kwargs
    if name != SERVICES_MODULE:
        raise ImportError(f"a preflight may import only {SERVICES_MODULE}, not {name!r}")
    from provisa.mv.preflight_services import services_module

    return services_module()


def compile_preprocess(source: str | None) -> Callable[..., Any] | None:
    """Validate then compile a preflight script into a ``preflight(streams, ctx)`` callable.

    Returns None for a blank/absent script (no hook — always continue). The compiled callable
    returns a verdict (normalized by :func:`provisa.mv.preflight.to_verdict`), never rows. It
    executes in a namespace whose ``__builtins__`` is the curated :data:`_SAFE_BUILTINS` only,
    so even a construct the static check missed has no dangerous builtin to reach.
    """
    if source is None or not source.strip():
        return None
    validate_preprocess(source)
    builtins = dict(_SAFE_BUILTINS)
    if uses_services(source):
        builtins["__import__"] = _services_import
    namespace: dict[str, Any] = {"__builtins__": builtins}
    code = compile(source, filename="<preprocess>", mode="exec")
    # S102/B102 flag any exec — the source passed the purity check above and runs against a
    # restricted builtins mapping, which is what makes compiling a check body possible at all.
    exec(code, namespace)  # noqa: S102  # nosec B102
    func = namespace.get(REQUIRED_FUNC)
    if not callable(func):
        raise PreprocessValidationError(f"{REQUIRED_FUNC} is not callable after compile")
    return func
