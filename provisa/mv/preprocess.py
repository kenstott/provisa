# Copyright (c) 2026 Kenneth Stott
# Canary: 1f4b9d02-8c6e-4a37-9b21-6d0f5e8a4c19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""User preprocess-hook validation + compilation (REQ-957, REQ-964).

REQ-957 collapses a node's processing envelope to ONE optional user hook —
``preprocess(rows, ctx) -> rows`` — run after produce and before land. Authors
express custom prep, enrichment, validation, quarantine, and rejection here.

REQ-964 (proof obligation 1) requires that hook to be DETERMINISTIC: its output
feeds the content hash that gates the re-post, so non-determinism ripples forever.
This module enforces that at REGISTRATION with a static AST purity check, then
compiles the source into a callable that executes in a restricted namespace with
no dangerous builtins — mirroring the SQL determinism gate in
:mod:`provisa.mv.determinism`.

The check is fail-closed: source that cannot be parsed, that imports anything, that
reaches wall-clock / randomness / process identity, or that touches dunder-escape
attributes is rejected. There is no partial trust — a rejected script never runs.
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
        abs, all, any, bool, dict, divmod, enumerate, filter, float, frozenset,
        int, len, list, map, max, min, range, reversed, round, set, sorted, str,
        sum, tuple, zip, bytes, complex, ord, chr, format,
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
        "eval", "exec", "compile", "open", "input", "__import__", "globals",
        "locals", "vars", "dir", "getattr", "setattr", "delattr", "hasattr",
        "help", "exit", "quit", "breakpoint", "object", "super", "type",
        "memoryview", "classmethod", "staticmethod", "property", "id", "hash",
        "repr", "print",
    }
)

REQUIRED_FUNC = "preprocess"


class PreprocessValidationError(ValueError):
    """REQ-964: a preprocess script failed the purity gate — rejected at registration."""


class _PurityVisitor(ast.NodeVisitor):
    """Walk the hook's AST and collect every purity/safety violation."""

    def __init__(self) -> None:
        self.errors: list[str] = []

    def visit_Import(self, node: ast.Import) -> None:
        del node
        self.errors.append("import statements are not allowed (non-deterministic / unsafe)")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        del node
        self.errors.append("import statements are not allowed (non-deterministic / unsafe)")

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

    Fail-closed: unparseable source, a missing ``preprocess(rows, ctx)`` definition,
    any import, any forbidden name, or any dunder attribute access is rejected.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return False, f"cannot be parsed: {exc}"

    # The module must DEFINE preprocess(rows, ctx). Enrichment helpers (other defs,
    # constant assignments) are allowed alongside it; top-level side effects are not.
    func: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if stmt.name == REQUIRED_FUNC:
                func = stmt
        elif isinstance(stmt, (ast.Assign, ast.AnnAssign, ast.Expr, ast.Pass)):
            continue  # module-level constants / docstrings are harmless
        else:
            return False, (
                f"only function definitions and constant assignments are allowed at "
                f"module level, found {type(stmt).__name__}"
            )

    if func is None:
        return False, f"must define a {REQUIRED_FUNC}(rows, ctx) function"
    positional = func.args.posonlyargs + func.args.args
    if len(positional) != 2:
        return False, f"{REQUIRED_FUNC} must take exactly two parameters (rows, ctx)"

    visitor = _PurityVisitor()
    visitor.visit(tree)
    if visitor.errors:
        return False, "; ".join(dict.fromkeys(visitor.errors))
    return True, ""


def validate_preprocess(source: str | None) -> None:
    """Enforce the preprocess purity gate at registration (REQ-957 / REQ-964).

    None / blank → no hook (identity), nothing to check. A non-empty script that
    fails the purity check raises :class:`PreprocessValidationError` so it is
    rejected loudly — a non-deterministic hook would ripple forever (REQ-964).
    """
    if source is None or not source.strip():
        return
    ok, reason = check_preprocess_purity(source)
    if not ok:
        raise PreprocessValidationError(f"invalid preprocess hook: {reason}")


def compile_preprocess(source: str | None) -> Callable[..., Any] | None:
    """Validate then compile a preprocess script into a ``preprocess(rows, ctx)`` callable.

    Returns None for a blank/absent script (identity — no hook). The compiled callable
    executes in a namespace whose ``__builtins__`` is the curated :data:`_SAFE_BUILTINS`
    only, so even a construct the static check missed has no dangerous builtin to reach.
    """
    if source is None or not source.strip():
        return None
    validate_preprocess(source)
    namespace: dict[str, Any] = {"__builtins__": dict(_SAFE_BUILTINS)}
    code = compile(source, filename="<preprocess>", mode="exec")
    exec(code, namespace)  # noqa: S102 — sandboxed: purity-checked source, restricted builtins
    func = namespace.get(REQUIRED_FUNC)
    if not callable(func):
        raise PreprocessValidationError(f"{REQUIRED_FUNC} is not callable after compile")
    return func
