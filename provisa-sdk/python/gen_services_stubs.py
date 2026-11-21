# Copyright (c) 2026 Kenneth Stott
# Canary: 4ab193f3-c670-4cd9-9c04-499d178e84b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes ``provisa_services-stubs/__init__.pyi`` from the real ``provisa_services`` module.

    python gen_services_stubs.py [--check]

The SDK ships the services module's types as PEP 561 stubs, so authors type-check
``from provisa_services import ReadContext`` while the host's own module is the one imported at run
time. The stubs are the module's public surface with every body and default elided, generated so
they cannot drift from it; ``--check`` exits 1 when the committed stubs differ.
"""

# Requirements: REQ-1984, REQ-2106
from __future__ import annotations

import ast
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parents[1] / "provisa" / "python" / "services" / "provisa_services" / "__init__.py"
STUBS = HERE / "provisa_services-stubs" / "__init__.pyi"
HEADER = (
    "# Generated from provisa/python/services/provisa_services/__init__.py by\n"
    "# provisa-sdk/python/gen_services_stubs.py. Do not edit.\n"
)
_ELLIPSIS = ast.Constant(Ellipsis)


def _public(name: str) -> bool:
    return not name.startswith("_") or (name.startswith("__") and name.endswith("__"))


def _docstring(body: list[ast.stmt]) -> list[ast.stmt]:
    first = body[0] if body else None
    if (
        isinstance(first, ast.Expr)
        and isinstance(first.value, ast.Constant)
        and isinstance(first.value.value, str)
    ):
        return [first]
    return []


def _elide_defaults(args: ast.arguments) -> None:
    args.defaults = [_ELLIPSIS for _ in args.defaults]
    args.kw_defaults = [None if d is None else _ELLIPSIS for d in args.kw_defaults]


def _attributes(init: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.stmt]:
    """``self.x`` assignments in ``__init__``, as class-level annotations: an annotated one by its
    annotation, an unannotated one taken straight from an annotated parameter by that one's."""
    hints = {a.arg: a.annotation for a in init.args.args if a.annotation is not None}
    out: list[ast.stmt] = []
    for node in ast.walk(init):
        target, annotation = None, None
        if isinstance(node, ast.AnnAssign):
            target, annotation = node.target, node.annotation
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(node.value, ast.Name):
                annotation = hints.get(node.value.id)
        if (
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
            and annotation is not None
        ):
            out.append(ast.AnnAssign(ast.Name(target.attr), annotation, value=None, simple=1))
    return out


def _function(node: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.stmt:
    _elide_defaults(node.args)
    node.body = [*_docstring(node.body), ast.Expr(_ELLIPSIS)]
    return node


def _class(node: ast.ClassDef) -> ast.ClassDef:
    body: list[ast.stmt] = list(_docstring(node.body))
    for item in node.body[len(body) :]:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and _public(item.name):
            if item.name == "__init__":
                body.extend(_attributes(item))
            body.append(_function(item))
        elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
            if item.value is not None:
                item.value = _ELLIPSIS
            body.append(item)
    node.body = body or [ast.Expr(_ELLIPSIS)]
    return node


def _type_var(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "TypeVar"
    )


def stubs(source: str) -> str:
    """The stub text of the module ``source``."""
    module = ast.parse(source)
    body: list[ast.stmt] = []
    for node in module.body[len(_docstring(module.body)) :]:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom)) or _type_var(node):
            body.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            body.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _public(node.name):
            body.append(_function(node))
        elif isinstance(node, ast.ClassDef) and _public(node.name):
            body.append(_class(node))
    return HEADER + "\n" + ast.unparse(ast.Module(body=body, type_ignores=[])) + "\n"


def main(argv: list[str]) -> int:
    text = stubs(SOURCE.read_text())
    if argv == ["--check"]:
        if STUBS.read_text() != text:
            print(f"{STUBS} differs from {SOURCE}: run gen_services_stubs.py", file=sys.stderr)
            return 1
        return 0
    STUBS.parent.mkdir(exist_ok=True)
    STUBS.write_text(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
