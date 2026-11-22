# Copyright (c) 2026 Kenneth Stott
# Canary: b719e421-b6e8-4aee-a73c-aecb85fda145
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SDK is markers and types only (REQ-1984: at runtime the SDK does nothing): a decorator
returns the function it is given with only its docstring changed, the module imports the standard
library alone and defines no class, and its stubs are the services module's own surface."""

# Requirements: REQ-1984
from __future__ import annotations

import ast
import importlib
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SDK = Path(__file__).resolve().parents[1]
MODULE = SDK / "provisa_sdk" / "__init__.py"
#: The standard library modules the SDK may import; anything else is a dependency.
STDLIB = {"__future__", "collections.abc", "datetime", "decimal", "inspect", "typing"}
#: The calls a decorator body may make: the one docstring append, and building its tag lines.
CALLS = {"_mark", "_inspect.cleandoc", "join", "lstrip", "splitlines", "append"}


@pytest.fixture(scope="module")
def sdk():
    sys.path.insert(0, str(SDK))
    try:
        yield importlib.import_module("provisa_sdk")
    finally:
        sys.path.remove(str(SDK))
        sys.modules.pop("provisa_sdk", None)


def _tree() -> ast.Module:
    return ast.parse(MODULE.read_text())


def _name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    assert isinstance(func, ast.Attribute)
    if isinstance(func.value, ast.Name) and func.value.id.startswith("_"):
        return f"{func.value.id}.{func.attr}"
    return func.attr


def test_the_module_imports_the_standard_library_only():
    tree = _tree()
    imported = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            imported |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    assert imported <= STDLIB


def test_the_services_types_are_named_for_the_type_checker_only():
    (guarded,) = [
        n
        for n in _tree().body
        if isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == "TYPE_CHECKING"
    ]
    for node in guarded.body:
        assert isinstance(node, (ast.ImportFrom, ast.AnnAssign))
        if isinstance(node, ast.ImportFrom):
            assert node.module == "provisa_services"


def test_the_module_holds_only_imports_aliases_and_decorators():
    for node in _tree().body[1:]:
        assert isinstance(
            node, (ast.Import, ast.ImportFrom, ast.If, ast.Assign, ast.AnnAssign, ast.FunctionDef)
        ), ast.dump(node)[:120]


def test_a_decorator_calls_nothing_but_the_docstring_append():
    for fn in [n for n in _tree().body if isinstance(n, ast.FunctionDef)]:
        for call in [c for c in ast.walk(fn) if isinstance(c, ast.Call)]:
            assert _name(call) in CALLS, f"{fn.name} calls {_name(call)}"
        for target in [t for n in ast.walk(fn) if isinstance(n, ast.Assign) for t in n.targets]:
            if isinstance(target, ast.Attribute):
                assert (fn.name, target.attr) == ("_mark", "__doc__")


def _decorations(sdk) -> list:
    return [
        sdk.table,
        sdk.table(),
        sdk.table(loader=True),
        sdk.table(loader=10),
        sdk.stream,
        sdk.command,
        sdk.command(insert="t"),
        sdk.query,
        sdk.columns,
        sdk.change(table="t", key=["k"], removed="r", cursor="c"),
        sdk.on_start,
        sdk.on_stop,
        sdk.health,
        sdk.requires("A", secret=True),
    ]


def test_a_decorator_returns_its_function_changing_its_docstring_alone(sdk):
    for decorate in _decorations(sdk):

        def fn(x: int = 1) -> list[int]:
            """Text."""
            return [x]

        code, attributes = fn.__code__, dict(vars(fn))
        assert decorate(fn) is fn
        assert fn.__code__ is code
        assert vars(fn) == attributes
        assert fn() == [1]
        assert fn.__doc__ is not None and fn.__doc__.startswith("Text.\n@")


def test_spelling_is_a_whole_number(sdk):
    assert type(sdk.SPELLING) is int and sdk.SPELLING >= 1


def test_the_stubs_are_generated_from_the_services_module():
    done = subprocess.run(
        [sys.executable, str(SDK / "gen_services_stubs.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
