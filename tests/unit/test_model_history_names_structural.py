# Copyright (c) 2026 Kenneth Stott
# Canary: 886c8be7-e424-4968-bb0f-0215d9c1919c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No writer names a model change by a serial id (REQ-1524, amended 2026-11-14).

Walks every ``model_change.name(...)`` call in ``provisa/`` and holds each to the rule: the change
names its object by a model URI built by a :mod:`provisa.core.model_uri` builder (through
``model_change.uri``), and nothing in the message is built from a serial -- a parameter typed
``int``, a name the repositories give a serial key, or a number turned into text. A new call site
that names an id fails here before it reaches a commit.
"""

# Requirements: REQ-1524, REQ-1385

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

from provisa.core import model_uri

ROOT = Path(__file__).resolve().parents[2] / "provisa"

#: The names the repositories give a serial (integer) key. A parameter typed ``int`` is caught by
#: its annotation; these catch a local or a row field read into one.
SERIAL_NAMES = frozenset(
    {"term_id", "table_id", "rule_id", "offer_id", "tid", "class_term_id", "successor"}
)

#: Calls that turn a number into message text.
TEXT_OF = frozenset({"str", "int", "repr", "format"})

#: The kinds REQ-1385's grammar gives no kind keyword, so no model URI: a scheduled trigger and a
#: judgment basis, each named by its own text id (a name, not a serial). Giving them a keyword
#: would reserve it as a domain name, which is the maintainer's to decide (J1315 open question);
#: until then their writers are held to the serial rule but not to naming a URI.
NO_URI_YET = {
    "provisa/core/repositories/scheduled_trigger.py": frozenset(
        {"create", "replace", "delete_one", "set_enabled", "load_from_config"}
    ),
    "provisa/judgments/bases.py": frozenset({"put_basis", "declare_changed"}),
}

BUILDERS = frozenset(
    n for n in dir(model_uri) if n.endswith("_uri") and callable(getattr(model_uri, n))
)


@dataclass
class Site:
    path: str
    line: int
    call: ast.Call
    function: str
    int_params: frozenset[str]
    # Names in the enclosing function that only ever hold a model URI: a parameter typed
    # ObjectUri, or a local every assignment of which is one.
    uri_names: frozenset[str]
    # What each local of the enclosing function is assigned: a message built from a local is
    # built from what the local holds.
    assigned: dict[str, list[ast.AST]]

    def __str__(self) -> str:
        return f"{self.path}:{self.line}"


def _is_name_call(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "name"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "model_change"
    )


def _is_uri(node: ast.AST) -> bool:
    """``model_change.uri(model_uri.<builder>, ...)``."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "uri"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "model_change"
        and bool(node.args)
        and isinstance(node.args[0], ast.Attribute)
        and isinstance(node.args[0].value, ast.Name)
        and node.args[0].value.id == "model_uri"
        and node.args[0].attr in BUILDERS
    )


def _is_uri_expr(node: ast.AST, uri_names: frozenset[str] = frozenset()) -> bool:
    if isinstance(node, ast.IfExp):
        return _is_uri_expr(node.body, uri_names) and _is_uri_expr(node.orelse, uri_names)
    if isinstance(node, ast.Name):
        return node.id in uri_names
    return _is_uri(node)


def _assigned(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, list[ast.AST]]:
    assigned: dict[str, list[ast.AST]] = {}
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assigned.setdefault(target.id, []).append(node.value)
        elif isinstance(node, ast.AnnAssign | ast.AugAssign) and isinstance(node.target, ast.Name):
            assigned.setdefault(node.target.id, []).append(node.value or node)
    return assigned


def _uri_names(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> frozenset[str]:
    every = [*fn.args.posonlyargs, *fn.args.args, *fn.args.kwonlyargs]
    typed = {
        a.arg
        for a in every
        if a.annotation is not None and "ObjectUri" in ast.unparse(a.annotation)
    }
    assigned = _assigned(fn)
    locals_ = {
        name
        for name, values in assigned.items()
        if name not in typed and all(_is_uri_expr(v) for v in values)
    }
    return frozenset(typed | locals_)


def _int_params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> frozenset[str]:
    every = [*fn.args.posonlyargs, *fn.args.args, *fn.args.kwonlyargs]
    return frozenset(
        a.arg
        for a in every
        if a.annotation is not None and "int" in ast.unparse(a.annotation).replace("'", "")
    )


def _sites_in(source: str, path: str) -> list[Site]:
    sites: list[Site] = []

    def visit(
        node: ast.AST,
        function: str,
        int_params: frozenset[str],
        uri_names: frozenset[str],
        assigned: dict[str, list[ast.AST]],
    ) -> None:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            function = node.name
            int_params = int_params | _int_params(node)
            uri_names = _uri_names(node)
            assigned = _assigned(node)
        if _is_name_call(node):
            assert isinstance(node, ast.Call)
            sites.append(Site(path, node.lineno, node, function, int_params, uri_names, assigned))
        for child in ast.iter_child_nodes(node):
            visit(child, function, int_params, uri_names, assigned)

    visit(ast.parse(source), "<module>", frozenset(), frozenset(), {})
    return sites


def _message_nodes(node: ast.AST, site: Site, followed: set[str]) -> list[ast.AST]:
    """Every node a message part is built from. An awaited call is a lookup -- the serial it is
    given is turned into a name (``await term_name(conn, term_id)``) -- so its arguments are not
    message text; a local is followed into what it is assigned."""
    found: list[ast.AST] = []
    pending = [node]
    while pending:
        current = pending.pop()
        found.append(current)
        if isinstance(current, ast.Await):
            continue
        if (
            isinstance(current, ast.Name)
            and current.id in site.assigned
            and current.id not in followed
        ):
            followed.add(current.id)
            pending.extend(site.assigned[current.id])
        pending.extend(ast.iter_child_nodes(current))
    return found


def _faults(site: Site) -> list[str]:
    faults: list[str] = []
    if site.call.keywords:
        faults.append("names its change with keywords")
    unaddressed = site.function in NO_URI_YET.get(site.path, frozenset())
    if not unaddressed and not any(_is_uri_expr(a, site.uri_names) for a in site.call.args):
        faults.append("names no object by a model_uri builder through model_change.uri")
    for arg in site.call.args:
        for node in _message_nodes(arg, site, set()):
            if isinstance(node, ast.Name) and node.id in site.int_params:
                faults.append(f"builds its message from {node.id!r}, a parameter typed int")
            elif isinstance(node, ast.Name) and node.id in SERIAL_NAMES:
                faults.append(f"builds its message from {node.id!r}, a serial key")
            elif isinstance(node, ast.Attribute) and node.attr in SERIAL_NAMES:
                faults.append(f"builds its message from .{node.attr}, a serial key")
            elif (
                isinstance(node, ast.Subscript)
                and isinstance(node.slice, ast.Constant)
                and node.slice.value in SERIAL_NAMES
            ):
                faults.append(f"builds its message from [{node.slice.value!r}], a serial key")
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in TEXT_OF
            ):
                faults.append(f"turns a value into text with {node.func.id}()")
    return faults


def _every_site() -> list[Site]:
    sites: list[Site] = []
    for file in sorted(ROOT.rglob("*.py")):
        if file.name == "model_change.py" and file.parent.name == "core":
            continue
        source = file.read_text()
        if "model_change" not in source:
            continue
        sites += _sites_in(source, str(file.relative_to(ROOT.parent)))
    return sites


SITES = _every_site()


def test_the_walk_finds_the_call_sites():
    # The repositories name their changes; a walk that found none would pass vacuously.
    assert len(SITES) > 40


@pytest.mark.parametrize("site", SITES, ids=str)
def test_each_change_is_named_by_model_uri_and_never_by_a_serial(site):
    assert _faults(site) == [], f"{site}: {_faults(site)}"


@pytest.mark.parametrize(
    ("source", "fault"),
    [
        (
            'async def retire(conn, term_id: int):\n    model_change.name("retire", "term", term_id)',
            "names no object",
        ),
        (
            "async def retire(conn, term_id: int):\n"
            '    model_change.name("retire", model_change.uri(model_uri.term_uri, term_id))',
            "a parameter typed int",
        ),
        (
            "async def drop(conn, row):\n"
            '    model_change.name("delete", model_change.uri(model_uri.term_uri, str(row.n)))',
            "str()",
        ),
        (
            "async def drop(conn, term_id: int):\n"
            "    text = str(term_id)\n"
            '    model_change.name("delete", model_change.uri(model_uri.term_uri, text))',
            "a parameter typed int",
        ),
        (
            "async def drop(conn, row):\n"
            '    model_change.name("delete", model_change.uri(model_uri.term_uri, row["name"]),'
            " f\"of {row['table_id']}\")",
            "a serial key",
        ),
    ],
)
def test_the_rule_catches_a_message_built_from_an_id(source, fault):
    (site,) = _sites_in(source, "example.py")
    assert any(fault in f for f in _faults(site)), _faults(site)
