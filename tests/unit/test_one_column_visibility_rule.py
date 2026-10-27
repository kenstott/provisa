# Copyright (c) 2026 Kenneth Stott
# Canary: 7b379b5c-7aca-4794-97ac-157854c7253b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which columns a role is served is decided in one function (REQ-2152).

``rights.column_served`` is the only rule, so a change to it is contained there. This guard reads
every module under ``provisa/`` and fails on code that decides visibility from a grant list by
itself: a membership test (``in`` / ``not in``, or a set method testing it) or a set
intersection (``&``) against a
``visible_to`` value, read directly or through a local bound to one. The places that read a
``visible_to`` list for something other than a column's visibility -- a metric's, a command's or
a webhook's assignment, or code that writes a grant -- are listed with why.
"""

# Requirements: REQ-2152

from __future__ import annotations

import ast
from pathlib import Path

import provisa

_ROOT = Path(provisa.__file__).parent

# (module, function) → why it reads a ``visible_to`` list.
_NOT_COLUMN_VISIBILITY: dict[tuple[str, str], str] = {
    ("security/rights.py", "column_served"): "the rule itself",
    ("api/admin/schema_query.py", "_sees_metric"): "a metric's grant",
    ("api/flight/catalog.py", "role_sees_metric"): "a metric's grant",
    ("api/mcp/tools.py", "_visible_metrics"): "a metric's grant",
    ("bolt/session.py", "_maybe_invoke_metric_call"): "a metric's grant",
    ("nl/runner.py", "run_nl_job"): "a metric's grant",
    ("pgwire/catalog_populate.py", "_build_catalog_db"): "a metric's grant",
    ("compiler/schema_gen.py", "_build_visible_tables"): "a metric's grant",
    ("security/mutation_authz.py", "command_assigned"): "a command's assignment",
    ("api/rest/openapi_spec.py", "generate_rest_openapi_spec"): "a function's assignment",
    ("compiler/actions_schema.py", "_build_action_fields"): "a command's assignment",
    ("compiler/actions_schema.py", "_build_callable_fields"): "a command's assignment",
    ("compiler/function_gen.py", "build_function_mutations"): "a function or webhook's assignment",
    ("pgwire/function_catalog.py", "_fn_visible"): "a function's assignment",
    ("hasura_v2/mapper.py", "_inherit_several"): "writes an imported role's grants",
    ("security/default_grant.py", "with_read_grant"): "writes a registration's grant (REQ-2154)",
    ("api/startup_seed.py", "_ensure_ops_steward_grant"): "writes the ops stewards' grant",
    ("api/demo_templates.py", "_sandbox_sees_what_its_rights_cover"): "writes a demo's grants",
    # calcite's: the lineage roles lens moves to the shared read-rights filter (REQ-2152).
    ("api/admin/lineage_router.py", "_role_seeds"): "calcite's lineage lens, being moved",
}

# Set methods that test membership as ``in`` and ``&`` do.
_SET_TESTS = {"intersection", "isdisjoint", "issubset", "issuperset"}

# A local bound to one of these reads the grant list itself (a copy of it, not a call that
# merely carries it to the rule).
_COPIES = {"set", "frozenset", "list", "tuple", "sorted"}


def _reads_grant(node: ast.AST, src: str) -> bool:
    """``node`` reads a ``visible_to`` list: an attribute, a subscript, a ``.get``, or a copy."""
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Name) and func.id in _COPIES:
            return any(_reads_grant(a, src) for a in node.args)
        if isinstance(func, ast.Attribute) and func.attr == "get":
            return any(isinstance(a, ast.Constant) and a.value == "visible_to" for a in node.args)
        return False
    if isinstance(node, ast.BoolOp):
        return any(_reads_grant(v, src) for v in node.values)
    if isinstance(node, ast.Attribute):
        return node.attr == "visible_to"
    if isinstance(node, ast.Subscript):
        return isinstance(node.slice, ast.Constant) and node.slice.value == "visible_to"
    return False


def _deciders() -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for path in sorted(_ROOT.rglob("*.py")):
        src = path.read_text(encoding="utf-8")
        for fn in ast.walk(ast.parse(src)):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            nodes = list(ast.walk(fn))
            bound = {"visible_to"}
            for n in nodes:
                if isinstance(n, ast.Assign) and _reads_grant(n.value, src):
                    bound |= {t.id for t in n.targets if isinstance(t, ast.Name)}

            def grant(e: ast.AST) -> bool:
                return _reads_grant(e, src) or (isinstance(e, ast.Name) and e.id in bound)

            for n in nodes:
                if isinstance(n, ast.Compare) and any(
                    isinstance(o, (ast.In, ast.NotIn)) for o in n.ops
                ):
                    hit = any(grant(c) for c in n.comparators)
                elif isinstance(n, ast.BinOp) and isinstance(n.op, ast.BitAnd):
                    hit = grant(n.left) or grant(n.right)
                elif (
                    isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and n.func.attr in _SET_TESTS
                ):
                    hit = grant(n.func.value) or any(grant(a) for a in n.args)
                else:
                    continue
                if hit:
                    found.add((path.relative_to(_ROOT).as_posix(), fn.name))
    return found


def test_no_module_but_the_rule_decides_column_visibility_from_a_grant_list():
    unlisted = sorted(_deciders() - set(_NOT_COLUMN_VISIBILITY))
    assert unlisted == [], (
        "these decide a column's visibility from visible_to themselves; read "
        "provisa.security.rights.column_served (or visibility.visible_tables, "
        f"rights.served_roles) instead: {unlisted}"
    )


def test_the_guard_finds_a_decision_made_outside_the_rule(tmp_path, monkeypatch):
    """The scan is not vacuous: a function testing membership in a column's grant is caught."""
    module = tmp_path / "pkg" / "leak.py"
    module.parent.mkdir()
    module.write_text(
        "def leaks(column, role_id):\n"
        "    granted = column['visible_to']\n"
        "    return role_id in granted\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(f"{__name__}._ROOT", module.parent)
    assert _deciders() == {("leak.py", "leaks")}
