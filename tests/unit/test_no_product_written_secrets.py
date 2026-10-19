# Copyright (c) 2026 Kenneth Stott
# Canary: a6eddc9f-5944-431b-b665-0e1090e4617d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product never makes up a secret's name and never writes an operator's vault.

A credential field holds exactly what the operator entered -- a value, or a ``${secret:NAME}``
/ ``${env:NAME}`` reference they typed -- and is resolved where it is used. So no product code
calls the secrets store's write or remove API, and none builds a ``${secret:…}`` reference:
the only writers are the admin route where an operator creates a secret by a name they chose
and the seeding of a demo's secrets.yaml, whose author chose its names; the only code that spells
a reference is the code that searches for, or reports, one the operator wrote.

``STILL_TO_REMOVE`` lists what is known to break the rule and is being removed. It only
shrinks: a file in it that no longer offends fails this test until it is taken out.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]

#: The store itself, the one route where an operator creates or removes a secret by name, and a
#: demo's own secrets.yaml, whose names and values its author declared (REQ-2095), written to the
#: vaults of the orgs built from that demo.
MAY_WRITE = frozenset(
    {
        "provisa/core/secrets_store.py",
        "provisa/api/admin/secrets_router.py",
        "provisa/core/demo_secrets.py",
    }
)

#: Code that spells a reference without making one up: the search for references an operator
#: wrote, and the resolver's own messages about a reference it was given.
MAY_SPELL = frozenset(
    {
        "provisa/core/secret_references.py",
        "provisa/core/secrets_providers.py",
        "provisa/core/secrets_store.py",
    }
)

#: Known offenders, each being removed. file -> why it is still here.
STILL_TO_REMOVE: dict[str, str] = {}

_WRITES = {"put", "remove"}


def _files() -> list[Path]:
    return sorted((REPO / "provisa").rglob("*.py"))


def _store_names(tree: ast.AST) -> set[str]:
    """The names ``provisa.core.secrets_store`` is known by in a module."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "provisa.core":
            names |= {a.asname or a.name for a in node.names if a.name == "secrets_store"}
        elif isinstance(node, ast.Import):
            names |= {
                a.asname or a.name for a in node.names if a.name == "provisa.core.secrets_store"
            }
    return names


def writes_a_vault(path: Path) -> list[int]:
    """Lines where ``path`` calls the secrets store's write or remove API."""
    tree = ast.parse(path.read_text())
    stores = _store_names(tree)
    direct = {
        (a.asname or a.name)
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "provisa.core.secrets_store"
        for a in node.names
        if a.name in _WRITES
    }
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in _WRITES:
            owner = func.value
            if isinstance(owner, ast.Name) and owner.id in stores:
                found.append(node.lineno)
        elif isinstance(func, ast.Name) and func.id in direct:
            found.append(node.lineno)
    return found


def builds_a_reference(path: Path) -> list[int]:
    """Lines where ``path`` assembles a ``${secret:…}`` reference from parts."""
    tree = ast.parse(path.read_text())
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            parts = [v.value for v in node.values if isinstance(v, ast.Constant)]
            holes = [v for v in node.values if isinstance(v, ast.FormattedValue)]
            if holes and any(isinstance(p, str) and "${secret:" in p for p in parts):
                found.append(node.lineno)
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            for side in (node.left, node.right):
                if (
                    isinstance(side, ast.Constant)
                    and isinstance(side.value, str)
                    and side.value.rstrip().endswith("${secret:")
                ):
                    found.append(node.lineno)
    return found


def _offences() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for path in _files():
        name = str(path.relative_to(REPO))
        said = []
        if name not in MAY_WRITE:
            said += [f"line {n}: writes or removes a secret" for n in writes_a_vault(path)]
        if name not in MAY_SPELL:
            said += [
                f"line {n}: builds a ${{secret:…}} reference" for n in builds_a_reference(path)
            ]
        if said:
            found[name] = said
    return found


def test_no_product_code_writes_a_vault_or_builds_a_reference():
    new = {name: said for name, said in _offences().items() if name not in STILL_TO_REMOVE}
    assert not new, "\n".join(f"{name}: {'; '.join(said)}" for name, said in sorted(new.items()))


def test_what_is_still_to_be_removed_only_shrinks():
    offending = _offences()
    clean = sorted(name for name in STILL_TO_REMOVE if name not in offending)
    assert not clean, f"no longer offends; take out of STILL_TO_REMOVE: {clean}"


def test_the_checks_see_what_they_are_for(tmp_path):
    sample = tmp_path / "sample.py"
    sample.write_text(
        "from provisa.core import secrets_store\n"
        "async def f(db, org, source_id, value):\n"
        '    name = f"source_{source_id}_password"\n'
        "    await secrets_store.put(db, org, name, value, owner_id='')\n"
        '    return f"${{secret:{name}}}"\n'
        "def g(name):\n"
        '    return "${secret:" + name + "}"\n'
        "def h(value):\n"
        '    return "${secret:" in value\n'
    )
    assert writes_a_vault(sample) == [4]
    assert builds_a_reference(sample) == [5, 7]
