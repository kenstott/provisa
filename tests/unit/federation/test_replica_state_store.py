# Copyright (c) 2026 Kenneth Stott
# Canary: 85873f94-858e-4a6c-b757-5bcfb1fb2a00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1920: replica state is read and written in one place, the state store module
``provisa/federation/replica_state.py``. Other code asks it; nothing else touches the table.
"""

import ast
import re
from pathlib import Path

from provisa.core import env_classes

_ROOT = Path(__file__).resolve().parents[3] / "provisa"
_STORE = _ROOT / "federation" / "replica_state.py"
# Where the table is declared, and where it is classified by name for environment copies.
_DECLARES = {_ROOT / "core" / "schema_org.py", _ROOT / "core" / "env_classes.py"}
_WRITES = re.compile(
    r"\b(insert\s+into|update|delete\s+from|truncate(\s+table)?)\s+\"?replica_state\b",
    re.IGNORECASE,
)


def _table_uses(path: Path) -> list[str]:
    """How ``path`` reaches the replica-state table: by importing the table object, by naming
    it as an attribute of the schema module, or in a statement's text."""
    tree = ast.parse(path.read_text(), filename=str(path))
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "provisa.core.schema_org":
            if any(alias.name == "replica_state" for alias in node.names):
                found.append(f"{path}:{node.lineno}: imports the table")
        elif isinstance(node, ast.Attribute) and node.attr == "replica_state":
            if isinstance(node.value, ast.Name) and node.value.id == "schema_org":
                found.append(f"{path}:{node.lineno}: schema_org.replica_state")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if _WRITES.search(node.value):
                found.append(f"{path}:{node.lineno}: a statement that writes the table")
    return found


def test_only_the_state_store_reads_or_writes_the_replica_state_table():
    offenders: list[str] = []
    for path in sorted(_ROOT.rglob("*.py")):
        if path == _STORE or path in _DECLARES:
            continue
        offenders += _table_uses(path)
    assert offenders == []
    # The scan sees what it is meant to see: the store itself uses the table.
    assert _table_uses(_STORE)


def test_replica_state_is_never_copied_to_another_environment():
    assert "replica_state" in env_classes.NEVER_RUNTIME
    assert "replica_state" not in env_classes.CARRIED
