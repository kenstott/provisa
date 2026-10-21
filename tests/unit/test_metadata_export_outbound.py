# Copyright (c) 2026 Kenneth Stott
# Canary: 53909912-79ef-4294-bda4-f6aa6280feee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The export port is outbound, and that is a structural property of the package.

REQ-1068's export publishes governance metadata to external catalogs. Reading a catalog back
is REQ-2122's, through its own port (``reader.CatalogReader``: ``search`` and ``read``), which a
steward's pick, refresh, import or lock drives; never through a helper of the export. An adapter
that added a ``fetch_from_catalog`` beside its publish would blur the two without breaking any
behavioural test, so the constraint is asserted against the source tree itself.
"""

# Requirements: REQ-1068, REQ-2122

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import provisa.api.metadata_export as export_package

_PACKAGE_DIR = Path(str(export_package.__file__)).parent

# Verbs that would mean "read the external catalog into Provisa". ``publish``, ``emit``,
# ``push``, ``send`` and ``health`` are the outbound vocabulary and stay allowed.
_INBOUND_PREFIXES = ("ingest", "import_", "pull", "fetch", "scan", "sync_from", "read_catalog")


def _module_paths() -> list[Path]:
    return sorted(p for p in _PACKAGE_DIR.rglob("*.py") if "__pycache__" not in p.parts)


def test_package_has_modules():
    assert _module_paths(), f"no modules found under {_PACKAGE_DIR}"


@pytest.mark.parametrize("path", _module_paths(), ids=lambda p: p.name)
def test_no_inbound_entry_points(path: Path):
    tree = ast.parse(path.read_text())
    offenders = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.lstrip("_").startswith(_INBOUND_PREFIXES)
    ]
    assert not offenders, (
        f"{path.name} defines inbound entry point(s) {offenders}; reading a catalog is the "
        "CatalogReader port's (search, read)"
    )


def test_port_declares_no_read_method():
    from provisa.api.metadata_export.provider import MetadataExport

    declared = {
        name
        for name in vars(MetadataExport)
        if not name.startswith("_") and callable(vars(MetadataExport)[name])
    }
    assert declared == {"publish", "health"}
