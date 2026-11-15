# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1f4d62-3a9c-4e75-a0d6-2c7e9f15b840
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The sources Provisa runs for itself carry ``is_system``, so the Register table picker omits them."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.api.admin._row_mappers import _source_from_row
from provisa.core.models import Source
from provisa.core.repositories.source import _source_values

_REPO = Path(__file__).resolve().parents[2]
_INSTALL = _REPO / "config" / "provisa-install.yaml"

CONFIG_SYSTEM_SOURCES = ["demo-profiler", "dq-checker", "dq-soda"]
SEEDED_SYSTEM_SOURCES = ["provisa-admin", "provisa-otel"]


def _configured() -> dict[str, Source]:
    raw = yaml.safe_load(_INSTALL.read_text())
    return {s["id"]: Source.model_validate(s) for s in raw["sources"]}


@pytest.mark.parametrize("source_id", CONFIG_SYSTEM_SOURCES)
def test_a_configured_system_source_is_is_system(source_id):
    assert _configured()[source_id].is_system is True


def test_a_user_source_is_not_is_system():
    user = [s for sid, s in _configured().items() if sid not in CONFIG_SYSTEM_SOURCES]
    assert user
    assert not any(s.is_system for s in user)


@pytest.mark.parametrize("source_id", SEEDED_SYSTEM_SOURCES + ["__derived__"])
def test_a_seeded_built_in_source_is_written_is_system(source_id):
    import ast

    tree = ast.parse((_REPO / "provisa/api/startup_seed.py").read_text())
    fn = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_seed_built_in_sources"
    )
    body = ast.get_source_segment((_REPO / "provisa/api/startup_seed.py").read_text(), fn)
    assert body is not None
    assert body.count('"is_system": True') == 3


def test_the_flag_is_stored_and_exposed():
    src = Source(id="dq-x", type="soda", is_system=True)
    assert _source_values(src)["is_system"] is True
    row = {
        "id": "dq-x",
        "type": "soda",
        "dialect": "",
        "replicate": None,
        "region": None,
        "is_system": True,
    }
    assert _source_from_row(row, connection=False).is_system is True
    assert _source_from_row({**row, "is_system": False}, connection=False).is_system is False
