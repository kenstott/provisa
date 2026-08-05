# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7d24f9-6e31-4a85-b3c0-5817ae42f906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1339: a new org's system sources exist before anything references them.

``load_config`` registers the config's tables, and a ``tables`` row whose ``source_id`` names a
system source needs that source row already present — the FK has to have a target. When the seed
runs after the load instead, the tables that pointed at ``__derived__`` are dropped, and the org
comes up looking merely incomplete rather than broken: no error, just missing tables.

The ordering is read out of the builder itself. Running the two calls in a fixture would prove
the pair works, not that this function calls them in this order, and the order is the whole
requirement.
"""

# Requirements: REQ-1266, REQ-1339

from __future__ import annotations

import ast

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Every source Provisa itself owns. __derived__ is the one that matters most here: governed views
# and MVs hang off it, so an org missing it loses its derived tables rather than one source.
SYSTEM_SOURCES = {"provisa-admin", "provisa-otel", "__derived__"}


def _function(module_path: Path, name: str) -> ast.AsyncFunctionDef:
    tree = ast.parse(module_path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {module_path}")


def _awaited_names(func: ast.AsyncFunctionDef) -> list[str]:
    """The awaited callables in source order, by their bare name."""
    names = []
    for node in ast.walk(func):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value.func
            if isinstance(call, ast.Name):
                names.append((node.lineno, call.id))
            elif isinstance(call, ast.Attribute):
                names.append((node.lineno, call.attr))
    return [n for _, n in sorted(names)]


def test_the_org_builder_seeds_system_sources_before_it_loads_the_config():
    awaited = _awaited_names(_function(_REPO_ROOT / "provisa/api/app.py", "build_org_runtime"))

    assert "_seed_built_in_sources" in awaited, "the org builder no longer seeds system sources"
    assert "load_config" in awaited, "the org builder no longer loads the config"
    assert awaited.index("_seed_built_in_sources") < awaited.index("load_config"), (
        "config load precedes the system-source seed — a tables row naming __derived__ has no FK "
        "target, and the table is dropped silently"
    )


def test_the_org_builder_seeds_into_the_org_being_built():
    """Seeding without the org id writes the DEFAULT org's rows, leaving the new org empty."""
    source = (_REPO_ROOT / "provisa/api/app.py").read_text()
    builder = source.split("async def build_org_runtime")[1]
    call = builder.split("await _seed_built_in_sources(")[1].split(")")[0]

    assert "org_id=org_id" in call


@pytest.mark.parametrize("source_id", sorted(SYSTEM_SOURCES))
def test_every_system_source_is_seeded(source_id):
    from provisa.core.config_loader import _SYSTEM_SOURCE_IDS

    seed = (_REPO_ROOT / "provisa/api/startup_seed.py").read_text()
    body = seed.split("async def _seed_built_in_sources")[1]

    # The loader's system-source list is the definition of "system source" — the seeder has to
    # cover exactly it, or a config load will reference a row nobody wrote.
    assert source_id in _SYSTEM_SOURCE_IDS
    # __derived__ is written through its constant rather than as a literal.
    seeded = f'"id": "{source_id}"' in body or (
        source_id == "__derived__" and '"id": DERIVED_SOURCE_ID' in body
    )
    assert seeded, f"{source_id} is no longer seeded for a new org"


def test_the_seed_is_an_upsert_so_a_rebuild_does_not_collide():
    """An org runtime is rebuilt on config reload; a plain insert would fail the second time and
    take the whole build with it."""
    seed = (_REPO_ROOT / "provisa/api/startup_seed.py").read_text()
    body = seed.split("async def _seed_built_in_sources")[1]

    assert body.count("upsert(") >= len(SYSTEM_SOURCES)
    assert "index_elements=[\"id\"]" in body
