# Copyright (c) 2026 Kenneth Stott
# Canary: 9a1d6e30-5b47-4c82-8f19-3e7c0b5d2a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The languages a code source can be written in (REQ-1984, REQ-1993, REQ-2200): one table the
shared layer asks, so nothing in it names a language, and Node and Python behave as they did."""

# Requirements: REQ-1984, REQ-1993, REQ-2200, REQ-2225
from __future__ import annotations

import pytest

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.languages import CODE_SOURCE_TYPES, LANGUAGES
from provisa.codesource.source import config_of
from provisa.core.models import SourceType
from provisa.federation.connector_contract import contract_for
from provisa.transpiler.router import CODE_SOURCES

pytestmark = pytest.mark.unit


def _code(kind: str) -> dict:
    return {"kind": kind, "package": "example.com/rows", "version": "v1.0.0"}


def test_every_code_source_type_is_a_language_of_the_table_and_a_source_type():
    # REQ-2225: spark, a built-in source type run by the Spark host on the pinned CPython.
    assert CODE_SOURCE_TYPES == {"node", "python", "go", "rust", "spark"} == CODE_SOURCES
    for source_type in CODE_SOURCE_TYPES:
        assert SourceType(source_type).value == source_type
        assert contract_for(source_type) is not None


@pytest.mark.parametrize(
    ("source_type", "kind", "runtime"),
    [
        ("node", "npm", "bun"),
        ("python", "pypi", "python"),
        ("go", "gomod", "go"),
        ("rust", "cargo", "rust"),  # REQ-2227
    ],
)
def test_a_source_names_its_languages_package_kind_and_runtime(source_type, kind, runtime):
    config = config_of(source_type, {"runtime": runtime, "code": _code(kind)})
    assert (config.language, config.runtime, config.code["kind"]) == (source_type, runtime, kind)
    package = LANGUAGES[source_type].package_code("example.com/rows", "v1.0.0")
    assert (
        type(package).__name__
        == {"npm": "NpmCode", "pypi": "PypiCode", "gomod": "GoModuleCode", "cargo": "CrateCode"}[
            kind
        ]
    )


def test_a_language_with_one_runtime_runs_on_it_and_node_names_its_own():
    assert config_of("go", {"code": _code("gomod")}).runtime == "go"
    assert config_of("rust", {"code": _code("cargo")}).runtime == "rust"
    assert config_of("python", {"code": _code("pypi")}).runtime == "python"
    assert config_of("spark", {"code": _code("pypi")}).runtime == "python"  # REQ-2225
    with pytest.raises(BundleRefused, match="a node source runs on node, bun, deno, not ''"):
        config_of("node", {"code": _code("npm")})


def test_another_languages_package_or_runtime_is_refused_by_name():
    with pytest.raises(BundleRefused, match="a go source's code is gomod, git, upload, directory"):
        config_of("go", {"code": _code("npm")})
    with pytest.raises(BundleRefused, match="a go source runs on go, not 'node'"):
        config_of("go", {"runtime": "node", "code": _code("gomod")})
    with pytest.raises(BundleRefused, match="'zig' is not a code source type"):
        config_of("zig", {"code": _code("gomod")})
    with pytest.raises(BundleRefused, match="the source's code needs version"):
        config_of("go", {"code": {"kind": "gomod", "package": "example.com/rows"}})


def test_spark_has_an_adapter_loader_registered_and_is_adapter_fetch_only():
    """REQ-2225: a spark source is a code source type; its replica is built by calling its function
    through the code loader, not by the engine. Without a registered loader the engine path is
    taken, which constructs an SQL reference DuckDB cannot resolve
    (``"spark_native"."numbers"`` as a bare catalog identifier)."""
    from types import SimpleNamespace

    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.events.source_loader import _ADAPTER_FETCH_ONLY
    from provisa.federation.engine import build_duckdb_engine

    engine_ns = SimpleNamespace(engine=build_duckdb_engine())
    loaders = build_adapter_loaders(SimpleNamespace(federation_engine=engine_ns), engine_ns)
    # The loader must be registered, the same as python and node.
    assert "spark" in loaders, (
        "spark is a code source type; build_adapter_loaders must register a code loader for it "
        "so its replica is never sent down the engine path"
    )
    # _ADAPTER_FETCH_ONLY is the safety net: if the loader is somehow absent at call time, the
    # engine path must still be blocked.
    assert "spark" in _ADAPTER_FETCH_ONLY, (
        "spark must be in _ADAPTER_FETCH_ONLY so the engine path is refused even as a fallback"
    )
