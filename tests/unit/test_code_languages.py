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

# Requirements: REQ-1984, REQ-1993, REQ-2200
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
    assert CODE_SOURCE_TYPES == {"node", "python", "go"} == CODE_SOURCES
    for source_type in CODE_SOURCE_TYPES:
        assert SourceType(source_type).value == source_type
        assert contract_for(source_type) is not None


@pytest.mark.parametrize(
    ("source_type", "kind", "runtime"),
    [("node", "npm", "bun"), ("python", "pypi", "python"), ("go", "gomod", "go")],
)
def test_a_source_names_its_languages_package_kind_and_runtime(source_type, kind, runtime):
    config = config_of(source_type, {"runtime": runtime, "code": _code(kind)})
    assert (config.language, config.runtime, config.code["kind"]) == (source_type, runtime, kind)
    package = LANGUAGES[source_type].package_code("example.com/rows", "v1.0.0")
    assert (
        type(package).__name__
        == {"npm": "NpmCode", "pypi": "PypiCode", "gomod": "GoModuleCode"}[kind]
    )


def test_a_language_with_one_runtime_runs_on_it_and_node_names_its_own():
    assert config_of("go", {"code": _code("gomod")}).runtime == "go"
    assert config_of("python", {"code": _code("pypi")}).runtime == "python"
    with pytest.raises(BundleRefused, match="a node source runs on node, bun, deno, not ''"):
        config_of("node", {"code": _code("npm")})


def test_another_languages_package_or_runtime_is_refused_by_name():
    with pytest.raises(BundleRefused, match="a go source's code is gomod, git, upload, directory"):
        config_of("go", {"code": _code("npm")})
    with pytest.raises(BundleRefused, match="a go source runs on go, not 'node'"):
        config_of("go", {"runtime": "node", "code": _code("gomod")})
    with pytest.raises(BundleRefused, match="'rust' is not a code source type"):
        config_of("rust", {"code": _code("gomod")})
    with pytest.raises(BundleRefused, match="the source's code needs version"):
        config_of("go", {"code": {"kind": "gomod", "package": "example.com/rows"}})
