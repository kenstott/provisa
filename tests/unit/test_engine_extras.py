# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3b5f21-6c4d-4a97-b0e2-91d7c3a5f604
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every selectable engine kind whose dialect is not a base dependency names the pyproject extra
that ships it, and selecting the kind without that extra is refused naming the extra."""

from __future__ import annotations

import importlib.util
import tomllib
from pathlib import Path

import pytest

from provisa.federation import engine as fe

_PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"

# The engine kinds whose SQLAlchemy dialect Provisa does not ship in its base install.
_EXTRA_KINDS = {
    "cockroachdb",
    "db2",
    "exasol",
    "firebird",
    "monetdb",
    "opengauss",
    "saphana",
    "vertica",
}


def _extras() -> dict[str, list[str]]:
    return tomllib.loads(_PYPROJECT.read_text())["project"]["optional-dependencies"]


def test_each_listed_kind_names_its_extra():
    assert set(fe.RDB_EXTRAS) == _EXTRA_KINDS
    rdb_kinds = {k for k, _label, _dsn in fe._RDB_KINDS}
    declared = _extras()
    for kind, (extra, modules) in fe.RDB_EXTRAS.items():
        assert kind in rdb_kinds, kind
        assert extra in declared, f"{kind}: extra {extra!r} is not in pyproject"
        assert declared[extra], f"{kind}: extra {extra!r} is empty"
        assert modules, kind


def test_all_drivers_installs_every_engine_extra():
    all_drivers = " ".join(_extras()["all-drivers"])
    for extra, _modules in fe.RDB_EXTRAS.values():
        assert extra in all_drivers or all(p in all_drivers for p in _extras()[extra]), extra


@pytest.fixture
def nothing_installed(monkeypatch):
    real = importlib.util.find_spec
    absent = {m for _extra, modules in fe.RDB_EXTRAS.values() for m in modules}
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a, **kw: None if name in absent else real(name, *a, **kw),
    )


@pytest.mark.parametrize("kind", sorted(_EXTRA_KINDS))
def test_missing_extra_is_refused_by_name(kind, nothing_installed):
    extra, modules = fe.RDB_EXTRAS[kind]
    with pytest.raises(fe.MissingEngineExtra) as refused:
        fe.build_engine(kind)
    assert refused.value.kind == kind
    assert refused.value.extra == extra
    assert refused.value.module == modules[0]
    assert f"provisa[{extra}]" in str(refused.value)


def test_registry_lists_a_kind_without_its_extra_unavailable_naming_it(nothing_installed):
    by_key = {e["key"]: e for e in fe.engine_registry()}
    for kind, (extra, _modules) in fe.RDB_EXTRAS.items():
        assert f"provisa[{extra}]" in by_key[kind]["unavailable"], kind
    assert by_key["mysql"]["unavailable"] is None


def test_installed_extra_is_not_refused(monkeypatch):
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a, **kw: object())
    for kind in fe.RDB_EXTRAS:
        fe.require_engine_extra(kind)


def test_absent_parent_package_is_refused_naming_the_extra(monkeypatch):
    def find_spec(name, *a, **kw):
        if name == "firebird.driver":
            raise ModuleNotFoundError(name="firebird")
        return object()

    monkeypatch.setattr(importlib.util, "find_spec", find_spec)
    with pytest.raises(fe.MissingEngineExtra, match=r"provisa\[firebird\]"):
        fe.require_engine_extra("firebird")


def test_base_install_kind_needs_no_extra(nothing_installed):
    fe.require_engine_extra("mysql")
    fe.require_engine_extra("sqlalchemy")
