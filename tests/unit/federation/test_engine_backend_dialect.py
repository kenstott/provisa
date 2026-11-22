# Copyright (c) 2026 Kenneth Stott
# Canary: 0d9f5b1e-6a53-4c8e-9a0e-3b6c1f2d7a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every federation engine kind has a known sqlglot dialect, or is refused by name at build."""

# Requirements: REQ-1922

from __future__ import annotations

import pytest
from sqlglot.dialects.dialect import Dialect

from provisa.federation import engine as engine_mod
from provisa.federation.engine import _RDB_KINDS, build_engine


@pytest.fixture(autouse=True)
def _no_extra_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dialect is a property of the kind, not of which driver extra this install has."""
    monkeypatch.setattr(engine_mod, "require_engine_extra", lambda kind: None)


_RDB_DSN = {key: dsn for key, _label, dsn in _RDB_KINDS}

# kind -> sqlglot dialect its backend transpiles to.
_EXPECTED = {
    "mysql": "mysql",
    "mariadb": "mysql",
    "tidb": "mysql",
    "oracle": "oracle",
    "mssql": "tsql",
    "redshift": "redshift",
    "greenplum": "postgres",
    "cockroachdb": "postgres",
    "yugabytedb": "postgres",
    "singlestore": "singlestore",
    "exasol": "exasol",
    "opengauss": "postgres",
    "db2": "db2",  # provisa/db2/dialect.py
}

# kinds whose product has no sqlglot dialect: refused by name when the engine is built.
_REFUSED = {"saphana", "monetdb", "firebird", "vertica"}


def test_every_rdb_kind_is_classified() -> None:
    assert set(_RDB_DSN) == set(_EXPECTED) | _REFUSED


@pytest.mark.parametrize("kind", sorted(_EXPECTED))
def test_rdb_kind_dialect_is_known(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROVISA_ENGINE_URL", _RDB_DSN[kind])
    engine = build_engine(kind)
    assert engine.dialect == _EXPECTED[kind]
    assert engine.dialect in Dialect.classes


@pytest.mark.parametrize("kind", sorted(_REFUSED))
def test_rdb_kind_without_dialect_refused_by_name(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROVISA_ENGINE_URL", _RDB_DSN[kind])
    with pytest.raises(ValueError, match=f"'{kind}'.*no sqlglot dialect"):
        build_engine(kind)


@pytest.mark.parametrize("scheme", ["postgresql", "postgresql+psycopg2", "mysql+pymysql"])
def test_sqlalchemy_kind_dialect_follows_url_scheme(
    scheme: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROVISA_ENGINE_URL", f"{scheme}://u:p@h:1/db")
    assert build_engine("sqlalchemy").dialect in Dialect.classes


def test_sqlalchemy_kind_unknown_scheme_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROVISA_ENGINE_URL", "hana+hdbcli://u:p@h:1/db")
    with pytest.raises(ValueError, match="'sqlalchemy'.*no sqlglot dialect"):
        build_engine("sqlalchemy")
