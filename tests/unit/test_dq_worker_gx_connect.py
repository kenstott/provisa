# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7c2a-91d4-4f3e-8a6b-2c7d1e9f4a30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests: the great_expectations checker can open a connection to Provisa's pg-wire endpoint.

GX connects through a SQLAlchemy ``postgresql+psycopg2`` engine. By default that dialect's connect
hook probes ``pg_type.typarray`` for hstore OIDs, and Provisa refuses that catalog column
(``[V003] Column 'typarray' is not visible to this role``) — so every GX dry run and scan died in
``test_connection`` before a single expectation ran (UI e2e core lane run 42233953096).

great_expectations is an optional extra the unit lane does not install, so a stand-in module
records what ``run_gx`` hands ``add_postgres`` and connects the way GX 1.20's
``SQLDatasource.test_connection`` does: ``create_engine(connection_string, **kwargs)`` then
``engine.connect()``. SQLAlchemy and psycopg2 are the real ones; only psycopg2's socket connection
is replaced, by an endpoint that answers like Provisa.
"""

# Requirements: REQ-1443, REQ-1742

from __future__ import annotations

import sys
import types
from unittest import mock

import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.extras
import pytest
import sqlalchemy

from provisa.dq import worker


class _PastHandshake(Exception):
    """The engine's connect hooks all passed and it sent its first ordinary query."""


class _ProvisaCursor:
    def __init__(self, queries: list[str]) -> None:
        self._queries = queries

    def execute(self, sql: str, *args, **kwargs) -> None:
        self._queries.append(sql)
        if "typarray" in sql:
            raise psycopg2.errors.InsufficientPrivilege(
                "[V003] Column 'typarray' is not visible to this role"
            )
        raise _PastHandshake(sql)

    def close(self) -> None:
        pass


def _provisa_connection(queries: list[str]) -> mock.MagicMock:
    connection = mock.MagicMock(spec=psycopg2.extensions.connection)
    connection.info.server_version = 160000
    connection.server_version = 160000
    connection.cursor.side_effect = lambda *a, **k: _ProvisaCursor(queries)
    return connection


def _stand_in_gx(add_postgres_calls: list[dict]) -> dict[str, types.ModuleType]:
    def add_postgres(*, name: str, connection_string: str, **fields):
        add_postgres_calls.append({"name": name, "connection_string": connection_string, **fields})
        engine = sqlalchemy.create_engine(connection_string, **fields.get("kwargs", {}))
        with engine.connect():
            pass
        raise AssertionError("the fake endpoint never completes a connection")

    context = types.SimpleNamespace(data_sources=types.SimpleNamespace(add_postgres=add_postgres))
    gx = types.ModuleType("great_expectations")
    gx.get_context = lambda mode: context  # type: ignore[attr-defined]
    core = types.ModuleType("great_expectations.core")
    core.ExpectationSuite = object  # type: ignore[attr-defined]
    validation_definition = types.ModuleType("great_expectations.core.validation_definition")
    validation_definition.ValidationDefinition = object  # type: ignore[attr-defined]
    return {
        "great_expectations": gx,
        "great_expectations.core": core,
        "great_expectations.core.validation_definition": validation_definition,
    }


_PAYLOAD = {
    "contract_text": "name: orders\nmeta: {dataset: provisa/sales/orders}\nexpectations: []\n",
    "connection": {
        "user": "dq",
        "password": "secret",
        "host": "provisa",
        "port": 5432,
        "database": "provisa",
    },
    "data_source_name": "provisa",
    "sampler_limit": 5,
}


def test_gx_connects_without_the_hstore_catalog_probe_provisa_refuses(monkeypatch):
    queries: list[str] = []
    add_postgres_calls: list[dict] = []
    for module_name, module in _stand_in_gx(add_postgres_calls).items():
        monkeypatch.setitem(sys.modules, module_name, module)
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: _provisa_connection(queries))
    # register_uuid installs a C-level typecaster that type-checks for a real libpq connection; it
    # issues no SQL, so it has no bearing on what the endpoint is asked.
    monkeypatch.setattr(psycopg2.extras, "register_uuid", lambda *a, **k: None)

    with pytest.raises(_PastHandshake):
        worker.run_gx(_PAYLOAD)

    assert len(add_postgres_calls) == 1
    assert not [q for q in queries if "typarray" in q]


def test_gx_hands_the_driver_credentials_exactly_as_configured(monkeypatch):
    # @ / : end the user/password part of a URL and a space would be read back as + by a
    # form-style escape: each must reach the driver exactly as the connection states it.
    user, password = "dq@org:1", "p@ss/w:rd +%x"
    payload = {
        **_PAYLOAD,
        "connection": {**_PAYLOAD["connection"], "user": user, "password": password},
    }
    for module_name, module in _stand_in_gx([]).items():
        monkeypatch.setitem(sys.modules, module_name, module)
    driver_kwargs: list[dict] = []

    def connect(*args, **kwargs):
        driver_kwargs.append(kwargs)
        raise _PastHandshake("the driver was called")

    monkeypatch.setattr(psycopg2, "connect", connect)

    with pytest.raises(_PastHandshake):
        worker.run_gx(payload)

    assert len(driver_kwargs) == 1
    sent = driver_kwargs[0]
    assert (sent["user"], sent["password"]) == (user, password)
    assert (sent["host"], sent["port"], sent["dbname"]) == ("provisa", 5432, "provisa")
