# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3c1e58-2b94-4d6f-9e01-c58d2f7b4a36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An error PostgreSQL raised reaches the client with PostgreSQL's SQLSTATE, whichever driver
carried it: psycopg2 (the pg engine's, provisa.federation.pg_runtime) names it ``pgcode`` and
repeats it on ``diag.sqlstate``; psycopg 3 and asyncpg name it ``sqlstate``. Each read as
XX000 before (polyfill triage 2026-11-22, G1)."""

# Requirements: REQ-2167

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.pg_error import driver_sqlstate, postgres_error
from provisa.pgwire.server import _sqlstate_of


class _Psycopg2Error(Exception):
    """psycopg2's error shape: ``pgcode`` and ``diag``, no ``sqlstate``."""

    def __init__(self, message: str, pgcode: str | None, diag_sqlstate: str | None) -> None:
        super().__init__(message)
        self.pgcode = pgcode
        self.diag = SimpleNamespace(sqlstate=diag_sqlstate, message_primary=message)


class _SqlstateError(Exception):
    """psycopg 3's and asyncpg's error shape: ``sqlstate`` on the error itself."""

    def __init__(self, message: str, sqlstate: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


_SHAPES = {
    "psycopg2 pgcode": lambda: _Psycopg2Error(
        "argument must be empty or one-dimensional array", "22000", None
    ),
    "psycopg2 diag.sqlstate": lambda: _Psycopg2Error(
        "argument must be empty or one-dimensional array", None, "22000"
    ),
    "psycopg3/asyncpg sqlstate": lambda: _SqlstateError(
        "argument must be empty or one-dimensional array", "22000"
    ),
}


@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_each_driver_s_code_is_read(shape):
    error = _SHAPES[shape]()
    assert driver_sqlstate(error) == "22000"
    assert _sqlstate_of(error) == "22000"
    answered = postgres_error(error)
    assert answered is not None
    assert (answered.sqlstate, str(answered)) == (
        "22000",
        "argument must be empty or one-dimensional array",
    )


@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_the_code_is_read_through_the_chain_that_carried_it(shape):
    try:
        try:
            raise _SHAPES[shape]()
        except Exception as cause:
            raise RuntimeError("statement failed") from cause
    except RuntimeError as wrapped:
        assert _sqlstate_of(wrapped) == "22000"


def test_an_error_with_no_driver_code_is_none():
    assert driver_sqlstate(ValueError("no code")) is None
    assert driver_sqlstate(_Psycopg2Error("no code", None, None)) is None
    assert _sqlstate_of(_Psycopg2Error("no code", None, None)) == "XX000"
