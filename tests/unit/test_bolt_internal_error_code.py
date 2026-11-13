# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0a0c52-6f0e-4a55-9a3e-2f1d3c7b8e41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An internal fault in a Bolt RUN is an internal error, not the client's syntax mistake: a
statement Provisa refuses on purpose keeps its client code; any other exception is sent as
Neo.DatabaseError.General.UnknownError with no internals in the message, logged once at error
level with its traceback. The session stays usable after either, as RESET restores it."""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

import provisa.bolt.messages as msg
from provisa.cypher.parser import CypherParseError
from tests.unit.test_bolt_tx_results_by_qid import _said, _session

pytestmark = pytest.mark.asyncio

_UNKNOWN = "Neo.DatabaseError.General.UnknownError"
_SYNTAX = "Neo.ClientError.Statement.SyntaxError"


async def _run(session, raises: BaseException | None, cypher: str = "RETURN 1") -> None:
    async def execute(*_args, **_kwargs):
        if raises is not None:
            raise raises
        return ["n"], [[1]], None

    with (
        patch.object(session, "_resolve_db", return_value=("analyst", False)),
        patch("provisa.bolt.session._execute_cypher", execute),
    ):
        await session.handle_run([cypher, {}, {}])


def _failures(said) -> list[dict]:
    return [f[0] for tag, f in said if tag == msg.FAILURE]


async def test_a_statement_the_client_got_wrong_keeps_its_client_code():
    session, writer = _session()
    await _run(session, ValueError("Syntax error at 'RETRUN'"))
    [failure] = _failures(await _said(writer))
    assert failure["code"] == _SYNTAX
    assert failure["message"] == "Syntax error at 'RETRUN'"


async def test_a_parse_error_the_translator_raises_keeps_its_client_code():
    session, writer = _session()
    await _run(session, ValueError(str(CypherParseError("unexpected token"))))
    [failure] = _failures(await _said(writer))
    assert failure["code"] == _SYNTAX


async def test_a_rights_refusal_keeps_its_forbidden_code():
    session, writer = _session()
    await _run(session, PermissionError("not allowed"))
    [failure] = _failures(await _said(writer))
    assert failure["code"] == "Neo.ClientError.Security.Forbidden"


async def test_a_python_crash_is_an_internal_error_with_no_internals(caplog):
    session, writer = _session()
    with caplog.at_level(logging.DEBUG):
        await _run(session, RuntimeError("secret table prod.users at /srv/x.py:12"))
    [failure] = _failures(await _said(writer))
    assert failure["code"] == _UNKNOWN
    assert "secret" not in failure["message"] and "/srv" not in failure["message"]
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is not None
    assert errors[0].exc_info[0] is RuntimeError
    assert not [r for r in caplog.records if r.levelno == logging.WARNING and "RUN" in r.message]


async def test_a_client_error_is_not_logged_at_error_level(caplog):
    session, _writer = _session()
    with caplog.at_level(logging.DEBUG):
        await _run(session, ValueError("bad"))
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.parametrize("raises", [ValueError("bad"), RuntimeError("crash")])
async def test_the_session_is_usable_after_either_failure_once_reset(raises):
    session, writer = _session()
    await _run(session, raises)
    session.handle_reset()
    await _run(session, None)
    session.handle_pull([{"n": -1}])
    said = await _said(writer)
    assert len(_failures(said)) == 1
    assert [f[0] for tag, f in said if tag == msg.RECORD] == [[1]]


async def test_a_read_refused_by_name_keeps_its_client_code_and_words():
    from provisa.core.read_refusal import SourceUnavailable

    session, writer = _session()
    refusal = SourceUnavailable("sales_pg")
    await _run(session, refusal)
    [failure] = _failures(await _said(writer))
    assert failure["code"] == _SYNTAX
    assert failure["message"] == str(refusal)
