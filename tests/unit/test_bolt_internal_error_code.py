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
from types import SimpleNamespace
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


# --- Each deliberate refusal over Bolt answers as Cypher over HTTP does: its own words, a client
# code. Only what HTTP lets fall through to its catch-all 500 is the internal error.

_FORBIDDEN = "Neo.ClientError.Security.Forbidden"
_NOT_FOUND = "Neo.ClientError.Procedure.ProcedureNotFound"
_ARGUMENT = "Neo.ClientError.Statement.ArgumentError"


def _refusals() -> list[tuple[str, BaseException, str]]:
    """(id, the refusal, the code it is answered with), one per class."""
    from provisa.compiler.sql_validator import GovernanceRefused, ValidationViolation
    from provisa.core.parameters import ParameterRequired
    from provisa.cypher.command_call import CommandCallRefused
    from provisa.cypher.expr_parser import CypherExprParseError
    from provisa.cypher.params import CypherParamError
    from provisa.cypher.parser import CypherParseMistake
    from provisa.cypher.write_translator import CypherWriteParseError
    from provisa.executor.errors import FederationError
    from provisa.federation.replica_state import ReplicaBuilding
    from provisa.gql.assembler import CypherAssemblyError
    from provisa.gql.path_translator import PathTranslateError
    from provisa.gql.translator_types import (
        CypherCrossSourceError,
        CypherMistake,
        CypherTranslateError,
        PathDepthExceeded,
    )
    from provisa.security.mutation_authz import CommandNotFound
    from provisa.security.rights import Capability, InsufficientRightsError, UnknownRoleError

    return [
        ("parse", CypherParseError("Unexpected token 'RETRUN'"), _SYNTAX),
        (
            "parse-mistake",
            CypherParseMistake("cypher.unknown_function", "There is no function foo()"),
            _SYNTAX,
        ),
        ("expr-parse", CypherExprParseError("cannot read the expression 1 +"), _SYNTAX),
        ("param", CypherParamError("Missing parameter $name"), _SYNTAX),
        ("write-parse", CypherWriteParseError("SET needs a property"), _SYNTAX),
        ("translate", CypherTranslateError("OPTIONAL MATCH of a path is not supported"), _SYNTAX),
        (
            "mistake",
            CypherMistake("cypher.unknown_label", "There is no label :Note", label="Note"),
            _SYNTAX,
        ),
        ("cross-source", CypherCrossSourceError("the two sources cannot be joined"), _SYNTAX),
        ("path", PathTranslateError("a path needs a relationship"), _SYNTAX),
        ("path-depth", PathDepthExceeded(10), _SYNTAX),
        ("assembly", CypherAssemblyError("Malformed JSON in column 'n'"), _SYNTAX),
        ("parameter-required", ParameterRequired("orders", "region"), _SYNTAX),
        ("replica-building", ReplicaBuilding("sales_pg", 12.0), _SYNTAX),
        (
            "engine",
            FederationError("USER_ERROR", "DIVISION_BY_ZERO", "Division by zero"),
            _SYNTAX,
        ),
        (
            "validator",
            GovernanceRefused([ValidationViolation("V001", "column 'ssn' is not visible")]),
            _FORBIDDEN,
        ),
        ("rights", InsufficientRightsError("analyst", Capability.QUERY_DEVELOPMENT), _FORBIDDEN),
        ("unknown-role", UnknownRoleError("ghost"), _FORBIDDEN),
        ("command-not-found", CommandNotFound("reindex"), _NOT_FOUND),
        ("command-call", CommandCallRefused("reindex takes no argument 'x'"), _ARGUMENT),
    ]


@pytest.mark.parametrize(
    ("refusal", "code"),
    [pytest.param(exc, code, id=name) for name, exc, code in _refusals()],
)
async def test_a_refusal_is_answered_in_its_own_words_with_a_client_code(refusal, code, caplog):
    session, writer = _session()
    with caplog.at_level(logging.DEBUG):
        await _run(session, refusal)
    [failure] = _failures(await _said(writer))
    assert failure["code"] == code
    assert failure["message"] == str(refusal)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


async def test_reading_a_label_nothing_declares_is_refused_by_name():
    from provisa.gql.mistakes import unknown_label

    session, writer = _session()
    refusal = unknown_label("Note", SimpleNamespace(nodes={}, nodes_by_table={}, domains={}))
    await _run(session, refusal, "MATCH (n:Note) RETURN n")
    [failure] = _failures(await _said(writer))
    assert failure["code"] == _SYNTAX
    assert failure["message"].startswith("There is no label :Note")


class _DriverError(Exception):
    """An engine driver's own error, told apart only by the engine (REQ-778)."""


def _engine(kind: str | None, refusal: BaseException | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        classify_error=lambda exc: kind if isinstance(exc, _DriverError) else None,
        refusal_of=lambda exc: refusal if isinstance(exc, _DriverError) else None,
    )


async def _run_on(engine: SimpleNamespace, raises: BaseException) -> list[dict]:
    from provisa.api.app import state

    session, writer = _session()
    with patch.object(type(state), "federation_engine", engine):
        await _run(session, raises)
    return _failures(await _said(writer))


@pytest.mark.parametrize("kind", ["query", "connection"])
async def test_an_engine_error_keeps_the_engines_message(kind):
    [failure] = await _run_on(_engine(kind), _DriverError("Conversion Error: division by zero"))
    assert failure["code"] == _SYNTAX
    assert failure["message"] == "Conversion Error: division by zero"


async def test_an_engine_error_the_engine_names_is_said_by_its_name():
    from provisa.core.read_refusal import SourceUnavailable

    refusal = SourceUnavailable("sales_pg")
    [failure] = await _run_on(_engine("query", refusal), _DriverError("no such function"))
    assert failure["code"] == _SYNTAX
    assert failure["message"] == str(refusal)


async def test_an_error_the_engine_does_not_know_is_the_internal_error(caplog):
    with caplog.at_level(logging.DEBUG):
        [failure] = await _run_on(_engine(None), RuntimeError("cursor closed at /srv/x.py"))
    assert failure["code"] == _UNKNOWN
    assert failure["message"] == "An internal error occurred; see the server log."
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None
