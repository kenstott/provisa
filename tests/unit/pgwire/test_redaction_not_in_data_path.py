# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e1f6a-2b7c-4c1e-9a3f-7e4b8c2d1a90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REDACTION IS OF WHAT IS SHOWN, NEVER OF WHAT IS RUN OR ANSWERED (REQ-2087). A credential whose
value is an ordinary word -- here a source's password equal to a schema's name, as the conformance
measures' was (Conformance (functions) run 43926612814) -- is matched in the error text a client is
told, and nowhere else: the statement the engine runs, a bound value, a column's name and a row's
value all hold the word as the client wrote it. Each statement goes through a real pgwire server
whose engine is a real DuckDB runtime."""

# Requirements: REQ-2087
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.core.secrets import resolve_secrets
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.pgwire.server import ProvisaSession
from provisa.security import redaction
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.pgwire_describe_parity import RuntimeEngine, registered_parameters, registered_shape
from tests.role_state import with_surfaces
from tests.unit.pgwire.test_extended_protocol_portals import _engine_plan
from tests.unit.pgwire.test_type_conversions import ROLE, conn, pgwire  # noqa: F401

#: The word that is both a schema's name and a credential's value.
WORD = "conformance"
VARIABLE = "J1425_SOURCE_PASSWORD"
MARKER = f"[REDACTED:auto:env:{VARIABLE}]"


@pytest.fixture(autouse=True)
def _the_word_is_a_registered_credential(monkeypatch: pytest.MonkeyPatch):
    redaction.reset()
    monkeypatch.setenv(VARIABLE, WORD)
    assert resolve_secrets(f"${{env:{VARIABLE}}}", credential=True) == WORD
    assert redaction.redact(f"{WORD}.probe") == f"{MARKER}.probe"
    yield
    redaction.reset()


#: The table the statements read: a row's value and a column's name are the word.
_REGISTRY = {"m": [("id", "integer"), ("label", "text"), (WORD, "integer")]}


@pytest.fixture
def stored(pgwire: int):
    """A connection to the server over a DuckDB runtime holding ``m``, bound values carried to
    the engine as test_extended_protocol_live.py's fixture carries them."""
    from provisa.pgwire._pipeline import _Described

    runtime = DuckDBFederationRuntime()
    runtime._con.execute(f"CREATE TABLE m (id INTEGER, label VARCHAR, {WORD} INTEGER)")
    runtime._con.execute(f"INSERT INTO m VALUES (1, 'one', 10), (2, '{WORD}.probe', 20)")
    ctx = MagicMock()
    ctx.tables = {}
    state = with_surfaces(MagicMock())
    state.contexts = {ROLE: ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = RuntimeEngine(runtime, "duckdb")

    def _plan_of(sql: str, params: list | None) -> Any:
        plan = _engine_plan(transpile_to_duckdb(sql))
        plan.exec_params = params or []
        return plan

    async def _govern(text, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        return _plan_of(text, params)

    async def _describe(text, role_id, *, as_of=None):
        return _Described(
            registered_shape(text, _REGISTRY),
            ("governed", text),  # type: ignore[arg-type]
            registered_parameters(text, _REGISTRY),
        )

    async def _plan(held, params, wire_formats=None, *, deliver):
        return _plan_of(held[1], params)

    async def _no_cache(plan, st):
        return None

    async def _resident(st, plan):
        return None

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.describe_pgwire_statement", _describe),
        patch("provisa.pgwire._pipeline.plan_pgwire_statement", _plan),
        patch(
            "provisa.pgwire._pipeline.governed_statement_is_current",
            lambda held, st, as_of=None: True,
        ),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
        patch("provisa.federation.query_residency.prepare_engine_residency", _resident),
        patch.object(ProvisaSession, "_finalize_audit", lambda self, governed, status, **kw: None),
    ):
        c = psycopg.connect(
            host="127.0.0.1",
            port=pgwire,
            user=ROLE,
            password="x",
            dbname="provisa",
            autocommit=True,
        )
        try:
            yield c.pgconn
        finally:
            c.close()


def _answered(pgconn: Any, sql: str, params: list[bytes] | None = None) -> Any:
    res = pgconn.exec_params(sql.encode(), params or [], result_format=0)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.ntuples == 1, res.ntuples
    return res


def test_a_statements_literal_reaches_the_engine_as_written(stored: Any):
    # The engine finds the row by the literal it was sent; the marker's text matches no row.
    res = _answered(stored, f"SELECT id FROM m WHERE label = '{WORD}.probe'")
    assert res.get_value(0, 0) == b"2"


def test_a_bound_value_reaches_the_engine_as_sent(stored: Any):
    res = _answered(stored, "SELECT id FROM m WHERE label = $1", [f"{WORD}.probe".encode()])
    assert res.get_value(0, 0) == b"2"


def test_a_rows_value_is_answered_as_the_engine_gave_it(stored: Any):
    res = _answered(stored, "SELECT label FROM m WHERE id = 2")
    assert res.get_value(0, 0) == f"{WORD}.probe".encode()


def test_an_identifier_is_run_and_answered_as_written(stored: Any):
    res = _answered(stored, f"SELECT {WORD} FROM m WHERE id = 2")
    assert res.fname(0) == WORD.encode()
    assert res.get_value(0, 0) == b"20"


def test_an_errors_text_is_redacted_and_its_code_is_not(conn: Any):
    res = conn.conn.pgconn.exec_params(
        f"SELECT '{WORD}.no_such_relation'::regclass::oid AS v".encode(), [], result_format=0
    )
    assert res.status == pq.ExecStatus.FATAL_ERROR
    message = (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode()
    # The relation was looked up by the name written: the refusal is of that name, and only the
    # text shown is redacted.
    assert message == f'relation "{MARKER}.no_such_relation" does not exist'
    assert res.error_field(pq.DiagnosticField.SQLSTATE) == b"42P01"
