# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2d6c41-3f5e-4b97-a1c0-7d9e2f4b6a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986: what each transport answers a parameter-binding refusal with, at the point it maps
an error.

HTTP surfaces answer the app's coded body (``code`` + ``params``): proven end to end in
``tests/integration/test_parameter_binding_e2e.py`` for SQL, and here for the handler every HTTP
surface shares. pgwire carries the code and params in the ErrorResponse's Detail field, as it
carries a warning's. Bolt and gRPC have no field for a product code: they answer the refusal as
the caller's mistake (never a server fault) with its message, which names the parameter.
"""

# Requirements: REQ-1986

from __future__ import annotations

import json

import pytest

from provisa.core.parameter_binding import (
    ParameterByEqualityOnly,
    ParameterCallsOverCap,
    ParameterConflict,
    ParameterUnbound,
)

_REFUSALS = [
    ParameterByEqualityOnly("_nf_petId", "<>"),
    ParameterUnbound("_nf_petId"),
    ParameterConflict("_nf_petId", 5, 6),
    ParameterCallsOverCap("pet_by_id", 3, 2),
]
_each = pytest.mark.parametrize("refusal", _REFUSALS, ids=lambda r: r.code)


@_each
def test_pgwire_sends_the_code_and_params_in_the_errors_detail_field(refusal):
    from provisa.pgwire import server

    sent: dict = {}

    class _Wire:
        def write(self, data: bytes) -> None:
            sent["bytes"] = data

        def flush(self) -> None:
            pass

    handler = object.__new__(server.ProvisaHandler)
    handler.wfile = _Wire()  # type: ignore[assignment]
    handler._send_pg_error(
        "ERROR", server._sqlstate_of(refusal), str(refusal), server._coded(refusal)
    )
    fields = {part[:1]: part[1:].decode() for part in sent["bytes"][5:].split(b"\x00") if part}
    assert json.loads(fields[b"D"]) == {"code": refusal.code, "params": refusal.params}
    assert fields[b"M"] == str(refusal)
    assert fields[b"C"] == "22023"  # invalid_parameter_value: the statement's, never XX000


def test_pgwire_sends_no_detail_for_an_error_that_has_no_code():
    from provisa.pgwire import server

    assert server._coded(ValueError("anything else")) is None


@_each
async def test_every_http_surface_answers_the_apps_coded_body(refusal):
    """The handler the app registers for the refusal: 400, its code and its params. /data/sql,
    /data/graphql, the REST endpoints, the gRPC proxy and explain let the refusal through to it."""
    from provisa.api.app import create_app

    app = create_app()
    handler = app.exception_handlers[type(refusal).__mro__[1]]
    response = await handler(None, refusal)  # type: ignore[arg-type]
    assert response.status_code == 400
    assert json.loads(response.body) == {
        "detail": str(refusal),
        "code": refusal.code,
        "params": refusal.params,
    }


@pytest.mark.parametrize(
    "module",
    [
        "provisa.api.data.endpoint",  # /data/graphql
        "provisa.api.data.endpoint_dev",  # /data/sql
        "provisa.api.data.endpoint_explain",
        "provisa.api.data.endpoint_grpc_proxy",
        "provisa.api.rest.generator",  # the generated REST endpoints
    ],
)
def test_an_http_surface_lets_the_refusal_through_to_the_apps_handler(module):
    """Each catches broad failures to answer them as its own 4xx/5xx; the binding refusal is
    named ahead of those, so it reaches the app's handler with its code."""
    import ast
    import importlib
    import inspect

    tree = ast.parse(inspect.getsource(importlib.import_module(module)))
    named = [
        handler
        for handler in ast.walk(tree)
        if isinstance(handler, ast.ExceptHandler)
        and handler.type is not None
        and "ParameterBindingRefused" in ast.unparse(handler.type)
    ]
    assert named, f"{module} does not name the refusal"
    # A tuple or union the surface re-raises or returns for its caller to raise: never swallowed.
    assert all(
        any(isinstance(node, (ast.Raise, ast.Return)) for node in ast.walk(handler))
        for handler in named
    )


@_each
def test_cypher_and_sparql_over_http_answer_the_refusals_own_code(refusal):
    from provisa.api.rest.cypher_router import cypher_error

    response = cypher_error(400, refusal.code, str(refusal), **refusal.params)
    body = json.loads(response.body)
    assert (body["code"], body["params"]) == (refusal.code, refusal.params)


@_each
def test_bolt_answers_a_statement_refusal_with_the_message(refusal):
    """Bolt's FAILURE holds a Neo4j status code and a message, and no other field."""
    from provisa.bolt.run_failure import STATEMENT_REFUSED, client_failure

    assert client_failure(refusal, None) == (STATEMENT_REFUSED, str(refusal))


@_each
def test_grpc_answers_invalid_argument(refusal):
    import grpc

    from provisa.grpc.server import _status_for_exception

    assert _status_for_exception(refusal) is grpc.StatusCode.INVALID_ARGUMENT
