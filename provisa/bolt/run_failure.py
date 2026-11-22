# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3f6a2e-4c1b-4e8f-b7a0-2e5c8d1f6b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a failed Bolt RUN tells its client: the Neo4j code and message of a statement Provisa
refused on purpose, or None for a fault inside Provisa (said by the session as the internal error,
with no internals, and logged there).

It follows Cypher over HTTP exception for exception (``provisa.api.rest.cypher_router`` and the
app's exception handlers in ``provisa.api.app``): what HTTP answers as a 4xx, or as a 5xx carrying
a message it means the caller to read (an engine's own error, a replica still being built), is a
client failure here in the same words; only what HTTP lets fall through to its catch-all 500 is
None. HTTP has no one classification to call -- its answers are spread over the route's own
``except`` clauses and handlers that need a request -- so the classes are listed here, as a table.
"""

from __future__ import annotations

import functools
from typing import Any

FORBIDDEN = "Neo.ClientError.Security.Forbidden"
PROCEDURE_NOT_FOUND = "Neo.ClientError.Procedure.ProcedureNotFound"
ARGUMENT_ERROR = "Neo.ClientError.Statement.ArgumentError"
# A statement refused on purpose, whatever refused it: Bolt's one code for the statement the
# client sent and cannot have run as it is.
STATEMENT_REFUSED = "Neo.ClientError.Statement.SyntaxError"

# An HTTP status the shared command executor answers with, as the Neo4j error a Bolt client reads.
_BOLT_CODE_FOR_STATUS = {403: FORBIDDEN, 404: PROCEDURE_NOT_FOUND}


@functools.cache
def _table() -> tuple[tuple[tuple[type[BaseException], ...], str], ...]:
    """(classes, code), the first a failure is an instance of deciding its code. Imported on
    first use: the classes live across the layers a RUN passes through."""
    from provisa.core.host_code import HostCodeRefused
    from provisa.core.read_refusal import ReadRefused
    from provisa.core.sandbox_limits import SandboxDiskFull
    from provisa.core.secrets_providers import SecretNameRefused
    from provisa.cypher.command_call import CommandCallRefused
    from provisa.cypher.expr_parser import CypherExprParseError
    from provisa.cypher.params import CypherParamError
    from provisa.cypher.parser import CypherParseError
    from provisa.cypher.write_translator import CypherWriteParseError
    from provisa.executor.errors import FederationError
    from provisa.federation.duckdb_runtime import WriteConflict
    from provisa.federation.pgwire_replica import SourceStillStartingError
    from provisa.federation.replica_state import ReplicaBuilding
    from provisa.gql.assembler import CypherAssemblyError
    from provisa.gql.path_translator import PathTranslateError
    from provisa.gql.translator_types import CypherTranslateError, PathDepthExceeded
    from provisa.security.mutation_authz import CommandNotFound
    from provisa.security.rights import InsufficientRightsError, UnknownRoleError

    return (
        # Rights: every PermissionError (governance's refusal of a read, the SQL validator's
        # GovernanceRefused, a domain capability, the operator's floor) is HTTP's 403, as is a
        # host-code refusal (a ValueError, so ahead of the statement refusals below).
        ((PermissionError, HostCodeRefused, InsufficientRightsError, UnknownRoleError), FORBIDDEN),
        # A command the role may not use is "not found", as HTTP's 404 says.
        ((CommandNotFound,), PROCEDURE_NOT_FOUND),
        ((CommandCallRefused,), ARGUMENT_ERROR),
        (
            (
                # The statement itself: its parse, its parameters, its translation (a CypherMistake
                # is a CypherTranslateError), a path, the rows it answers. HTTP's 400s.
                CypherParseError,
                CypherExprParseError,
                CypherParamError,
                CypherWriteParseError,
                CypherTranslateError,
                PathTranslateError,
                PathDepthExceeded,
                CypherAssemblyError,
                # Every ValueError a refusal raises (each of the app's 400 and 422 handlers' classes
                # is one, as is a ParameterRequired).
                ValueError,
                # A refusal by name: what the deployment cannot answer now (HTTP's 503), a replica
                # still being built (504), a source still starting (503), a write that met
                # another (409), a sandbox out of disk (507), a secret's name (400).
                ReadRefused,
                ReplicaBuilding,
                SourceStillStartingError,
                WriteConflict,
                SandboxDiskFull,
                SecretNameRefused,
                # The engine's own error for the statement (division by zero, a value it cannot
                # convert): its words are the client's to read.
                FederationError,
            ),
            STATEMENT_REFUSED,
        ),
    )


def _message(exc: BaseException) -> str:
    """The words HTTP answers ``exc`` with."""
    from provisa.executor.errors import SystemCatalogUnavailable

    if isinstance(exc, SystemCatalogUnavailable):
        return f"the engine's {exc.catalog!r} catalog is being registered; try again shortly"
    return str(exc)


def client_failure(exc: BaseException, engine: Any) -> tuple[str, str] | None:
    """The (code, message) a Bolt client is answered with for ``exc``, or None when ``exc`` is
    a fault inside Provisa. ``engine`` is the federation engine the statement ran on: a driver's
    own error is told apart by it, as HTTP tells it apart (``refusal_of``, ``classify_error``)."""
    from starlette.exceptions import HTTPException

    from provisa.bolt.databases import DatabaseCommandError

    if isinstance(exc, HTTPException):
        # The shared executor's answer (a command call), and the app's coded errors (ApiError).
        return _BOLT_CODE_FOR_STATUS.get(exc.status_code, ARGUMENT_ERROR), str(exc.detail)
    if isinstance(exc, DatabaseCommandError):
        return exc.code, str(exc)
    for classes, code in _table():
        if isinstance(exc, classes):
            return code, _message(exc)
    if not isinstance(exc, Exception):
        return None
    refused = engine.refusal_of(exc)
    if refused is not None:
        # What the engine cannot answer because of what it is, said by name (REQ-778).
        return STATEMENT_REFUSED, str(refused)
    if engine.classify_error(exc) is not None:
        # The engine refused the statement, or could not be reached for it: HTTP's 400 and
        # 503, each in the engine's own words.
        return STATEMENT_REFUSED, str(exc)
    return None
