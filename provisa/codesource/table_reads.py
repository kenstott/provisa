# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2f5c17-e4a9-4b36-92c0-6a1e7bf30d58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What makes a table argument readable for one call of a code source's function (REQ-2226).

The call's table arguments are given what the code needs to read them: an address, a token scoped
to this call and bound to the calling role, and the statement that reads the declared columns. The
token ends with the call.

Where the table is read (REQ-2270):

- A local worker reads it over the minted-token pgwire listener, on loopback: its sandbox allows
  that one port of Provisa's and no other, so it never reaches the Arrow Flight server, which on a
  deployment that authenticates nobody would serve it as any role.
- A remote host running Python, such as the Spark connector's, reads it over Arrow Flight SQL at
  the deployment's host name: the token is its bearer credential and the call's id its
  ``x-provisa-call`` header, and the Flight server admits it as the calling role and no other,
  through the same governed pipeline and audit (provisa/api/flight/server.py). Python's host always
  has pyarrow, whose Flight client reads it (``TableRef.read``).
- A remote host in another language (Go, Rust, TypeScript) keeps the pgwire listener: their SDKs
  read a table with a PostgreSQL client and carry no Flight client."""

# Requirements: REQ-2226, REQ-2270
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from provisa.otel_compat import MintedSession

_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1"})
_DATABASE = "provisa"
#: The languages whose remote hosts read a table over Arrow Flight SQL (REQ-2270): those whose host
#: carries a Flight client.
FLIGHT_LANGUAGES = frozenset({"python"})


class TableNotReadable(ValueError):
    """A table argument the call cannot give its code, said naming why."""

    code = "query.code_command_table_not_readable"

    def __init__(self, reason: str) -> None:
        super().__init__(f"a code command cannot read its table: {reason}")
        self.params = {"reason": reason}


def _remote_host(*, listener: str) -> str:
    """The deployment's host name, as a remote host reaches ``listener``; refused when it names
    this machine's loopback, which a remote host cannot reach."""
    from provisa.core import settings_registry

    host = settings_registry.value("server.hostname")
    if host in _LOOPBACK:
        raise TableNotReadable(
            f"a remote host cannot reach the {listener} at {host!r}; set server.hostname"
        )
    return host


def _flight_address(state: Any) -> str:
    """REQ-2270: the Arrow Flight server's location, as a remote host reaches it. Only over TLS:
    the token is a bearer credential, and never crosses the network in the clear."""
    from provisa.core import settings_registry

    server = state._flight_server
    if server is None:
        raise TableNotReadable("the Arrow Flight server is not running (server.flight_port)")
    host = _remote_host(listener="Arrow Flight server")
    if not server.tls:
        raise TableNotReadable(
            "a remote host's token would cross the network in the clear; "
            "give the Arrow Flight server a certificate (PROVISA_FLIGHT_CERT, PROVISA_FLIGHT_KEY)"
        )
    return f"grpc+tls://{host}:{settings_registry.value('server.flight_port')}"


def _address(state: Any, *, remote: bool) -> tuple[str, int, str]:
    """The listener's host and port as this worker reaches it, and the TLS it must use."""
    from provisa.core import settings_registry

    port = settings_registry.value("server.minted_pgwire_port")
    server = state._minted_pgwire_server
    if not port or server is None:
        raise TableNotReadable(
            "the minted-token listener is not running (server.minted_pgwire_port)"
        )
    if not remote:
        return "127.0.0.1", port, "disable"  # its own loopback: nothing crosses a network
    host = _remote_host(listener="minted-token listener")
    if server.ssl_ctx is None:
        raise TableNotReadable(
            "a remote host's token would cross the network in the clear; "
            "give the listener a certificate (tls.pgwire_cert, tls.pgwire_key)"
        )
    return host, port, "require"


def _caller(correlation_id: str) -> str:
    """Who the audit rows of the session name: the caller, or (a call no person made, such as a
    scheduled build) the call itself."""
    from provisa.audit.context import current_audit_identity

    identity = current_audit_identity()
    return identity.user_id if identity is not None else f"call:{correlation_id}"


@asynccontextmanager
async def readable(
    state: Any, session: MintedSession, payload: dict, *, remote: dict | None
) -> AsyncIterator[None]:
    """For the block, ``payload``'s table references carry what their code reads them with: over
    Arrow Flight SQL for a remote host (``remote``, its ``language`` among them; None: a local
    worker) of a :data:`FLIGHT_LANGUAGES` language, else over the minted-token pgwire listener
    (see the module's docstring)."""
    from provisa.core.request_context import current_org, current_session_vars
    from provisa.entitlements.runtime import captured_entitlements
    from provisa.pgwire.minted import MintedCall, MintedTokenStore, read_statement, table_key
    from provisa.security.meta_role import acting_roles

    references = [
        v for v in payload.values() if isinstance(v, dict) and v.get("kind") == "table_ref"
    ]
    if not references:
        yield
        return
    role = session.role_id
    if role is None:
        raise TableNotReadable("the call has no role to read as")
    over_flight = remote is not None and remote["language"] in FLIGHT_LANGUAGES
    connect = _flight_address(state) if over_flight else _address(state, remote=remote is not None)
    tables = tuple(str(r["ref"]) for r in references)
    for table in tables:
        table_key(table)  # a reference that is not a table name is refused here, by name
    call = MintedCall(
        call_id=session.correlation_id,
        role_id=role,
        members=acting_roles(state, role),
        qualifiers=tuple(state.meta_qualifiers.get(role, ())),
        user_id=_caller(session.correlation_id),
        org_id=current_org.get() or state.org_id,
        tables=tables,
        session_vars=dict(current_session_vars.get()),
        entitlements=captured_entitlements(),
    )
    store = MintedTokenStore(state.admin_db)
    token = await store.mint(call)
    try:
        for reference in references:
            if isinstance(connect, str):
                reference["flight"] = {"uri": connect, "token": token, "call": call.call_id}
            else:
                host, port, sslmode = connect
                reference["connection"] = {
                    "host": host,
                    "port": port,
                    "user": call.call_id,
                    "password": token,
                    "database": _DATABASE,
                    "sslmode": sslmode,
                }
            reference["sql"] = read_statement(str(reference["ref"]), reference.get("columns", []))
        yield
    finally:
        await store.retire(token)
