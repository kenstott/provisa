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

The call's table arguments are given what the code needs to read them: the minted-token listener's
address, a token scoped to this call and bound to the calling role, and the statement that reads
the declared columns. A local worker reaches the listener on loopback (its sandbox allows that one
port); a remote host, such as a Spark cluster, is given the deployment's host name. The token ends
with the call."""

# Requirements: REQ-2226
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from provisa.otel_compat import MintedSession

_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1"})
_DATABASE = "provisa"


class TableNotReadable(ValueError):
    """A table argument the call cannot give its code, said naming why."""

    code = "query.code_command_table_not_readable"

    def __init__(self, reason: str) -> None:
        super().__init__(f"a code command cannot read its table: {reason}")
        self.params = {"reason": reason}


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
    host = settings_registry.value("server.hostname")
    if host in _LOOPBACK:
        raise TableNotReadable(
            f"a remote host cannot reach this server at {host!r}; set server.hostname"
        )
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
    state: Any, session: MintedSession, payload: dict, *, remote: bool
) -> AsyncIterator[None]:
    """For the block, ``payload``'s table references carry what their code reads them with."""
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
    host, port, sslmode = _address(state, remote=remote)
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
