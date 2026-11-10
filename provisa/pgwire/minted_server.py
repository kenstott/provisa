# Copyright (c) 2026 Kenneth Stott
# Canary: b14e7a2c-5d06-49f3-a8c1-2e6f0d93b741
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pgwire listener for minted tokens (REQ-2226, A LISTENER FOR MINTED TOKENS).

A sandboxed worker may reach this one port and a remote host is given its address; nothing else
of Provisa is reachable from a sandbox. So it accepts a minted token and nothing else: no password,
no personal access token, and no trust mode, under which the main listener admits whoever the
startup packet names. The token, not the connection, says who the session is."""

# Requirements: REQ-2226
from __future__ import annotations

import ssl
import threading
from typing import Any, cast

from provisa.pgwire.minted import CallScope, CallTokenRefused, MintedTokenStore, hash_token
from provisa.pgwire.server import ProvisaConnection, ProvisaHandler, ProvisaServer, log


class MintedTokenHandler(ProvisaHandler):
    """A connection that authenticates only by a token minted for one call."""

    def _trust_mode(self) -> bool:
        return False  # whatever the deployment's auth is: a role is never named here

    def _scram_offered(self) -> bool:
        return False  # the token is an opaque secret, which SCRAM cannot carry

    def handle_md5_password(self, ctx: Any, payload: bytes) -> None:
        from provisa.core.connection_loop import run_on_connection_loop

        token = payload.decode("utf-8").rstrip("\x00")
        call_id = ctx.params.get("user", "")
        state = self._app_state()
        store = MintedTokenStore(state.admin_db)
        call = run_on_connection_loop(store.resolve(token, call_id), timeout=60)
        if call is None:
            # One answer for a wrong, an expired and another call's token.
            self._send_pg_error("FATAL", "28P01", f'minted token refused for user "{call_id}"')
            return
        session = ctx.session
        session.call_scope = CallScope(call, hash_token(token), store, state)
        session.act_as(call.role_id, call.members, call.qualifiers)
        session.user_id = call.user_id
        session.org_id = call.org_id
        session.system_user = f"minted:{call.call_id}"
        self._meter.bind_org(call.org_id)
        self.send_authentication_ok()
        self.handle_post_auth(ctx)

    def _process_query_stmts(self, ctx: Any, stmts: list[str]) -> None:
        try:
            cast(CallScope, ctx.session.call_scope).check_all(stmts)
        except CallTokenRefused as exc:
            self._send_pg_error("ERROR", "42501", str(exc))
            ctx.mark_error()
            self.send_ready_for_query(ctx)
            return
        super()._process_query_stmts(ctx, stmts)


def start_minted_pgwire_server(
    host: str, port: int, ssl_ctx: ssl.SSLContext | None
) -> ProvisaServer:
    """Start the minted-token listener in a daemon thread. Each connection is served on its own
    thread, as the main listener's are (REQ-1882), and the workers share the port (REQ-1900)."""
    server = ProvisaServer((host, port), ProvisaConnection(), ssl_ctx, handler=MintedTokenHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("[PGWIRE] minted-token listener on %s:%d (TLS=%s)", host, port, ssl_ctx is not None)
    return server
