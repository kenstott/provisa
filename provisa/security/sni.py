# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1a7e02-6b34-4de1-9a58-1f2c0d94b7ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org selector as it arrives on a wire protocol — TLS SNI (REQ-1234).

Under multitenancy an org is addressed by hostname: ``acme.provisa.dev`` is org ``acme``. Over HTTP
that name is in the ``Host`` header. A pgwire or Bolt client sends no such header, but it does send
the hostname it dialed in the TLS ClientHello, and that is the same string doing the same job.

An ``sni_callback`` on the listener's :class:`ssl.SSLContext` stashes the indicated name on the
connection object the handshake produced, which is the only object that outlives the callback and is
reachable from the protocol handler — the callback itself has no other channel back to the
connection. pgwire reads it off the wrapped socket; Bolt reads it off the transport's ``ssl_object``.

The name is a *request*, exactly as ``Host`` and ``x-provisa-org`` are: it names an org, it does not
grant one. It reaches :func:`provisa.api.org_resolve.resolve_session_org` as ``requested_org``, which
refuses any org the authenticated principal is not a member of and lacks the cross-org right for. A
client can therefore dial any hostname it likes without reaching data that is not its own.

Only pgwire and Bolt are wired here, because only they terminate TLS through a stdlib
``ssl.SSLContext``. gRPC hands its certificates to ``grpc.ssl_server_credentials`` and Flight to
pyarrow, neither of which exposes a servername callback; those transports keep their existing
explicit org channels (``x-provisa-org`` metadata).
"""

from __future__ import annotations

import ssl
from typing import Protocol

from provisa.core.org_ids import is_org_id

# Requirements: REQ-1234, REQ-1276

# Stashed on the connection rather than in a module-level map keyed by identity: a map would have to
# be cleaned up when the connection closes, and a missed cleanup would hand one client's org to the
# next connection that reused the address.
_ATTR = "_provisa_sni_host"

# The control-plane host. Its leftmost label is not an org — the deployment's own console lives
# there, and its org comes from an explicit request rather than from the name it was dialed by.
_CONTROL_PLANE_LABEL = "cloud"


class _Stashable(Protocol):
    """Whatever the handshake produced — an ``SSLSocket`` (pgwire) or an ``SSLObject`` (Bolt)."""


def install(context: ssl.SSLContext) -> None:  # REQ-1234
    """Have ``context`` record the hostname each client indicates.

    Returning None from the callback accepts the name; any other return value would make OpenSSL
    send an alert and drop the handshake. Nothing is validated here on purpose — an unknown or
    hostile name is refused later, by the org resolver, where the principal is known.
    """

    def _record(connection, server_name: str | None, _context: ssl.SSLContext) -> None:
        setattr(connection, _ATTR, server_name)

    context.sni_callback = _record


def indicated_host(connection: _Stashable | None) -> str | None:  # REQ-1234
    """The hostname this connection's ClientHello named, or None.

    None covers three cases that are all the same to the caller: the connection is plaintext, the
    client sent no SNI extension (an IP-address dial does not), or the listener has no callback
    installed because it terminates no TLS.
    """
    if connection is None:
        return None
    return getattr(connection, _ATTR, None)


def _labels(host: str | None) -> list[str]:
    """The hostname's labels, with the port and case stripped."""
    if not host:
        return []
    bare = host.split(":")[0].strip().lower()
    return bare.split(".") if bare else []


def is_control_plane_host(host: str | None) -> bool:  # REQ-1276
    """Whether this hostname is the control plane, whose leftmost label names no org."""
    return _labels(host)[:1] == [_CONTROL_PLANE_LABEL]


def org_from_host(host: str | None) -> str | None:  # REQ-1234, REQ-1276
    """The org a hostname addresses — ``acme.provisa.dev`` → ``acme``.

    None for the control-plane host, and for anything with fewer than three labels: an apex domain
    and ``localhost`` name no org, and reading their leftmost label as one would turn ``localhost``
    into an org named ``localhost``.

    None too when the leftmost label could not be an org id under REQ-1309 — ``127.0.0.1`` has
    four labels and would otherwise be read as an org named ``127``. The id rule is the same one
    org creation enforces, so a label that names an org here is a label that could have been
    created there.
    """
    labels = _labels(host)
    if labels[:1] == [_CONTROL_PLANE_LABEL]:
        return None
    if len(labels) < 3:
        return None
    return labels[0] if is_org_id(labels[0]) else None
