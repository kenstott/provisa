# Copyright (c) 2026 Kenneth Stott
# Canary: 68d1c3a5-b92e-4f07-8c44-0e7a5b3d9f21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Refusals for settings that cannot work together (REQ-1913).

Every operator setting is editable, including the ones a wrong value of which leaves the
deployment unreachable. Most of those take effect at the next start, so a value that cannot work is
refused when it is saved rather than discovered by a server that does not come up: two listeners
on one port, a TLS file that is not there, a certificate and key that do not match, a plain Redis
URL where TLS is required.

Each check looks at the values the deployment WOULD run on once the save is stored — the saved
values over what is in force for the rest — so a pair saved together is judged together.
"""

# Requirements: REQ-1913, REQ-1226

from __future__ import annotations

import os
import ssl
from typing import Any

from provisa.core import settings_registry

# The listeners a deployment binds, by the setting that names each one's port (0 = not started).
_LISTENER_PORTS = (
    "server.grpc_port",
    "server.flight_port",
    "server.pgwire_port",
    "server.bolt_port",
    "server.airport_port",
    "mcp.port",
)

# REQ-1226: the node's certificate pair, and each protocol's own.
_TLS_PAIRS = (
    ("tls.cert", "tls.key"),
    ("tls.grpc_cert", "tls.grpc_key"),
    ("tls.flight_cert", "tls.flight_key"),
    ("tls.pgwire_cert", "tls.pgwire_key"),
    ("tls.bolt_cert", "tls.bolt_key"),
)


class Refused(Exception):
    """A saved value cannot work with the rest of the deployment's settings."""

    def __init__(self, field: str, reason: str, **params: Any) -> None:
        super().__init__(f"setting {field} refused: {reason}")
        self.field = field
        self.reason = reason
        self.params = params


def _ports(values: dict[str, Any]) -> None:
    changed = [key for key in _LISTENER_PORTS if key in values]
    if not changed:
        return
    ports = {key: settings_registry.prospective(key, values) for key in _LISTENER_PORTS}
    for key in changed:
        if ports[key] == 0:
            continue
        for other, port in ports.items():
            if other != key and port == ports[key]:
                raise Refused(key, "port_in_use", other=other)


def _tls(values: dict[str, Any]) -> None:
    for cert_key, key_key in _TLS_PAIRS:
        changed = [k for k in (cert_key, key_key) if k in values]
        if not changed:
            continue
        cert = settings_registry.prospective(cert_key, values)
        key = settings_registry.prospective(key_key, values)
        for setting_key, path in ((cert_key, cert), (key_key, key)):
            if setting_key in changed and path is not None and not os.access(path, os.R_OK):
                raise Refused(setting_key, "file_not_found")
        if cert is None or key is None:
            continue  # one half alone starts no TLS listener (app_startup._resolve_tls)
        try:
            ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER).load_cert_chain(cert, key)
        except (ssl.SSLError, OSError):
            field = changed[0]
            raise Refused(
                field, "invalid_tls_pair", other=key_key if field == cert_key else cert_key
            ) from None


def _redis(values: dict[str, Any]) -> None:
    changed = [k for k in ("cache.redis_url", "redis.require_tls") if k in values]
    if not changed:
        return
    url = settings_registry.prospective("cache.redis_url", values)
    if (
        settings_registry.prospective("redis.require_tls", values)
        and url is not None
        and not url.startswith("rediss://")
    ):
        raise Refused(changed[0], "redis_tls_required")


_SUPERUSER = ("security.superuser.username", "security.superuser.password")


def _superuser(values: dict[str, Any]) -> None:
    """The stored break-glass account is a pair: a name with no password (or the reverse) is an
    account nobody can sign in to, or one half of the configured account silently kept."""
    changed = [key for key in _SUPERUSER if key in values]
    if not changed:
        return
    # What would be STORED: the saved value where there is one, else what is stored now.
    stored = {
        key: values[key] if key in values else settings_registry.stored_value(key)
        for key in _SUPERUSER
    }
    username, password = (stored[key] is not None for key in _SUPERUSER)
    if username != password:
        field = changed[0]
        other = _SUPERUSER[1] if field == _SUPERUSER[0] else _SUPERUSER[0]
        raise Refused(field, "superuser_incomplete", other=other)


def check(values: dict[str, Any]) -> None:
    """Raise :class:`Refused` for the first saved value that cannot work with the rest."""
    _superuser(values)
    _ports(values)
    _tls(values)
    _redis(values)
