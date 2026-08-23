# Copyright (c) 2026 Kenneth Stott
# Canary: 2f81c6d4-93ab-4f17-b0e2-7d5a48c1e096
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Mutual TLS — client-certificate verification on every wire server (REQ-1228).

REQ-1228 already puts TLS on pgwire, Bolt, gRPC, Arrow Flight and MCP, but TLS alone authenticates
only the server: any client that trusts the server's certificate may open a connection and start
guessing credentials. Client-certificate verification moves the first check to the handshake, so a
caller without a certificate the deployment's CA signed never reaches the credential layer at all.

The four transports need the same decision expressed four ways — a stdlib ``ssl.SSLContext`` for
pgwire and Bolt, ``grpc.ssl_server_credentials(root_certificates=..., require_client_auth=...)`` for
gRPC, and ``verify_client``/``root_certificates`` for Flight — so the decision is made once here and
translated per transport, rather than each server growing its own reading of the environment.

Configuration mirrors :func:`provisa.api.app_startup._resolve_tls`: a per-protocol override, else the
node-wide setting.

* ``PROVISA_MTLS_CLIENT_CA`` — PEM bundle of the CA(s) permitted to sign client certificates.
* ``PROVISA_MTLS_MODE`` — ``required`` (default when a CA is configured) or ``optional``.
* ``PROVISA_MTLS_BIND_PRINCIPAL`` — when true, the certificate's common name must equal the
  username the connection then authenticates as.

Nothing here is inferred. A mode without a CA raises rather than quietly serving unverified
connections, and an unrecognized mode raises rather than being read as the safest neighbour: a
deployment that believes it requires client certificates and does not is worse off than one that
fails to start.
"""

from __future__ import annotations

import os
import ssl
from typing import NamedTuple

# Requirements: REQ-1228

_TRUE = {"1", "true", "yes", "on"}


class ClientAuth(NamedTuple):  # REQ-1228
    """What a wire server must demand of its clients' certificates."""

    ca_path: str
    required: bool
    bind_principal: bool


def _env(name: str, node_wide: str) -> str | None:
    """A per-protocol override, else the node-wide setting. Empty is unset, not a value."""
    return os.environ.get(name) or os.environ.get(node_wide) or None


def resolve_client_auth(ca_env: str, mode_env: str, bind_env: str) -> ClientAuth | None:  # REQ-1228
    """The client-certificate policy for one protocol, or None when mTLS is off there.

    Raises ``ValueError`` on a configuration that cannot mean what it says — a verification mode
    with no CA to verify against, or a mode outside the two the design defines.
    """
    ca_path = _env(ca_env, "PROVISA_MTLS_CLIENT_CA")
    mode = _env(mode_env, "PROVISA_MTLS_MODE")
    bind = _env(bind_env, "PROVISA_MTLS_BIND_PRINCIPAL")

    if ca_path is None:
        if mode is not None:
            raise ValueError(
                f"{mode_env} is set to {mode!r} but no client CA is configured; set "
                f"{ca_env} or PROVISA_MTLS_CLIENT_CA to the PEM bundle that signs client "
                "certificates"
            )
        return None
    if not os.path.exists(ca_path):
        raise ValueError(f"client CA bundle {ca_path!r} does not exist")

    # REQ-1228: configuring a CA and no mode means required. Naming a trust anchor is the act of
    # deciding client certificates matter; defaulting that to optional would hand back exactly the
    # unverified connections the operator configured the CA to exclude.
    if mode is None:
        required = True
    elif mode == "required":
        required = True
    elif mode == "optional":
        required = False
    else:
        raise ValueError(f"{mode_env}={mode!r} is not one of 'required', 'optional'")

    return ClientAuth(
        ca_path=ca_path,
        required=required,
        bind_principal=bind.lower() in _TRUE if bind else False,
    )


def apply_to_context(ctx: ssl.SSLContext, auth: ClientAuth | None) -> None:  # REQ-1228
    """Put a client-certificate policy on a stdlib context — pgwire and Bolt."""
    if auth is None:
        return
    ctx.verify_mode = ssl.CERT_REQUIRED if auth.required else ssl.CERT_OPTIONAL
    ctx.load_verify_locations(cafile=auth.ca_path)


def peer_common_name(peer_cert: dict | None) -> str | None:  # REQ-1228
    """The common name of a verified peer certificate.

    ``getpeercert()`` returns ``{}`` for a connection that presented nothing and ``None`` before the
    handshake completes; both mean there is no verified name to report. Only a certificate the CA
    signed reaches here — the ``ssl`` module rejects the rest during the handshake — so the name may
    be trusted once it is found.
    """
    if not peer_cert:
        return None
    for rdn in peer_cert.get("subject", ()):
        for key, value in rdn:
            if key == "commonName":
                return value
    return None


def assert_principal_binding(
    auth: ClientAuth | None, peer_cert: dict | None, username: str
) -> None:
    """Refuse a connection whose certificate names someone other than the authenticating user.

    Only under ``PROVISA_MTLS_BIND_PRINCIPAL`` (REQ-1228). Without it a client certificate proves
    the caller belongs on the network and the credential proves who they are, which is the usual
    arrangement; with it the two must agree, so a stolen password is useless without that user's
    certificate and a shared service certificate cannot be used to log in as anyone.
    """
    if auth is None or not auth.bind_principal:
        return
    common_name = peer_common_name(peer_cert)
    if common_name is None:
        raise PermissionError(
            "client certificate carries no common name to bind against the authenticating user"
        )
    if common_name != username:
        raise PermissionError(
            f"client certificate names {common_name!r}, which cannot authenticate as {username!r}"
        )


def grpc_server_credentials(cert_pem: bytes, key_pem: bytes, auth: ClientAuth | None):  # REQ-1228
    """gRPC's spelling of the same policy.

    ``require_client_auth=True`` makes gRPC reject a handshake with no certificate; with it False
    and roots supplied, a presented certificate is still verified against them and an absent one is
    allowed through — which is what ``optional`` means.
    """
    import grpc

    if auth is None:
        return grpc.ssl_server_credentials(
            [(key_pem, cert_pem)]  # pyright: ignore[reportArgumentType]
        )
    with open(auth.ca_path, "rb") as handle:
        roots = handle.read()
    return grpc.ssl_server_credentials(
        [(key_pem, cert_pem)],  # pyright: ignore[reportArgumentType]
        root_certificates=roots,
        require_client_auth=auth.required,
    )


def flight_tls_kwargs(auth: ClientAuth | None) -> dict:  # REQ-1228
    """Flight's spelling: ``verify_client`` plus the roots to verify against.

    Flight has no optional tier — ``verify_client`` either demands a certificate or ignores one — so
    ``optional`` supplies the roots without demanding, matching what the other transports do with
    it as closely as the library allows.
    """
    if auth is None:
        return {}
    with open(auth.ca_path, "rb") as handle:
        roots = handle.read()
    return {"verify_client": auth.required, "root_certificates": roots}
