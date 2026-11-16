# Copyright (c) 2026 Kenneth Stott
# Canary: 424b4408-9f72-4a37-af1b-c751f2628e21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A started, multi-tenant server and the HTTP calls the membership e2e tests make to it.

One real server process over a database the harness creates, with the ``basic`` sign-in provider
(accounts with an email, kept in the platform control plane), bootstrap claiming on, and the
deployment's break-glass account. Nothing is seeded behind the API: the break-glass account
registers the people (``/auth/register`` by a caller holding user_management), each signs in with
``/auth/login``, and everything after that is what a person or an administrator would send.

A local account's email is whatever was typed when it was made, so it is matched to nothing
(REQ-1261). The tests of joining by email address start the server on an OIDC issuer instead
(:class:`Issuer`), whose tokens say whether the address is verified.

Lands on the TEST instance only."""

from __future__ import annotations

import datetime
import http.server
import json
import os
import threading
import urllib.error
import urllib.request
from typing import Any

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from tests.integration.worker_boot_harness import WorkerBoot

PASSWORD = "correct horse battery"
_BREAK_GLASS = ("breakglass", "open sesame, operator")
# The role an auto-joining member receives: the standard analyst (REQ-2155). It carries no
# platform right (an auto-join role may not: provisa/api/admin/orgs_router.py, _validate_org_policy).
MEMBER_ROLE = "analyst"


_CLIENT_ID = "provisa-tenancy-e2e"
_KEY_ID = "tenancy-e2e"


class Issuer:
    """An OIDC issuer on this machine that a started server trusts: its discovery document, its
    signing keys, and the ID tokens it issues. Each token says of its address what the test says
    an identity provider found (``email_verified``). The caller owns ``close()``."""

    def __init__(self) -> None:
        self._key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(self._key.public_key()))
        documents: dict[str, dict] = {}

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802  # the name http.server calls
                document = documents.get(self.path)
                if document is None:
                    self.send_error(404)
                    return
                body = json.dumps(document).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002  # the name http.server calls
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self.discovery_url = f"{self.url}/.well-known/openid-configuration"
        documents["/.well-known/openid-configuration"] = {
            "issuer": self.url,
            "jwks_uri": f"{self.url}/jwks",
        }
        documents["/jwks"] = {"keys": [{**jwk, "kid": _KEY_ID, "use": "sig", "alg": "RS256"}]}
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def token(self, subject: str, email: str, *, email_verified: bool) -> str:
        """An ID token for ``subject`` (their user id here), carrying ``email``."""
        now = datetime.datetime.now(datetime.timezone.utc)
        return jwt.encode(
            {
                "iss": self.url,
                "aud": _CLIENT_ID,
                "sub": subject,
                "name": subject,
                "email": email,
                "email_verified": email_verified,
                "iat": now,
                "exp": now + datetime.timedelta(hours=1),
            },
            self._key,
            algorithm="RS256",
            headers={"kid": _KEY_ID},
        )

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def boot_server(issuer: Issuer | None = None) -> WorkerBoot:
    """A started multi-tenant server: on the ``basic`` provider, or, given ``issuer``, signing
    people in with its tokens (the ``oidc`` provider). The caller owns ``cleanup()``."""
    sign_in_with: dict[str, Any] = (
        {"provider": "basic"}
        if issuer is None
        else {
            "provider": "oidc",
            "oidc": {"discovery_url": issuer.discovery_url, "client_id": _CLIENT_ID},
        }
    )
    boot = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        extra_config={
            "multitenancy": True,
            "auth": {
                **sign_in_with,
                "jwt_secret": "tenancy-e2e-signing-key-not-a-secret",
                "bootstrap_superadmin": True,
                # Roles are the ones Provisa records (grants, invitations, auto-join), as on a
                # deployment made through /setup. Left at the default ("claims") a member's roles
                # are read from the sign-in's claims and a granted role is never reported: both
                # auto-join cases failed there, `assert 'analyst' in []` (run 37874425906).
                "assignments_source": "provisa",
                "superuser": {"username": _BREAK_GLASS[0], "password": _BREAK_GLASS[1]},
            },
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    boot.start()
    boot.wait_all_ready(timeout=300)
    return boot


def call(
    boot: WorkerBoot,
    method: str,
    path: str,
    *,
    token: str | None = None,
    body: dict | None = None,
    org: str | None = None,
) -> tuple[int, Any]:
    """One HTTP call. ``org`` names the org the caller acts in (the ``X-Org-Provisa`` header):
    in a multi-tenant deployment an org nobody named is refused, never chosen (REQ-1235), for
    the break-glass account as for everyone (REQ-1935). Sign-in, the caller's own identity and
    the org-administration paths act on the platform plane and need none."""
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if org is not None:
        headers["X-Org-Provisa"] = org
    request = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            status, raw = response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read().decode()
    try:
        return status, json.loads(raw)
    except json.JSONDecodeError:
        return status, raw


def said(boot: WorkerBoot, what: str, status: int, body: Any) -> str:
    """An assertion message: the answer, and the end of the server's log."""
    return f"{what}: HTTP {status} {body!r}\n--- server log ---\n{boot.log_text()[-3000:]}"


def operator_token(boot: WorkerBoot) -> str:
    """The break-glass account's session: the deployment's own credential (REQ-125)."""
    status, body = call(
        boot,
        "POST",
        "/auth/superuser-login",
        body={"username": _BREAK_GLASS[0], "password": _BREAK_GLASS[1]},
    )
    assert status == 200, said(boot, "break-glass sign-in", status, body)
    return body["access_token"]


def register(boot: WorkerBoot, operator: str, username: str, email: str) -> str:
    """Create ``username``'s account as the operator would; the new account's user id."""
    status, body = call(
        boot,
        "POST",
        "/auth/register",
        token=operator,
        body={"username": username, "password": PASSWORD, "email": email},
        org=boot.org_id,  # the break-glass account names the org it acts in
    )
    assert status == 200, said(boot, f"register {username}", status, body)
    return body["user_id"]


def sign_in(boot: WorkerBoot, username: str) -> str:
    status, body = call(
        boot, "POST", "/auth/login", body={"username": username, "password": PASSWORD}
    )
    assert status == 200, said(boot, f"sign in {username}", status, body)
    return body["access_token"]


def me(boot: WorkerBoot, token: str, *, org: str | None = None) -> dict:
    """Who the caller is and which orgs they belong to. With ``org`` named (one they belong
    to), ``assignments`` are their roles in that org; with none, their platform roles only."""
    status, body = call(boot, "GET", "/auth/me", token=token, org=org)
    assert status == 200, said(boot, "/auth/me", status, body)
    return body


def member_of(identity: dict) -> list[str]:
    return [m["org_id"] for m in identity["org_memberships"]]


def roles_of(identity: dict) -> list[str]:
    return [a["role_id"] for a in identity["assignments"]]


def claim_platform_admin(boot: WorkerBoot, token: str) -> dict:
    status, body = call(boot, "POST", "/auth/claim-bootstrap", token=token)
    assert status == 200, said(boot, "claim bootstrap", status, body)
    return body


def let_matching_emails_join(boot: WorkerBoot, admin: str, org_id: str, domain: str) -> None:
    """Set ``org_id``'s join policy: anyone whose email is at ``domain`` joins as MEMBER_ROLE."""
    status, body = call(
        boot,
        "PATCH",
        f"/admin/orgs/{org_id}/settings",
        token=admin,
        org=org_id,
        body={
            "email_rule": "@" + domain.replace(".", r"\.") + "$",
            "auto_join": True,
            "auto_join_role": MEMBER_ROLE,
            "auto_join_risk_acknowledged": True,
        },
    )
    assert status == 200, said(boot, "set the join policy", status, body)
    assert (body["auto_join"], body["auto_join_role"]) == (True, MEMBER_ROLE), body


def grant_org_admin(boot: WorkerBoot, platform_admin: str, org_id: str, user_id: str) -> None:
    """The platform administrator grants ``user_id`` org_admin of ``org_id`` (REQ-1303)."""
    status, body = call(
        boot, "POST", f"/admin/orgs/{org_id}/admins/{user_id}", token=platform_admin, org=org_id
    )
    assert status == 200, said(boot, "grant org_admin", status, body)


def org_admin(boot: WorkerBoot, operator: str, platform_admin: str, org_id: str) -> str:
    """A signed-in org_admin of ``org_id``. A platform administrator holds no right over an
    org's own data -- who is in it included (REQ-1605) -- so the tests read membership as the
    org's administrator does: a person the platform administrator grants org_admin (REQ-1303)."""
    user_id = register(boot, operator, f"admin-of-{org_id}", f"admin-of-{org_id}@hq.test")
    grant_org_admin(boot, platform_admin, org_id, user_id)
    return sign_in(boot, f"admin-of-{org_id}")


def member_ids(boot: WorkerBoot, org_admin_token: str, org_id: str) -> list[str]:
    """Who the org's own member list has (GET /admin/orgs/{org}/members answers a list of rows)."""
    status, rows = call(
        boot, "GET", f"/admin/orgs/{org_id}/members", token=org_admin_token, org=org_id
    )
    assert status == 200, said(boot, "list members", status, rows)
    return [row["user_id"] for row in rows]
