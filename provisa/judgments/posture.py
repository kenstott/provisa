# Copyright (c) 2026 Kenneth Stott
# Canary: 5eea36bc-ef7d-44c0-99f9-5876e5590186
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Posture, observed and not enforced (REQ-2094): what the deployment and the org are set up to
do that is worth a look, each written to the queue as a ``worth_a_look`` finding with what was
observed. Nothing here refuses anything.

1. ``plain_text_credential``: a source's credential holds a value that is not a reference.
2. ``test_reads_prod``: a Test (fake) or Test (synthetic) environment's source reads its parent's
   connection, copied, and the parent is prod.
3. ``unsecured_auth``: no auth section, or ``provider: none``; the role header is honoured as given.
4. ``unclaimed_bootstrap``: the bootstrap administrator slot is unclaimed.
5. ``listener_without_tls``: a protocol listener serves without TLS.
6. ``unbounded_domain_access``: a role whose domain access is every domain, or a source whose
   empty ``allowed_domains`` admits every domain.
7. ``expired_in_force``: a tag assignment whose ``expires_on`` has passed is still in force.
"""

# Requirements: REQ-2094

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import select

from provisa.judgments import findings as fq

PLAIN_TEXT_CREDENTIAL = "plain_text_credential"
TEST_READS_PROD = "test_reads_prod"
UNSECURED_AUTH = "unsecured_auth"
UNCLAIMED_BOOTSTRAP = "unclaimed_bootstrap"
LISTENER_WITHOUT_TLS = "listener_without_tls"
UNBOUNDED_DOMAIN_ACCESS = "unbounded_domain_access"
EXPIRED_IN_FORCE = "expired_in_force"
FINDINGS = (
    PLAIN_TEXT_CREDENTIAL,
    TEST_READS_PROD,
    UNSECURED_AUTH,
    UNCLAIMED_BOOTSTRAP,
    LISTENER_WITHOUT_TLS,
    UNBOUNDED_DOMAIN_ACCESS,
    EXPIRED_IN_FORCE,
)

#: The listeners that take a certificate pair, by the environment variables naming their own.
_CERTIFIED_LISTENERS = (
    ("grpc", "PROVISA_GRPC_CERT", "PROVISA_GRPC_KEY"),
    ("flight", "PROVISA_FLIGHT_CERT", "PROVISA_FLIGHT_KEY"),
    ("pgwire", "PROVISA_PGWIRE_CERT", "PROVISA_PGWIRE_KEY"),
    ("bolt", "PROVISA_BOLT_CERT", "PROVISA_BOLT_KEY"),
)


def is_reference(value: str) -> bool:
    """Whether a stored credential is a ``${provider:ref}`` reference, the whole value."""
    from provisa.core.secrets import _SECRET_PATTERN

    return bool(_SECRET_PATTERN.fullmatch(value.strip()))


async def plain_text_credentials(conn: Any) -> list[tuple[str, dict]]:
    from provisa.core.schema_org import sources

    rows = (await conn.execute_core(select(sources.c.id, sources.c.password_ref))).fetchall()
    return [
        (f"source:{sid}", {"source": sid, "field": "password"})
        for sid, stored in rows
        if stored and not is_reference(stored)
    ]


async def test_reads_prod(state: Any, conn: Any, org_id: str) -> list[tuple[str, dict]]:
    from provisa.core.env_classes import COPIED, TEST_FAKE, TEST_SYNTHETIC
    from provisa.core.env_data import _table
    from provisa.core.environments import PROD, org_schema
    from provisa.core.env_store import list_envs

    out = []
    for env in await list_envs(state.admin_db, org_id):
        if env["data_mode"] not in (TEST_FAKE, TEST_SYNTHETIC) or env["parent"] != PROD:
            continue
        src = _table("sources", org_schema(org_id, env["name"]))
        rows = await conn.execute_core(select(src.c.id).where(src.c.binding == COPIED))
        out += [
            (
                f"environment:{env['name']}:source:{r[0]}",
                {"environment": env["name"], "source": r[0], "parent": PROD},
            )
            for r in rows.fetchall()
        ]
    return out


def unsecured_auth(state: Any) -> list[tuple[str, dict]]:
    if getattr(state, "auth_config", None) is None:
        return [("deployment:auth", {"auth": "none"})]
    return []


async def unclaimed_bootstrap(state: Any) -> list[tuple[str, dict]]:
    from provisa.core.schema_admin import superadmin_bootstrap

    cfg = getattr(state, "auth_config", None)
    if cfg is None or not cfg.get("bootstrap_superadmin", False):
        return []
    async with state.admin_db.acquire() as conn:
        claimed = (
            await conn.execute_core(select(superadmin_bootstrap.c.user_id).limit(1))
        ).fetchone() is not None
    return [] if claimed else [("deployment:bootstrap_admin", {"claimed": False})]


def listeners_without_tls() -> list[tuple[str, dict]]:
    from provisa.api.app_startup import _resolve_tls
    from provisa.core import settings_registry

    out = [
        (f"listener:{name}", {"listener": name, "tls": False})
        for name, cert_env, key_env in _CERTIFIED_LISTENERS
        if _resolve_tls(cert_env, key_env) is None
    ]
    if settings_registry.value("mcp.port") and not settings_registry.value("mcp.tls"):
        out.append(("listener:mcp", {"listener": "mcp", "tls": False}))
    return out


async def unbounded_roles(conn: Any) -> list[tuple[str, dict]]:
    from provisa.core.schema_org import roles
    from provisa.security.rights import reaches_all_domains

    from provisa.core.schema_org import sources

    rows = (await conn.execute_core(select(roles.c.id, roles.c.domain_access))).fetchall()
    out = [
        (f"role:{rid}", {"role": rid, "domain_access": list(access or [])})
        for rid, access in rows
        if reaches_all_domains(access)
    ]
    # A source's empty allowed_domains is every domain (rights.domains_added).
    held = (await conn.execute_core(select(sources.c.id, sources.c.allowed_domains))).fetchall()
    out += [
        (f"source:{sid}", {"source": sid, "allowed_domains": list(allowed or [])})
        for sid, allowed in held
        if not [d for d in (allowed or []) if d] or reaches_all_domains(allowed)
    ]
    return out


async def expired_in_force(conn: Any, today: dt.date) -> list[tuple[str, dict]]:
    from provisa.core.schema_org import tag_assignments as ta
    from provisa.judgments.inventory import tag_key

    rows = await conn.execute_core(
        select(ta.c.base_tag_id, ta.c.object_key, ta.c.expires_on).where(
            ta.c.expires_on.is_not(None)
        )
    )
    return [
        (tag_key(b, k), {"tag": b, "object": k, "expires_on": e})
        for b, k, e in rows.fetchall()
        if dt.date.fromisoformat(e) < today
    ]


async def check_org(state: Any, conn: Any, org_id: str, env: str | None, at: dt.datetime) -> int:
    """Run every posture check for ``org_id`` and record what each observes. Returns how many
    findings are open after the run."""
    observed: list[tuple[str, str, dict]] = []
    for kind, found in (
        (PLAIN_TEXT_CREDENTIAL, await plain_text_credentials(conn)),
        (TEST_READS_PROD, await test_reads_prod(state, conn, org_id) if env is None else []),
        (UNSECURED_AUTH, unsecured_auth(state)),
        (UNCLAIMED_BOOTSTRAP, await unclaimed_bootstrap(state)),
        (LISTENER_WITHOUT_TLS, listeners_without_tls()),
        (UNBOUNDED_DOMAIN_ACCESS, await unbounded_roles(conn)),
        (EXPIRED_IN_FORCE, await expired_in_force(conn, at.date())),
    ):
        observed += [(kind, key, what) for key, what in found]
    seen = [
        await fq.record_finding(
            conn,
            object_type="posture",
            object_key=key,
            judgment=fq.POSTURE,
            category=fq.WORTH_A_LOOK,
            finding=kind,
            observed=what,
            at=at,
        )
        for kind, key, what in observed
    ]
    await fq.clear_unseen(conn, seen, judgments=(fq.POSTURE,), findings=FINDINGS, at=at)
    return len(seen)
