# Copyright (c) 2026 Kenneth Stott
# Canary: 1f3f1524-1daa-4efc-8b49-6817aad93159
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The roles a test server's MCP agent may act as (REQ-2075), and the principal it acts for
(REQ-1105).

A loopback MCP call with no bearer acts only within the roles the operator pinned in mcp.role
(PROVISA_MCP_ROLE). A test server's operator is its harness, and the agent it drives acts as the
roles the server's configuration declares and the reserved org_admin (REQ-1349): the harness pins
exactly those. A test that pins otherwise sets PROVISA_MCP_ROLE in the environment it passes,
which is applied after the harness's.

Such a call also acts for the principal the operator names in mcp.principal
(PROVISA_MCP_PRINCIPAL), and is refused unless that principal is in the org it is served in. The
harness names :data:`PRINCIPAL` (:func:`principal_env`) and makes it a real one: an active local
account in the server's platform control plane (:func:`seat_principal`), which a single-org
deployment counts as in its org. It is seated once the server is up -- the server creates the
control plane's tables at its start -- and before any MCP call the harness's tests make.
"""

from __future__ import annotations

import secrets
from pathlib import Path

#: The local account a test server's MCP agent acts for.
PRINCIPAL = "mcp-harness-agent"


def pinned_roles(config: dict) -> str:
    """``mcp.role`` for a server on ``config``: org_admin and every role it declares."""
    declared = [r["id"] for r in config.get("roles") or []]
    return ",".join(dict.fromkeys(["org_admin", *declared]))


def pinned_roles_of(config_path: str | Path) -> str:
    """:func:`pinned_roles` of the configuration file at ``config_path``, its includes read."""
    from provisa.core.config_loader import read_config_with_includes

    return pinned_roles(read_config_with_includes(Path(config_path)))


def principal_env() -> dict[str, str]:
    """The environment naming :data:`PRINCIPAL` as the principal local MCP calls act for."""
    return {"PROVISA_MCP_PRINCIPAL": PRINCIPAL}


def seat_principal(platform_url: str) -> None:
    """Make :data:`PRINCIPAL` an active local account in the platform control plane at
    ``platform_url``. Idempotent: servers that share a control plane each seat it, and the first
    one's row is the one every server reads. No one can sign in as it: its password is a random
    secret held nowhere."""
    import bcrypt
    import sqlalchemy as sa
    from sqlalchemy.exc import IntegrityError

    from provisa.core.database import create_engine_from_url
    from provisa.core.schema_admin import local_users

    engine = create_engine_from_url(platform_url)
    try:
        held = sa.select(local_users.c.is_active).where(local_users.c.id == PRINCIPAL)
        with engine.connect() as conn:
            seated = conn.execute(held).first() is not None
        if not seated:
            secret = secrets.token_urlsafe(32).encode("utf-8")
            try:
                with engine.begin() as conn:
                    conn.execute(
                        sa.insert(local_users).values(
                            id=PRINCIPAL,
                            username=PRINCIPAL,
                            password_hash=bcrypt.hashpw(secret, bcrypt.gensalt()).decode("utf-8"),
                            display_name="MCP test agent",
                            is_active=True,
                        )
                    )
            except IntegrityError:
                pass  # a server sharing this control plane seated it first
        with engine.connect() as conn:
            active = conn.execute(held).scalar()
        if active is not True:
            raise RuntimeError(f"the MCP test principal {PRINCIPAL!r} is not an active account")
    finally:
        engine.dispose()
