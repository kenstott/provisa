# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e9d40-6f1a-4c83-a5d2-9e0c3f8b1a57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org vault a replica refresh resolves its sources' secrets in (REQ-1695).

A source created through the admin API keeps its password in its org's vault and carries a
``${secret:NAME}`` reference. The reference is resolved when the engine dials the source — inside
the attach walk and the row loaders a refresh runs — and that resolution needs the org's vault
bound (``core.secrets_store``). A refresh is not always reached through a path that bound it: the
read-time land runs before the statement's terminal, the reconcile after a registration and at
boot runs outside any statement, and a scheduled refresh has no request at all. So the refresh
binds the vault itself, here, for the org whose registry the sources were read from.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from typing import Any

#: The reference only an org's vault can answer (``${env:...}`` needs nothing bound).
_ORG_SECRET_REF = "${secret:"

#: The connection fields an attach resolves (``EngineBackend._merged_source``).
_CONNECTION_FIELDS = ("host", "base_url", "database", "username", "password", "path")
_CONNECTION_MAPS = ("federation_hints", "mapping")


def names_org_secret(source: Any) -> bool:
    """Whether any connection value of ``source`` is a reference into an org's vault."""
    for name in _CONNECTION_FIELDS:
        value = getattr(source, name, None)
        if isinstance(value, str) and _ORG_SECRET_REF in value:
            return True
    for name in _CONNECTION_MAPS:
        for value in (getattr(source, name, None) or {}).values():
            if isinstance(value, str) and _ORG_SECRET_REF in value:
                return True
    return False


@asynccontextmanager
async def org_vault(state: Any, sources: Iterable[Any]) -> AsyncIterator[None]:
    """Bind, for the block, the vault of the org ``sources`` are registered in.

    ``sources`` is what the block may dial — every registered source when the block can run the
    engine's attach walk, which resolves them all. Nothing is bound (and the control plane is not
    read) when none of them names an org secret, or when that org's vault is already bound.

    The org is the one whose registry produced ``sources``: ``state.active_org_id``, the same org
    ``state.tenant_db`` resolved to when they were read. It is not chosen here.
    """
    if not any(names_org_secret(s) for s in sources):
        yield
        return
    from provisa.core import secrets_store

    org_id = state.active_org_id
    if secrets_store.bound_org_id() == org_id:
        yield
        return
    async with secrets_store.bound(state.admin_db, org_id):
        yield
