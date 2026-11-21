# Copyright (c) 2026 Kenneth Stott
# Canary: 5c0e7a41-2b9d-4f36-8e15-9a3d6c2f7b80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Retired model objects in the admin API (REQ-2248).

A retired source, domain, table, column, relationship, tag or metric is in no ordinary list. It is listed here,
with who retired it, when and why, as a retired glossary term is on its own record, and it is
restored from here: the same object, its retirement cleared, served again. A write that would put
another object at a retired name is refused naming the retired one (:func:`retired_name_result`).
"""

# Requirements: REQ-2248

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import strawberry

from provisa.api.admin.types import MutationResult
from provisa.core.repositories.retirement import NameHeld, RetiredNameTaken

if TYPE_CHECKING:
    from strawberry.types import Info as StrawberryInfo

    from provisa.core.database import Connection

#: The kinds a model object is retired as, in the order they are listed.
KINDS = ("source", "domain", "table", "column", "relationship", "tag", "metric")

#: REQ-2248: the right that deletes, lists and restores a retired source or domain (its delete's).
_HOLDER_CAPABILITY = {"source": "source_registration", "domain": "org_settings"}


def retired_name_result(taken: RetiredNameTaken) -> MutationResult:
    """The refusal of a write at a retired object's name: it names the retired object and says it
    can be restored."""
    return MutationResult(
        success=False,
        message=str(taken),
        code="schema.retired_name_taken",
        params={"kind": taken.kind, "name": taken.name},
    )


@strawberry.type
class RetiredObjectType:
    """One retired model object: its kind, what names it (``key``: the key a retired source or
    domain is kept under, the table id, the table id and column name as ``<id>.<column>``, the
    relationship id, the tag id or the metric name), its model name (a source's or domain's id
    while live), and who retired it, when and why."""

    kind: str
    key: str
    name: str
    domain_id: str | None
    retired_by: str | None
    retired_at: datetime | None
    reason: str | None


def _retired(kind: str, key: str, name: str, domain_id: str | None, row: dict) -> RetiredObjectType:
    return RetiredObjectType(
        kind=kind,
        key=key,
        name=name,
        domain_id=domain_id,
        retired_by=row.get("status_by"),
        retired_at=row.get("status_at"),
        reason=row.get("status_reason"),
    )


def _held(info: "StrawberryInfo", domains: set[str]) -> bool:
    """Whether the caller may act in every one of ``domains`` (REQ-1531)."""
    from provisa.api.admin.capabilities import require_domain

    try:
        for domain_id in sorted(domains):
            require_domain(info, domain_id)
    except PermissionError:
        return False
    return True


async def retired_objects(info: "StrawberryInfo", conn: "Connection") -> list[RetiredObjectType]:
    """Every retired model object the caller may act on, by kind then name: a source or domain
    when it holds the right that deletes one, a table or column in a domain it holds, a
    relationship whose tables' domains it holds, a metric whose read domains it holds, and every
    retired tag."""
    from provisa.api.admin.domain_guard import metric_domains
    from provisa.core.repositories import metric as metric_repo
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import table as table_repo
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.schema_org import registered_tables
    from sqlalchemy import select

    from provisa.api.admin.capabilities import has_capability
    from provisa.core.repositories import domain as domain_repo
    from provisa.core.repositories import source as source_repo

    found: list[RetiredObjectType] = []
    for kind, repo in (("source", source_repo), ("domain", domain_repo)):
        if has_capability(info, _HOLDER_CAPABILITY[kind]):
            for h in await repo.list_all(conn, retired=True):
                found.append(_retired(kind, h["id"], h["retired_id"], None, h))
    every_table = {
        int(r.id): dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    registered_tables.c.id,
                    registered_tables.c.domain_id,
                    registered_tables.c.table_name,
                    registered_tables.c.alias,
                )
            )
        ).fetchall()
    }

    def model_name(table_id: int) -> str:
        t = every_table[table_id]
        return str(t["alias"] or t["table_name"])

    for t in await table_repo.list_all(conn, retired=True, parts_of=lambda _: False):
        if _held(info, {t["domain_id"]}):
            found.append(_retired("table", str(t["id"]), model_name(t["id"]), t["domain_id"], t))
    for table_id, t in sorted(every_table.items()):
        if not _held(info, {t["domain_id"]}):
            continue
        for c in await table_repo.retired_columns(conn, table_id):
            name = f"{model_name(table_id)}.{c['alias'] or c['column_name']}"
            found.append(
                _retired("column", f"{table_id}.{c['column_name']}", name, t["domain_id"], c)
            )
    for r in await rel_repo.list_all(conn, retired=True):
        ends = {
            every_table[int(i)]["domain_id"]
            for i in (r["source_table_id"], r["target_table_id"], r["via_table_id"])
            if i is not None and int(i) in every_table
        }
        if _held(info, ends):
            found.append(_retired("relationship", r["id"], r["alias"] or r["id"], None, r))
    for g in await tag_repo.list_all(conn, retired=True):
        found.append(_retired("tag", g["id"], g["id"], None, g))
    for m in await metric_repo.list_all(conn, retired=True):
        if _held(info, await metric_domains(conn, m["name"]) or set()):
            found.append(_retired("metric", m["name"], m["name"], None, m))
    return sorted(found, key=lambda o: (KINDS.index(o.kind), o.name))


def _not_retired(kind: str, key: str) -> MutationResult:
    return MutationResult(
        success=False,
        message=f"there is no retired {kind} {key!r} to restore",
        code="schema.not_retired",
        params={"kind": kind, "key": key},
    )


def _restored(kind: str, key: str) -> MutationResult:
    return MutationResult(
        success=True,
        message=f"{kind} {key!r} restored",
        code="schema.restored",
        params={"kind": kind, "key": key},
    )


async def restore(
    info: "StrawberryInfo", conn: "Connection", kind: str, key: str
) -> MutationResult:
    """Restore one retired object, gated as its delete is: the right that deletes it, in every
    domain it belongs to. Links re-pointed while it was retired are not moved back."""
    from provisa.api.admin.capabilities import require_capability, require_right_in_domains
    from provisa.api.admin.domain_guard import (
        metric_domains,
        relationship_domains,
        require_domains,
        require_table_domain,
    )
    from provisa.core.repositories import metric as metric_repo
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import table as table_repo
    from provisa.core.repositories import tag as tag_repo
    from provisa.security.rights import ALL_DOMAINS
    from provisa.security.sensitive import SENSITIVE_DATA

    done: bool
    if kind in _HOLDER_CAPABILITY:
        from provisa.core.repositories import domain as domain_repo
        from provisa.core.repositories import source as source_repo

        require_capability(info, _HOLDER_CAPABILITY[kind])
        repo = source_repo if kind == "source" else domain_repo
        try:
            done = await repo.restore(conn, key)
        except NameHeld as held:
            return MutationResult(
                success=False,
                message=str(held),
                code="schema.restore_name_held",
                params={"kind": held.kind, "name": held.name},
            )
    elif kind in ("table", "column"):
        require_capability(info, "table_registration")
        table_part, _, column = key.partition(".")
        if not table_part.isdigit() or (kind == "column") != bool(column):
            return _not_retired(kind, key)
        table_id = int(table_part)
        if await table_repo.get(conn, table_id) is None:
            return _not_retired(kind, key)
        await require_table_domain(info, conn, table_id)
        if kind == "table":
            done = await table_repo.restore(conn, table_id)
        else:
            done = await table_repo.restore_column(conn, table_id, column)
    elif kind == "relationship":
        require_capability(info, "create_relationship")
        domains = await relationship_domains(conn, key)
        if domains is None:
            return _not_retired(kind, key)
        require_domains(info, domains)
        done = await rel_repo.restore(conn, key)
    elif kind == "tag":
        held = await tag_repo.get(conn, key)
        if held is not None and held.get("sensitive"):
            require_right_in_domains(info, SENSITIVE_DATA, {ALL_DOMAINS})  # REQ-1943
        else:
            require_capability(info, "table_registration")
        done = await tag_repo.restore(conn, key)
    elif kind == "metric":
        require_capability(info, "table_registration")
        domains = await metric_domains(conn, key)
        if domains is not None:
            require_domains(info, domains)
        done = await metric_repo.restore(conn, key)
    else:
        return MutationResult(
            success=False,
            message=f"{kind!r} is not a kind of model object that is retired",
            code="schema.retired_kind_unknown",
            params={"kind": kind, "kinds": list(KINDS)},
        )
    return _restored(kind, key) if done else _not_retired(kind, key)
