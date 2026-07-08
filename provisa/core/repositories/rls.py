# Copyright (c) 2026 Kenneth Stott
# Canary: 167bd755-fcdb-478f-8ff9-c11e0cbb9669
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""RLS rule repository — CRUD for row-level security rules, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-041, REQ-402, REQ-403

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select

from provisa.core.models import RLSRule
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import rls_rules
from provisa.encryption import encryption_service

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _encrypt_filter(filter_expr: str) -> bytes:  # REQ-686
    """Encrypt an RLS filter for storage. The predicate is injected as SQL at every
    governance read, so it is sensitive metadata — kept ciphertext at rest (BYTEA)."""
    return encryption_service().encrypt(filter_expr.encode("utf-8"))


def _decrypt_row(row) -> dict:  # REQ-686
    """Return the row as a dict with ``filter_expr`` decrypted back to SQL text."""
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    raw = d.get("filter_expr")
    if raw is not None:
        d["filter_expr"] = encryption_service().decrypt(bytes(raw)).decode("utf-8")
    return d


async def upsert(conn: "Connection", rule: RLSRule) -> None:  # REQ-041, REQ-402, REQ-686
    """Upsert an RLS rule. Resolves table_id from table name for table-level rules."""
    filter_enc = _encrypt_filter(rule.filter)
    if rule.domain_id:
        await conn.upsert(
            rls_rules,
            {"domain_id": rule.domain_id, "role_id": rule.role_id, "filter_expr": filter_enc},
            index_elements=["domain_id", "role_id"],
            update_columns=["filter_expr"],
        )
    else:
        if not rule.table_id:
            raise ValueError("Either table_id or domain_id must be provided")
        tbl = await table_repo.find_by_table_name(conn, rule.table_id)
        if tbl is None:
            raise ValueError(f"Table not registered: {rule.table_id}")
        await conn.upsert(
            rls_rules,
            {"table_id": tbl["id"], "role_id": rule.role_id, "filter_expr": filter_enc},
            index_elements=["table_id", "role_id"],
            update_columns=["filter_expr"],
        )


async def get_for_table_role(  # REQ-041, REQ-403
    conn: "Connection", table_id: int, role_id: str
) -> dict | None:
    result = await conn.execute_core(
        select(rls_rules).where(rls_rules.c.table_id == table_id, rls_rules.c.role_id == role_id)
    )
    row = result.fetchone()
    return _decrypt_row(row) if row is not None else None


async def list_for_role(
    conn: "Connection", role_id: str
) -> list[dict]:  # REQ-041, REQ-402, REQ-403
    result = await conn.execute_core(
        select(rls_rules).where(rls_rules.c.role_id == role_id).order_by(rls_rules.c.id)
    )
    return [_decrypt_row(r) for r in result.fetchall()]


async def list_all(conn: "Connection") -> list[dict]:  # REQ-041, REQ-402
    result = await conn.execute_core(select(rls_rules).order_by(rls_rules.c.id))
    return [_decrypt_row(r) for r in result.fetchall()]


async def delete(  # REQ-041, REQ-402
    conn: "Connection",
    role_id: str,
    table_id: int | None = None,
    domain_id: str | None = None,
) -> bool:
    if domain_id:
        stmt = _delete(rls_rules).where(
            rls_rules.c.domain_id == domain_id, rls_rules.c.role_id == role_id
        )
    else:
        stmt = _delete(rls_rules).where(
            rls_rules.c.table_id == table_id, rls_rules.c.role_id == role_id
        )
    result = await conn.execute_core(stmt)
    return (result.rowcount or 0) > 0
