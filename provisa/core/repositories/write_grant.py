# Copyright (c) 2026 Kenneth Stott
# Canary: 054ee03b-e3e6-40dd-9279-a4658a30a736
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each role may write to each table, per operation (REQ-2074), via SQLAlchemy Core.

A grant's rows and check are row predicates, injected as SQL at every write a grant
admits, so they are kept ciphertext at rest as a row filter is (REQ-686)."""

# Requirements: REQ-2074, REQ-686

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as _delete, select

from provisa.core.models import WriteGrant
from provisa.core.schema_org import table_write_grants
from provisa.encryption import encryption_service

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _sealed(predicate: str | None) -> bytes | None:
    return None if predicate is None else encryption_service().encrypt(predicate.encode("utf-8"))


def _opened(raw: Any) -> str | None:
    return None if raw is None else encryption_service().decrypt(bytes(raw)).decode("utf-8")


def _grant(row: Any) -> dict[str, Any]:
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    return {
        "role": d["role_id"],
        "operation": d["operation"],
        "columns": list(d["columns"] or []),
        "rows": _opened(d["rows_expr"]),
        "check": _opened(d["check_expr"]),
        "presets": list(d["presets"] or []),
        "why": d.get("why"),  # REQ-2094
    }


async def replace_for_table(
    conn: "Connection", table_id: int, grants: list[WriteGrant], *, unheld: set[str]
) -> None:
    """Make ``grants`` the table's whole set of write grants. A grant to a role in ``unheld``
    (a control-plane role, which holds no data-plane right: REQ-1337) is not stored. REQ-2094:
    each grant's why is stamped when the grant or its why changes."""
    from provisa.core.schema_org import registered_tables
    from provisa.judgments.evidence import evidence
    from provisa.judgments.why import stamp_for

    held = {(g["role"], g["operation"]): g for g in await for_table(conn, table_id)}

    async def _evidence() -> dict:
        reg = (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.id == table_id)
            )
        ).fetchone()
        return {} if reg is None else await evidence(conn, None, dict(reg._mapping))

    whys = {}
    for grant in grants:
        was = held.get((grant.role, grant.operation))
        bare = {
            "columns": list(grant.columns),
            "rows": grant.rows,
            "check": grant.check,
            "presets": [p.model_dump(exclude_none=True) for p in grant.presets],
        }
        whys[(grant.role, grant.operation)] = await stamp_for(
            conn,
            None if was is None else was["why"],
            grant.why,
            changed=was is None or {k: was[k] for k in bare} != bare,
            where=f"{grant.operation} grant to {grant.role}",
            evidence=_evidence,
        )
    await conn.execute_core(
        _delete(table_write_grants).where(table_write_grants.c.table_id == table_id)
    )
    for grant in grants:
        if grant.role in unheld:
            continue
        await conn.execute_core(
            table_write_grants.insert().values(
                table_id=table_id,
                role_id=grant.role,
                operation=grant.operation,
                columns=list(grant.columns),
                rows_expr=_sealed(grant.rows),
                check_expr=_sealed(grant.check),
                presets=[p.model_dump(exclude_none=True) for p in grant.presets],
                why=whys[(grant.role, grant.operation)],  # REQ-2094
            )
        )


async def for_table(conn: "Connection", table_id: int) -> list[dict[str, Any]]:
    """The table's write grants, ordered by role and operation."""
    result = await conn.execute_core(
        select(table_write_grants)
        .where(table_write_grants.c.table_id == table_id)
        .order_by(table_write_grants.c.role_id, table_write_grants.c.operation)
    )
    return [_grant(r) for r in result.fetchall()]


#: Every grant, ordered as :func:`by_table` orders them, for a reader that speaks SQL text.
BY_TABLE_SQL = (
    "SELECT table_id, role_id, operation, columns, rows_expr, check_expr, presets, why "
    "FROM table_write_grants ORDER BY table_id, role_id, operation"
)


def grouped(rows: list[Any]) -> dict[int, list[dict[str, Any]]]:
    """Rows of :data:`BY_TABLE_SQL` as each table's grants, by table id."""
    grants: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        grants.setdefault(d["table_id"], []).append(_grant(d))
    return grants


async def by_table(conn: "Connection") -> dict[int, list[dict[str, Any]]]:
    """Every table's write grants, by table id."""
    result = await conn.execute_core(
        select(table_write_grants).order_by(
            table_write_grants.c.table_id,
            table_write_grants.c.role_id,
            table_write_grants.c.operation,
        )
    )
    grants: dict[int, list[dict[str, Any]]] = {}
    for row in result.fetchall():
        grants.setdefault(row._mapping["table_id"], []).append(_grant(row))
    return grants
