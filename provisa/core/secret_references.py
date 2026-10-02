# Copyright (c) 2026 Kenneth Stott
# Canary: 61400649-c176-4903-82d5-a9068dae6a51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where an org's stored values name one of its secrets (REQ-1918).

An org secret is referred to by text: ``${secret:NAME}`` in a source's password reference, its
host or path, a hint, a webhook header, a setting. A secret is deleted only when nothing stored
in the org names it, so the delete asks here first and is refused with the list.

The search is over STORED ROWS: every text and JSON column of every table in each environment
schema of the org, and of the org's own rows in the platform plane. The columns come from the
schema's own definitions (``schema_org.metadata``, ``schema_admin.metadata``), so a column added
later is searched without anything being registered for it. It runs only when a secret is being
deleted. It compares stored text with the reference's literal text; it never resolves a
reference, and it reads no secret's value.
"""

# Requirements: REQ-1918, REQ-1558

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import JSON, LargeBinary, String, Text, cast, select
from sqlalchemy import Table as SATable

from provisa.core import schema_admin, schema_org

if TYPE_CHECKING:
    from collections.abc import Mapping

    from provisa.core.database import Connection, Database

#: Columns that are NOT searched, each with why. They hold ciphertext: the stored bytes are an
#: envelope, so the reference's text cannot be found in them by comparison, and finding it would
#: mean decrypting values this search must not read. Two of them CAN hold a reference inside
#: what they encrypt — a secret named only there is not seen, and its delete is not refused.
NOT_SEARCHED: dict[tuple[str, str], str] = {
    # --- an environment's schema ---
    ("rls_rules", "filter_expr"): (
        "a row filter's SQL predicate, encrypted at rest; a predicate is never resolved as a "
        "secret reference, so none can take effect there"
    ),
    ("query_audit_log", "query_text_enc"): (
        "the text of a statement someone ran, encrypted at rest; a record of the past, never "
        "resolved"
    ),
    ("api_sources", "auth"): (
        "an API source's auth config, encrypted at rest; its fields are resolved at call time, "
        "so a reference CAN live inside it and is not seen by this search"
    ),
    ("org_secrets", "value_enc"): (
        "an org's own service keys, encrypted at rest; the value is resolved when read, so a "
        "reference CAN live inside it and is not seen by this search"
    ),
    # --- the platform plane ---
    ("secrets_store", "value"): "the vault's own envelope: the secrets themselves",
    ("org_config", "encrypted_dek"): "key material",
    ("org_config", "iv"): "key material",
    ("org_config", "ciphertext"): "the org's encrypted configuration blob",
    ("org_encryption_keys", "wrapped_key"): "key material",
    ("orgs", "engine_url_enc"): "the org's engine DSN, encrypted at rest; used as it is stored",
    ("orgs", "storage_url_enc"): "the org's store DSN, encrypted at rest; used as it is stored",
    ("orgs", "branding_logo"): "an image",
}

#: Platform-plane tables that belong to the vault itself and are never a "reference".
_VAULT_TABLES = frozenset({"secrets_store"})

#: The column that names a row to an operator, in the order tried.
_NAME_COLUMNS = ("name", "table_name", "column_name", "title", "key")


@dataclass(frozen=True)
class SecretReference:
    """One stored value that names the secret: the row's table, its key and the name an operator
    knows it by, the column, and the environment it is in (None for the org's platform rows)."""

    table: str
    id: Any
    name: str
    column: str
    environment: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.table,
            "id": self.id,
            "name": self.name,
            "column": self.column,
            "environment": self.environment,
        }


def reference_text(name: str) -> str:
    """The literal text that refers to the org secret ``name``."""
    return "${secret:" + name + "}"


def searched_columns(table: SATable) -> list[Any]:
    """The columns of ``table`` this search reads: its text and JSON columns. Binary columns are
    ciphertext and are the ones listed in :data:`NOT_SEARCHED`."""
    return [
        column
        for column in table.columns
        if isinstance(column.type, (Text, String, JSON))
        and (table.name, column.name) not in NOT_SEARCHED
    ]


def binary_columns(metadata: Any) -> set[tuple[str, str]]:
    """Every binary column the given schema defines — what :data:`NOT_SEARCHED` must account for."""
    return {
        (table.name, column.name)
        for table in metadata.tables.values()
        for column in table.columns
        if isinstance(column.type, LargeBinary)
    }


async def _in_table(
    conn: "Connection", table: SATable, literal: str, *, environment: str | None, where: Any = None
) -> list[SecretReference]:
    columns = searched_columns(table)
    if not columns:
        return []
    key = list(table.primary_key.columns)
    named = next((table.c[n] for n in _NAME_COLUMNS if n in table.c), None)
    found: list[SecretReference] = []
    for column in columns:
        statement = select(*key, *([named] if named is not None else [])).where(
            cast(column, Text).contains(literal, autoescape=True)
        )
        if where is not None:
            statement = statement.where(where)
        for row in (await conn.execute_core(statement)).fetchall():
            ident = row[0] if len(key) == 1 else tuple(row[: len(key)])
            found.append(
                SecretReference(
                    table=table.name,
                    id=ident if not isinstance(ident, tuple) else list(ident),
                    name=str(row[len(key)]) if named is not None else str(ident),
                    column=column.name,
                    environment=environment,
                )
            )
    return found


async def references(
    admin_db: "Database", org_id: str, name: str, *, environments: "Mapping[str, Database]"
) -> list[SecretReference]:
    """Every stored value of the org that names the org secret ``name``.

    ``environments`` is the control plane of each environment the org holds, by environment
    name. The org's platform-plane rows are those carrying its ``org_id`` (and its own row in
    ``orgs``).
    """
    literal = reference_text(name)
    found: list[SecretReference] = []
    for environment in sorted(environments):
        async with environments[environment].acquire() as conn:
            for table in schema_org.metadata.sorted_tables:
                found.extend(await _in_table(conn, table, literal, environment=environment))
    async with admin_db.acquire() as conn:
        for table in schema_admin.metadata.sorted_tables:
            if table.name in _VAULT_TABLES:
                continue
            if table.name == "orgs":
                scope = table.c.id == org_id
            elif "org_id" in table.c:
                scope = table.c.org_id == org_id
            else:
                continue  # not an org's row: deployment-wide records are not the org's values
            found.extend(await _in_table(conn, table, literal, environment=None, where=scope))
    return found
