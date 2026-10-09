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

The search is over STORED ROWS: every text and JSON column of every model-store table in each
environment schema of the org, and of the org's own rows in the platform plane. The columns come from the
schema's own definitions (``schema_org.metadata``, ``schema_admin.metadata``), so a column added
later is searched without anything being registered for it. It runs only when a secret is being
deleted. It compares stored text with the reference's literal text; it never resolves a
reference.

Two columns hold what they store ENCRYPTED and are resolved for references at the moment they
are used: an API source's auth config and an org's own service keys. A reference can therefore
live inside them, so for these two — and only these — the check decrypts each value in memory,
as the server already does at call time, and looks for the literal. Nothing decrypted is
returned, logged or put in a refusal: a hit names the row like any other. A value that cannot be
decrypted is never read as "no reference": that row is named in the refusal as unreadable.
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

#: Encrypted columns whose plaintext is resolved for secret references when it is used, so a
#: reference CAN live inside them. These are decrypted in memory by the check and searched.
SEARCHED_DECRYPTED: dict[tuple[str, str], str] = {
    ("api_sources", "auth"): (
        "an API source's auth config; its fields are resolved at call time "
        "(provisa/api_source/caller.py)"
    ),
    ("org_secrets", "value_enc"): (
        "an org's own service keys; the value is resolved when read (provisa/core/org_secrets.py)"
    ),
}

#: Columns that are NOT searched, each with why. They hold ciphertext or bytes that are never
#: resolved as a secret reference, so none can take effect there.
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
    # --- the platform plane ---
    ("secrets_store", "value"): "the vault's own envelope: the secrets themselves",
    ("org_config", "encrypted_dek"): "key material",
    ("org_config", "iv"): "key material",
    ("org_config", "ciphertext"): "the org's encrypted configuration blob",
    ("org_encryption_keys", "wrapped_key"): "key material",
    ("orgs", "engine_url_enc"): "the org's engine DSN, encrypted at rest; used as it is stored",
    ("orgs", "storage_url_enc"): "the org's store DSN, encrypted at rest; used as it is stored",
    ("orgs", "branding_logo"): "an image",
    ("source_sign_ins", "verifier"): (
        "a pending sign-in's one-time code verifier, sealed by the vault's cipher; random, "
        "never resolved as a reference, and gone when the sign-in is"
    ),
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
    #: The value is encrypted and could not be decrypted, so whether it names the secret is not
    #: known. It blocks the delete: an unreadable value is never taken for "no reference".
    unreadable: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.table,
            "id": self.id,
            "name": self.name,
            "column": self.column,
            "environment": self.environment,
            "unreadable": self.unreadable,
        }


def _model_tables() -> list[SATable]:
    """The environment tables the search reads: the model store's (REQ-1919, REQ-1922).

    Each environment is searched through its model handle, which refuses a region's state and
    record tables. Those hold what a region did (builds, change events, the request record), not
    a stored value that is resolved as a secret reference, so no reference can take effect there.
    """
    from provisa.core.store_sides import RECORD, STATE

    return [t for t in schema_org.metadata.sorted_tables if t.name not in STATE | RECORD]


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


def decrypted_columns(table: SATable) -> list[Any]:
    """The encrypted columns of ``table`` the check decrypts in memory and searches."""
    return [c for c in table.columns if (table.name, c.name) in SEARCHED_DECRYPTED]


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
    for column in decrypted_columns(table):
        statement = select(*key, *([named] if named is not None else []), column).where(
            column.is_not(None)
        )
        if where is not None:
            statement = statement.where(where)
        for row in (await conn.execute_core(statement)).fetchall():
            ident = row[0] if len(key) == 1 else list(row[: len(key)])
            verdict = _names_it(row[-1], literal)
            if verdict is False:
                continue
            found.append(
                SecretReference(
                    table=table.name,
                    id=ident,
                    name=str(row[len(key)]) if named is not None else str(ident),
                    column=column.name,
                    environment=environment,
                    unreadable=verdict is None,
                )
            )
    return found


def _names_it(ciphertext: Any, literal: str) -> bool | None:
    """Whether the encrypted value names the secret: True or False, or None when it cannot be
    decrypted. The plaintext lives only inside this function; nothing of it leaves."""
    from cryptography.exceptions import InvalidTag

    from provisa.encryption.runtime import encryption_service

    try:
        plaintext = encryption_service().decrypt(bytes(ciphertext)).decode("utf-8")
    except (ValueError, InvalidTag, RuntimeError):
        # What a blob this worker cannot open raises: an envelope naming a key it does not hold
        # or malformed (ValueError, which UnicodeDecodeError is too), a failed authentication tag
        # (InvalidTag), an org on the key roster with no ring loaded (RuntimeError).
        return None
    return literal in plaintext


async def references(
    admin_db: "Database", org_id: str, name: str, *, environments: "Mapping[str, Database]"
) -> list[SecretReference]:
    """Every stored value of the org that names the org secret ``name``.

    ``environments`` is the control plane of each environment the org holds, by environment
    name. The org's platform-plane rows are those carrying its ``org_id`` (and its own row in
    ``orgs``).
    """
    from provisa.core.request_context import reset_current_org, set_current_org

    literal = reference_text(name)
    found: list[SecretReference] = []
    # The org is bound while its environments are read: the two encrypted columns are written
    # under the ORG's key when it holds one, and the encryption service follows the bound org.
    token = set_current_org(org_id)
    try:
        for environment in sorted(environments):
            async with environments[environment].acquire() as conn:
                for table in _model_tables():
                    found.extend(await _in_table(conn, table, literal, environment=environment))
    finally:
        reset_current_org(token)
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
