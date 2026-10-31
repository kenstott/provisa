# Copyright (c) 2026 Kenneth Stott
# Canary: a90e3d71-58c2-47fb-b6e4-3d0f18c7e6a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a statement's catalog views read of the request (REQ-2190): the settings the admin terminal
sets in the statement's own transaction, and no other way. A view cannot see who is asking; these
are how it is told. Nothing a client writes reaches them: a statement calling set_config on that
terminal is refused (:func:`refuse_client_settings`)."""

# Requirements: REQ-2190

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

#: Every setting a catalog view reads.
NAMES = (
    "role",
    "can_write",
    "database",
    "sql_convention",
    "gql_convention",
    "domain_prefix",
    "pid",
    "application_name",
    "backend_start",
    "query",
    "scope",
    "asked",
)
_PREFIX = "provisa.catalog"
_WRITES_SETTINGS = re.compile(r"\bset_config\b", re.IGNORECASE)


#: The role a client's SQL runs as on the control plane: it may read the views and call what
#: PostgreSQL gives every role, and it is no member of pg_read_server_files, pg_read_all_stats or
#: anything else. Its schema grants are written with the views' (provisa.api.startup_seed).
CLIENT_ROLE = "provisa_catalog_client"


def enter_client_role() -> str:
    """The statement that makes the rest of this transaction's SQL the client role's."""
    return f"SET LOCAL ROLE {CLIENT_ROLE}"


def setting(name: str) -> str:
    """SQL reading setting ``name``: NULL or empty on a statement that was not given it."""
    assert name in NAMES, name
    return f"NULLIF(CURRENT_SETTING('{_PREFIX}.{name}', TRUE), '')"


def scope_ids_sql() -> str:
    """The table ids a role's meta rows are scoped to, told once by the statement's settings: a
    list written into the statement would be parsed, governed and planned for each view it
    reads. A statement that was not told them reads none."""
    return f"SELECT UNNEST(CAST({setting('scope')} AS INTEGER[]))"


def asked_sql(*names: str) -> str:
    """Whether a row with any of ``names`` (SQL) is one the statement asks about: the relation
    names a statement's pin tells its views (provisa.pgwire.catalog_pin), all of them where none
    was told."""
    asked = f"CAST({setting('asked')} AS TEXT[])"
    matches = " OR ".join(f"{name} = ANY({asked})" for name in names)
    return f"({setting('asked')} IS NULL OR {matches})"


def relation_names_literal(names: Iterable[str]) -> str:
    """``names`` as the array literal :func:`asked_sql` reads."""
    written = (name.replace("\\", "\\\\").replace('"', '\\"') for name in sorted(names))
    return "{" + ",".join(f'"{name}"' for name in written) + "}"


def set_statement(values: Mapping[str, str]) -> tuple[str, tuple[str, ...]]:
    """The statement setting ``values`` for the rest of this transaction, and its arguments."""
    unknown = set(values) - set(NAMES)
    assert not unknown, unknown
    names = sorted(values)
    calls = ", ".join(f"SET_CONFIG('{_PREFIX}.{n}', ${i}, TRUE)" for i, n in enumerate(names, 1))
    return f"SELECT {calls}", tuple(values[n] for n in names)


def statement_settings(
    role_id: str,
    can_write: bool,
    database: str,
    domain_prefix: bool,
    *,
    session: Mapping[str, str] | None = None,
    scope: str | None = None,
) -> dict[str, str]:
    """What a statement's catalog views are told: who asks, what database it is on, the naming in
    force, where a connection asks, that connection (pid, application_name, backend_start,
    query), and where the role's meta rows are scoped, to which tables."""
    from provisa.compiler.naming import active_gql_convention, active_sql_form

    return {
        "role": role_id,
        "can_write": "true" if can_write else "false",
        "database": database,
        "sql_convention": active_sql_form(),
        "gql_convention": active_gql_convention(),
        "domain_prefix": "true" if domain_prefix else "false",
        **(session or {}),
        **({"scope": scope} if scope is not None else {}),
    }


class ClientSettingRefused(PermissionError):
    """A statement for the admin terminal that sets a setting."""

    code = "query.function_unsupported"

    def __init__(self) -> None:
        self.params = {"function": "set_config", "dialect": "meta"}
        super().__init__("set_config() is not available on a statement reading the meta domain")


class CatalogViewRefused(ValueError):
    """A catalog view on a route other than the admin terminal: only that terminal tells a statement
    who asks, and its row scope and the views read that, so no other route can answer it."""

    code = "query.catalog_view_refused"
    sqlstate = "0A000"  # feature_not_supported

    def __init__(self, view: str) -> None:
        self.params = {"view": view}
        super().__init__(
            f"{view} is a catalog view of the meta domain and cannot be read in one statement with "
            "a table of a data source: read them in two statements"
        )


def refuse_client_settings(sql: str) -> None:
    """Raise :class:`ClientSettingRefused` for a statement that calls set_config, by any spelling:
    the settings a view trusts are the terminal's alone. A statement that merely holds the word,
    in a string or an identifier, is not refused."""
    if _WRITES_SETTINGS.search(sql) is None:
        return
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except SqlglotError as exc:
        raise ClientSettingRefused from exc  # unread: it names the function
    if any(call.name.lower() == "set_config" for call in tree.find_all(exp.Anonymous)):
        raise ClientSettingRefused
