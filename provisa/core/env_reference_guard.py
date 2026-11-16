# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1c9a0e-7b42-4e86-9a3f-2c6e8b7d4f10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A member of a held organization neither writes nor edits an environment reference (REQ-1557).

The process environment is the operator's. In a held organization (``sandbox_egress.held``: a
sandbox organization on any install, and every organization but the deployment's own on a server
of many organizations) the text a member saves is not the operator's, so it may not name a
variable of that environment: neither as ``${env:NAME}`` in any field, nor outright in a field
that holds a variable's name (:data:`NAMED_OUTRIGHT`).

WHERE IT IS REFUSED. At the save, never at the resolver: stored text resolves at use exactly as
before, so a demo's own sources (the operator's, written with ``${env:...}``) keep working inside
a sandbox. Three points refuse:

* the write seam (``Connection.execute_core`` and the writes beside it, REQ-1525's seam,
  ``env_secrets.guard_statement`` / ``guard_rows``): :func:`check_written` refuses a value that
  carries a reference, and :func:`check_edited` refuses an UPDATE of a row that already carries
  one. Such a row is the operator's (a demo's own source), and a member changing any field of it
  (its host, say) would point the operator's credential somewhere else;
* a handler that resolves what a member ENTERED before, or without, saving it (a new source's
  connection, a connection test): :func:`refuse_entered`;
* a save that writes the model as a whole with no person acting (a configuration import): its own
  route calls :func:`refuse_entered` on the parsed document before anything is resolved.

WHO IS A MEMBER. A person acting (``model_row_rules.person_acting``: the same test that decides
whose writes a row rule narrows). A boot, a configuration load, a sandbox's creation from its
template and an environment copy write the operator's or already-stored text and are not held.
"""

# Requirements: REQ-1557, REQ-1525, REQ-2006

from __future__ import annotations

import functools
import json
import re
from collections.abc import Iterator
from typing import Any

from provisa.core.env_classes import CLASSIFIED, NEVER_RUNTIME, NEVER_SENSITIVE

_ENV_REFERENCE = re.compile(r"\$\{env:([^}]+)\}")

#: Fields whose value names an environment variable outright: an AI endpoint's and a vector
#: model's key variable (``core.secrets.resolve_api_key_field``, ``llm.client``). A value there
#: that is another provider's reference (``${secret:NAME}``) names no variable.
NAMED_OUTRIGHT: frozenset[str] = frozenset({"api_key_env"})

#: Tables whose rows are not text anyone resolves: runtime state and evidence (a query's own text
#: in the audit log may hold ``${env:`` as a literal), and identities.
_NOT_RESOLVED: frozenset[str] = NEVER_RUNTIME | NEVER_SENSITIVE


@functools.cache
def _held_tables() -> frozenset[str]:
    """The control plane's tables whose text may be resolved: every table of an organization's
    schema (``env_classes.CLASSIFIED``, which a test holds equal to the schema, so a table added
    later is held until someone lists it as runtime state) and of the admin schema (an
    organization's registries, mail platform and repository remote), less :data:`_NOT_RESOLVED`.
    A table that is neither is not the control plane's: a source's or a store's own rows."""
    from provisa.core import schema_admin

    return (CLASSIFIED | frozenset(schema_admin.metadata.tables)) - _NOT_RESOLVED


class EnvReferenceRefused(PermissionError):
    """A member of a held organization saved, or entered, text that names a variable of the
    server's environment. ``code`` and ``params`` are what the caller is answered with."""

    code = "secrets.env_reference_refused"

    def __init__(self, name: str) -> None:
        super().__init__(
            f"{name} is a variable of the server's own environment, which this organization "
            "does not hold. Store the value as an organization secret and write ${secret:NAME}."
        )
        self.params = {"name": name}


class EnvReferenceEditRefused(PermissionError):
    """A member of a held organization changed an object that carries a reference to the
    server's environment: the object is the operator's."""

    code = "secrets.env_reference_object_locked"

    def __init__(self, table: str) -> None:
        super().__init__(
            f"This {table} object is the operator's (it reads the server's own environment) "
            "and cannot be changed here. Add one of your own instead."
        )
        self.params = {"object": table}


def _text_names(text: str) -> Iterator[str]:
    for reference in _ENV_REFERENCE.findall(text):
        yield reference.partition(":-")[0]
    # Text that is itself a JSON document (a mapping stored as text) holds its references under
    # JSON's own escapes; it is read as the document it will be read as at use.
    if text[:1] in "{[" and "\\" in text:
        try:
            document = json.loads(text)
        except ValueError:
            return  # not a document: its text was searched above
        yield from env_names(document)


def env_names(value: Any, *, key: str | None = None) -> Iterator[str]:
    """The environment variables ``value`` names: each ``${env:NAME}`` in any text it holds, and
    the value of a field that names a variable outright. ``value`` is text or a structure."""
    if isinstance(value, str):
        if key in NAMED_OUTRIGHT and value and "${" not in value:
            yield value
        yield from _text_names(value)
    elif isinstance(value, dict):
        for name, item in value.items():
            yield from env_names(item, key=name if isinstance(name, str) else None)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from env_names(item, key=key)


def held_org() -> str | None:
    """The organization bound to this context when it is held, else None."""
    from provisa.core.request_context import current_org
    from provisa.security.sandbox_egress import held

    org_id = current_org.get()
    return org_id if held(org_id) else None


def _member_writes() -> bool:
    """Whether this context is a member of a held organization writing."""
    from provisa.core.model_row_rules import person_acting

    return person_acting() and held_org() is not None


def refuse_entered(*values: Any) -> None:
    """Refuse what a caller entered into a held organization when it names a variable of the
    server's environment. Called where a request's own text is resolved before, or without,
    being saved, and by a save with no person acting (a configuration import); the variable is
    not read."""
    if held_org() is None:
        return
    for value in values:
        for name in env_names(value):
            raise EnvReferenceRefused(name)


def _holds(table: Any) -> bool:
    return getattr(table, "name", None) in _held_tables()


def check_values(table: Any, values: dict[Any, Any]) -> None:
    """Refuse a row a member of a held organization writes to ``table`` when a value in it names
    a variable of the server's environment."""
    if not _holds(table) or not _member_writes():
        return
    for column, value in values.items():
        literal = getattr(value, "value", value)
        column_name = getattr(column, "name", column)
        for name in env_names(literal, key=column_name if isinstance(column_name, str) else None):
            raise EnvReferenceRefused(name)


def check_written(stmt: Any) -> None:
    """:func:`check_values` for every row a Core INSERT or UPDATE carries."""
    table = stmt.table
    if not _holds(table) or not _member_writes():
        return
    check_values(table, stmt._values or {})
    for batch in stmt._multi_values:
        for row in batch:
            check_values(table, row if isinstance(row, dict) else dict(zip(table.columns, row)))


def check_raw(sql: str, args: tuple[Any, ...]) -> None:
    """Refuse a raw SQL INSERT or UPDATE a member of a held organization runs when a value bound
    to it names a variable of the server's environment. The raw writes of tables whose text is
    resolved are listed, each with why it carries no entered text
    (tests/unit/test_env_reference_save_paths.py)."""
    if not args or not _member_writes():
        return
    from provisa.core.model_change import raw_target

    target = raw_target(sql)
    if target is None or target[0] == "delete" or target[1] not in _held_tables():
        return
    for value in args:
        for name in env_names(value):
            raise EnvReferenceRefused(name)


def edit_to_check(stmt: Any) -> Any | None:
    """The SELECT of the rows ``stmt`` would change, when ``stmt`` is a member's UPDATE in a held
    organization (its rows are then read by :func:`check_edited`); None when it is not held to
    the rule. An UPDATE that retires a row is the delete it stands for (REQ-2248), which this
    rule does not restrict."""
    from sqlalchemy import Update, select

    from provisa.core.model_row_rules import retires

    if not isinstance(stmt, Update) or retires(stmt):
        return None
    if not _holds(stmt.table) or not _member_writes():
        return None
    query = select(stmt.table)
    return query if stmt.whereclause is None else query.where(stmt.whereclause)


def check_edited(table: Any, rows: list[Any]) -> None:
    """Refuse the UPDATE whose rows are ``rows`` when one already carries a reference to the
    server's environment: it is the operator's object."""
    for row in rows:
        if next(env_names(dict(row._mapping)), None) is not None:
            raise EnvReferenceEditRefused(str(table.name))


def refuse_named_by_org(variable: str | None) -> None:
    """Refuse, at use, a key variable a held organization's own settings name. One cannot be
    saved (:func:`check_values`); a row that holds one all the same is not read. The operator's
    own endpoints (the deployment's configuration, a sandbox tier's) are not in the
    organization's settings and are read as before."""
    if not variable or held_org() is None:
        return
    from provisa.core.request_context import acting_org_overrides

    own = acting_org_overrides()
    named = set(env_names({k: own.get(k) for k in ("ai_endpoints", "vector_models")}))
    for name in env_names({"api_key_env": variable}):
        if name in named:
            raise EnvReferenceRefused(name)
