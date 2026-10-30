# Copyright (c) 2026 Kenneth Stott
# Canary: b80dc38d-4b7f-45a0-95b2-d68d7ffcb223
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CREATE DATABASE and DROP DATABASE over Bolt, kept in the principal's graph session
(REQ-807, REQ-2176).

A Neo4j server holds many databases. Provisa's are the role databases (``provisa_<role>``,
``provisa_ops_<role>``), the home database (``neo4j``, ``provisa``) and ``system`` (REQ-807). A
database a client creates is a name for the home database, the business view of the principal's
default role, kept in the principal's one graph session (REQ-2176): every connection and worker of
the principal selects it, and it ends as the graph session ends (cleared, past its retention, or
its principal's rights changed). Nothing persistent is made, and no store: a statement run in a
created database is the home database's statement, its writes governed as there. DROP DATABASE
removes the name; what was written through it is the home database's and stays.

As on Neo4j, both are administration commands run against ``system``; another database refuses
them (NotSystemDatabaseError). Supported forms::

    CREATE [OR REPLACE] DATABASE name [IF NOT EXISTS] [WAIT [n [SEC[OND[S]]]] | NOWAIT]
    DROP DATABASE name [IF EXISTS] [DUMP DATA | DESTROY DATA] [WAIT [n [SEC[OND[S]]]] | NOWAIT]

``name`` is a plain or backquoted name, or a ``$parameter``. TOPOLOGY, OPTIONS and composite
databases describe a store Provisa does not have, and are refused by name.
"""

# Requirements: REQ-807, REQ-2176, REQ-2167

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

#: The databases every Provisa server has, which a client neither creates nor drops.
RESERVED = frozenset({"system", "neo4j", "provisa"})

NOT_SYSTEM = "Neo.ClientError.Statement.NotSystemDatabaseError"
EXISTS = "Neo.ClientError.Database.ExistingDatabaseFound"
NOT_FOUND = "Neo.ClientError.Database.DatabaseNotFound"
ARGUMENT = "Neo.ClientError.Statement.ArgumentError"
UNSUPPORTED = "Neo.ClientError.Statement.UnsupportedAdministrationCommand"

# A database name as Neo4j takes one: an ASCII letter, then letters, digits, dots and dashes,
# 3 to 63 characters in all.
_VALID = re.compile(r"[a-z][a-z0-9.\-]{2,62}")

_NAME = r"(?P<name>`(?:[^`]|``)+`|\$[A-Za-z_]\w*|[A-Za-z][\w.\-]*)"
_WAIT = r"(?:\s+(?P<wait>WAIT(?:\s+\d+(?:\s+SEC(?:OND(?:S)?)?)?)?|NOWAIT))?"
_END = r"\s*;?\s*"
_HEAD = re.compile(r"\s*(?:CREATE(?:\s+OR\s+REPLACE)?|DROP)\s+(?:COMPOSITE\s+)?DATABASE\b", re.I)
_CREATE = re.compile(
    rf"\s*CREATE(?P<replace>\s+OR\s+REPLACE)?\s+DATABASE\s+{_NAME}"
    rf"(?P<cond>\s+IF\s+NOT\s+EXISTS)?{_WAIT}{_END}",
    re.I | re.S,
)
_DROP = re.compile(
    rf"\s*DROP\s+DATABASE\s+{_NAME}(?P<cond>\s+IF\s+EXISTS)?"
    rf"(?:\s+(?:DUMP|DESTROY)\s+DATA)?{_WAIT}{_END}",
    re.I | re.S,
)

#: The answer a WAIT gives: the one server, caught up.
WAIT_COLUMNS = ["address", "state", "message", "success"]
_ADDRESS = "localhost:17687"


class DatabaseCommandError(Exception):
    """A database command refused: ``code`` is the Neo4j status the client is told."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class DatabaseCommand:
    """One CREATE DATABASE or DROP DATABASE, as written."""

    create: bool
    name: str  # as written: plain, `quoted` or $parameter
    conditional: bool  # IF NOT EXISTS / IF EXISTS
    replace: bool  # OR REPLACE
    wait: bool


def parse(cypher: str) -> DatabaseCommand | None:
    """The database command ``cypher`` is, or None when it is none (any other statement)."""
    head = _HEAD.match(cypher)
    if head is None:
        return None
    if re.search(r"\bCOMPOSITE\b", head.group(0), re.I):
        raise DatabaseCommandError(
            UNSUPPORTED, "composite databases are not supported: Provisa has one graph per role"
        )
    create = _CREATE.fullmatch(cypher)
    drop = None if create else _DROP.fullmatch(cypher)
    found = create or drop
    if found is None:
        raise DatabaseCommandError(
            UNSUPPORTED,
            "only CREATE [OR REPLACE] DATABASE name [IF NOT EXISTS] [WAIT|NOWAIT] and DROP "
            "DATABASE name [IF EXISTS] [DUMP|DESTROY DATA] [WAIT|NOWAIT] are supported: a "
            "created database names the home database, so TOPOLOGY and OPTIONS have no store "
            "to describe",
        )
    wait = (found.group("wait") or "").upper()
    replace = bool(create and create.group("replace"))
    if replace and found.group("cond"):
        raise DatabaseCommandError(
            ARGUMENT, "CREATE OR REPLACE DATABASE cannot be used with IF NOT EXISTS"
        )
    return DatabaseCommand(
        create=create is not None,
        name=found.group("name"),
        conditional=bool(found.group("cond")),
        replace=replace,
        wait=wait.startswith("WAIT"),
    )


def database_name(written: str, parameters: dict[str, Any]) -> str:
    """The database a command's name names, as Neo4j compares names: lower case."""
    if written.startswith("$"):
        value = parameters.get(written[1:])
        if not isinstance(value, str):
            raise DatabaseCommandError(
                ARGUMENT, f"parameter {written} must be a database name (a string), got {value!r}"
            )
        name = value
    elif written.startswith("`"):
        name = written[1:-1].replace("``", "`")
    else:
        name = written
    return name.lower()


def may_be_created(name: str) -> bool:
    """Whether ``name`` (a RUN's database) could name a created database: a valid name, none of
    the server's own. A role database (``provisa_<role>``) never could: its underscore is no
    character a created name holds."""
    lowered = name.lower()
    return (
        _VALID.fullmatch(lowered) is not None
        and not lowered.startswith("system")
        and lowered not in RESERVED
    )


def _valid(name: str) -> str:
    if not may_be_created(name):
        raise DatabaseCommandError(
            ARGUMENT,
            f"{name!r} is not a valid database name: 3 to 63 ASCII letters, digits, dots and "
            "dashes, starting with a letter and not with 'system'",
        )
    return name


def _held(session: Any) -> set[str]:
    from provisa.ephemeral import store

    return store.graph(session).databases


def created(session: Any) -> set[str]:
    """The databases created in ``session`` (a graph session's TempSession)."""
    return set(_held(session))


def run(
    command: DatabaseCommand,
    *,
    database: Any,
    parameters: dict[str, Any],
    multitenancy: bool,
    org_id: str | None,
) -> tuple[list[str], list[list[Any]]]:
    """Run ``command`` in the graph session bound now, the RUN naming ``database``.

    Under multi-tenancy a database name carries its org (``<org>.<name>``, REQ-1235): the org is
    the session's own, and the name is kept without it."""
    from provisa.api import graph_session
    from provisa.api.org_resolve import OrgResolutionError
    from provisa.bolt.session import select_database

    verb = "CREATE DATABASE" if command.create else "DROP DATABASE"
    if database != "system":
        raise DatabaseCommandError(
            NOT_SYSTEM,
            "This is an administration command and it should be executed against the system "
            f"database: {verb}",
        )
    bound = graph_session.bound()
    if bound is None:
        raise RuntimeError("a database command runs in its principal's graph session")
    written = database_name(command.name, parameters)
    try:
        org, name = select_database(written, multitenancy=multitenancy)
    except OrgResolutionError as exc:
        raise DatabaseCommandError(ARGUMENT, str(exc)) from exc
    if org is not None and org != org_id:
        raise DatabaseCommandError(
            ARGUMENT, f"this session is bound to org {org_id!r}; {written!r} names {org!r}"
        )
    held = _held(bound.session)
    if name in RESERVED or name.startswith("provisa_"):
        if command.create and command.conditional:
            return _answer(command)
        raise DatabaseCommandError(
            EXISTS if command.create else ARGUMENT,
            f"database {written!r} is one of the server's own and cannot be "
            + ("created" if command.create else "dropped"),
        )
    name = _valid(name)
    if command.create:
        if name in held and not command.replace:
            if command.conditional:
                return _answer(command)
            raise DatabaseCommandError(EXISTS, f"database {written!r} already exists")
        held.add(name)
    else:
        if name not in held:
            if command.conditional:
                return _answer(command)
            raise DatabaseCommandError(NOT_FOUND, f"database {written!r} does not exist")
        held.discard(name)
    return _answer(command)


def _answer(command: DatabaseCommand) -> tuple[list[str], list[list[Any]]]:
    """What the command answers: WAIT, the one server caught up; NOWAIT or neither, nothing."""
    if not command.wait:
        return [], []
    return WAIT_COLUMNS, [[_ADDRESS, "CaughtUp", "caught up", True]]
