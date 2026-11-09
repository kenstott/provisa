# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e1f64-8a27-4d5b-b0e6-7f2a91d4c583
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A token minted for one call of a code source's function (REQ-2226, A LISTENER FOR MINTED TOKENS).

The token is the only credential the minted-token listener (:mod:`provisa.pgwire.minted_server`)
accepts. It is scoped to one call and bound to the calling role: a connection presents the call's
id as its user and the token as its password, and is admitted as that role and no other, whatever
the deployment's authentication is. What it may run is limited to reading the tables the call was
given (:func:`refuse_outside_call`), and every statement goes through the one governed pipeline as
that role, so a column the role cannot see is not available to the code.

The tokens live in the platform control plane, because a connection reaches any process of a
``uvicorn --workers N`` server (the listeners share one port, REQ-1900) and any node: a token
minted in one must be good in all, and gone from all when the call ends. Only the SHA-256 of the
token is stored (256 bits of machine-generated entropy, as a personal access token's, REQ-1263).
A token ends with its call, and a process that dies mid-call leaves one that the expiry ends."""

# Requirements: REQ-2226
from __future__ import annotations

import datetime
import hashlib
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp
from sqlalchemy import delete, insert, select
from sqlglot.optimizer.scope import Scope, traverse_scope

from provisa.core.schema_admin import minted_call_tokens

TOKEN_PREFIX = "provisa_call_"
_TOKEN_BYTES = 32

#: Bounds a token whose call never ended (a process that died mid-call); a call's end retires it
#: long before this. Longer than any call runs, not a limit on one.
EXPIRY_S = 6 * 3600.0

#: Schemas every PostgreSQL driver reads as it starts and describes a result. They answer what the
#: role may see, and name no data.
_CATALOG_SCHEMAS = frozenset({"pg_catalog", "information_schema"})

#: What a driver sends around its reads; nothing else but a read runs here.
_SESSION_STATEMENTS = (exp.Set, exp.Transaction, exp.Commit, exp.Rollback)
_SESSION_COMMANDS = frozenset({"SET", "RESET", "SHOW", "BEGIN", "START", "COMMIT", "ROLLBACK"})

#: Settings that would name another principal: the token decides who the session is.
_PRINCIPAL_SETTINGS = ("role", "session authorization", "session_authorization", "provisa.role")

_WRITES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Copy,
)


class CallTokenRefused(PermissionError):
    """A statement a minted session may not run, or a table the call cannot be given, named."""


@dataclass(frozen=True)
class MintedCall:
    """What a valid token stands for."""

    call_id: str
    role_id: str
    members: tuple[str, ...]
    qualifiers: tuple[str, ...]
    user_id: str
    org_id: str | None
    tables: tuple[str, ...]
    session_vars: dict[str, str]
    entitlements: dict | None


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _aware(value: datetime.datetime) -> datetime.datetime:
    # A control plane on SQLite reads back naive datetimes (as pat.py's _as_aware).
    return value.replace(tzinfo=datetime.timezone.utc) if value.tzinfo is None else value


class MintedTokenStore:
    """Issues, resolves and retires the tokens in the platform control plane."""

    def __init__(self, admin_db: Any) -> None:
        self._db = admin_db

    async def mint(self, call: MintedCall, *, ttl_s: float = EXPIRY_S) -> str:
        """A new token for ``call``; shown here once, then only its hash exists."""
        token = TOKEN_PREFIX + secrets.token_urlsafe(_TOKEN_BYTES)
        now = _utcnow()
        async with self._db.acquire() as conn:
            await conn.execute_core(
                delete(minted_call_tokens).where(minted_call_tokens.c.expires_at <= now)
            )
            await conn.execute_core(
                insert(minted_call_tokens).values(
                    token_hash=hash_token(token),
                    call_id=call.call_id,
                    role_id=call.role_id,
                    members=list(call.members),
                    qualifiers=list(call.qualifiers),
                    user_id=call.user_id,
                    org_id=call.org_id,
                    tables=list(call.tables),
                    session_vars=call.session_vars,
                    entitlements=call.entitlements,
                    expires_at=now + datetime.timedelta(seconds=ttl_s),
                )
            )
        return token

    async def resolve(self, token: str, call_id: str) -> MintedCall | None:
        """What ``token`` stands for when presented for ``call_id``; None for a wrong token, an
        expired one, one retired with its call, or one minted for another call."""
        if not token.startswith(TOKEN_PREFIX):
            return None
        row = await self._row(hash_token(token))
        if row is None or row["call_id"] != call_id:
            return None
        return MintedCall(
            call_id=row["call_id"],
            role_id=row["role_id"],
            members=tuple(row["members"]),
            qualifiers=tuple(row["qualifiers"]),
            user_id=row["user_id"],
            org_id=row["org_id"],
            tables=tuple(row["tables"]),
            session_vars=dict(row["session_vars"]),
            entitlements=row["entitlements"],
        )

    async def live(self, token_hash: str) -> bool:
        return await self._row(token_hash) is not None

    async def _row(self, token_hash: str) -> Any:
        async with self._db.acquire() as conn:
            found = await conn.execute_core(
                select(minted_call_tokens).where(minted_call_tokens.c.token_hash == token_hash)
            )
            row = found.fetchone()
        if row is None:
            return None
        row = row._mapping
        return row if _aware(row["expires_at"]) > _utcnow() else None

    async def retire(self, token: str) -> None:
        async with self._db.acquire() as conn:
            await conn.execute_core(
                delete(minted_call_tokens).where(
                    minted_call_tokens.c.token_hash == hash_token(token)
                )
            )


def _name_part(identifier: Any) -> str:
    return identifier.name if identifier.quoted else identifier.name.lower()


def _table_key(table: exp.Table) -> tuple[str, ...]:
    return tuple(
        _name_part(table.args[part])
        for part in ("catalog", "db", "this")
        if table.args.get(part) is not None
    )


def _reference(ref: str) -> exp.Table:
    """``ref`` parsed as a table name and nothing more: no alias, no join, no function."""
    try:
        table = sqlglot.parse_one(ref, into=exp.Table, dialect="postgres")
    except sqlglot.errors.SqlglotError as exc:
        raise CallTokenRefused(f"{ref!r} is not a table reference") from exc
    extras = {k for k, v in table.args.items() if v and k not in ("this", "db", "catalog")}
    if extras or not isinstance(table.this, exp.Identifier):
        raise CallTokenRefused(f"{ref!r} is not a table reference")
    return table


def table_key(ref: str) -> tuple[str, ...]:
    """A table reference as a call names it, as :func:`refuse_outside_call` compares names."""
    return _table_key(_reference(ref))


def read_statement(ref: str, columns: list[str]) -> str:
    """The statement that reads a call's table: its declared columns, or all the role sees."""
    projection = [exp.column(c, quoted=True) for c in columns] if columns else [exp.Star()]
    return exp.select(*projection).from_(_reference(ref)).sql(dialect="postgres")


def _refuse_session_statement(tree: exp.Expr) -> None:
    text = tree.sql(dialect="postgres").lower()
    if any(setting in text for setting in _PRINCIPAL_SETTINGS):
        raise CallTokenRefused("a minted session cannot change the role its token is bound to")


def _cte_references(tree: exp.Expr) -> set[int]:
    """The ``exp.Table`` nodes (by identity) that name a common table expression. Scopes resolve
    this, not the names: in a non-recursive ``WITH t AS (SELECT * FROM t)``, the inner ``t`` is a
    real table."""
    references: set[int] = set()
    for scope in traverse_scope(tree):
        for table in scope.tables:
            if isinstance(scope.sources.get(table.alias_or_name), Scope):
                references.add(id(table))
    return references


def _refuse_outside_tables(tree: exp.Expr, keys: set[tuple[str, ...]]) -> None:
    references = _cte_references(tree)
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise CallTokenRefused("a minted session reads its call's tables, not a function")
        if id(table) in references:
            continue
        schema = table.db.lower()
        if schema in _CATALOG_SCHEMAS or (not schema and table.name.lower().startswith("pg_")):
            continue
        if _table_key(table) not in keys:
            raise CallTokenRefused(
                f"a minted session reads only the tables given to its call; "
                f"{table.sql(dialect='postgres')!r} was not"
            )


def _parsed(sql: str) -> list[exp.Expr]:
    try:
        return [tree for tree in sqlglot.parse(sql, dialect="postgres") if tree is not None]
    except sqlglot.errors.SqlglotError as exc:
        raise CallTokenRefused("a minted session runs only statements it can read") from exc


def _refuse_hidden_columns(sql: str, role_id: str, org_id: str | None, state: Any) -> None:
    """Raise :class:`CallTokenRefused` if the statement selects a column the calling role cannot
    see, regardless of whether the governed pipeline would refuse it. This is the admission-layer
    enforcement: it fires before the query reaches the pipeline and prevents a column-level grant
    bypass that could occur when the minted session's governance context is built without the full
    table map (REQ-2226)."""
    from provisa.security.inheritance import expand_column_grants
    from provisa.security.rights import column_served, reaches_domain

    # state.tables requires current_org; bind it explicitly since check() fires before _with_org.
    if org_id is not None:
        from provisa.core.request_context import reset_current_org, set_current_org

        _tok = set_current_org(org_id)
        try:
            tables = list(getattr(state, "tables", []))
            roles: dict = dict(getattr(state, "roles", {}) or {})
        finally:
            reset_current_org(_tok)
    else:
        tables = list(getattr(state, "tables", []))
        roles = dict(getattr(state, "roles", {}) or {})

    role = roles.get(role_id)
    if role is None:
        return
    chains: dict[str, list[str]] = {rid: d.get("chain", [rid]) for rid, d in roles.items()}

    for tree in _parsed(sql):
        if not isinstance(tree, exp.Query):
            continue
        select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
        if select is None:
            continue
        from_part = select.args.get("from")
        if from_part is None:
            continue
        # Build a map of alias/name → registered table row for every real table in this SELECT.
        tables_in_scope: dict[str, dict] = {}
        for tbl in from_part.find_all(exp.Table):
            if not isinstance(tbl.this, exp.Identifier):
                continue
            tbl_name = tbl.name.lower()
            tbl_qualifier = (tbl.db or "").lower()
            for raw in tables:
                if (raw.get("table_name") or "").lower() != tbl_name:
                    continue
                raw_domain = (raw.get("domain_id") or "").lower()
                raw_schema = (raw.get("schema_name") or "").lower()
                if tbl_qualifier and tbl_qualifier not in (raw_domain, raw_schema):
                    continue
                alias = (tbl.alias or tbl.name).lower()
                tables_in_scope[alias] = raw
                break

        for expr in select.expressions:
            if isinstance(expr, exp.Star):
                continue
            for col in expr.find_all(exp.Column):
                qualifier = col.table.lower() if col.table else None
                raw = (
                    tables_in_scope.get(qualifier)
                    if qualifier
                    else next(iter(tables_in_scope.values()), None)
                )
                if raw is None:
                    continue
                col_name = col.name
                col_dict = next(
                    (
                        c
                        for c in (raw.get("columns") or [])
                        if (c.get("column_name") or "").lower() == col_name.lower()
                    ),
                    None,
                )
                if col_dict is None:
                    continue
                # Expand grants down descendant chains (a grant to ancestor reaches child).
                col_copy = {**col_dict, "visible_to": list(col_dict.get("visible_to") or [])}
                expand_column_grants([{"columns": [col_copy]}], chains)
                domain_id = raw.get("domain_id")
                reaches = not domain_id or reaches_domain(role.get("domain_access", []), domain_id)
                if not column_served({"id": role_id}, domain_id, col_copy, reaches=reaches):
                    raise CallTokenRefused(
                        f"column {col_name!r} of {raw.get('table_name')!r} "
                        f"is not visible to role {role_id!r}"
                    )


def refuse_outside_call(sql: str, tables: tuple[str, ...], state: Any) -> None:
    """Raise :class:`CallTokenRefused` for anything but a read of ``tables`` (and the catalog a
    driver reads), or a session statement. The AST decides; a statement that does not parse is
    refused, and no table the walk cannot place is let through."""
    from provisa.pgwire._pipeline import _calls_a_registered_command

    keys = {table_key(t) for t in tables}
    for tree in _parsed(sql):
        if isinstance(tree, _SESSION_STATEMENTS):
            _refuse_session_statement(tree)
            continue
        if isinstance(tree, exp.Command):
            if tree.name.upper() not in _SESSION_COMMANDS:
                raise CallTokenRefused(f"a minted session cannot run {tree.name.upper()}")
            _refuse_session_statement(tree)
            continue
        if not isinstance(tree, exp.Query) or tree.find(*_WRITES) is not None:
            raise CallTokenRefused("a minted session only reads")
        if _calls_a_registered_command(tree, state):
            raise CallTokenRefused("a minted session cannot call a command")
        _refuse_outside_tables(tree, keys)


class CallScope:
    """One admitted minted connection: what it may run, and in whose name."""

    def __init__(
        self, call: MintedCall, token_hash: str, store: MintedTokenStore, state: Any
    ) -> None:
        self.call = call
        self._token_hash = token_hash
        self._store = store
        self._state = state
        self._checked: list[str] = []

    def check(self, sql: str) -> None:
        """Refuse ``sql`` unless the call is still live and the statement is within it."""
        from provisa.core.connection_loop import run_on_connection_loop

        if not run_on_connection_loop(self._store.live(self._token_hash), timeout=30):
            raise CallTokenRefused("the call this session was minted for has ended")
        refuse_outside_call(sql, self.call.tables, self._state)
        # REQ-2226: enforce column-level visibility at the admission layer so a column the
        # calling role cannot see is never available to the code, independent of whether the
        # pipeline's governance context holds the full table map for this table.
        _refuse_hidden_columns(sql, self.call.role_id, self.call.org_id, self._state)

    def check_all(self, statements: list[str]) -> None:
        """Check a simple Query's statements before any of them runs: COPY and DO blocks are
        served on that path without passing :meth:`admit`, and the rest pass it once."""
        for sql in statements:
            self.check(sql)
        self._checked = list(statements)

    def admit(self, sql: str) -> None:
        """What every statement of the connection passes, on either protocol."""
        if sql in self._checked:
            self._checked.remove(sql)
            return
        self.check(sql)

    @contextmanager
    def bound(self) -> Iterator[None]:
        """The caller's session variables and entitlements for the statement, and the call's
        correlation id for its audit row (REQ-1682, REQ-2219, REQ-886): the session sees what
        the caller sees, no more."""
        from provisa.auth.derived_roles import session_vars_scope
        from provisa.entitlements.runtime import restored_entitlements
        from provisa.otel_compat import udf_correlation_scope

        with (
            session_vars_scope(self.call.session_vars),
            restored_entitlements(self.call.entitlements),
            udf_correlation_scope(self.call.call_id),
        ):
            yield
