# Copyright (c) 2026 Kenneth Stott
# Canary: f2f24357-fb14-4532-ab67-b234c588c41c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``resolve(uri, as_of?)``: the governed value a semantic address names (REQ-1365).

A semantic address is the REQ-1385 table address extended to one row and, optionally, one
column of it::

    provisa://<org>/<domain path>/tables/<table>/pk=<value>[/<column>]

``<org>`` is present only in a multi-tenant deployment, exactly as the exported addresses carry
it. A composite key is ``pk=<v1>,<v2>`` in the model's key-column order, each value
percent-encoded. The column may also be written as the REQ-1385 fragment ``#field:<column>``;
the path form is canonical. With no column the whole governed row is returned.

There is one function here, :func:`resolve`, and every surface (the GraphQL ``resolve`` field,
REST ``GET /data/rest/resolve``, the MCP ``resolve`` tool) is a thin wrapper over it. It builds
ONE ``SELECT`` over the table's semantic name with the key values as bound parameters and hands
it to the settled pipeline (``_govern_and_route`` then ``_execute_plan``). Role scoping, RLS,
masking, the row cap, the audit record and its provenance all come from that path; nothing here
reimplements any of them, and there is no second pipeline.

What is refused, by name:

* an address that is not ``provisa://`` (a physical address: a connection URL, an object-store
  path, a source/schema/table triple);
* an address in another org;
* a table with no primary key in the model (row selection is by key, never free-form SQL);
* ``as_of`` on a table with no bitemporal mode. Answering a historical address with today's
  value would be a wrong answer that looks right.

A table or column the caller's role cannot see, and a row RLS hides, are indistinguishable from
ones that do not exist: the first two raise the same :class:`AddressNotFound`, the last returns
no row.

With regions, a resolve runs in the node's region like any other read; it has no path of its
own.
"""

# Requirements: REQ-1365, REQ-1385

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import unquote

SCHEME = "provisa"
_TABLES = "tables"
_KEY_PREFIX = "pk="
_FIELD_PREFIX = "field:"


class AddressError(ValueError):
    """The address cannot be dereferenced; the message says why."""


class AddressNotFound(LookupError):
    """The address names nothing the caller may see."""


@dataclass(frozen=True)
class SemanticAddress:
    """A parsed semantic address. ``org_id`` is None when the address carries no org."""

    org_id: str | None
    domain_id: str
    table: str
    key: tuple[str, ...]
    column: str | None


def parse_address(uri: str, *, multitenant: bool) -> SemanticAddress:
    """Parse a semantic address. Pure; raises :class:`AddressError` for anything else."""
    scheme, sep, rest = uri.partition("://")
    if not sep or scheme != SCHEME:
        raise AddressError(
            f"{uri!r} is not a semantic address: only provisa:// addresses are dereferenceable, "
            "a physical address is refused"
        )
    rest, _, fragment = rest.partition("#")
    segments = rest.split("/")
    if "" in segments:
        raise AddressError(f"{uri!r} has an empty path segment")
    org_id: str | None = None
    if multitenant:
        org_id = unquote(segments[0])
        segments = segments[1:]
    if _TABLES not in segments:
        raise AddressError(
            f"{uri!r} does not address a table value: expected .../tables/<table>/pk=<value>"
        )
    marker = segments.index(_TABLES)
    domain_id = "/".join(unquote(part) for part in segments[:marker])
    tail = segments[marker + 1 :]
    if len(tail) not in (2, 3) or not tail[1].startswith(_KEY_PREFIX):
        raise AddressError(
            f"{uri!r} does not address a row: expected .../tables/<table>/pk=<value>[/<column>]"
        )
    key = tuple(unquote(value) for value in tail[1][len(_KEY_PREFIX) :].split(","))
    if "" in key:
        raise AddressError(f"{uri!r} has an empty primary-key value")
    column = unquote(tail[2]) if len(tail) == 3 else None  # noqa: PLR2004 - table, key, column
    if fragment:
        if not fragment.startswith(_FIELD_PREFIX) or column is not None:
            raise AddressError(
                f"{uri!r}: the fragment must be #field:<column>, and only when the path names "
                "no column"
            )
        column = unquote(fragment[len(_FIELD_PREFIX) :])
    return SemanticAddress(org_id, domain_id, unquote(tail[0]), key, column)


@dataclass(frozen=True)
class ResolvedStatement:
    """The one statement a resolve runs, and what it reads."""

    sql: str
    params: list[str]
    table_name: str


def _not_found(uri: str) -> AddressNotFound:
    return AddressNotFound(f"{uri!r} names nothing visible to this role")


def _table_meta(ctx: Any, address: SemanticAddress) -> Any:
    """The table the address names in the role's compiled context, by domain and business name
    (the alias when the table has one, else its registered name, as ``refs.table_uri`` writes
    it). None when the role's schema holds no such table."""
    seen: set[int] = set()
    for meta in ctx.tables.values():
        if meta.table_id in seen:
            continue
        seen.add(meta.table_id)
        if meta.domain_id != address.domain_id:
            continue
        business_name = meta.display_name or meta.original_table_name or meta.table_name
        if business_name == address.table:
            return meta
    return None


def _visible_columns(ctx: Any, table_id: int) -> dict[str, str]:
    """``{physical column: name the semantic SQL surface exposes}`` for the role's visible
    columns of one table."""
    from provisa.compiler.naming import apply_sql_name

    return {
        physical: exposed or apply_sql_name(physical)
        for (tid, physical), exposed in ctx.physical_to_sql.items()
        if tid == table_id
    }


def build_statement(
    uri: str, address: SemanticAddress, ctx: Any, *, as_of: str | None, bitemporal: set[str]
) -> ResolvedStatement:
    """The ``SELECT`` for ``address`` against the role's compiled context ``ctx``. Pure."""
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    meta = _table_meta(ctx, address)
    if meta is None:
        raise _not_found(uri)
    columns = _visible_columns(ctx, meta.table_id)
    key_columns = ctx.pk_columns.get(meta.table_id) or []
    if not key_columns:
        raise AddressError(
            f"table {address.table!r} has no primary key in the model, so a row of it cannot "
            "be addressed"
        )
    if len(key_columns) != len(address.key):
        raise AddressError(
            f"table {address.table!r} is keyed by {len(key_columns)} column(s) "
            f"({', '.join(key_columns)}); the address gives {len(address.key)} value(s)"
        )
    registered_name = meta.original_table_name or meta.table_name
    if as_of is not None and registered_name not in bitemporal:
        raise AddressError(
            f"table {address.table!r} keeps no history (it has no bitemporal mode), so it "
            "cannot be resolved as of a point in time"
        )
    if address.column is None:
        projection = "*"
    else:
        exposed = {name: name for name in columns.values()} | columns
        if address.column not in exposed:
            raise _not_found(uri)
        projection = f'"{exposed[address.column]}"'
    predicate = " AND ".join(
        f'"{columns[column]}" = ${position}' for position, column in enumerate(key_columns, 1)
    )
    table_ref = f'"{domain_to_sql_name(meta.domain_id)}"."{semantic_table_name(meta)}"'
    return ResolvedStatement(
        sql=f"SELECT {projection} FROM {table_ref} WHERE {predicate}",
        params=list(address.key),
        table_name=address.table,
    )


async def resolve(
    state: Any,
    role_id: str,
    uri: str,
    *,
    org_id: str | None,
    as_of: str | None = None,
    as_recorded: str | None = None,
    as_of_literal: str | None = None,
    session_vars: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Dereference ``uri`` for ``role_id`` through the one query pipeline (REQ-1365).

    ``org_id`` is the caller's org; an address in another org is refused. ``as_of`` is an ISO
    8601 point in time on the view's clock and ``as_recorded`` one on Provisa's record (REQ-1163),
    both validated here; ``as_of_literal`` is the canonical value a surface has already validated
    (the GraphQL endpoint's ``X-Provisa-As-Of`` and ``X-Provisa-As-Recorded``). Returns
    ``{"uri", "as_of", "columns", "row"}`` with ``row`` None when the key matches nothing the
    caller may see; for an as-of range (REQ-2064), ``versions`` -- every version of the row in
    force within it -- in place of ``row``."""
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    from provisa.mv.bitemporal import AsOf, request_as_of

    if as_of is not None or as_recorded is not None:
        try:
            # Validated SQL timestamp literals, in the canonical form (REQ-1163).
            as_of_literal = request_as_of(as_of, as_recorded)
        except ValueError as exc:
            raise AddressError(f"invalid as_of or as_recorded: {exc}") from exc
    address = parse_address(uri, multitenant=bool(state.multitenancy))
    if address.org_id is not None and address.org_id != org_id:
        raise AddressError(f"{uri!r} is an address in another organization")
    from provisa.api.role_surface import role_surface

    surface = role_surface(state, role_id)
    if surface is None:
        raise PermissionError(f"No schema for role {role_id!r}")
    statement = build_statement(
        uri,
        address,
        surface.context,
        as_of=as_of_literal,
        bitemporal=set(state.bitemporal_view_reads),
    )
    plan = await _govern_and_route(
        statement.sql,
        role_id,
        session_vars=session_vars,
        as_of=as_of_literal,
        params=statement.params,
    )
    result = await _execute_plan(plan, state)
    columns = list(result.column_names)
    rows = [dict(zip(columns, r, strict=True)) for r in result.rows]
    answer = {"uri": uri, "as_of": as_of_literal, "columns": columns}
    if as_of_literal is not None and AsOf.from_key(as_of_literal).is_range:
        # REQ-2064: a range answers each version of the row in force within it, in source order.
        return {**answer, "versions": rows}
    return {**answer, "row": rows[0] if rows else None}


# -- the GraphQL field (REQ-1929: the other surfaces offer what GraphQL offers) -------------------


def resolve_selections(document: Any) -> tuple[list[Any], list[str]]:
    """The ``resolve`` root selections of a GraphQL document, and the names of its other root
    fields."""
    from graphql import FieldNode

    from provisa.compiler.schema_gen import RESOLVE_FIELD

    found: list[Any] = []
    others: list[str] = []
    for definition in document.definitions:
        selection_set = getattr(definition, "selection_set", None)
        if selection_set is None:
            continue
        for selection in selection_set.selections:
            if not isinstance(selection, FieldNode):
                continue
            if selection.name.value == RESOLVE_FIELD:
                found.append(selection)
            else:
                others.append(selection.name.value)
    return found, others


async def execute_resolve_fields(
    selections: list[Any],
    state: Any,
    role_id: str,
    variables: dict | None,
    *,
    org_id: str | None,
    as_of: str | None,
) -> dict[str, Any]:
    """Answer each ``resolve`` root selection, keyed by its response name. ``as_of`` is the
    request's already-validated time travel; a field's own ``as_of``/``as_recorded`` arguments win
    over it."""
    from provisa.compiler.sql_where import _extract_value

    data: dict[str, Any] = {}
    for selection in selections:
        # The schema types both arguments String (uri: String!, as_of: String), so validation has
        # already made them strings.
        args = cast(
            dict[str, str],
            {a.name.value: _extract_value(a.value, variables) for a in selection.arguments},
        )
        key = selection.alias.value if selection.alias else selection.name.value
        own, own_recorded = args.get("as_of"), args.get("as_recorded")
        data[key] = await resolve(
            state,
            role_id,
            args["uri"],
            org_id=org_id,
            as_of=own,
            as_recorded=own_recorded,
            as_of_literal=None if (own or own_recorded) else as_of,
        )
    return data
