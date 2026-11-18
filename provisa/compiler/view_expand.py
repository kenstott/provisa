# Copyright (c) 2026 Kenneth Stott
# Canary: d50b6b33-25ce-413b-aafa-4da7dea9e634
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Inline expansion of non-materialized views at query time.

Replaces table references to view tables with their defining SQL as subqueries.
"""

# Requirements: REQ-133, REQ-134, REQ-135, REQ-136

from __future__ import annotations

import re
import secrets

from provisa.compiler.sql_gen import CompiledQuery

#: The alias prefix a view's SQL is marked with where the statement governance validates is
#: expanded (:func:`expand_view_refs` with ``mark``): ``(SELECT * FROM (<view SQL>) AS
#: "<prefix><view>") <alias>``. The relationship guard reads a marked derived table as the view
#: it expands -- a registered table whose own relationships relate it -- and only one carrying
#: this process's prefix, which no statement a caller writes can name (REQ-603).
VIEW_MARK = f"__view_{secrets.token_hex(8)}__"


def marked_view(alias: str) -> str | None:
    """The view a derived table aliased ``alias`` expands, when the alias is a view mark."""
    return alias[len(VIEW_MARK) :] if alias.startswith(VIEW_MARK) else None


# Keywords that can follow a table reference; never consume one as an alias.
_KW_AFTER_TABLE = (
    "ON|WHERE|GROUP|ORDER|LIMIT|OFFSET|HAVING|JOIN|LEFT|RIGHT|INNER|"
    "OUTER|FULL|CROSS|UNION|EXCEPT|INTERSECT|USING|NATURAL|LATERAL"
)
# Optional table alias: quoted ("t0", "ab") or a bare identifier that is not a
# trailing SQL keyword. `AS` is optional.
_ALIAS = rf'(?P<alias>\s+(?:AS\s+)?(?:"[^"]+"|(?!(?i:{_KW_AFTER_TABLE})\b)[A-Za-z_]\w*))?'


#: The clauses whose comma-separated items are tables: a FROM list, continued past a join and
#: its condition. A comma in any other clause separates expressions, a column among them.
_TABLE_LIST_CLAUSES = frozenset({"FROM", "JOIN", "ON", "USING"})
_CLAUSE_WORDS = _TABLE_LIST_CLAUSES | frozenset(
    {
        "SELECT",
        "WHERE",
        "GROUP",
        "HAVING",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "WINDOW",
        "QUALIFY",
        "VALUES",
        "SET",
        "RETURNING",
    }
)
_TOKEN = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[(),]|[A-Za-z_]\w*")


def _table_position(text: str, start: int) -> bool:
    """Whether the reference at ``start`` of ``text`` is read as a table: right after FROM or
    JOIN, or after a comma of a FROM list. A column shares a view's name as often as not (a
    shared dimension is named for its column, REQ-413), and ``t.name`` is that column."""
    clause: list[str | None] = [None]
    last = ""
    for m in _TOKEN.finditer(text, 0, start):
        tok = m.group()
        if tok == "(":
            clause.append(None)
        elif tok == ")":
            if len(clause) > 1:
                clause.pop()
        elif tok[0] not in "'\"," and tok.upper() in _CLAUSE_WORDS:
            clause[-1] = tok.upper()
        last = tok.upper() if tok[0] not in "'\"" else tok
    if last in ("FROM", "JOIN"):
        return True
    return last == "," and clause[-1] in _TABLE_LIST_CLAUSES


def _expand_one(sql: str, view_table: str, view_sql: str, mark: bool = False) -> str:
    """Replace catalog- and schema-qualified refs to one view table.

    Preserves an explicit alias when present; otherwise injects the view-table
    name as the subquery alias so column references qualified by that name (e.g.
    ``shelter__animalBreeds.name``) still resolve after expansion.
    """

    if mark:
        view_sql = f'SELECT * FROM ({view_sql}) AS "{VIEW_MARK}{view_table}"'

    def repl(m: re.Match) -> str:
        kw = m.groupdict().get("kw") or ""
        if not kw and not _table_position(m.string, m.start()):
            return m.group()  # a column named like the view, not the view
        alias = m.group("alias")
        if alias:
            return f"{kw}({view_sql}){alias}"
        return f"{kw}({view_sql}) {view_table}"

    if "." in view_table:
        return _expand_in_schema(sql, view_table, view_sql)
    vt = re.escape(view_table)
    # Catalog-qualified: "catalog"."schema"."view_table" [alias]
    sql = re.sub(rf'"[^"]+"\."[^"]+"\."{vt}"{_ALIAS}', repl, sql)
    # Schema-qualified: "schema"."view_table" [alias]
    sql = re.sub(rf'"[^"]+"\."{vt}"{_ALIAS}', repl, sql)
    # Unquoted schema-qualified: a view's own definition (e.g. `views.e2e_mv_a`) is raw,
    # user-typed semantic SQL — never quoted/catalog-rewritten like the outer query is
    # before this runs. Nested view chains inline that raw text as-is, so a second (or
    # deeper) level of view reference only ever appears in this unquoted form.
    # Never inside a string literal: a compiled read names its table there (``'city.view_x' AS
    # "_name_"``), which is a value, not a reference.
    sql = re.sub(rf'(?<![\w."\'])(?P<kw>FROM\s+|JOIN\s+)?[A-Za-z_]\w*\.{vt}\b{_ALIAS}', repl, sql)
    # Unquoted bare reference (no schema prefix), only directly after FROM/JOIN so a
    # same-named column or alias elsewhere in the query is never mistaken for the table.
    sql = re.sub(rf"\b(?P<kw>FROM\s+|JOIN\s+){vt}\b{_ALIAS}", repl, sql)
    return sql


def _expand_in_schema(sql: str, view_table: str, view_sql: str) -> str:
    """Replace refs to a view keyed ``schema.name`` -- an ontology Class (REQ-2163), whose name a
    registered table may share -- only where they name its schema: ``"catalog"."schema"."name"``,
    ``"schema"."name"`` or ``schema.name``. The name alone, or under another schema, is not it."""
    schema, name = view_table.split(".", 1)

    def repl(m: re.Match) -> str:
        kw = m.groupdict().get("kw") or ""
        if not kw and not _table_position(m.string, m.start()):
            return m.group()  # a column named like the view, not the view
        alias = m.group("alias")
        return f"{kw}({view_sql}){alias}" if alias else f'{kw}({view_sql}) "{name}"'

    s, n = re.escape(schema), re.escape(name)
    sql = re.sub(rf'"[^"]+"\."{s}"\."{n}"{_ALIAS}', repl, sql)
    sql = re.sub(rf'(?<![\w."]\.)"{s}"\."{n}"{_ALIAS}', repl, sql)
    return re.sub(rf'(?<![\w."\'])(?P<kw>FROM\s+|JOIN\s+)?{s}\.{n}\b{_ALIAS}', repl, sql)


#: What a view's reference is replaced by when only asked whether it is there.
_PROBE = f"SELECT 1 AS {VIEW_MARK}"


def reads_view(sql: str, view_table: str) -> bool:  # REQ-135
    """Whether ``sql`` reads ``view_table`` as a table: what :func:`expand_view_refs` replaces.
    A column sharing the view's name is not a read of it."""
    return _expand_one(sql, view_table, _PROBE) != sql


def expand_view_refs(  # REQ-135
    sql: str, view_sql_map: dict[str, str], *, mark: bool = False
) -> str:
    """Replace view-table references in a SQL string with inline subqueries.

    For each view table in view_sql_map, replaces a catalog- or schema-qualified
    reference  "catalog"."schema"."view_table" [alias]  with  (view_sql) [alias].
    Shared by the GraphQL/SQL path (via expand_views) and the Cypher path.

    A view may itself reference another (nested) view — e.g. a two-level virtual-view
    chain (REQ-966/158/133) — so re-expanding runs to a fixed point: each pass may inline
    text that exposes a further view reference, bounded by map size + 1 to guarantee
    termination (a cycle could never have validated at view-creation time).

    ``mark``: each view's SQL is wrapped in a derived table carrying :data:`VIEW_MARK`, for the
    statement governance validates (REQ-603).
    """
    if not view_sql_map:
        return sql
    for _ in range(len(view_sql_map) + 1):
        changed = False
        for view_table, view_sql in view_sql_map.items():
            new_sql = _expand_one(sql, view_table, view_sql, mark)
            if new_sql != sql:
                changed = True
                sql = new_sql
        if not changed:
            break
    return sql


def expand_views(  # REQ-133, REQ-134, REQ-135, REQ-136
    compiled: CompiledQuery,
    view_sql_map: dict[str, str],
) -> CompiledQuery:
    """Replace view table references in compiled SQL with inline subqueries.

    For each view table in view_sql_map, replaces:
        FROM "schema"."view_table" "alias"
    with:
        FROM (view_sql) "alias"

    Also handles catalog-qualified references:
        FROM "catalog"."schema"."view_table" "alias"
    """
    if not view_sql_map:
        return compiled

    sql = expand_view_refs(compiled.sql, view_sql_map)

    if sql == compiled.sql:
        return compiled

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=compiled.sources,
        table_ids=compiled.table_ids,
    )
