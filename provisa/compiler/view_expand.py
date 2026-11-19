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

import bisect
import re
import secrets
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, SqlglotError

from provisa.compiler.definitions import NotAvailableHere
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


def without_marks(text: str) -> str:
    """``text`` with every view mark taken out: a marked alias ``"<mark>v"`` reads ``"v"``. The
    mark is this process's secret (REQ-039): no text that leaves the server carries it."""
    return text.replace(VIEW_MARK, "")


def parse_marked(sql: str, read: object = "postgres") -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """``sqlglot.parse_one`` of a statement whose views may be expanded marked. An error parsing
    it is the error the statement raises read without its marks -- the same tokens, a mark only
    lengthening a quoted identifier -- so the context sqlglot quotes never holds a mark, whole or
    cut at its window's edge (REQ-039)."""
    try:
        return sqlglot.parse_one(sql, read=read)  # pyright: ignore[reportArgumentType]
    except SqlglotError:
        if VIEW_MARK not in sql:
            raise
    sqlglot.parse_one(without_marks(sql), read=read)  # pyright: ignore[reportArgumentType]
    # Read without its marks it parses: what failed is the marks' own text, which no client wrote.
    raise ParseError("a statement whose views were expanded could not be read") from None


@contextmanager
def marks_withheld() -> Iterator[None]:
    """Run a step over a marked statement so an error it raises carries no view mark: each
    string argument of the error, and of every error it was raised from, is read
    :func:`without_marks`, and the same error is raised on (REQ-039)."""
    try:
        yield
    except Exception as exc:
        seen: set[int] = set()
        err: BaseException | None = exc
        while err is not None and id(err) not in seen:
            seen.add(id(err))
            err.args = tuple(without_marks(a) if isinstance(a, str) else a for a in err.args)
            err = err.__cause__ or err.__context__
        raise


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


def _sub(pattern: str, repl: Callable[[re.Match[str]], str | None], sql: str) -> str:
    """``re.sub`` of ``pattern`` over the code of ``sql`` only: a match is looked for in
    :func:`_code_view` of ``sql`` (comments and string-literal contents blanked) and never starts
    inside a quoted identifier, so text a comment, a literal or an identifier holds is never read
    as a reference. ``repl`` returns ``None`` to keep the matched text."""
    view, inert = _code_view(sql)
    out: list[str] = []
    pos = 0
    for m in re.finditer(pattern, view):
        if _inside(inert, m.start()):
            continue
        replacement = repl(m)
        if replacement is None:
            continue
        out.append(sql[pos : m.start()])
        out.append(replacement)
        pos = m.end()
    out.append(sql[pos:])
    return "".join(out)


#: What blanks a character a comment or a string literal holds in :func:`_code_view`: no pattern
#: here matches it, not even ``\s``.
_BLANK = "\x00"
_DOLLAR_TAG = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")


def _code_view(sql: str) -> tuple[str, list[tuple[int, int]]]:  # noqa: C901  # allow-complexity: one SQL lexer
    """``sql`` with every comment blanked whole and every string literal's contents blanked (its
    quotes kept), the same length; and the spans inside quoted identifiers. Read as PostgreSQL
    lexes it: ``--`` to the line's end, nested ``/* */``, ``'...'`` with ``''``, ``E'...'`` with
    backslash escapes too, ``$tag$...$tag$``, ``"..."`` with ``""``. An unterminated one runs to
    the end."""
    chars = list(sql)
    inert: list[tuple[int, int]] = []
    n = len(sql)
    i = 0

    def blank(a: int, b: int) -> None:
        for k in range(a, b):
            chars[k] = _BLANK

    while i < n:
        c = sql[i]
        if c == "-" and sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end < 0 else end
            blank(i, end)
            i = end
        elif c == "/" and sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif sql.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif c == "'":
            escapes = i > 0 and sql[i - 1] in "Ee" and (i < 2 or not _word(sql[i - 2]))
            j = i + 1
            while j < n:
                if escapes and sql[j] == "\\":
                    j += 2
                elif sql[j] == "'":
                    if sql.startswith("''", j):
                        j += 2
                    else:
                        break
                else:
                    j += 1
            end = min(j, n)
            blank(i + 1, end)
            i = end + 1
        elif c == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    if sql.startswith('""', j):
                        j += 2
                        continue
                    break
                j += 1
            inert.append((i + 1, min(j + 1, n)))
            i = j + 1
        elif c == "$" and (i == 0 or not _word(sql[i - 1])) and (tag := _DOLLAR_TAG.match(sql, i)):
            close = sql.find(tag.group(), tag.end())
            end = n if close < 0 else close
            blank(tag.end(), end)
            i = end + len(tag.group())
        else:
            i += 1
    return "".join(chars), inert


def _word(c: str) -> bool:
    return c.isalnum() or c in "_$"


def _inside(spans: list[tuple[int, int]], pos: int) -> bool:
    """Whether ``pos`` is in one of ``spans`` (sorted, disjoint, end exclusive)."""
    k = bisect.bisect_right(spans, pos, key=lambda span: span[0]) - 1
    return k >= 0 and spans[k][0] <= pos < spans[k][1]


#: A CTE a WITH defines: its name, an optional column list, then ``AS [NOT] [MATERIALIZED] (``.
_CTE = re.compile(
    r'(?i)(?:\bWITH\s+(?:RECURSIVE\s+)?|,\s*)("(?:[^"]|"")+"|[A-Za-z_]\w*)\s*'
    r"(?:\([^()]*\)\s*)?AS\s+(?:NOT\s+)?(?:MATERIALIZED\s+)?\("
)


def _folded_name(name: str) -> str:
    """``name`` as PostgreSQL folds it: quoted exactly, unquoted in lower case."""
    return name[1:-1].replace('""', '"') if name.startswith('"') else name.lower()


def _cte_names(sql: str) -> set[str]:
    """The names of the CTEs ``sql`` defines, folded, in its code only (:func:`_code_view`). The
    whole statement's: a bare reference to any of them is not expanded as a view here, where
    :func:`_mark_tree` reads each CTE's own scope."""
    return {_folded_name(m.group(1)) for m in _CTE.finditer(_code_view(sql)[0])}


def _expand_one(sql: str, view_table: str, view_sql: str) -> str:
    """Replace catalog- and schema-qualified refs to one view table.

    Preserves an explicit alias when present; otherwise injects the view-table
    name as the subquery alias so column references qualified by that name (e.g.
    ``shelter__animalBreeds.name``) still resolve after expansion. Only a reference in the
    statement's code is replaced: never text a comment, a string literal or a quoted identifier
    holds (:func:`_sub`).
    """

    def repl(m: re.Match[str]) -> str | None:
        kw = m.groupdict().get("kw") or ""
        if not kw and not _table_position(m.string, m.start()):
            return None  # a column named like the view, not the view
        alias = m.group("alias")
        if alias:
            return f"{kw}({view_sql}){alias}"
        return f"{kw}({view_sql}) {view_table}"

    def bare(m: re.Match[str]) -> str | None:
        return None if _folded_name(m.group("name")) in _cte_names(m.string) else repl(m)

    if "." in view_table:
        return _expand_in_schema(sql, view_table, view_sql)
    vt = re.escape(view_table)
    # Catalog-qualified: "catalog"."schema"."view_table" [alias]
    sql = _sub(rf'"[^"]+"\."[^"]+"\."{vt}"{_ALIAS}', repl, sql)
    # Schema-qualified: "schema"."view_table" [alias]
    sql = _sub(rf'"[^"]+"\."{vt}"{_ALIAS}', repl, sql)
    if view_table != view_table.lower():
        return sql  # an unquoted name folds to lower case: none names this view
    # Unquoted schema-qualified: a view's own definition (e.g. `views.e2e_mv_a`) is raw,
    # user-typed semantic SQL — never quoted/catalog-rewritten like the outer query is
    # before this runs. Nested view chains inline that raw text as-is, so a second (or
    # deeper) level of view reference only ever appears in this unquoted form.
    # Never inside a string literal: a compiled read names its table there (``'city.view_x' AS
    # "_name_"``), which is a value, not a reference.
    sql = _sub(
        rf'(?<![\w."\'])(?P<kw>(?i:FROM|JOIN)\s+)?[A-Za-z_]\w*\.(?i:{vt})\b{_ALIAS}', repl, sql
    )
    # Unquoted bare reference (no schema prefix), only directly after FROM/JOIN so a
    # same-named column or alias elsewhere in the query is never mistaken for the table; nor
    # a CTE of the statement named like it, which PostgreSQL reads first.
    sql = _sub(rf"\b(?P<kw>(?i:FROM|JOIN)\s+)(?P<name>(?i:{vt}))\b{_ALIAS}", bare, sql)
    return sql


def _expand_in_schema(sql: str, view_table: str, view_sql: str) -> str:
    """Replace refs to a view keyed ``schema.name`` -- an ontology Class (REQ-2163), whose name a
    registered table may share -- only where they name its schema: ``"catalog"."schema"."name"``,
    ``"schema"."name"`` or ``schema.name``. The name alone, or under another schema, is not it."""
    schema, name = view_table.split(".", 1)

    def repl(m: re.Match[str]) -> str | None:
        kw = m.groupdict().get("kw") or ""
        if not kw and not _table_position(m.string, m.start()):
            return None  # a column named like the view, not the view
        alias = m.group("alias")
        return f"{kw}({view_sql}){alias}" if alias else f'{kw}({view_sql}) "{name}"'

    s, n = re.escape(schema), re.escape(name)
    sql = _sub(rf'"[^"]+"\."{s}"\."{n}"{_ALIAS}', repl, sql)
    sql = _sub(rf'(?<![\w."]\.)"{s}"\."{n}"{_ALIAS}', repl, sql)
    if view_table != view_table.lower():
        return sql  # an unquoted name folds to lower case: none names this view
    return _sub(rf'(?<![\w."\'])(?P<kw>(?i:FROM|JOIN)\s+)?(?i:{s})\.(?i:{n})\b{_ALIAS}', repl, sql)


class ViewClauseNotSupported(NotAvailableHere):
    """A view referenced with a clause that applies only to a stored relation (TABLESAMPLE, time
    travel, WITH ORDINALITY, ...): refused naming the clause, never expanded without it."""

    def __init__(self, view: str, clause: str) -> None:
        self.view = view
        self.clause = clause
        if clause == "sample":
            # As PostgreSQL refuses it on a view.
            message = f"TABLESAMPLE clause can only be applied to tables, not view {view!r}"
        else:
            message = f"{_CLAUSE_NAMES.get(clause, clause)} cannot be applied to view {view!r}"
        super().__init__(message)


#: What a table reference may carry besides its name: its alias and (in a parenthesised join
#: group) its joins, both carried onto the view's derived table; ONLY, a no-op on a view, which
#: has no inheritance children (PostgreSQL reads it so).
_CARRIED_TABLE_ARGS = frozenset({"this", "db", "catalog", "alias", "joins", "only"})
_CLAUSE_NAMES = {
    "version": "time travel (FOR ... AS OF)",
    "system_time": "time travel (FOR SYSTEM_TIME)",
    "ordinality": "WITH ORDINALITY",
    "rows_from": "ROWS FROM",
    "pivots": "PIVOT",
    "laterals": "LATERAL VIEW",
    "partition": "PARTITION",
    "changes": "CHANGES",
    "when": "WHEN",
    "hints": "a table hint",
    "indexed": "INDEXED BY",
    "format": "FORMAT",
    "pattern": "PATTERN",
}


def _folded(ident: exp.Expr) -> str:  # pyright: ignore[reportPrivateImportUsage]
    """The name ``ident`` is, as PostgreSQL folds it: quoted exactly, unquoted in lower case."""
    if isinstance(ident, exp.Identifier):
        return ident.this if ident.quoted else ident.this.lower()
    return ident.name.lower()


def _shadowed_by_cte(tbl: exp.Table) -> bool:
    """Whether unqualified ``tbl`` names a CTE in its scope, which PostgreSQL reads before any
    relation of that name: a WITH it sits within -- among a WITH's own CTEs, only those before
    the one it is in, unless the WITH is RECURSIVE."""
    if tbl.args.get("db") is not None:
        return False
    name = _folded(tbl.this)
    within: set[int] = {id(tbl)}
    node = tbl.parent
    while node is not None:
        within.add(id(node))
        with_clause = node.args.get("with_")
        if isinstance(with_clause, exp.With):
            for cte in with_clause.expressions:
                if id(cte) in within and not with_clause.args.get("recursive"):
                    break  # a non-recursive CTE, and those after it, are not in its own scope
                if _folded(cte.args["alias"].this) == name:
                    return True
        node = node.parent
    return False


def _view_named(tbl: exp.Table, view_sql_map: dict[str, str]) -> str | None:
    """The view of ``view_sql_map`` the table reference ``tbl`` names, its identifiers folded as
    PostgreSQL folds them: a view keyed ``schema.name`` only under that schema, any other by its
    name. A CTE of that name in scope is the CTE, not the view."""
    if not isinstance(tbl.this, exp.Identifier):
        return None
    name = _folded(tbl.this)
    db = tbl.args.get("db")
    if db is not None and f"{_folded(db)}.{name}" in view_sql_map:
        return f"{_folded(db)}.{name}"
    if name not in view_sql_map or _shadowed_by_cte(tbl):
        return None
    return name


def _mark_tree(tree: exp.Expr, view_sql_map: dict[str, str], depth: int) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """``tree`` with every table reference to a view replaced by the view's parsed SQL, marked:
    ``(SELECT * FROM (<view SQL>) AS "<VIEW_MARK><view>") <alias>``. A view's SQL is expanded the
    same way first, ``depth`` levels deep (a cycle could never have validated). The reference's
    alias (with its column list) and its joins are carried onto the derived table; any other
    clause it carries is refused (:class:`ViewClauseNotSupported`)."""
    for tbl in list(tree.find_all(exp.Table)):
        view = _view_named(tbl, view_sql_map)
        if view is None:
            continue
        for arg, value in tbl.args.items():
            if arg not in _CARRIED_TABLE_ARGS and value not in (None, False, []):
                raise ViewClauseNotSupported(view, arg)
        body = sqlglot.parse_one(view_sql_map[view], read="postgres")
        if depth > 0:
            body = _mark_tree(body, view_sql_map, depth - 1)
        marked = exp.select("*").from_(
            exp.Subquery(
                this=body,
                alias=exp.TableAlias(this=exp.to_identifier(f"{VIEW_MARK}{view}", quoted=True)),
            )
        )
        alias = tbl.args.get("alias") or exp.TableAlias(
            this=exp.to_identifier(view.rsplit(".", 1)[-1], quoted="." in view)
        )
        replacement = exp.Subquery(this=marked, alias=alias, joins=tbl.args.get("joins"))
        if tbl is tree:
            return replacement
        tbl.replace(replacement)
    return tree


def _expand_marked(sql: str, view_sql_map: dict[str, str]) -> str:
    """:func:`expand_view_refs` with ``mark``: on the parsed statement, so a mark can only come
    from a view this process expands -- never from text a comment, a string literal or a quoted
    identifier of the statement holds (REQ-039, REQ-603)."""
    tree = parse_marked(sql)
    return _mark_tree(tree, view_sql_map, len(view_sql_map)).sql(dialect="postgres")


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
    if mark:
        return _expand_marked(sql, view_sql_map)
    for _ in range(len(view_sql_map) + 1):
        changed = False
        for view_table, view_sql in view_sql_map.items():
            new_sql = _expand_one(sql, view_table, view_sql)
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
