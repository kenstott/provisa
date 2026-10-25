# Copyright (c) 2026 Kenneth Stott
# Canary: 7643abaf-8a1a-4212-b72d-3c2e57fa4e9f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Leaf helpers for the SQL→Cypher translator.

Pure functions shared by sql_to_cypher and sql_to_cypher_agg: label resolution,
JOIN-ON parsing, and minimal SQL-expression→Cypher rewriting. No dependency on
either caller module.
"""

from __future__ import annotations

import re
from collections.abc import Callable

import sqlglot.expressions as exp

from provisa.gql.label_map import RelationshipMapping


class UntranslatableSubquery(Exception):
    """A correlated subquery references a table this pass could not bind to a graph pattern.

    Raised instead of emitting the SQL alias into Cypher, where it would be an unbound variable.
    semantic_sql_to_cypher catches it and returns None (the documented "cannot translate" result).
    """


def _resolve_label(
    tbl: exp.Table,
    domain_to_label: dict[tuple[str, str], str],
) -> str | None:
    """Map a sqlglot Table node to a Cypher node label using the domain lookup."""
    db = tbl.db or ""
    name = tbl.name or ""
    return domain_to_label.get((db, name)) or domain_to_label.get(("", name))


def _rel_type_from_on(
    on_expr: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    join_to_rel: dict[tuple[str, str, str], RelationshipMapping | None],
    tgt_label: str | None = None,
) -> str | None:
    """Extract Cypher relationship type from a JOIN ON condition."""
    if on_expr is None:
        return None
    for eq in on_expr.find_all(exp.EQ):
        left, right = eq.this, eq.expression
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            lc = left.name
            rc = right.name
            rel = (
                (join_to_rel.get((lc, rc, tgt_label)) or join_to_rel.get((rc, lc, tgt_label)))
                if tgt_label is not None
                else None
            )
            if rel:
                return rel.rel_type
    return None


def rel_pattern(  # REQ-464
    src_label: str | None,
    tgt_label: str | None,
    src_tgt_to_rel: dict[tuple[str, str], str | None],
    *,
    rel_type: str | None = None,
) -> str:
    """The Cypher relationship pattern joining ``src_label`` to ``tgt_label``.

    ``rel_type``, when given, is a type already resolved from the JOIN ON columns — the most
    precise source there is, and oriented source→target by construction.

    Otherwise the ORDERED PAIR decides. Keying on the target label alone (which is what the
    translator used to fall back on) has no single answer once two relationships land on the same
    label — a junction table, or two foreign keys onto one table — and the last-writer-wins map
    answered with a real-but-wrong type: a playgroup-membership join came out as
    ``(:Playgroups)-[:SHARES_ENCLOSURE]->(:Pets)``. When the pair resolves only in reverse the
    arrow flips; when it does not resolve, or the pair itself is ambiguous, the pattern is
    anonymous and undirected, which still matches the intended edge instead of naming another one.
    """
    if rel_type is not None:
        return f"-[:{rel_type}]->"
    if src_label is not None and tgt_label is not None:
        forward = src_tgt_to_rel.get((src_label, tgt_label))
        if forward is not None:
            return f"-[:{forward}]->"
        reverse = src_tgt_to_rel.get((tgt_label, src_label))
        if reverse is not None:
            return f"<-[:{reverse}]-"
    return "-[]-"


def _src_alias_from_on(
    on_expr: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    tgt_sql_alias: str,
    default_alias: str,
    *,
    also_target: str | None = None,
) -> str:
    """Return the source table alias from a JOIN ON condition.

    Looks for column references whose table qualifier is not the join target —
    that's the source side of the relationship.  Falls back to default_alias.

    ``also_target`` names a second qualifier that is still the target side: a LATERAL subquery's
    inner WHERE qualifies its own columns by the inner TABLE name, not by the subquery alias, so
    without it the target's own name is read as the source.
    """
    if on_expr is None:
        return default_alias
    targets = {tgt_sql_alias, also_target}
    for eq in on_expr.find_all(exp.EQ):
        for col in (eq.this, eq.expression):
            if isinstance(col, exp.Column) and col.table and col.table not in targets:
                return col.table
    return default_alias


_SQL_STRING = re.compile(r"'(?:[^']|'')*'")
_CYPHER_STRING = re.compile(r"'(?:[^'\\]|\\.)*'")
#: A regex's metacharacters: each written escaped, so a LIKE pattern's character is itself.
_REGEX_META = frozenset("\\.^$|?*+()[]{}")


def outside_strings(text: str, rewrite: Callable[[str], str]) -> str:
    """``rewrite`` applied to the parts of the Cypher ``text`` outside its string literals: a
    literal's characters are the value it is, never a name to rewrite."""
    out: list[str] = []
    at = 0
    for m in _CYPHER_STRING.finditer(text):
        out += [rewrite(text[at : m.start()]), m.group()]
        at = m.end()
    out.append(rewrite(text[at:]))
    return "".join(out)


def _cypher_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def like_regex(pattern: str, *, case_insensitive: bool) -> str:
    """A SQL LIKE pattern as the regex ``=~`` matches the same values with: ``%`` any run of
    characters (newlines among them), ``_`` any one, a backslash (LIKE's escape character) the
    character after it itself, every other character itself; anchored at both ends."""
    out = ["(?is)^" if case_insensitive else "(?s)^"]
    chars = iter(pattern)
    for c in chars:
        if c == "\\":
            c = next(chars, None)
            if c is None:
                raise UntranslatableSubquery(f"the LIKE pattern {pattern!r}, ending in its escape")
            out.append("\\" + c if c in _REGEX_META else c)
        elif c == "%":
            out.append(".*")
        elif c == "_":
            out.append(".")
        else:
            out.append("\\" + c if c in _REGEX_META else c)
    return "".join(out) + "$"


def _code_to_cypher(code: str) -> str:
    # Remove double-quote wrapping from identifiers (sqlglot emits them)
    result = re.sub(r'"(\w+)"', r"\1", code)
    return result.replace("TRUE", "true").replace("FALSE", "false")


def _sql_to_cypher_expr(sql_expr: str) -> str:
    """Minimally rewrite a SQL expression fragment to Cypher syntax: its string literals written
    as Cypher writes them, and ``x LIKE 'p'`` / ``x ILIKE 'p'`` / ``x LIKE LOWER('p')`` as
    ``x =~ 'regex'`` matching the same values (like_regex). A LIKE over anything but a literal, or with an ESCAPE clause, has no
    exact Cypher form here and is refused (UntranslatableSubquery)."""
    out: list[str] = []
    like: str | None = None  # the LIKE whose pattern the next literal is
    lowered = False  # that pattern is LOWER('p') (how a GraphQL ilike compiles)
    at = 0
    for m in [*_SQL_STRING.finditer(sql_expr), None]:
        code = sql_expr[at : m.start() if m is not None else len(sql_expr)]
        if like is not None and code.lstrip().upper().startswith("ESCAPE"):
            raise UntranslatableSubquery(f"a {like} with an ESCAPE clause")
        if lowered:
            # LOWER('p') closes after its pattern.
            if not code.startswith(")"):
                raise UntranslatableSubquery(f"a LIKE whose pattern is no literal: {sql_expr}")
            code = code[1:]
        op = re.search(r" (I?LIKE) (LOWER\()?$", code)
        if re.search(r"\bI?LIKE\b", code[: op.start()] if op else code):
            raise UntranslatableSubquery(f"a LIKE whose pattern is no literal: {sql_expr}")
        if m is None:
            out.append(_code_to_cypher(code))
            break
        value = m.group()[1:-1].replace("''", "'")
        if op is not None:
            like, lowered = op.group(1), op.group(2) is not None
            out += [_code_to_cypher(code[: op.start()]), " =~ "]
            value = like_regex(
                value.lower() if lowered else value, case_insensitive=like == "ILIKE"
            )
        else:
            like, lowered = None, False
            out.append(_code_to_cypher(code))
        out.append(_cypher_string(value))
        at = m.end()
    return "".join(out)
