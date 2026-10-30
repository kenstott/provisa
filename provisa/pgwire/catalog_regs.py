# Copyright (c) 2026 Kenneth Stott
# Canary: 0a6e3f81-4c2d-4b97-8e15-d9b7a2c40f68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The object identifier types a statement casts to, over the catalog (REQ-2167, PGWIRE CATALOG
AND TYPES): regtype, regclass and regnamespace.

A cast of a name to one is the oid of the object the name names, read from the catalog (an error
naming it when there is none, as regclassin's is); a cast of one to text is the object's name, as
regclassout writes it (qualified when its schema is not on the search path, the number itself for
an oid naming no object). A statement's own result column that is a regclass or a regnamespace is
the object's oid and its name together, which the wire sends as the name in text and the oid in
binary; a regtype result column is its oid, sent by its own wire type."""

# Requirements: REQ-2167

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.pgwire.catalog_pg17 import TYPES
from provisa.core.pg_error import raising

#: The object identifier types a projected value of is its oid and its name together.
NAMED_REG_TYPES = frozenset({"regclass", "regnamespace"})
#: The object identifier types this module resolves.
REG_TYPES = NAMED_REG_TYPES | {"regtype"}

#: pg_partition_ancestors as a result column: each ancestor's oid and name together.
NAMED_ANCESTORS = "pg_partition_ancestors_named"

#: The schemas on PostgreSQL's search path here, pg_catalog first.
_SEARCH_PATH = ("pg_catalog", "public")

_TYPE_BY_NAME: dict[str, int] = {}
for _oid, _entry in TYPES.items():
    if _entry[0] == "pg_catalog":
        _TYPE_BY_NAME.setdefault(_entry[1], _oid)
        _TYPE_BY_NAME.setdefault(_entry[-1], _oid)


def reg_type_of(data_type: exp.DataType) -> str | None:
    """The object identifier type ``data_type`` names (``pg_catalog.regclass`` is regclass)."""
    name = data_type.sql(dialect="postgres").lower().removeprefix("pg_catalog.")
    return name if name in REG_TYPES else None


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _name_parts(text: str) -> list[str]:
    """A possibly qualified name's parts, as PostgreSQL reads them: a quoted part kept as it is,
    an unquoted one folded to lower case."""
    parts: list[str] = []
    i = 0
    while i <= len(text):
        if i < len(text) and text[i] == '"':
            end = i + 1
            chars: list[str] = []
            while end < len(text):
                if text[end] == '"':
                    if end + 1 < len(text) and text[end + 1] == '"':
                        chars.append('"')
                        end += 2
                        continue
                    break
                chars.append(text[end])
                end += 1
            parts.append("".join(chars))
            i = end + 1
        else:
            end = text.find(".", i)
            end = len(text) if end < 0 else end
            parts.append(text[i:end].strip().lower())
            i = end
        if i < len(text) and text[i] == ".":
            i += 1
            continue
        break
    return parts


def _quoted(name_sql: str) -> str:
    from provisa.pgwire.catalog_macros import quoted_identifier

    return quoted_identifier(name_sql)


#: A kind of object's undefined_table (relation), undefined_schema or undefined_object SQLSTATE.
_UNDEFINED = {"relation": "42P01", "schema": "3F000", "type": "42704"}


def _required(lookup: str, kind: str, written: str) -> str:
    """``lookup``, or PostgreSQL's error when it names nothing."""
    message = _literal(f'{kind} "{written}" does not exist')
    missing = raising(_UNDEFINED[kind], message)
    return f"CASE WHEN ({lookup}) IS NULL THEN {missing} ELSE ({lookup}) END"


def _catalog_class_oids() -> dict[str, int]:
    """PostgreSQL's own oids of its catalog relations, which a cast of their names gives
    (catalog_rewrite: the oids the catalog's references to them hold)."""
    from provisa.pgwire.catalog_rewrite import _CATALOG_CLASS_OIDS

    return _CATALOG_CLASS_OIDS


def _class_oid(written: str) -> str:
    parts = _name_parts(written)
    if len(parts) == 1 or parts[-2] == "pg_catalog":
        known = _catalog_class_oids().get(parts[-1])
        if known is not None:
            return str(known)
    relation = _literal(parts[-1])
    joined = "FROM _pg_class _rc JOIN _pg_namespace _rn ON _rn.oid = _rc.relnamespace"
    if len(parts) > 1:
        lookup = (
            f"SELECT _rc.oid {joined} WHERE _rc.relname = {relation} "
            f"AND _rn.nspname = {_literal(parts[-2])}"
        )
    else:
        path = ", ".join(_literal(s) for s in _SEARCH_PATH)
        lookup = (
            f"SELECT _rc.oid {joined} WHERE _rc.relname = {relation} "
            f"AND _rn.nspname IN ({path}) ORDER BY _rn.nspname = 'pg_catalog' DESC LIMIT 1"
        )
    return _required(lookup, "relation", written)


def _namespace_oid(written: str) -> str:
    (name,) = _name_parts(written)[-1:]
    lookup = f"SELECT _rn.oid FROM _pg_namespace _rn WHERE _rn.nspname = {_literal(name)}"
    return _required(lookup, "schema", written)


def _type_oid(written: str) -> str:
    name = written.strip().lower().removeprefix("pg_catalog.")
    if name in _TYPE_BY_NAME:
        return str(_TYPE_BY_NAME[name])
    lookup = f"SELECT _rt.oid FROM _pg_type _rt WHERE _rt.typname = {_literal(name)}"
    return _required(lookup, "type", written)


def _oid_of(reg: str, operand: exp.Expr) -> str:
    if isinstance(operand, exp.Literal) and operand.is_string:
        return {"regclass": _class_oid, "regnamespace": _namespace_oid, "regtype": _type_oid}[reg](
            operand.this
        )
    return f"CAST(({operand.sql(dialect='duckdb')}) AS BIGINT)"


def _name_of(reg: str, oid_sql: str) -> str:
    """The name regclassout, regnamespaceout or regtypeout writes for the oid ``oid_sql``: the
    oid's number when it names no object, as each of them writes it."""
    if reg == "regclass":
        path = ", ".join(_literal(s) for s in _SEARCH_PATH)
        known = " ".join(
            f"WHEN {oid} THEN {_literal(name)}" for name, oid in _catalog_class_oids().items()
        )
        name = (
            f"CASE {oid_sql} {known} ELSE (SELECT CASE WHEN _rn.nspname IN ({path}) THEN {_quoted('_rc.relname')} "
            f"ELSE {_quoted('_rn.nspname')} || '.' || {_quoted('_rc.relname')} END "
            "FROM _pg_class _rc JOIN _pg_namespace _rn ON _rn.oid = _rc.relnamespace "
            f"WHERE _rc.oid = {oid_sql}) END"
        )
    elif reg == "regnamespace":
        name = f"(SELECT {_quoted('_rn.nspname')} FROM _pg_namespace _rn WHERE _rn.oid = {oid_sql})"
    else:
        from provisa.pgwire.catalog_macros import type_display_name

        name = type_display_name(oid_sql)
    return f"coalesce({name}, CAST({oid_sql} AS VARCHAR))"


def _duckdb(sql: str) -> exp.Expr:
    return sqlglot.parse_one(sql, read="duckdb")


def reg_oid(reg: str, operand: exp.Expr) -> exp.Expr:
    """``operand::reg`` as the oid it is (``operand`` already rewritten for the catalog)."""
    return _duckdb(_oid_of(reg, operand))


def reg_name(reg: str, operand: exp.Expr) -> exp.Expr:
    """``operand::reg::text``: the name of the object the oid names."""
    if reg == "regtype" and isinstance(operand, exp.Literal) and not operand.is_string:
        oid = int(operand.this)
        if oid in TYPES:
            return exp.Literal.string(TYPES[oid][-1])
    return _duckdb(_name_of(reg, f"({_oid_of(reg, operand)})"))


def reg_value(reg: str, operand: exp.Expr) -> exp.Expr:
    """``operand::reg`` as a statement's result column: a regclass or regnamespace is its oid
    and its name together; a regtype its oid."""
    oid_sql = _oid_of(reg, operand)
    if reg not in NAMED_REG_TYPES:
        return _duckdb(oid_sql)
    pair = f"struct_pack(oid := ({oid_sql}), name := {_name_of(reg, f'({oid_sql})')})"
    return _duckdb(f"CASE WHEN ({oid_sql}) IS NOT NULL THEN {pair} END")


def project_reg_values(tree: exp.Expr) -> None:
    """Each result column of ``tree`` (a SELECT) that is a cast to an object identifier type,
    replaced by its value (``reg_value``). The value's operand is written in DuckDB's dialect and
    is rewritten for the catalog with the rest of the statement."""
    if not isinstance(tree, exp.Select):
        return
    for select in tree.selects:
        target = select.this if isinstance(select, exp.Alias) else select
        call = target.expression if isinstance(target, exp.Dot) else target
        if isinstance(call, exp.Anonymous) and call.name.lower() == "pg_partition_ancestors":
            # A regclass result column: the oid its FROM form joins on is the macro's own.
            target.replace(exp.Anonymous(this=NAMED_ANCESTORS, expressions=call.expressions))
            continue
        if not isinstance(target, exp.Cast):
            continue
        reg = reg_type_of(target.to)
        if reg is None:
            continue
        value = reg_value(reg, target.this)
        if select is target:
            select.replace(exp.alias_(value, reg))
        else:
            target.replace(value)
