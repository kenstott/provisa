# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d2a94-0c37-4be8-92a1-5d7e3c8b1f60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A client's catalog statement, addressed to the meta domain (REQ-2190): routing and nothing else.
A pg_catalog or information_schema relation is its view in meta, by name
(:func:`~provisa.transpiler.metadata_views.catalog_view_name`). A function on a catalog is the
compiler's form (:func:`~provisa.transpiler.metadata_functions.expand`). What a statement would
otherwise ask the control plane about itself -- who is connected, what database this is, what an
object's oid is -- is answered for the session, so the statement that runs is about the model and
governed as any read of it."""

# Requirements: REQ-2190, REQ-127

from __future__ import annotations

import contextvars
import itertools
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp

from provisa.pgwire.catalog_pg17 import RELATION_CLASS, RELATIONS, TYPES
from provisa.pgwire.catalog_pin import asked_relations, narrow_kinds
from provisa.pgwire.ext_surfaces import extension_rows, surface_pg_type_rows
from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import expand
from provisa.transpiler.metadata_views import catalog_view_name

#: The schemas a client names a catalog in.
INTERCEPT_SCHEMAS = frozenset(schema for schema, _ in RELATIONS)
CATALOG_NAMES = frozenset(name for _, name in RELATIONS)
_META = "meta"
_PG = "pg_catalog"
_IS = "information_schema"

#: The set-returning functions a client's catalog SQL reads in FROM, each of its arguments alone or
#: of what every PostgreSQL ships. Any other function in FROM is refused as a relation (V006).
CLIENT_TABLE_FUNCTIONS = frozenset(
    {
        "_pg_expandarray",
        "aclexplode",
        "generate_subscripts",
        "json_array_elements",
        "json_array_elements_text",
        "json_each",
        "json_each_text",
        "json_object_keys",
        "jsonb_array_elements",
        "jsonb_array_elements_text",
        "jsonb_each",
        "jsonb_each_text",
        "jsonb_object_keys",
        "pg_get_keywords",
        "pg_options_to_table",
        "pg_timezone_abbrevs",
        "pg_timezone_names",
        "regexp_matches",
        "regexp_split_to_table",
        "string_to_table",
        "ts_token_type",
        "unnest",
    }
)

#: System columns: a view has none, and a catalog keeps no row versions.
_ROW_VERSION_COLUMNS = frozenset({"xmin", "xmax", "cmin", "cmax", "ctid"})

VERSION = "PostgreSQL 14.0 on Provisa"
CURRENT_SCHEMA = "public"
#: The schemas on the search path, as current_schemas(false) lists them.
_SEARCH_PATH = (CURRENT_SCHEMA,)
#: PostgreSQL's FirstNormalTransactionId: xids 1 and 2 are the bootstrap's, always past.
_TXID_COUNTER = itertools.count(3)


def next_txid() -> int:
    """The next monotonic bigint transaction id for txid_current(). Provisa has no real MVCC
    transaction ids; clients only require a stable increasing bigint."""
    return next(_TXID_COUNTER)


@dataclass(frozen=True)
class CatalogRequest:
    """What a catalog statement is answered for: the role, the database it was told it is on, its
    privilege facts, the session settings it reads and the connection that asks."""

    role_id: str
    database: str
    facts: CatalogFacts
    settings: Mapping[str, str]
    pid: int = 0
    session: Mapping[str, str] = field(default_factory=dict)
    #: Each listed table's engine row count, by relation oid, for a statement reading reltuples.
    reltuples: Mapping[int, float] = field(default_factory=dict)


#: The request of the catalog statement being governed, read by the pipeline's three decisions on
#: it (its guard, its route, and what its terminal tells the views). Unset for every other.
current_request: contextvars.ContextVar[CatalogRequest | None] = contextvars.ContextVar(
    "provisa_catalog_request", default=None
)


# --- what sqlglot cannot read of PostgreSQL's spelling -------------------------------------------

#: ``::oid[]``: sqlglot reads the brackets as a subscript. PostgreSQL's own name of each array type
#: says the same.
_ARRAY_CAST = re.compile(
    r"::\s*(?:pg_catalog\.)?(oid|regtype|regclass|name|aclitem|regproc|xid|cid|int2vector|oidvector)"
    r"\[\]",
    re.IGNORECASE,
)
#: Functions sqlglot reads as syntax of its own: a qualified, quoted call it reads as a call, and
#: PostgreSQL as the same function.
_SYNTAX_CALLS = re.compile(r"(?:\bpg_catalog\.)?\b(convert|json_object)\s*\(", re.IGNORECASE)
#: sqlglot reads ``::bit varying`` as a bit and an alias.
_BIT_VARYING = re.compile(r"::\s*bit varying\b", re.IGNORECASE)
_CHAR_ARRAY_CAST = re.compile(r'::\s*(?:pg_catalog\.)?"char"\[\]', re.IGNORECASE)


def _readable(sql: str) -> str:
    sql = _SYNTAX_CALLS.sub(lambda m: f'pg_catalog."{m[1].lower()}"(', sql)
    sql = _BIT_VARYING.sub("::varbit", sql)
    sql = _CHAR_ARRAY_CAST.sub("::_char", sql)
    return _ARRAY_CAST.sub(lambda m: f"::_{m[1].lower()}", sql)


# --- names ---------------------------------------------------------------------------------------


def _relation_of(db: str, name: str) -> tuple[str, str] | None:
    """The catalog relation a statement's ``db.name`` addresses: a bare name is pg_catalog's first,
    PostgreSQL's search path having it first."""
    if db:
        return (db, name) if (db, name) in RELATIONS else None
    for schema in (_PG, _IS):
        if (schema, name) in RELATIONS:
            return schema, name
    return None


def _cte_names(tree: exp.Expr) -> frozenset[str]:
    return frozenset(c.alias.lower() for c in tree.find_all(exp.CTE))


def _surface_rows() -> dict[tuple[str, str], list[dict[str, object]]]:
    """The rows no view can hold: a process's extension surfaces (REQ-892), which its environment
    enables. Empty unless a deployment opts in."""
    extensions = extension_rows()
    types = surface_pg_type_rows()
    if not extensions and not types:
        return {}
    return {
        (_PG, "pg_extension"): [
            dict(zip(("oid", "extname", "extowner", "extnamespace", "extrelocatable", "extversion"), row))
            for row in extensions
        ],
        (_PG, "pg_available_extensions"): [
            {"name": row[1], "default_version": row[5], "installed_version": row[5]}
            for row in extensions
        ],
        (_PG, "pg_available_extension_versions"): [
            {
                "name": row[1],
                "version": row[5],
                "installed": True,
                "superuser": False,
                "relocatable": False,
                "schema": _PG,
            }
            for row in extensions
        ],
        (_PG, "pg_type"): [
            dict(
                zip(
                    (
                        "oid", "typname", "typnamespace", "typlen", "typtype", "typcategory",
                        "typnotnull", "typbasetype", "typbyval", "typalign", "typstorage",
                    ),
                    row,
                )
            )
            for row in types
        ],
    }  # fmt: skip


def _value(value: object, pg_type: str) -> str:
    written = "NULL" if value is None else _literal(str(value))
    return f"CAST({written} AS {_VIEW_TYPES.get(pg_type, pg_type)})"


def _with_rows(relation: tuple[str, str], view: str, rows: list[dict[str, object]]) -> exp.Expr:
    """``view`` and ``rows`` together, as the one relation a statement reads."""
    arms = [f"SELECT * FROM {_META}.{view}"]
    for row in rows:
        values = ", ".join(_value(row.get(name), t) for name, t, _ in RELATIONS[relation])
        arms.append(f"SELECT {values}")
    return _parsed(" UNION ALL ".join(arms))


def _addressed(table: exp.Table, cte_names: frozenset[str], extra: Mapping) -> exp.Expr:
    """``table`` as the view of the relation it names; the table itself when it names none."""
    db = (table.db or "").lower()
    name = (table.name or "").lower()
    if not db and name in cte_names:
        return table
    relation = _relation_of(db, name)
    if relation is None:
        return table
    view = catalog_view_name(*relation)
    # The statement's own name still qualifies its columns (pg_class.oid).
    alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(name))
    if relation in extra:
        return exp.Subquery(this=_with_rows(relation, view, extra[relation]), alias=alias)
    addressed = exp.table_(view, db=_META)
    addressed.set("alias", alias)
    return addressed


#: The two a view holds as text (provisa.api._pg_catalog_views).
_VIEW_TYPES = {"anyarray": "text", "pg_node_tree": "text", "oid[]": "_oid", "aclitem[]": "_aclitem"}


# --- object identifiers --------------------------------------------------------------------------

_CATALOG_CLASS_OIDS: dict[str, int] = {
    name: oid for (schema, name), (oid, _) in RELATION_CLASS.items() if schema == _PG
}
_TYPE_BY_NAME: dict[str, int] = {}
for _oid, _entry in TYPES.items():
    if _entry[0] == _PG:
        _TYPE_BY_NAME.setdefault(_entry[1], _oid)
        _TYPE_BY_NAME.setdefault(_entry[-1], _oid)
#: What resolves against the model. Any other object identifier type names what PostgreSQL ships,
#: and reads it from the same place a client's PostgreSQL would.
_RESOLVED_REGS = frozenset({"regclass", "regtype", "regnamespace", "regrole"})
_NAMED_IN_RESULT = frozenset({"regclass", "regnamespace"})
_UNDEFINED = {"regclass": "42P01", "regnamespace": "3F000", "regtype": "42704", "regrole": "42704"}
_KIND = {"regclass": "relation", "regnamespace": "schema", "regtype": "type", "regrole": "role"}
_TEXT_TYPES = frozenset({"text", "varchar", "character varying", "name"})


def _reg_of(data_type: exp.Expr) -> str:
    return data_type.sql(dialect="postgres").lower().removeprefix("pg_catalog.")


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _parts(written: str) -> list[str]:
    """A possibly qualified name's parts: a quoted part as it is, an unquoted one in lower case."""
    return [
        quoted.replace('""', '"') if quoted else bare.strip().lower()
        for quoted, bare in re.findall(r'"((?:[^"]|"")*)"|([^.]+)', written)
    ]


def _raises(reg: str, written: str) -> str:
    message = _literal(f'{_KIND[reg]} "{written}" does not exist')
    return f"provisa_raise({_literal(_UNDEFINED[reg])}, {message})"


def _required(lookup: str, reg: str, written: str) -> str:
    """``lookup``, or PostgreSQL's error when it names nothing."""
    return f"COALESCE(({lookup}), CAST(CASE WHEN TRUE THEN {_raises(reg, written)} END AS OID))"


_CLASSES = (
    "FROM meta.catalog_pg_class _rc JOIN meta.catalog_pg_namespace _rn "
    "ON _rn.oid = _rc.relnamespace"
)


def _class_oid(written: str) -> str:
    parts = _parts(written)
    name = parts[-1]
    if len(parts) > 1:
        if parts[-2] == _PG and name in _CATALOG_CLASS_OIDS:
            return str(_CATALOG_CLASS_OIDS[name])
        lookup = (
            f"SELECT _rc.oid {_CLASSES} WHERE _rc.relname = {_literal(name)} "
            f"AND _rn.nspname = {_literal(parts[-2])}"
        )
    else:
        if name in _CATALOG_CLASS_OIDS:
            return str(_CATALOG_CLASS_OIDS[name])
        lookup = (
            f"SELECT _rc.oid {_CLASSES} WHERE _rc.relname = {_literal(name)} "
            f"AND _rn.nspname IN ({_literal(_PG)}, {_literal(CURRENT_SCHEMA)}) "
            "ORDER BY _rn.nspname = 'pg_catalog' DESC LIMIT 1"
        )
    return _required(lookup, "regclass", written)


def _namespace_oid(written: str) -> str:
    name = _parts(written)[-1]
    return _required(
        f"SELECT _rn.oid FROM meta.catalog_pg_namespace _rn WHERE _rn.nspname = {_literal(name)}",
        "regnamespace",
        written,
    )


def _type_oid(written: str) -> str:
    name = written.strip().lower().removeprefix("pg_catalog.")
    if name in _TYPE_BY_NAME:
        return str(_TYPE_BY_NAME[name])
    return _required(
        f"SELECT _rt.oid FROM meta.catalog_pg_type _rt WHERE _rt.typname = {_literal(name)}",
        "regtype",
        written,
    )


def _role_oid(written: str) -> str:
    return _required(
        f"SELECT _ro.oid FROM meta.catalog_pg_roles _ro WHERE _ro.rolname = {_literal(written)}",
        "regrole",
        written,
    )


_OID_OF: dict[str, Callable[[str], str]] = {
    "regclass": _class_oid,
    "regnamespace": _namespace_oid,
    "regtype": _type_oid,
    "regrole": _role_oid,
}


def _oid_sql(reg: str, operand: exp.Expr) -> str:
    if isinstance(operand, exp.Literal) and operand.is_string:
        return _OID_OF[reg](operand.this)
    return f"CAST({operand.sql(dialect='postgres')} AS OID)"


def _quoted(name: str) -> str:
    return f"QUOTE_IDENT(CAST({name} AS TEXT))"


def _name_sql(reg: str, oid_sql: str) -> str:
    """The name regclassout, regnamespaceout, regtypeout or regroleout writes for ``oid_sql``: the
    oid's number when it names no object, as each writes it."""
    if reg == "regclass":
        name = (
            f"(SELECT CASE WHEN _rn.nspname IN ({_literal(_PG)}, {_literal(CURRENT_SCHEMA)}) "
            f"THEN {_quoted('_rc.relname')} "
            f"ELSE {_quoted('_rn.nspname')} || '.' || {_quoted('_rc.relname')} END "
            f"{_CLASSES} WHERE _rc.oid = {oid_sql})"
        )
    elif reg == "regnamespace":
        name = (
            f"(SELECT {_quoted('_rn.nspname')} FROM meta.catalog_pg_namespace _rn "
            f"WHERE _rn.oid = {oid_sql})"
        )
    elif reg == "regrole":
        name = (
            f"(SELECT {_quoted('_ro.rolname')} FROM meta.catalog_pg_roles _ro "
            f"WHERE _ro.oid = {oid_sql})"
        )
    else:
        name = f"format_type({oid_sql}, NULL)"
    return f"COALESCE({name}, CAST({oid_sql} AS TEXT))"


def _parsed(sql: str) -> exp.Expr:
    return sqlglot.parse_one(sql, read="postgres")


def _reg_cast(node: exp.Cast) -> exp.Expr | None:
    """``node`` where it casts to or from an object identifier type the model resolves: an oid, a
    name where a statement reads one as text, and otherwise left alone."""
    target = _reg_of(node.to)
    inner = node.this
    if isinstance(inner, exp.Cast):
        from_reg = _reg_of(inner.to)
        if from_reg in _RESOLVED_REGS and target in _TEXT_TYPES:
            return _parsed(_name_sql(from_reg, f"({_oid_sql(from_reg, inner.this)})"))
    if target in _RESOLVED_REGS:
        return _parsed(f"({_oid_sql(target, inner)})")
    return None


#: Calls answering a relation or a schema: an oid wherever a statement computes with one, and with
#: its name where it is a result column, as a cast to the type is.
_CALLS_OF_REG_TYPE = {
    "pg_filenode_relation": "regclass",
    "pg_get_replica_identity_index": "regclass",
    "pg_partition_root": "regclass",
    "regclass": "regclass",
    "to_regclass": "regclass",
    "to_regnamespace": "regnamespace",
}


def _as_cast(select: exp.Expr) -> None:
    target = select.this if isinstance(select, exp.Alias) else select
    called = _call(target)
    if called is None:
        return
    name = called[0].name.lower()
    if name == "pg_partition_ancestors":
        called[0].set("this", "pg_partition_ancestors_named")
        if select is target:
            select.replace(exp.alias_(called[1].copy(), name))
        return
    reg = _CALLS_OF_REG_TYPE.get(name)
    if reg is None:
        return
    operand = called[0].expressions[0] if name == "regclass" else called[1]
    cast_to = exp.Cast(
        this=operand.copy(), to=exp.DataType.build(reg, dialect="postgres", udt=True)
    )
    if select is target:
        select.replace(exp.alias_(cast_to, name))
    else:
        target.replace(cast_to)


def _result_regs(tree: exp.Expr) -> None:
    """A regclass or regnamespace result column is sent with its oid and its name together: the
    wire's text form is the name, its binary form the oid."""
    if not isinstance(tree, exp.Select):
        return
    for select in list(tree.selects):
        _as_cast(select)
    for select in tree.selects:
        target = select.this if isinstance(select, exp.Alias) else select
        if not isinstance(target, exp.Cast):
            continue
        reg = _reg_of(target.to)
        if reg not in _NAMED_IN_RESULT:
            continue
        oid = f"({_oid_sql(reg, target.this)})"
        pair = _parsed(
            f"CASE WHEN {oid} IS NOT NULL THEN "
            f"JSONB_BUILD_OBJECT('oid', CAST({oid} AS BIGINT), 'name', {_name_sql(reg, oid)}) END"
        )
        if select is target:
            select.replace(exp.alias_(pair, reg))
        else:
            target.replace(pair)


#: A string literal compared with an object identifier value is read as a name of that type, as
#: PostgreSQL coerces an unknown-typed literal to the other operand's type.
_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)


def _coerce_reg_comparisons(tree: exp.Expr) -> None:
    for comparison in list(tree.find_all(*_COMPARISONS)):
        for own, other in (("this", "expression"), ("expression", "this")):
            side = comparison.args.get(own)
            literal = comparison.args.get(other)
            if (
                isinstance(side, exp.Cast)
                and _reg_of(side.to) in _RESOLVED_REGS
                and isinstance(literal, exp.Literal)
                and literal.is_string
            ):
                comparison.set(other, exp.Cast(this=literal.copy(), to=side.to.copy()))


# --- the session ---------------------------------------------------------------------------------


def _typed(value: exp.Expr, pg_type: str) -> exp.Expr:
    return exp.Cast(
        this=value, to=exp.DataType.build(pg_type, dialect="postgres", udt=True, copy=False)
    )


def _name_value(text: str) -> exp.Expr:
    return _typed(exp.Literal.string(text), "name")


def _current_schemas(args: list[exp.Expr]) -> exp.Expr:
    """The search path's schemas, pg_catalog first where implicit ones are asked for."""
    if not (args and isinstance(args[0], exp.Boolean)):
        return _typed(exp.null(), "_name")  # strict: a NULL argument
    schemas = (_PG, *_SEARCH_PATH) if args[0].this else _SEARCH_PATH
    literals = ", ".join(_literal(s) for s in schemas)
    return _parsed(f"CAST(ARRAY[{literals}] AS _name)")


def _current_setting(args: list[exp.Expr], settings: Mapping[str, str]) -> exp.Expr:
    """A setting of the session, never of the connection this statement runs on: a literal name
    is its value, and any other is looked up among the ones a session has."""
    if not args:
        return exp.null()
    name = args[0]
    if isinstance(name, exp.Literal):
        return exp.Literal.string(settings.get(name.name.lower(), ""))
    if isinstance(name, exp.Null) or (
        isinstance(name, exp.Cast) and isinstance(name.this, exp.Null)
    ):
        return exp.cast(exp.null(), "TEXT")
    cases = " ".join(f"WHEN {_literal(k)} THEN {_literal(v)}" for k, v in sorted(settings.items()))
    return _parsed(f"CASE LOWER({name.sql(dialect='postgres')}) {cases} ELSE '' END")


def _set_config(args: list[exp.Expr]) -> exp.Expr:
    """set_config answers the value it set. Nothing is set: what a client sets is the session's,
    and what the catalog views read of a statement is the terminal's alone."""
    return exp.cast(args[1].copy(), "TEXT") if len(args) > 1 else exp.null()


def _session_call(name: str, args: list[exp.Expr], request: CatalogRequest) -> exp.Expr | None:
    if name in ("current_user", "session_user", "getpgusername"):
        return _name_value(request.role_id)
    if name == "current_database":
        return _name_value(request.database)
    if name == "current_schema":
        return _name_value(CURRENT_SCHEMA)
    if name == "current_schemas":
        return _current_schemas(args)
    if name == "version":
        return exp.cast(exp.Literal.string(VERSION), "TEXT")
    if name == "current_setting":
        return _current_setting(args, request.settings)
    if name == "set_config":
        return _set_config(args)
    if name == "pg_backend_pid":
        return exp.cast(exp.Literal.number(request.pid), "INTEGER")
    if name == "system_user":
        return exp.cast(exp.null(), "TEXT")  # no authenticated identity to name
    if name == "pg_is_in_recovery":
        return exp.false()
    if name in ("txid_current", "pg_current_xact_id"):
        return exp.cast(exp.Literal.number(next_txid()), "BIGINT")
    if name in ("pg_postmaster_start_time", "pg_conf_load_time"):
        return exp.cast(exp.null(), "TIMESTAMPTZ")
    return None


_SYNTAX_SESSION: dict[type[exp.Expr], str] = {
    exp.CurrentUser: "current_user",
    exp.SessionUser: "session_user",
    exp.CurrentDatabase: "current_database",
    exp.CurrentSchema: "current_schema",
    exp.CurrentCatalog: "current_database",
}
#: A keyword PostgreSQL reads as a function of the session, which sqlglot reads as a column.
_SESSION_COLUMNS = {"current_user", "session_user", "user", "current_catalog", "current_schema"}
_SESSION_COLUMN_CALLS = {"user": "current_user", "current_catalog": "current_database"}


def _call(node: exp.Expr) -> tuple[exp.Anonymous, exp.Expr] | None:
    """The call ``node`` is, its ``pg_catalog.`` qualifier aside, and the node holding it."""
    if (
        isinstance(node, exp.Dot)
        and isinstance(node.this, exp.Identifier)
        and node.this.name.lower() == _PG
        and isinstance(node.expression, exp.Anonymous)
    ):
        return node.expression, node
    return (node, node) if isinstance(node, exp.Anonymous) else None


def _named(value: exp.Expr, node: exp.Expr, name: str) -> exp.Expr:
    """``value`` in ``node``'s place, a result column being named for the function it was."""
    if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
        return exp.alias_(value, name)
    return value


def _reltuples(node: exp.Column, counts: Mapping[int, float]) -> exp.Expr:
    """pg_class.reltuples: an engine's statistic, which no view holds, written in where a statement
    reads it, so a statement that does not pays nothing for it."""
    oid = exp.column("oid", table=node.table)
    return exp.Case(
        this=oid,
        ifs=[
            exp.If(this=exp.Literal.number(k), true=exp.Literal.number(v))
            for k, v in sorted(counts.items())
        ],
        default=node.copy(),
    )


def _named_relation_call(name: str, args: list[exp.Expr]) -> exp.Expr | None:
    """A call taking a regclass, given a name: the name is resolved as the call is read, as
    PostgreSQL reads the argument, and one naming no relation is its error."""
    if not (args and isinstance(args[0], exp.Literal) and args[0].is_string):
        return None
    if name == "setval":
        oid = _parsed(f"({_class_oid(args[0].this)})")
        return exp.Anonymous(this=name, expressions=[oid, *(a.copy() for a in args[1:])])
    return None


def _rewriter(request: CatalogRequest, cte_names: frozenset[str]):
    extra = _surface_rows()

    def rewrite(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Table) and isinstance(node.this, (exp.Identifier, str)):
            return _addressed(node, cte_names, extra)
        if type(node) in _SYNTAX_SESSION:
            name = _SYNTAX_SESSION[type(node)]
            answered = _session_call(name, [], request)
            assert answered is not None
            return _named(answered, node, name)
        if isinstance(node, exp.CurrentVersion):
            return _named(exp.cast(exp.Literal.string(VERSION), "TEXT"), node, "version")
        called = _call(node)
        if called is not None:
            call, _ = called
            name = call.name.lower()
            args = list(call.expressions)
            answered = _session_call(name, args, request)
            if answered is not None:
                return _named(answered, node, name)
            named_relation = _named_relation_call(name, args)
            if named_relation is not None:
                return named_relation
            return node
        if isinstance(node, exp.Cast):
            return _reg_cast(node) or node
        if isinstance(node, exp.Column):
            column = node.name.lower()
            if (
                column == "reltuples"
                and request.reltuples
                and not isinstance(node.parent, exp.Case)
            ):
                return _named(_reltuples(node, request.reltuples), node, column)
            if column in _ROW_VERSION_COLUMNS:
                return _named(_typed(exp.Literal.number(0), "xid"), node, column)
            if not node.table and column in _SESSION_COLUMNS and not node.this.quoted:
                name = _SESSION_COLUMN_CALLS.get(column, column)
                answered = _session_call(name, [], request)
                assert answered is not None
                return _named(answered, node, column)
            qualifier = node.args.get("db")
            if qualifier is not None and qualifier.name.lower() in INTERCEPT_SCHEMAS:
                return exp.column(node.name, table=node.table)
        return node

    return rewrite


def _server_state(tree: exp.Expr, extra: Mapping) -> exp.Expr:
    """``tree`` with what forms leave to the session: ``provisa_next_xid()``, one id for the
    statement however many times it is named, and a relation's rows no view holds."""
    calls = [n for n in tree.find_all(exp.Anonymous) if n.name.lower() == "provisa_next_xid"]
    if calls:
        xid = next_txid()
        for call in calls:
            call.replace(exp.cast(exp.Literal.number(xid), "BIGINT"))
    views = {catalog_view_name(*r): r for r in extra}
    for table in list(tree.find_all(exp.Table)):
        if (
            (table.db or "").lower() == _META
            and table.name in views
            and table.alias.startswith("_")
        ):
            relation = views[table.name]
            table.replace(
                exp.Subquery(
                    this=_with_rows(relation, table.name, extra[relation]),
                    alias=table.args.get("alias"),
                )
            )
    return tree


def address(sql: str, request: CatalogRequest) -> tuple[str, frozenset[str] | None]:
    """``sql``, a client's statement on PostgreSQL's catalog, as a statement on the meta domain, and
    the relations it is about where it says (:mod:`provisa.pgwire.catalog_pin`)."""
    tree = _parsed(_readable(sql))
    _coerce_reg_comparisons(tree)
    _result_regs(tree)
    routed = tree.transform(_rewriter(request, _cte_names(tree)))
    asked = asked_relations(routed)
    narrow_kinds(routed)
    return (
        _server_state(expand(routed, request.facts), _surface_rows()).sql(dialect="postgres"),
        asked,
    )


def addressed_statement(sql: str, request: CatalogRequest) -> str:
    """``sql`` as :func:`address` writes it."""
    return address(sql, request)[0]
