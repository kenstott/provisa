# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a91c2-7d48-4e15-b0a9-5c2e8d17f643
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source operation's function, read in FROM (REQ-1986).

An operation of a source that requires a parameter is a function: its parameters are its
arguments and it returns its rows. ``fn(a, b)`` in FROM is the operation's parameterized table
with each argument assigned to its parameter by equality -- the same relation, written as a
call. The call is replaced by that relation here, before the statement is governed, so the one
pipeline governs the table it reads (row rules, masks, visibility), binds the assignments
(``nf_extractor.bind_parameters``) and makes the calls: nothing of a function call is run apart
from the statement.

An argument is written as any value an equality on the parameter takes: a literal, a bound
``$N``, a column of another table the statement reads (one call for each distinct value). An
argument given NULL is not given. A required one that is not given, and a value an equality
cannot assign, are refused where the table form refuses them.
"""

# Requirements: REQ-1986

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

#: The command kind a source operation that reads rows is registered as.
SOURCE_OPERATION = "source_operation"

# id(registry) -> (registry, {sql name (lower): function}); one entry per live registry.
_BY_REGISTRY: dict[int, tuple[Any, dict[str, dict]]] = {}
_MAX_REGISTRIES = 64


def is_operation_function(command: dict) -> bool:
    """Whether ``command`` is a source operation that returns rows: a query of its source,
    returning the rows of the registered table it names."""
    return (
        command.get("impl_kind") == SOURCE_OPERATION
        and command.get("kind") == "query"
        and bool(command.get("returns"))
    )


def _functions(state: object) -> dict[str, dict]:  # object-ok: AppState lives above compiler
    """The operation functions by their SQL name, read once per registry."""
    from provisa.core.command_bindings import binding_name

    registry = getattr(state, "tracked_functions", None) or {}
    held = _BY_REGISTRY.get(id(registry))
    if held is not None and held[0] is registry:
        return held[1]
    by_name: dict[str, dict] = {}
    for f in registry.values():
        name = binding_name(f, "sql") if is_operation_function(f) else None
        if name is not None:
            by_name[name.lower()] = f
    if len(_BY_REGISTRY) >= _MAX_REGISTRIES:
        _BY_REGISTRY.pop(next(iter(_BY_REGISTRY)))
    _BY_REGISTRY[id(registry)] = (registry, by_name)
    return by_name


class OperationFunctionRefused(ValueError):
    """A call of an operation function that names no relation: arguments it does not declare,
    or rows of a table the role does not read."""


def _signature(name: str, declared: list[dict]) -> str:
    return f"{name}({', '.join(a['name'] for a in declared)})"


def returned_table(command: dict, ctx: Any) -> Any:
    """The table whose rows ``command`` returns, as ``ctx`` (a role's CompilationContext) holds
    it (its TableMeta), or None when the role reads no such table."""
    for meta in ctx.tables.values():
        if meta.source_id != command["source_id"]:
            continue
        names = {meta.table_name, meta.original_table_name} - {""}
        if any(f"{meta.schema_name}.{n}" == command["returns"] for n in names):
            return meta
    return None


def _assigned(name: str, command: dict, call: exp.Anonymous) -> dict[str, exp.Expression]:
    """Each argument ``call`` gives, by its declared name: by position, then by name
    (``name => value``). One it does not declare, and one given twice, are refused."""
    declared = [a for a in command.get("arguments") or [] if a.get("name")]
    signature = _signature(name, declared)
    by_name = {a["name"].lower(): a["name"] for a in declared}
    given: dict[str, exp.Expression] = {}
    positional = [e for e in call.expressions if not isinstance(e, (exp.Kwarg, exp.PropertyEQ))]
    if len(positional) > len(declared):
        raise OperationFunctionRefused(
            f"{signature} takes {len(declared)} argument(s); {len(positional)} given"
        )
    for arg, value in zip(declared, positional):
        given[arg["name"]] = value
    for kwarg in call.expressions:
        if not isinstance(kwarg, (exp.Kwarg, exp.PropertyEQ)):
            continue
        key = by_name.get(kwarg.this.name.lower())
        if key is None:
            raise OperationFunctionRefused(
                f"{signature}: argument {kwarg.this.name!r} is not one it declares"
            )
        if key in given:
            raise OperationFunctionRefused(f"{signature}: argument {key!r} is given twice")
        given[key] = kwarg.expression
    return given


def _relation(name: str, command: dict, call: exp.Anonymous, ctx: Any) -> exp.Select:
    """The relation a call of ``command`` is: its table, each given argument assigned to its
    parameter by equality."""
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name
    from provisa.core.parameters import canonical_name

    meta = returned_table(command, ctx)
    if meta is None:
        raise OperationFunctionRefused(
            f"function {name!r} returns the rows of a table this role does not read"
        )
    columns = {canonical_name(f.name): f.column for f in ctx.parameters.get(meta.table_id, ())}
    table = exp.table_(
        semantic_table_name(meta), db=domain_to_sql_name(meta.domain_id), quoted=True, alias="_op"
    )
    relation = exp.select("*").from_(table)
    for argument, value in _assigned(name, command, call).items():
        if isinstance(value, exp.Null):
            continue  # an argument given NULL is not given
        column = columns.get(canonical_name(argument))
        if column is None:
            raise OperationFunctionRefused(
                f"function {name!r}: its argument {argument!r} is no parameter of the table "
                f"it returns ({command['returns']})"
            )
        relation = relation.where(
            exp.EQ(this=exp.column(column, table="_op", quoted=True), expression=value.copy())
        )
    return relation


def expand(
    tree: exp.Expression,
    raw_sql: str,
    role_id: str,
    state: object,  # object-ok: AppState lives in provisa.api, above the compiler
) -> bool:
    """Replace, in place, each call in FROM of an operation function ``role_id`` may call by
    the relation it is. True when one was replaced. A function the role may not call is left
    as written, and reads as a name never registered does."""
    functions = _functions(state)
    if not functions:
        return False
    text = raw_sql.lower()
    named = [name for name in functions if name in text]
    if not named:
        return False
    from provisa.security.mutation_authz import command_reachable
    from provisa.security.rights import require_role

    role = require_role(getattr(state, "roles"), role_id)
    usable = {n: functions[n] for n in named if command_reachable(functions[n], role)}
    calls = [
        table
        for table in tree.find_all(exp.Table)
        if isinstance(table.this, exp.Anonymous) and table.this.name.lower() in usable
    ]
    if not calls:
        return False
    ctx = getattr(state, "contexts")[role_id]
    for table in calls:
        call = table.this
        relation = _relation(call.name, usable[call.name.lower()], call, ctx)
        alias = table.args.get("alias") or exp.TableAlias(
            this=exp.to_identifier(call.name, quoted=True)
        )
        subquery: exp.Expression = exp.Subquery(this=relation, alias=alias)
        # A function in FROM reads the tables before it (PostgreSQL): an argument that is a
        # column of one makes the relation a lateral one.
        if any(isinstance(c, exp.Column) for a in call.expressions for c in a.find_all(exp.Column)):
            subquery = exp.Lateral(this=exp.Subquery(this=relation), alias=alias)
        table.replace(subquery)
    return True
