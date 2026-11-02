# Copyright (c) 2026 Kenneth Stott
# Canary: f1fdfd5d-faa2-4e32-9957-6fde643996b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``$expand``: a navigation property read in its parent's one statement (REQ-2193).

A navigation property is a relationship field of the read's GraphQL text, compiled as GraphQL
compiles it: one correlated JSON subquery per field, so a parent stays one row whatever it expands,
and two to-many fields do not multiply each other. GraphQL's own ``where`` and ``order_by`` on a
relationship compile to a lateral join instead, which repeats the parent per related row, so a
nested ``$filter``, ``$orderby`` and ``$skip`` are added to that subquery's rows here, as ``$filter``
is added to the root's. ``validate_sql`` and ``apply_governance`` then read the subquery as they read
any SELECT: the related table's masks, row rules and column grants are theirs, and a role's row
ceiling bounds the list.

A to-many field reads one row past its page: that row says the list has more, and is not returned.
The rest is read from ``set(key)/navigation``, which is the same read for one parent, so the parent
must be addressable: a set with no key, or whose key the role reads masked, has no address.
"""

# Requirements: REQ-2193

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any
from urllib.parse import quote, urlencode

import orjson
import sqlglot
from sqlglot import exp

from provisa.api.odata.edm import EdmType, edm_value
from provisa.api.odata.errors import QueryOptionRefused
from provisa.api.odata.filter_sql import filter_condition
from provisa.api.odata.model import EntitySet, Navigation, entity_set
from provisa.api.odata.query import Expansion, QueryOptions
from provisa.api.role_surface import RoleSurface
from provisa.compiler.sql_types import CompiledQuery

EXPAND_PAGE_SIZE = 1000

#: The key properties a set's rows are addressed by, for this role: none where it has no address.
Addressing = Callable[[EntitySet], tuple[str, ...]]


def page_of(top: int | None, size: int) -> tuple[int, int]:
    """The rows a page holds, and the one past them read to see whether the rows go on: none when
    ``top`` is what ends them."""
    page = size if top is None else min(top, size)
    return page, 1 if top is None or top > page else 0


def _no_property(name: str, entity: EntitySet) -> QueryOptionRefused:
    # A column the role cannot see is unknown, as one that does not exist is.
    return QueryOptionRefused(400, "invalid_query_option", f"no property {name!r} of {entity.name}")


def selected(entity: EntitySet, options: QueryOptions) -> tuple[str, ...]:
    if options.select is None:
        return tuple(p.name for p in entity.properties)
    for name in options.select:
        if entity.property(name) is None:
            raise _no_property(name, entity)
    return options.select


def ordered_by(entity: EntitySet, options: QueryOptions) -> list[dict[str, str]]:
    """The client's order, then the read's own, so a page boundary between rows equal in the
    client's keys loses none. OData sorts a null first ascending and last descending."""
    for key in options.order_by:
        if entity.property(key.name) is None:
            raise _no_property(key.name, entity)
    asked = [
        {"field": key.name, "dir": "desc_nulls_last" if key.descending else "asc_nulls_first"}
        for key in options.order_by
    ]
    named = {key.name for key in options.order_by}
    return asked + [{"field": n, "dir": "asc"} for n in entity.ordering if n not in named]


@dataclass(frozen=True)
class Plan:
    """One expansion against the role's model."""

    navigation: Navigation
    target: EntitySet
    parent_key: tuple[str, ...]  # the parent's address; empty when it has none
    options: QueryOptions
    raw: tuple[tuple[str, str], ...]
    fields: tuple[str, ...]  # the properties written
    children: tuple[Plan, ...]
    page: int
    probe: int


def _plan(
    surface: RoleSurface, parent: EntitySet, expansion: Expansion, addressing: Addressing
) -> Plan:
    navigation = parent.navigation(expansion.name)
    if navigation is None:
        # A relationship into a table the role cannot see is unknown, as one that does not exist.
        raise QueryOptionRefused(
            400,
            "invalid_query_option",
            f"no navigation property {expansion.name!r} of {parent.name}",
        )
    target = entity_set(surface, navigation.target_set, navigations=True)
    assert target is not None  # _navigations keeps only a target the role has a set for
    options = expansion.options
    if not navigation.collection and (
        options.filter is not None or options.order_by or options.top is not None or options.skip
    ):
        raise QueryOptionRefused(
            400,
            "invalid_query_option",
            f"$expand: {navigation.name} is single-valued, so it takes only $select and $expand",
        )
    page, probe = page_of(options.top, EXPAND_PAGE_SIZE) if navigation.collection else (1, 0)
    return Plan(
        navigation,
        target,
        addressing(parent),
        options,
        expansion.raw,
        selected(target, options),
        plans_of(surface, target, options.expand, addressing),
        page,
        probe,
    )


def plans_of(
    surface: RoleSurface,
    entity: EntitySet,
    expansions: tuple[Expansion, ...],
    addressing: Addressing,
) -> tuple[Plan, ...]:
    return tuple(_plan(surface, entity, e, addressing) for e in expansions)


def read_fields(fields: tuple[str, ...], plans: tuple[Plan, ...]) -> list[str]:
    """The GraphQL selection of one level: its properties, the key a nextLink addresses its rows
    by (read whether or not it is written), and each expansion. ``$top=0`` selects no rows."""
    keys = [k for plan in plans if plan.navigation.collection for k in plan.parent_key]
    return [
        *dict.fromkeys((*fields, *keys)),
        *(_field(plan) for plan in plans if plan.page > 0),
    ]


def _field(plan: Plan) -> str:
    arguments = f"(limit: {plan.page + plan.probe})" if plan.navigation.collection else ""
    selection = " ".join(read_fields(plan.fields, plan.children))
    return f"{plan.navigation.name}{arguments} {{ {selection} }}"


def _value_of(select: exp.Select, name: str) -> exp.Expr:
    """What ``select`` holds for the relationship field ``name``: a projection of the root, a
    key of a relationship's json_object."""
    for projection in select.expressions:
        if projection.alias == name:
            return projection.this
        value = projection.unalias()
        if isinstance(value, exp.JSONObject):
            for pair in value.expressions:
                if pair.this.name == name:
                    return pair.expression
    raise AssertionError(f"the statement has no field {name}")


def _from_of(select: exp.Select) -> exp.Expr:
    return (select.args.get("from_") or select.args["from"]).this


def _rows_of(value: exp.Expr, collection: bool) -> exp.Select:
    """The SELECT that reads a relationship's rows: ``_build_rel_json_expr``'s forms, of which a
    to-many field always takes the limited one."""
    select = (value.this if isinstance(value, exp.Coalesce) else value).this
    return _from_of(select).this if collection else select


def _ordering(rows: exp.Select, plan: Plan) -> list[exp.Ordered]:
    alias = _from_of(rows).alias_or_name
    keys = []
    for key in ordered_by(plan.target, plan.options):
        found = plan.target.property(key["field"])
        assert found is not None
        keys.append(
            exp.Ordered(
                this=exp.column(found.column, table=alias, quoted=True),
                desc=key["dir"] == "desc_nulls_last",
                nulls_first=key["dir"] == "asc_nulls_first",
            )
        )
    return keys


def _narrow(rows: exp.Select, plan: Plan, params: list) -> None:
    if plan.options.filter is not None:
        where, bound = filter_condition(plan.target, plan.options.filter, len(params) + 1)
        rows.where(where, copy=False)
        params.extend(bound)
    keys = _ordering(rows, plan)
    if keys:
        rows.order_by(*keys, copy=False)
    if plan.options.skip:
        rows.offset(plan.options.skip, copy=False)


def _shape(select: exp.Select, plans: tuple[Plan, ...], params: list) -> None:
    for plan in plans:
        if plan.page == 0:
            continue
        collection = plan.navigation.collection
        rows = _rows_of(_value_of(select, plan.navigation.name), collection)
        if collection:
            _narrow(rows, plan, params)
        _shape(rows, plan.children, params)


def with_expansions(compiled: CompiledQuery, plans: tuple[Plan, ...]) -> CompiledQuery:
    """``compiled`` with each expansion's filter, order and skip on its rows."""
    if not plans:
        return compiled
    if any(column.nested_in is not None for column in compiled.columns):
        # A to-one field over a table replicated row by row is joined, not aggregated
        # (sql_where._joins_row_level_to_one), and has no subquery to read.
        raise QueryOptionRefused(
            501, "not_implemented", "$expand of a table replicated row by row is not served"
        )
    statement = sqlglot.parse_one(compiled.sql, read="postgres")
    assert isinstance(statement, exp.Select)
    params = list(compiled.params)
    _shape(statement, plans, params)
    return replace(compiled, sql=statement.sql(dialect="postgres"), params=params)


def page_link(
    url: str, items: tuple[tuple[str, str], ...], options: QueryOptions, taken: int
) -> str:
    """The read at ``url`` from the row after this page, keeping the client's own options."""
    query = [(k, v) for k, v in items if k not in ("$top", "$skip")]
    if options.top is not None:
        query.append(("$top", str(options.top - taken)))
    query.append(("$skip", str(options.skip + taken)))
    return f"{url}?{urlencode(query, safe='$,*')}"


def _key_literal(value: Any, edm: EdmType) -> str:  # noqa: ANN401 — a statement's value
    """``value`` as a key predicate writes it, which ``expression.parse_key`` reads back."""
    written = edm_value(value, edm)
    if edm.name == "Edm.String":
        return "'" + str(written).replace("'", "''") + "'"
    if isinstance(written, bool):
        return "true" if written else "false"
    if isinstance(written, orjson.Fragment):
        return format(Decimal(str(value)), "f")
    return str(written)


def _address(entity: EntitySet, key: tuple[str, ...], values: Mapping[str, Any]) -> str:
    def literal(name: str) -> str:
        found = entity.property(name)
        assert found is not None
        return _key_literal(values[name], found.type)

    if len(key) == 1:
        return f"{entity.name}({literal(key[0])})"
    return f"{entity.name}({','.join(f'{name}={literal(name)}' for name in key)})"


def _link(root: str, entity: EntitySet, values: Mapping[str, Any], plan: Plan) -> str:
    name = plan.navigation.name
    if not plan.parent_key:
        # Never a list cut short with nothing to say so.
        raise QueryOptionRefused(
            501,
            "not_implemented",
            f"{name} of {entity.name} holds more than {plan.page} rows, and a row of "
            f"{entity.name} has no key this role can address the rest by",
        )
    address = quote(_address(entity, plan.parent_key, values), safe="(),='")
    return page_link(f"{root}/{address}/{name}", plan.raw, plan.options, plan.page)


def _decoded(value: Any) -> Any:  # noqa: ANN401 — a relationship's JSON, or an engine's reading of it
    """A relationship's value, which an engine may hand back as text. A number stays exact: a
    float would round a long decimal."""
    return json.loads(value, parse_float=Decimal) if isinstance(value, str) else value


def _expanded(
    written: dict,
    values: Mapping[str, Any],
    entity: EntitySet,
    plan: Plan,
    root: str,
) -> None:
    name = plan.navigation.name
    if not plan.navigation.collection:
        found = _decoded(values[name])
        written[name] = (
            None if found is None else write(found, plan.target, plan.fields, plan.children, root)
        )
        return
    rows = _decoded(values[name]) if plan.page > 0 else []
    written[name] = [
        write(row, plan.target, plan.fields, plan.children, root) for row in rows[: plan.page]
    ]
    if len(rows) > plan.page:
        written[f"{name}@odata.nextLink"] = _link(root, entity, values, plan)


def write(
    values: Mapping[str, Any],
    entity: EntitySet,
    fields: tuple[str, ...],
    plans: tuple[Plan, ...],
    root: str,
) -> dict:
    """One row as an entity: its ``fields`` by their Edm types, and each expansion nested under
    its navigation property. ``root`` is the service's address, which a nextLink starts from."""
    types = {p.name: p.type for p in entity.properties}
    written = {name: edm_value(values[name], types[name]) for name in fields}
    for plan in plans:
        _expanded(written, values, entity, plan, root)
    return written


def context_selection(options: QueryOptions) -> str:
    """What a read selects, as a context URL states it: an expansion appears where it narrows what
    is read of its target."""
    selection = list(options.select or ())
    nested = [
        f"{e.name}({inner})" for e in options.expand if (inner := context_selection(e.options))
    ]
    if nested and options.select is None:
        selection.append("*")
    return ",".join([*selection, *nested])
