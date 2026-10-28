# Copyright (c) 2026 Kenneth Stott
# Canary: 3b725ea5-23a0-48ae-8720-bab605fcad7f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ontology schema's Class tables (REQ-2163).

Each Class term a served table is grounded in (its ``class_term_id``) is a table of the ontology
schema, and each Property term bound to a column of one of those tables is a column of it. The
table is a view, generated with the model and never stored: the union of the Class's tables, one
branch per table, each row an instance from one table, with ``_source_table`` naming that table.
A Property column a table does not bind is null on its rows; one bound to several columns of a
table is a list of them. A Property's columns of differing types are read as their common
supertype (provisa.compiler.supertype).

Read rights are the bound tables' own. A reader is served a Class column when it is served one of
the columns it is bound to, and reads the view through a body made for it (:func:`reader_body`):
a branch for each table it is served a column of, with each column it is not served there read as
null. That body is then governed as any view's is, so each table's masks and row rules apply.

A Class that cannot be offered -- its name, or one of its Properties', gives no identifier, or two
of its Properties give one -- is refused by itself, naming why; every other Class is served.
"""

# Requirements: REQ-2163

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from provisa.compiler.naming import apply_sql_name, ontology_name, semantic_relation_names
from provisa.compiler.supertype import common_supertype

#: The column naming the table an instance comes from. Underscored, as system fields are, so no
#: Property's column takes it.
SOURCE_TABLE_COLUMN = "_source_table"

#: Served columns of a table by id: None when every column is served.
Served = Callable[[int], "frozenset[str] | None"]


def class_table_id(term_id: int) -> int:
    """A Class table's id: never a registered table's, which are positive."""
    return -term_id


def is_class_table_id(table_id: int) -> bool:
    """Whether ``table_id`` is a Class table's (:func:`class_table_id`)."""
    return table_id < 0


def _relation(table: dict) -> str:
    """The semantic name a view's SQL gives ``table``: domain-qualified."""
    return semantic_relation_names(
        table["domain_id"], apply_sql_name(table.get("alias") or table["table_name"])
    )[0]


def _column(name: str, data_type: str, *, is_list: bool) -> dict[str, Any]:
    """A Class column, in the shape a registered table's column takes."""
    return {
        "column_name": name,
        "data_type": f"array({data_type})" if is_list else data_type,
        "visible_to": [],
        "masks": [],
        "alias": None,
        "description": None,
        "path": None,
        "is_primary_key": False,
        "object_fields": [],
        "native_filter_type": None,
        "native_filter_required": None,
        "required_by_steward": False,
        "scope": "domain",
        "epoch_unit": None,
        "array_depth": 1 if is_list else 0,
        "element_type": data_type if is_list else None,
        "native_type": f"array({data_type})" if is_list else None,
        "native_platform": None,
        "fake": None,
        "fake_stable": False,
        "fake_stable_version": None,
        "synthetic_rule": None,
        "fake_why": None,
        "embedding": False,
        "embedding_model": None,
    }


def _row(term_id: int, name: str, columns: list[dict], bindings: list[dict]) -> dict[str, Any]:
    """A Class table, in the shape a registered view's row takes."""
    from provisa.core.models import DERIVED_SOURCE_ID
    from provisa.security.rights import ONTOLOGY_DOMAIN_ID

    return {
        "id": class_table_id(term_id),
        "source_id": DERIVED_SOURCE_ID,
        "domain_id": ONTOLOGY_DOMAIN_ID,
        "schema_name": ONTOLOGY_DOMAIN_ID,
        "table_name": name,
        "alias": None,
        "description": None,
        "unique_constraints": [],
        "l1_cluster": None,
        "l2_cluster": None,
        "l3_cluster": None,
        "enable_aggregates": False,
        "enable_group_by": False,
        "view_sql": None,
        "dq_contract": None,
        "profiler_source_id": None,
        "live": {},
        "push_debounce_quiet": 0.0,
        "push_debounce_max_delay": 5.0,
        "cache_ttl": None,
        "row_materialize": False,
        "role_ttl": {},
        "replicate": None,
        "load_protected": None,
        "region": None,
        "draft": False,
        "off_peak_window": None,
        "off_peak_tz": None,
        "file_glob": None,
        "source_file_column": None,
        "delta": None,
        "table_format": None,
        "change_signal": None,
        "watermark_column": None,
        "probe_type": None,
        "pagination": None,
        "landing_filter": None,
        "query_template": None,
        "business_key": None,
        "valid_time": None,
        "recorded_time": None,
        "mv_refresh_interval": 300,
        "mv_allowed_lateness": 0.0,
        "write_grants": [],
        "columns": columns,
        # What the Class is the union of: each bound table, and the columns each Class column
        # reads there with their types.
        "class_term_id": term_id,
        "class_bindings": bindings,
    }


def class_tables(
    tables: list[dict],
    groundings: dict[int, int],
    terms: dict[int, str],
    refs: list[tuple[int, int, str]],
) -> tuple[list[dict], dict[int, str]]:
    """The Class tables of a model: ``(tables, refused)``.

    ``tables`` are the served tables; ``groundings`` the Class term of each table grounded in one
    (its ``class_term_id``), served or not; ``terms`` the name of every term in service; ``refs``
    each (term, table, column) binding. ``refused`` maps each Class term that cannot be offered to
    why."""
    by_class: dict[int, list[dict]] = {}
    for table in tables:
        term_id = groundings.get(table["id"])
        if term_id is not None and term_id in terms:
            by_class.setdefault(term_id, []).append(table)
    bound: dict[int, dict[str, list[int]]] = {}
    for term_id, table_id, column in refs:
        if term_id in terms:
            bound.setdefault(table_id, {}).setdefault(column, []).append(term_id)

    result: list[dict] = []
    refused: dict[int, str] = {}
    named: dict[str, int] = {}
    for term_id in sorted(by_class):
        reason, row = _class_table(term_id, terms, by_class[term_id], bound)
        if row is not None and row["table_name"] in named:
            other = terms[named[row["table_name"]]]
            reason = f"Class {terms[term_id]!r} and Class {other!r} both name the table {row['table_name']!r}"
        if reason is not None:
            refused[term_id] = reason
            continue
        assert row is not None
        named[row["table_name"]] = term_id
        result.append(row)
    return result, refused


def _class_table(
    term_id: int,
    terms: dict[int, str],
    grounded: list[dict],
    bound: dict[int, dict[str, list[int]]],
) -> tuple[str | None, dict | None]:
    """One Class's table, or why it cannot be offered."""
    name = ontology_name(terms[term_id])
    if name is None:
        return f"Class {terms[term_id]!r} gives no table name", None
    properties: dict[str, int] = {}
    per_table: list[tuple[dict, dict[str, list[tuple[str, str]]]]] = []
    for table in sorted(grounded, key=lambda t: t["id"]):
        types = {c["column_name"]: c.get("data_type") or "varchar" for c in table["columns"]}
        reads: dict[str, list[tuple[str, str]]] = {}
        for column in [c["column_name"] for c in table["columns"]]:
            for prop in bound.get(table["id"], {}).get(column, []):
                prop_name = ontology_name(terms[prop])
                if prop_name is None:
                    return (
                        f"Property {terms[prop]!r} of Class {terms[term_id]!r} gives no column name",
                        None,
                    )
                if prop_name == SOURCE_TABLE_COLUMN.lstrip("_"):
                    # A field name drops the underscore, so it would be _source_table's field.
                    return (
                        f"Property {terms[prop]!r} of Class {terms[term_id]!r} names the field "
                        f"the column {SOURCE_TABLE_COLUMN!r} takes"
                    ), None
                if properties.setdefault(prop_name, prop) != prop:
                    return (
                        f"Properties {terms[properties[prop_name]]!r} and {terms[prop]!r} of Class "
                        f"{terms[term_id]!r} both name the column {prop_name!r}"
                    ), None
                reads.setdefault(prop_name, []).append((column, types[column]))
        per_table.append((table, reads))
    columns = [_column(SOURCE_TABLE_COLUMN, "varchar", is_list=False)]
    for prop_name in sorted(properties):
        bound_types = [t for _, reads in per_table for _, t in reads.get(prop_name, [])]
        is_list = any(len(reads.get(prop_name, [])) > 1 for _, reads in per_table)
        columns.append(_column(prop_name, common_supertype(bound_types), is_list=is_list))
    bindings = [
        {
            "table_id": table["id"],
            "relation": _relation(table),
            "columns": {p: [list(r) for r in rs] for p, rs in reads.items()},
        }
        for table, reads in per_table
    ]
    return None, _row(term_id, name, columns, bindings)


def served_class_columns(row: dict, served: Served) -> frozenset[str]:
    """The columns of the Class table ``row`` a reader is served: each one bound to a column it is
    served in one of the tables, and ``_source_table`` when it is served any of them."""
    result: set[str] = set()
    for binding in row["class_bindings"]:
        visible = served(binding["table_id"])
        reads = {
            p for p, rs in binding["columns"].items() if any(_is_served(c, visible) for c, _ in rs)
        }
        if visible is None or visible:
            result.add(SOURCE_TABLE_COLUMN)
        result |= reads
    return frozenset(result)


def _is_served(column: str, visible: frozenset[str] | None) -> bool:
    return visible is None or column in visible


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _read(reads: list[list[str]], column: dict, visible: frozenset[str] | None) -> str:
    """One branch's expression for one Class column."""
    data_type = column["element_type"] or column["data_type"]

    def cast(name: str, of_type: str) -> str:
        return _quoted(name) if of_type == data_type else f"CAST({_quoted(name)} AS {data_type})"

    served = [cast(c, t) for c, t in reads if _is_served(c, visible)]
    if column["array_depth"]:
        if not served:
            return f"CAST(NULL AS {data_type}[])"
        return f"ARRAY[{', '.join(served)}]"
    return served[0] if served else f"CAST(NULL AS {data_type})"


def reader_body(row: dict, served: Served) -> str:
    """The SQL a reader reads the Class table ``row`` through: a branch for each of its tables the
    reader is served a column of, reading each column it is not served there as null. A reader
    served none of them reads no rows, and is served none of the Class's columns either
    (:func:`served_class_columns`), so a statement naming one is refused as for any table."""
    branches = []
    for binding in row["class_bindings"]:
        visible = served(binding["table_id"])
        if visible is not None and not visible:
            continue
        items = [f"'{binding['relation']}' AS {_quoted(SOURCE_TABLE_COLUMN)}"]
        for column in row["columns"][1:]:
            reads = binding["columns"].get(column["column_name"], [])
            items.append(f"{_read(reads, column, visible)} AS {_quoted(column['column_name'])}")
        branches.append(f"SELECT {', '.join(items)} FROM {binding['relation']}")
    if not branches:
        nulls = [f"{_read([], c, None)} AS {_quoted(c['column_name'])}" for c in row["columns"]]
        return f"SELECT {', '.join(nulls)} WHERE 1 = 0"
    return " UNION ALL ".join(branches)


def whole_body(row: dict) -> str:
    """The Class table's SQL read whole: what a read for no role (a build) reads."""
    return reader_body(row, lambda _table_id: None)
