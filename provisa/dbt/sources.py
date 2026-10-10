# Copyright (c) 2026 Kenneth Stott
# Canary: 90442437-753a-417a-88cf-def3d115e6cb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governed catalog as a dbt sources file (REQ-1967).

For a dbt project that does not use the semantic layer: every registered table a role is
served, as a dbt source table under its source and schema, with its description, its columns'
descriptions and the standard dbt tests the model can state:

- ``unique`` and ``not_null`` for a primary key of one column; ``not_null`` for each column of
  a primary key of several;
- ``unique`` for a unique constraint of one column;
- ``relationships`` for a registered relationship between two tables the role is both served.

What the model states that no standard dbt test can (uniqueness over several columns) is not
written as a test; the file names each such key in its opening comment. The model records no
nullability, so ``not_null`` is stated for key columns only.

The file carries definitions and tests. Row security, masking, roles and lineage stay in
Provisa, and the file's opening comment says so. It is derived from the model on every read.
"""

# Requirements: REQ-1967
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from provisa.core.models import ProvisaConfig, Table
from provisa.security.inheritance import flatten_role_dicts
from provisa.security.visibility import visible_tables

FILENAME = "provisa.sources.yml"

#: What the file says of itself, before anything else in it.
GOVERNANCE = (
    "Definitions and tests only. Row security, masking, roles and lineage stay in Provisa: "
    "a dbt project built from this file is governed only when it reads through Provisa's "
    "own SQL endpoint."
)


class UnknownRole(LookupError):
    def __init__(self, role_id: str) -> None:
        super().__init__(f"No role {role_id!r} exists")
        self.role_id = role_id


@dataclass
class DbtSources:
    """The sources document, and what the model states that it could not be given as a test."""

    document: dict
    #: A key over several columns, as ``(source name, table, kind, columns)``.
    not_stated: list[tuple[str, str, str, tuple[str, ...]]] = field(default_factory=list)


def _served(config: ProvisaConfig, role_id: str) -> list[tuple[Table, set[str]]]:
    """Each table ``role_id`` is served, with the names of the columns it is served: the rule
    the schema build and SQL governance read (``security.visibility.visible_tables``)."""
    roles = flatten_role_dicts([role.model_dump() for role in config.roles])
    role = next((r for r in roles if r["id"] == role_id), None)
    if role is None:
        raise UnknownRole(role_id)
    as_dicts = [
        {
            "at": index,
            "domain_id": table.domain_id,
            "columns": [column.model_dump() for column in table.columns],
        }
        for index, table in enumerate(config.tables)
    ]
    return [
        (config.tables[seen["at"]], {column["name"] for column in seen["columns"]})
        for seen in visible_tables(as_dicts, role)
    ]


def _source_names(tables: list[Table]) -> dict[tuple[str, str], str]:
    """The dbt source each (source, schema) is written as: the source's id, or the id and the
    schema where a source has tables in more than one schema (a dbt source has one schema)."""
    schemas: dict[str, set[str]] = {}
    for table in tables:
        schemas.setdefault(table.source_id, set()).add(table.schema_name)
    return {
        (source_id, schema): source_id if len(names) == 1 else f"{source_id}_{schema}"
        for source_id, names in schemas.items()
        for schema in names
    }


def build_dbt_sources(config: ProvisaConfig, role_id: str) -> DbtSources:
    """The sources document for what ``role_id`` is served of ``config``."""
    served = _served(config, role_id)
    names = _source_names([table for table, _ in served])
    # A relationship names its tables by alias or table name, as the model's loader resolves.
    by_name: dict[str, tuple[Table, set[str]]] = {}
    for table, columns in served:
        by_name[table.alias or table.table_name] = (table, columns)
        by_name.setdefault(table.table_name, (table, columns))

    relationships: dict[tuple[int, str], list[dict]] = {}
    for rel in config.relationships:
        from_end, to_end = by_name.get(rel.source_table_id), by_name.get(rel.target_table_id)
        if from_end is None or to_end is None or not rel.target_column:
            continue  # an end the role is not served, or a relationship to a function
        (source, source_columns), (target, target_columns) = from_end, to_end
        if rel.source_column not in source_columns or rel.target_column not in target_columns:
            continue
        to = names[(target.source_id, target.schema_name)]
        relationships.setdefault((id(source), rel.source_column), []).append(
            {
                # A generic test's arguments are nested under ``arguments``, as dbt 1.10.5
                # and later ask (the top-level form is deprecated there). dbt 1.9 and
                # earlier, themselves out of support, do not read this form.
                "relationships": {
                    "arguments": {
                        "to": f"source('{to}', '{target.table_name}')",
                        "field": rel.target_column,
                    }
                }
            }
        )

    out = DbtSources({"version": 2, "sources": []})
    sources: dict[str, dict] = {}
    for table, columns in served:
        name = names[(table.source_id, table.schema_name)]
        source = sources.get(name)
        if source is None:
            source = sources[name] = {"name": name, "schema": table.schema_name, "tables": []}
            out.document["sources"].append(source)
        primary_key = [c.name for c in table.columns if c.is_primary_key]
        unique = [list(uc.columns) for uc in table.unique_constraints]
        if len(primary_key) > 1:
            out.not_stated.append((name, table.table_name, "primary key", tuple(primary_key)))
        for key in unique:
            if len(key) > 1:
                out.not_stated.append((name, table.table_name, "unique constraint", tuple(key)))
        written: dict[str, Any] = {"name": table.table_name}
        if table.description:
            written["description"] = table.description
        written["columns"] = []
        for column in table.columns:
            if column.name not in columns:
                continue
            tests: list[Any] = []
            if primary_key == [column.name] or [column.name] in unique:
                tests.append("unique")
            if column.name in primary_key:
                tests.append("not_null")
            tests.extend(relationships.get((id(table), column.name), []))
            entry: dict[str, Any] = {"name": column.name}
            if column.description:
                entry["description"] = column.description
            if tests:
                entry["data_tests"] = tests
            written["columns"].append(entry)
        source["tables"].append(written)
    return out


def dbt_sources_yaml(config: ProvisaConfig, role_id: str, *, derived_at: str) -> str:
    """The sources file as text: its opening comment, then the document."""
    sources = build_dbt_sources(config, role_id)
    comment = [
        f"dbt sources from Provisa's governed model, as role {role_id!r} is served it.",
        f"Derived {derived_at}; derived again on every download.",
        GOVERNANCE,
    ]
    if sources.not_stated:
        comment.append(
            "The model also states these keys, which no standard dbt test can check "
            "(a key over several columns):"
        )
        comment.extend(
            f"  {source}.{table}: {kind} ({', '.join(columns)})"
            for source, table, kind, columns in sources.not_stated
        )
    head = "".join(f"# {line}\n" for line in comment)
    return head + yaml.safe_dump(sources.document, sort_keys=False, allow_unicode=True)
