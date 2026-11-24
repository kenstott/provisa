# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e2a9d-6b13-4f58-9e0a-1d3b5c8f7a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The stored names table (REQ-194): the last model build's names, its refused clashes, and the
aliases the disambiguate policy froze. The build is the only writer of the first two; the third
is a model change of its own."""

# Requirements: REQ-194, REQ-1914

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, bindparam, delete, insert, select, update

from provisa.core.repositories.build_output import replacing_build_output
from provisa.core.schema_org import (
    catalog_bindings,
    glossary_term_refs,
    glossary_terms,
    model_name_clashes,
    model_names,
    registered_tables,
    table_columns,
)

if TYPE_CHECKING:
    from provisa.compiler.names_table import Estate


_NAME_KEY = ("object_kind", "object_id", "surface")


def _stored_row(r: Any) -> dict[str, Any]:
    """A names-table row (names_table.NameRow) as ``model_names`` stores it."""
    return {
        "object_kind": r.object_kind,
        "object_id": r.object_id,
        "surface": r.surface,
        "name": r.name,
        "physical_name": r.physical_name,
        "label": r.label,
        "domain_id": r.domain_id,
        "table_id": r.table_id,
        "is_alias": r.alias,
        "warnings": [{"code": w.code, "message": w.message} for w in r.warnings],
        "prime_term": r.prime_term,
        "prime_term_ontology_name": r.prime_term_ontology_name,
    }


def names_table_changes(
    stored: Iterable[Mapping[str, Any]], rows: Iterable[Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """What turns the ``stored`` names table into the one of ``rows`` (names_table.NameRow): the
    keys of the stored rows to delete (each gone or changed) and the rows to insert (each new or
    changed). A row the build left as it was is in neither, so a registration writes the names
    of the tables it changed, not the whole model's."""
    wanted = {tuple(v[k] for k in _NAME_KEY): v for v in map(_stored_row, rows)}
    held = {tuple(s[k] for k in _NAME_KEY): dict(s) for s in stored}
    gone = [
        {f"key_{k}": part for k, part in zip(_NAME_KEY, key, strict=True)}
        for key, s in held.items()
        if wanted.get(key) != s
    ]
    new = [v for key, v in wanted.items() if held.get(key) != v]
    return gone, new


async def write_names_table(conn: Any, rows: Iterable[Any]) -> None:
    """Make the stored names table the one of ``rows`` (names_table.NameRow) and clear the refused
    clashes: a build that stored a names table refused none. Only the rows that differ from the
    stored ones are written (:func:`names_table_changes`)."""
    async with replacing_build_output(conn, model_names):
        # Read under the lock: the change is made to what the last build committed.
        stored = (await conn.execute_core(select(*model_names.c))).mappings().all()
        gone, new = names_table_changes(stored, rows)
        # One statement each however many names: the keys and rows travel beside it (a multi-row
        # VALUES binds ten parameters a name, and PostgreSQL refuses a statement of more than
        # 65535).
        await conn.execute_core_many(
            delete(model_names).where(
                and_(*(model_names.c[k] == bindparam(f"key_{k}") for k in _NAME_KEY))
            ),
            gone,
        )
        await conn.execute_core(delete(model_name_clashes))
        await conn.execute_core_many(insert(model_names), new)


async def write_refused_clashes(conn: Any, clashes: Iterable[Any]) -> None:
    """Replace the stored refused clashes with ``clashes`` (names_table.NameClash). The names
    table stays as the last version built stored it."""
    values = {
        (c.surface, c.name, c.newcomer): {
            "surface": c.surface,
            "name": c.name,
            "holder": c.holder,
            "newcomer": c.newcomer,
            "message": c.message(),
        }
        for c in clashes
    }
    async with replacing_build_output(conn, model_names):
        await conn.execute_core(delete(model_name_clashes))
        await conn.execute_core_many(insert(model_name_clashes), list(values.values()))


async def freeze_aliases(conn: Any, frozen: Iterable[Any]) -> None:
    """Keep each name the disambiguate policy derived (names_table.FrozenAlias) as its object's
    alias, so later builds never rename it (REQ-194, CLASH POLICY). An object that has an alias
    keeps it: only a steward changes one. A table's or view's is ``registered_tables.alias``, a
    column's ``table_columns.alias`` (the column's SQL name)."""
    frozen = list(frozen)
    if not frozen:
        return
    from provisa.core import model_change

    async with model_change.scope("the names table froze disambiguated names as aliases"):
        for f in frozen:
            if f.object_kind in ("table", "view"):
                await conn.execute_core(
                    update(registered_tables)
                    .where(
                        and_(
                            registered_tables.c.id == int(f.object_id),
                            registered_tables.c.alias.is_(None),
                        )
                    )
                    .values(alias=f.alias)
                )
            elif f.object_kind == "column":
                table_id, column = f.object_id.split(":", 1)
                await conn.execute_core(
                    update(table_columns)
                    .where(
                        and_(
                            table_columns.c.table_id == int(table_id),
                            table_columns.c.column_name == column,
                            table_columns.c.alias.is_(None),
                        )
                    )
                    .values(alias=f.alias)
                )
            else:
                raise ValueError(f"a {f.object_kind} has no stored alias to freeze {f.alias!r} in")


async def read_estate(
    conn: Any,
    *,
    org_id: str | None,
    engine: str,
    replicas: Mapping[tuple[str, str, str], tuple[str, str]],
) -> Estate:
    """REQ-194 (THE NAMES TABLE SPANS THE DATA ESTATE): what the stored names table records of
    the estate beside the model's own names (names_table.Estate), with the bound ``engine``'s
    name and the ``replicas`` it keeps: every name an export target carries
    (``catalog_bindings``: what was exported to it, nothing else), and each glossary term in
    force with the columns it owns and the tables it is the Class of."""
    from provisa.compiler.names_table import Estate

    exports: dict[str, dict[str, str]] = {}
    for b in (
        await conn.execute_core(
            select(
                catalog_bindings.c.target,
                catalog_bindings.c.semantic_uri,
                catalog_bindings.c.physical_key,
            )
        )
    ).fetchall():
        exports.setdefault(b.target, {})[b.semantic_uri] = b.physical_key
    in_force = ~glossary_terms.c.retired
    terms = {
        r.id: r.name
        for r in (
            await conn.execute_core(
                select(glossary_terms.c.id, glossary_terms.c.name).where(in_force)
            )
        ).fetchall()
    }
    column_terms = {
        (r.table_id, r.column_name): r.term_id
        for r in (
            await conn.execute_core(
                select(
                    glossary_term_refs.c.table_id,
                    glossary_term_refs.c.column_name,
                    glossary_term_refs.c.term_id,
                )
                .join(glossary_terms, glossary_terms.c.id == glossary_term_refs.c.term_id)
                .where(in_force)
            )
        ).fetchall()
    }
    table_terms = {
        r.id: r.class_term_id
        for r in (
            await conn.execute_core(
                select(registered_tables.c.id, registered_tables.c.class_term_id)
                .join(glossary_terms, glossary_terms.c.id == registered_tables.c.class_term_id)
                .where(in_force)
            )
        ).fetchall()
    }
    return Estate(
        org_id=org_id,
        engine=engine,
        replicas=dict(replicas),
        exports=exports,
        terms=terms,
        column_terms=column_terms,
        table_terms=table_terms,
    )
