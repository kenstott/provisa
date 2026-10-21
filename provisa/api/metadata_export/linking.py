# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4a1f63-2b97-4e05-a3c8-6f19d0e7b254
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Taking a table's descriptive metadata from a catalog (REQ-2122).

A table works with its domain's catalogs. For each, :func:`candidates` is the pick list of the
catalog's table assets, the linked or same-physical asset preselected; :func:`offer` reads the
picked asset and lays its description, column descriptions, owners, tags and glossary terms beside
the table's own; :func:`apply` writes what the steward accepted, links the table to the asset (the
binding an export records too) and records what was imported, when, from which catalog.

Tags and terms apply only where a registry tag or a live glossary term of the same name exists;
the rest are listed as unmatched and nothing is created. Owners are shown, never stored. Provisa
links a glossary term to a column, so a term the catalog puts on the table itself is listed as
having no place to go.
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update

from provisa.api.metadata_export.reader import (
    AssetMetadata,
    CatalogAsset,
    CatalogReaderNotAvailable,
    catalog_reader,
    rank,
    readable_providers,
)
from provisa.api.metadata_export.refs import table_uri
from provisa.core.models import CatalogConnection, CatalogsConfig, Table, TagAssignment
from provisa.core.repositories import catalog_link
from provisa.core.secrets import resolve_secrets
from provisa.core.schema_org import (
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    sources,
    table_columns,
    tag_assignments,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: How many assets one catalog's pick list offers.
PICK_LIST_SIZE = 25


class LinkRefused(ValueError):  # REQ-2122
    """A link or an import that cannot be made, named by code for the API to answer."""

    def __init__(self, code: str, message: str, *, status: int = 400, **params: Any) -> None:
        super().__init__(message)
        self.code = f"catalog_link.{code}"
        self.status = status
        self.params = params


@dataclass(frozen=True)
class TableContext:  # REQ-2122
    """A registered table as linking reads it: where it lives, and its descriptive fields now."""

    id: int
    domain_id: str
    semantic_uri: str
    name: str
    physical: tuple[str, ...]
    description: str
    columns: dict[str, str]
    tags: frozenset[tuple[str | None, str]]
    terms: dict[str, str]


async def table_context(conn: "Connection", table_id: int, uri_org_id: str | None) -> TableContext:
    """The table ``table_id`` as linking reads it. An unknown table is refused (404)."""
    row = (
        await conn.execute_core(
            select(registered_tables, sources.c.database)
            .join(sources, sources.c.id == registered_tables.c.source_id)
            .where(registered_tables.c.id == table_id)
        )
    ).fetchone()
    if row is None:
        raise LinkRefused("no_table", f"There is no table {table_id}", status=404, table=table_id)
    columns = (
        await conn.execute_core(
            select(table_columns.c.column_name, table_columns.c.description)
            .where(table_columns.c.table_id == table_id)
            .order_by(table_columns.c.id)
        )
    ).fetchall()
    tags = (
        await conn.execute_core(
            select(tag_assignments.c.column_name, tag_assignments.c.tag_id).where(
                tag_assignments.c.table_id == table_id
            )
        )
    ).fetchall()
    terms = (
        await conn.execute_core(
            select(glossary_term_refs.c.column_name, glossary_terms.c.name)
            .join(glossary_terms, glossary_terms.c.id == glossary_term_refs.c.term_id)
            .where(glossary_term_refs.c.table_id == table_id)
        )
    ).fetchall()
    business = Table.model_construct(
        alias=row.alias, table_name=row.table_name, domain_id=row.domain_id
    )
    return TableContext(
        id=table_id,
        domain_id=row.domain_id,
        semantic_uri=table_uri(uri_org_id, business),
        name=row.table_name,
        # The source's database is held as entered (a value, or a reference resolved at use).
        physical=tuple(
            p for p in (resolve_secrets(row.database or ""), row.schema_name, row.table_name) if p
        ),
        description=row.description or "",
        columns={c.column_name: c.description or "" for c in columns},
        tags=frozenset((t.column_name, t.tag_id) for t in tags),
        terms={t.column_name: t.name for t in terms},
    )


async def locked_uris(
    conn: "Connection", catalog_id: str, uri_org_id: str | None
) -> frozenset[str]:
    """The semantic URIs of the tables locked to ``catalog_id``: the export does not write their
    descriptive fields there."""
    table_ids = await catalog_link.locked_to(conn, catalog_id)
    if not table_ids:
        return frozenset()
    rows = (
        await conn.execute_core(
            select(
                registered_tables.c.alias,
                registered_tables.c.table_name,
                registered_tables.c.domain_id,
            ).where(registered_tables.c.id.in_(table_ids))
        )
    ).fetchall()
    return frozenset(
        table_uri(
            uri_org_id,
            Table.model_construct(alias=r.alias, table_name=r.table_name, domain_id=r.domain_id),
        )
        for r in rows
    )


def domain_catalogs(catalogs: CatalogsConfig, domain_id: str) -> list[CatalogConnection]:
    """The catalogs a table of ``domain_id`` works with that can be read, in panel order."""
    readable = set(readable_providers())
    return [c for c in catalogs.catalogs if domain_id in c.domains and c.provider in readable]


def catalog_of(catalogs: CatalogsConfig, ctx: TableContext, catalog_id: str) -> CatalogConnection:
    """The catalog ``catalog_id`` of the table's domain, or a refusal naming why it is not one."""
    for catalog in domain_catalogs(catalogs, ctx.domain_id):
        if catalog.id == catalog_id:
            return catalog
    raise LinkRefused(
        "not_the_domains",
        f"catalog {catalog_id!r} is not a catalog of domain {ctx.domain_id!r}",
        catalog=catalog_id,
        domain=ctx.domain_id,
    )


def _asset_view(asset: CatalogAsset) -> dict[str, Any]:
    return {
        "ref": asset.ref,
        "name": asset.name,
        "physical": list(asset.physical),
        "description": asset.description,
        "url": asset.url,
    }


async def candidates(
    conn: "Connection",
    catalogs: CatalogsConfig,
    ctx: TableContext,
    *,
    search: str | None,
) -> list[dict[str, Any]]:
    """The pick list from each catalog of the table's domain, read side by side. A catalog that
    cannot be read answers with its reason, beside the others' lists."""
    linked = await catalog_link.links(conn, ctx.semantic_uri)

    async def _one(catalog: CatalogConnection) -> dict[str, Any]:
        link = linked.get(catalog.id)
        try:
            reader = catalog_reader(catalog)
            found = await reader.search(search or ctx.name, PICK_LIST_SIZE)
            if link is not None and all(a.ref != link.vendor_ref for a in found):
                # The linked asset is offered even when the search did not return it.
                found = [(await reader.read(link.vendor_ref)).asset, *found]
        except Exception as exc:  # noqa: BLE001 - allow-blind-except: the reason IS the answer
            # The reader raises what its transport raised; the steward needs that text, and the
            # other catalogs' lists still stand.
            return {
                "catalog": catalog.id,
                "provider": catalog.provider,
                "error": f"{type(exc).__name__}: {exc}",
            }
        ranked = rank(
            found,
            name=ctx.name,
            physical=ctx.physical,
            linked_ref=link.vendor_ref if link is not None else None,
        )
        return {
            "catalog": catalog.id,
            "provider": catalog.provider,
            "linked": link.vendor_ref if link is not None else None,
            "preselected": ranked.preselected.ref if ranked.preselected else None,
            "assets": [_asset_view(a) for a in ranked.candidates],
        }

    return list(await asyncio.gather(*(_one(c) for c in domain_catalogs(catalogs, ctx.domain_id))))


def _named(name: str, known: dict[str, str]) -> str | None:
    """The registry name ``name`` is, matched as written or by its last dotted segment, case
    aside: a catalog qualifies a tag or term by its classification or glossary."""
    for candidate in (name, name.rsplit(".", 1)[-1]):
        hit = known.get(candidate.lower())
        if hit is not None:
            return hit
    return None


async def _registry(conn: "Connection") -> tuple[dict[str, str], dict[str, str]]:
    """The registry's tag ids and the live glossary terms' names, each by its lower-cased form."""
    from provisa.core.models import SYSTEM_TAG_IDS
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.repositories import tag as tag_repo

    tag_ids = [t["id"] for t in await tag_repo.list_all(conn)] + list(SYSTEM_TAG_IDS)
    live = await glossary_repo.live_ids(conn)
    rows = (
        await conn.execute_core(
            select(glossary_terms.c.id, glossary_terms.c.name).where(glossary_terms.c.id.in_(live))
        )
    ).fetchall()
    return {t.lower(): t for t in tag_ids}, {r.name.lower(): r.name for r in rows}


def _match_column(name: str, catalog_columns: list[str]) -> str | None:
    by_lower = {c.lower(): c for c in catalog_columns}
    return by_lower.get(name.lower())


async def offer(
    conn: "Connection",
    catalog: CatalogConnection,
    ctx: TableContext,
    ref: str,
) -> dict[str, Any]:
    """The asset ``ref``'s descriptive metadata laid beside the table's own, each value shown
    with its catalog, for the steward to accept field by field."""
    meta = await catalog_reader(catalog).read(ref)
    return await _offer_of(conn, catalog, ctx, meta)


async def _offer_of(
    conn: "Connection", catalog: CatalogConnection, ctx: TableContext, meta: AssetMetadata
) -> dict[str, Any]:
    tags, terms = await _registry(conn)
    catalog_columns = list(meta.columns)
    imported = (await catalog_link.imports(conn, ctx.id)).get(catalog.id)
    return {
        "catalog": catalog.id,
        "provider": catalog.provider,
        "asset": _asset_view(meta.asset),
        "description": {"current": ctx.description, "catalog": meta.description},
        "columns": [
            {
                "column": name,
                "current": current,
                "catalog_column": _match_column(name, catalog_columns),
            }
            for name, current in ctx.columns.items()
        ],
        "catalog_columns": [
            {
                "name": name,
                "description": meta.columns[name],
                "tags": [
                    {"name": t, "tag": _named(t, tags)} for t in meta.column_tags.get(name, ())
                ],
                "terms": [
                    {"name": t, "term": _named(t, terms)} for t in meta.column_terms.get(name, ())
                ],
            }
            for name in catalog_columns
        ],
        "owners": list(meta.owners),
        "tags": [{"name": t, "tag": _named(t, tags)} for t in meta.tags],
        # Provisa links a term to a column, so a term on the table itself has nowhere to go.
        "terms": [{"name": t, "term": None} for t in meta.terms],
        "current_tags": sorted([c or "", t] for c, t in ctx.tags),
        "current_terms": dict(sorted(ctx.terms.items())),
        "imported_at": imported.imported_at.isoformat()
        if imported is not None and imported.imported_at
        else None,
        "locked": imported.locked if imported is not None else False,
    }


@dataclass(frozen=True)
class Accepted:  # REQ-2122
    """What the steward accepted of an offer.

    description: whether the table takes the asset's description.
    columns: each Provisa column that takes a catalog column's description, by the Provisa
        column's name.
    tags: each (Provisa column, or None for the table; registry tag id) to assign.
    terms: each Provisa column and the live glossary term to link it to.
    """

    description: bool = False
    columns: dict[str, str] | None = None
    tags: tuple[tuple[str | None, str], ...] = ()
    terms: dict[str, str] | None = None


async def apply(
    conn: "Connection",
    catalog: CatalogConnection,
    ctx: TableContext,
    meta: AssetMetadata,
    accepted: Accepted,
) -> dict[str, Any]:
    """Write what the steward accepted of ``meta`` to the table, link the table to the asset and
    record what was imported. Refused by name: a column the table or the asset does not have, a
    tag the registry does not have, a term that is not live."""
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.repositories import tag as tag_repo

    columns = accepted.columns or {}
    terms = accepted.terms or {}
    for column, catalog_column in columns.items():
        if column not in ctx.columns:
            raise LinkRefused("no_column", f"The table has no column {column!r}", column=column)
        if catalog_column not in meta.columns:
            raise LinkRefused(
                "no_catalog_column",
                f"The catalog's asset has no column {catalog_column!r}",
                column=catalog_column,
            )
    tag_ids, live_terms = await _registry(conn)
    for column, tag in accepted.tags:
        if column is not None and column not in ctx.columns:
            raise LinkRefused("no_column", f"The table has no column {column!r}", column=column)
        if tag.lower() not in tag_ids:
            raise LinkRefused("no_tag", f"The registry has no tag {tag!r}", tag=tag)
    for column, term in terms.items():
        if column not in ctx.columns:
            raise LinkRefused("no_column", f"The table has no column {column!r}", column=column)
        if term.lower() not in live_terms:
            raise LinkRefused("no_term", f"There is no live glossary term {term!r}", term=term)

    imported: dict[str, Any] = {}
    if accepted.description:
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == ctx.id)
            .values(description=meta.description or None)
        )
        imported["description"] = meta.description
    for column, catalog_column in columns.items():
        await conn.execute_core(
            update(table_columns)
            .where(table_columns.c.table_id == ctx.id, table_columns.c.column_name == column)
            .values(description=meta.columns[catalog_column] or None)
        )
        imported.setdefault("columns", {})[column] = meta.columns[catalog_column]
        # Which catalog column it came from, so a later import reads the same one.
        imported.setdefault("column_map", {})[column] = catalog_column
    for column, tag in accepted.tags:
        await tag_repo.assign(
            conn,
            TagAssignment(
                tag_id=tag_ids[tag.lower()],
                object_type="table" if column is None else "column",
                table_id=ctx.id,
                column_name=column,
            ),
        )
        imported.setdefault("tags", []).append([column, tag_ids[tag.lower()]])
    if terms:
        term_ids = {
            r.name: r.id
            for r in (
                await conn.execute_core(
                    select(glossary_terms.c.id, glossary_terms.c.name).where(
                        glossary_terms.c.name.in_([live_terms[t.lower()] for t in terms.values()])
                    )
                )
            ).fetchall()
        }
        for column, term in terms.items():
            await glossary_repo.move_ref(conn, ctx.id, column, term_ids[live_terms[term.lower()]])
            imported.setdefault("terms", {})[column] = live_terms[term.lower()]

    await catalog_link.pick(conn, catalog.id, ctx.semantic_uri, meta.asset.ref, meta.asset.name)
    held = (await catalog_link.imports(conn, ctx.id)).get(catalog.id)
    await catalog_link.record_import(conn, ctx.id, catalog.id, _merged(held, imported))
    return imported


def _merged(held: catalog_link.Imported | None, imported: dict[str, Any]) -> dict[str, Any]:
    """The values as imported so far: this import's over the previous ones, field by field."""
    values = dict((held.values if held is not None else None) or {})
    if "description" in imported:
        values["description"] = imported["description"]
    if "columns" in imported:
        values["columns"] = {**values.get("columns", {}), **imported["columns"]}
    if "tags" in imported:
        kept = [t for t in values.get("tags", []) if t not in imported["tags"]]
        values["tags"] = kept + imported["tags"]
    if "column_map" in imported:
        values["column_map"] = {**values.get("column_map", {}), **imported["column_map"]}
    if "terms" in imported:
        values["terms"] = {**values.get("terms", {}), **imported["terms"]}
    return values


def column_map(
    ctx: TableContext, meta: AssetMetadata, held: catalog_link.Imported | None
) -> dict[str, str]:
    """Which catalog column each Provisa column reads: the one it was last imported from, else the
    one of the same name (case aside). A column with neither reads none."""
    previous = ((held.values if held is not None else None) or {}).get("column_map", {})
    by_lower = {c.lower(): c for c in meta.columns}
    mapping: dict[str, str] = {}
    for column in ctx.columns:
        source = previous.get(column)
        if source is not None and source in meta.columns:
            mapping[column] = source
        elif column.lower() in by_lower:
            mapping[column] = by_lower[column.lower()]
    return mapping


@dataclass(frozen=True)
class Change:  # REQ-2122
    """One descriptive field the catalog holds differently from the table.

    field: "description", "column", "tag" or "term". column: the Provisa column, or None for the
    table. current / catalog: the two values. local_edit: the table's value was edited in Provisa
    since it was last imported, so it is never replaced without a steward's review.
    """

    field: str
    column: str | None
    current: str
    catalog: str
    local_edit: bool


async def changes(
    conn: "Connection", ctx: TableContext, meta: AssetMetadata, held: catalog_link.Imported | None
) -> list[Change]:
    """Every descriptive field the asset holds differently from the table, with the catalog's
    tags and terms that Provisa has a tag or live term of the same name for."""
    imported = (held.values if held is not None else None) or {}
    tags, terms = await _registry(conn)
    found: list[Change] = []

    def _edited(current: str, was: str | None) -> bool:
        return was is not None and current != was

    if meta.description and meta.description != ctx.description:
        found.append(
            Change(
                "description",
                None,
                ctx.description,
                meta.description,
                _edited(ctx.description, imported.get("description")),
            )
        )
    for column, source in column_map(ctx, meta, held).items():
        theirs, ours = meta.columns[source], ctx.columns[column]
        if theirs and theirs != ours:
            was = imported.get("columns", {}).get(column)
            found.append(Change("column", column, ours, theirs, _edited(ours, was)))
        for name in meta.column_tags.get(source, ()):
            tag = _named(name, tags)
            if tag is not None and (column, tag) not in ctx.tags:
                found.append(Change("tag", column, "", tag, False))
        for name in meta.column_terms.get(source, ()):
            term = _named(name, terms)
            if term is not None and ctx.terms.get(column) != term:
                current = ctx.terms.get(column, "")
                was = imported.get("terms", {}).get(column)
                found.append(Change("term", column, current, term, _edited(current, was)))
    for name in meta.tags:
        tag = _named(name, tags)
        if tag is not None and (None, tag) not in ctx.tags:
            found.append(Change("tag", None, "", tag, False))
    return found


def accept_all(
    ctx: TableContext, meta: AssetMetadata, held: catalog_link.Imported | None, found: list[Change]
) -> Accepted:
    """Every change in ``found`` accepted: what an import applies to a table locked to the catalog,
    or what a steward accepts for a table at once."""
    mapping = column_map(ctx, meta, held)
    return Accepted(
        description=any(c.field == "description" for c in found),
        columns={c.column: mapping[c.column] for c in found if c.field == "column" and c.column},
        tags=tuple((c.column, c.catalog) for c in found if c.field == "tag"),
        terms={c.column: c.catalog for c in found if c.field == "term" and c.column},
    )


__all__ = [
    "Accepted",
    "Change",
    "accept_all",
    "changes",
    "column_map",
    "CatalogReaderNotAvailable",
    "LinkRefused",
    "TableContext",
    "apply",
    "candidates",
    "catalog_of",
    "domain_catalogs",
    "offer",
    "table_context",
]
