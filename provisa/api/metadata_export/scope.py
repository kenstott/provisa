# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1b7c3a-94d2-4f6e-b0a8-2c7d9e41f863
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What one catalog is published: the org's snapshot narrowed to the catalog's domains (REQ-2122).

Each domain works with its own catalogs. A table and its columns publish to the catalogs of their
domain, a data product to those of its owning domain, and the glossary to every catalog. Every
other fact (a source, a relationship, a lineage edge, a tag, a quality check, a term's link)
publishes with the assets it is about, so nothing published to a catalog points at an asset that
catalog was not given.
"""

# Requirements: REQ-2122

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import replace

from provisa.api.metadata_export.model import (
    AssetRef,
    DataProductAsset,
    DomainAsset,
    MetadataSnapshot,
    TableAsset,
)


def for_domains(snapshot: MetadataSnapshot, domains: Iterable[str]) -> MetadataSnapshot:
    """``snapshot`` as a catalog serving ``domains`` is published it."""
    wanted = set(domains)
    tables = [t for t in snapshot.tables if t.domain_id in wanted]
    products = [p for p in snapshot.data_products if p.domain_id in wanted]
    return _with_assets(snapshot, tables, products, [d for d in snapshot.domains if d.id in wanted])


def for_role(
    snapshot: MetadataSnapshot, served: Callable[[TableAsset], set[str] | None]
) -> MetadataSnapshot:
    """``snapshot`` as a target naming a role is published it (REQ-2152, ruling Q1): exactly
    what the role reads. ``served(table)`` is the columns of ``table`` the role is served (None:
    all of them); a table it is served none of is not published, nor a product with no member
    left, and every other fact goes only with the assets kept."""
    tables = []
    for table in snapshot.tables:
        columns = served(table)
        if columns is None:
            tables.append(table)
            continue
        kept = [c for c in table.columns if c.name in columns]
        if kept:
            tables.append(replace(table, columns=kept))
    members = {t.ref for t in tables}
    products = [
        replace(p, members=tuple(m for m in p.members if m in members))
        for p in snapshot.data_products
        if any(m in members for m in p.members)
    ]
    domain_ids = {t.domain_id for t in tables} | {p.domain_id for p in products}
    if tables == snapshot.tables and products == snapshot.data_products:
        return snapshot
    return _with_assets(
        snapshot, tables, products, [d for d in snapshot.domains if d.id in domain_ids]
    )


def _with_assets(
    snapshot: MetadataSnapshot,
    tables: list[TableAsset],
    products: list[DataProductAsset],
    domains: list[DomainAsset],
) -> MetadataSnapshot:
    """``snapshot`` keeping ``tables`` and ``products``, and every other fact only with them."""
    source_ids = {t.source_id for t in tables}
    sources = [s for s in snapshot.sources if s.id in source_ids]
    kept: set[AssetRef] = {t.ref for t in tables}
    kept.update(c.ref for t in tables for c in t.columns)
    kept.update(s.ref for s in sources)
    kept.update(p.ref for p in products)
    relationships = [
        r
        for r in snapshot.relationships
        if r.source in kept and (r.target is None or r.target in kept)
    ]
    relationship_ids = {r.id for r in relationships}
    return replace(
        snapshot,
        sources=sources,
        domains=domains,
        tables=tables,
        data_products=products,
        relationships=relationships,
        lineage=[e for e in snapshot.lineage if e.upstream in kept and e.downstream in kept],
        governance_tags=[g for g in snapshot.governance_tags if g.asset in kept],
        model_tags=[
            m
            for m in snapshot.model_tags
            if (m.asset is not None and m.asset in kept)
            or (m.relationship_id is not None and m.relationship_id in relationship_ids)
        ],
        glossary_terms=[
            replace(term, refs=tuple(ref for ref in term.refs if ref in kept))
            for term in snapshot.glossary_terms
        ],
        assertions=[a for a in snapshot.assertions if a.asset in kept],
    )
