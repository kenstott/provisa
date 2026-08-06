# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4f2b91-3a67-4c05-9d2e-71b8f0c6a453
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Vendor-side identity bindings for published catalog assets (REQ-1389).

One row per (provider, published asset): ``vendor_ref`` is the catalog's own id for the
asset (Atlas guid, OpenMetadata entity UUID, Collibra asset UUID, DataHub dataset URN) and
``physical_key`` is the vendor-side name-key it was published under. The exporter captures
both on every successful publish and reads them back on the next one, so a physical
re-address rebinds the SAME catalog entity instead of trusting the vendor's name-keyed
upsert to find it. Portable SQL via SQLAlchemy Core — both the SQLite and PostgreSQL
control planes are real.
"""

# Requirements: REQ-1389

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, func, select

from provisa.core.schema_org import catalog_bindings

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert_bindings(
    conn: "Connection", provider: str, bindings: dict[str, tuple[str, str]]
) -> None:
    """Record ``{semantic_uri: (vendor_ref, physical_key)}`` for ``provider``."""
    for semantic_uri, (vendor_ref, physical_key) in bindings.items():
        await conn.upsert(
            catalog_bindings,
            {
                "provider": provider,
                "semantic_uri": semantic_uri,
                "vendor_ref": vendor_ref,
                "physical_key": physical_key,
            },
            index_elements=["provider", "semantic_uri"],
            update_columns=["vendor_ref", "physical_key"],
            set_extra={"updated_at": func.now()},
        )


async def load_bindings(conn: "Connection", provider: str) -> dict[str, tuple[str, str]]:
    """``{semantic_uri: (vendor_ref, physical_key)}`` for every binding of ``provider``."""
    result = await conn.execute_core(
        select(
            catalog_bindings.c.semantic_uri,
            catalog_bindings.c.vendor_ref,
            catalog_bindings.c.physical_key,
        ).where(catalog_bindings.c.provider == provider)
    )
    return {row.semantic_uri: (row.vendor_ref, row.physical_key) for row in result.fetchall()}


async def remove_stale_bindings(
    conn: "Connection", provider: str, keep_uris: set[str]
) -> int:
    """Drop ``provider`` bindings for assets no longer in the published model.

    An asset absent from the snapshot will never be republished, so its binding can never
    be used again; keeping it would let a later, unrelated asset reuse the URI and rebind
    a dead catalog entity. Returns how many were removed.
    """
    stmt = _delete(catalog_bindings).where(catalog_bindings.c.provider == provider)
    if keep_uris:
        stmt = stmt.where(catalog_bindings.c.semantic_uri.not_in(keep_uris))
    result = await conn.execute_core(stmt)
    return result.rowcount or 0
