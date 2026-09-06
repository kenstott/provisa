# Copyright (c) 2026 Kenneth Stott
# Canary: 3e2d3e46-e42a-46d6-b2e2-a419c58c2b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Snowflake Horizon Catalog adapter (REQ-1635).

Engine-native, unlike every other adapter in this package: OpenMetadata/Atlas/DataHub/Atlan/
Collibra all speak to a REMOTE REST catalog reached over ``MetadataExportConfig.endpoint`` with
its own credentials. Horizon Catalog is not remote — it is Snowflake's own governance surface,
living inside the SAME Snowflake account the engine already runs governed SQL against. So this
adapter opens its own :class:`SnowflakeFederationRuntime` against the engine's configured DSN
(``configured_engine_url()``) rather than reading ``endpoint``/``api_key`` — those fields describe
a second, unrelated connection this adapter has no use for.

Only a DataProduct publishes here (REQ-1592/REQ-1634): each one becomes a Snowflake SHARE over its
member tables' PHYSICAL addresses, granted USAGE/SELECT, then wrapped in a Horizon Catalog listing
(``CREATE LISTING ... FOR SHARE ...``) so it surfaces as a first-class Data Product / Marketplace
listing. A member table's physical address is resolved the same way the engine resolves it for
query execution (:func:`provisa.core.catalog._to_catalog_name` on the source id) — REQ-1637 lands
a non-attachable source as a VIEW at that physical name, and a share over the view works exactly
like a share over a table.

``MetadataSnapshot.data_products`` does not exist yet (REQ-1634 Phase 4, landing separately); this
adapter reads it via ``getattr`` and no-ops when absent, per the feature's own precondition — a
snapshot with no DataProducts publishes nothing here, which is correct, not a fallback masking a
bug. Once that field lands, no change is required here as long as each entry exposes ``id``,
``name``, ``description`` and ``member_tables: list[AssetRef]`` (table-kind refs, ``(source_id,
schema_name, table_name)`` — the same shape :func:`provisa.api.metadata_export.refs.table_ref`
already produces).
"""

# Requirements: REQ-1068, REQ-1635

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from provisa.api.metadata_export.model import AssetKind
from provisa.api.metadata_export.provider import (
    AssetError,
    AssetRefStub,
    MetadataExport,
    PublishResult,
)
from provisa.api.metadata_export.registry import register_provider
from provisa.federation.engine import configured_engine_url
from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import AssetRef, MetadataSnapshot

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(raw: str) -> str:
    name = raw.replace("-", "_")
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"invalid Snowflake identifier: {raw!r}")
    return name


def _escape(value: str) -> str:
    return value.replace("'", "''")


def physical_parts(ref: "AssetRef") -> tuple[str, str, str]:
    """A table ref's governed physical address — mirrors ``SnowflakeFederationRuntime._phys_parts``,
    which takes a ``Source``-like object rather than a ref; this takes the ref directly since that
    is all a published ``AssetRef`` carries."""
    from provisa.core.catalog import _to_catalog_name

    if ref.kind is not AssetKind.TABLE or len(ref.parts) != 3:
        raise ValueError(f"expected a table ref (source, schema, table); got {ref!r}")
    source_id, schema_name, table_name = ref.parts
    return _to_catalog_name(source_id), schema_name, table_name


def share_statements(
    share_name: str, description: str, tables: list[tuple[str, str, str]]
) -> list[str]:
    """DDL creating (or reusing) a share and granting it read access to ``tables``. Pure — no I/O —
    so the shape is testable without a live Snowflake connection."""
    stmts = [f"CREATE SHARE IF NOT EXISTS \"{share_name}\" COMMENT = '{_escape(description)}';"]
    granted_databases: set[str] = set()
    granted_schemas: set[tuple[str, str]] = set()
    for database, schema, table in tables:
        if database not in granted_databases:
            stmts.append(f'GRANT USAGE ON DATABASE "{database}" TO SHARE "{share_name}";')
            granted_databases.add(database)
        if (database, schema) not in granted_schemas:
            stmts.append(f'GRANT USAGE ON SCHEMA "{database}"."{schema}" TO SHARE "{share_name}";')
            granted_schemas.add((database, schema))
        stmts.append(
            f'GRANT SELECT ON TABLE "{database}"."{schema}"."{table}" TO SHARE "{share_name}";'
        )
    return stmts


def listing_statements(
    listing_name: str, share_name: str, name: str, description: str
) -> list[str]:
    """DDL registering the share as a Horizon Catalog / internal Marketplace listing and publishing
    it — the step that makes the DataProduct a first-class Data Product listing, not just a share."""
    manifest = f'title: "{_escape(name)}"\ndescription: "{_escape(description)}"\n'
    return [
        f'CREATE OR REPLACE LISTING "{listing_name}"\nFOR SHARE "{share_name}"\nAS\n$$\n{manifest}$$;',
        f'ALTER LISTING "{listing_name}" PUBLISH;',
    ]


def _table_exists(runtime: SnowflakeFederationRuntime, parts: tuple[str, str, str]) -> bool:
    database, schema, table = parts
    cur = runtime.connection.cursor()
    try:
        cur.execute(f'SHOW OBJECTS LIKE \'{_escape(table)}\' IN SCHEMA "{database}"."{schema}"')
        return bool(cur.fetchall())
    finally:
        cur.close()


@register_provider
class SnowflakeHorizonExport(MetadataExport):  # REQ-1635
    """Registers each DataProduct as a Snowflake Horizon Catalog Data Product / Marketplace
    listing backed by a share over its member tables. Runs ONLY when Snowflake is the configured
    engine (``configured_engine_url()`` scheme) — on any other engine, or a snapshot with no
    DataProducts, ``publish`` is a documented no-op."""

    provider_name = "snowflake_horizon"

    def _runtime_or_none(self) -> SnowflakeFederationRuntime | None:
        url = configured_engine_url()
        if not url or not url.startswith("snowflake://"):
            return None
        return SnowflakeFederationRuntime(url=url)

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        products = getattr(snapshot, "data_products", None) or []
        if not products:
            return result
        runtime = self._runtime_or_none()
        if runtime is None:
            return result
        try:
            published = sum(
                1 for product in products if self._publish_product(runtime, product, result)
            )
            if published:
                result.published["data_products"] = published
        finally:
            runtime.close()
        return result

    def _publish_product(
        self, runtime: SnowflakeFederationRuntime, product: Any, result: PublishResult
    ) -> bool:
        try:
            tables = [physical_parts(ref) for ref in product.member_tables]
        except ValueError as exc:
            result.errors.append(AssetError(AssetRefStub(product.name), str(exc)))
            return False

        missing = [t for t in tables if not _table_exists(runtime, t)]
        if missing:
            joined = ", ".join(".".join(t) for t in missing)
            result.errors.append(
                AssetError(
                    AssetRefStub(product.name),
                    f"Snowflake landing terminal missing for member table(s): {joined}",
                )
            )
            return False

        try:
            share_name = _identifier(f"provisa_{product.id}_share")
            listing_name = _identifier(f"provisa_{product.id}_listing")
        except ValueError as exc:
            result.errors.append(AssetError(AssetRefStub(product.name), str(exc)))
            return False

        statements = share_statements(share_name, product.description, tables) + listing_statements(
            listing_name, share_name, product.name, product.description
        )
        cur = runtime.connection.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
        except Exception as exc:  # noqa: BLE001 - runtime.connection is an opaque DBAPI cursor
            # (REQ-1635: SnowflakeFederationRuntime is the sole owner of the snowflake.connector
            # import; this adapter must not import its driver-specific exception types to
            # distinguish them, so any DBAPI failure from this cursor is reported as-is).
            result.errors.append(AssetError(AssetRefStub(product.name), str(exc)))
            return False
        finally:
            cur.close()
        return True

    async def health(self) -> None:
        url = configured_engine_url()
        if not url or not url.startswith("snowflake://"):
            raise RuntimeError(
                "snowflake_horizon export requires Snowflake as the configured engine "
                "(configured_engine_url() must be a snowflake:// DSN)"
            )
        runtime = SnowflakeFederationRuntime(url=url)
        try:
            runtime.run_arrow("SELECT 1")
        finally:
            runtime.close()
