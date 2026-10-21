# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Databricks Unity Catalog as a data product target (REQ-2117).

Unity Catalog has no data product entity, so a product is published as the closest thing in its
own terms: one schema, ``<catalog>.<product id>``, in the catalog the Databricks engine works in,
holding a view over each member. Nothing is copied: a view reads its member where the engine keeps
it -- the member's replica (a Delta table in the org's replicas schema, REQ-1912), or for a lake
table the engine reads in place, its external table (``DatabricksFederationRuntime._phys_parts``).

- The view is named for the member's business name; its columns and the view itself carry the
  member's descriptions, and each governance fact is a ``provisa_governance:<signal>`` tag naming
  the rule, never its body.
- The schema's comment holds the product's purpose, limitations, usage, service level and
  support; its owner, team, version and status are ``provisa_product:<field>`` tags (a tag value
  holds 256 characters, so the prose stays in the comment).
- A view in the product's schema that is no longer a member is dropped.
- With ``delta_share`` set, the schema's views are also published as the Delta Share
  ``provisa_<product id>``, for other data teams of the enterprise; a recipient is granted it in
  Databricks. Sharing needs the member in Delta, so a member that is not is reported and left out.

It runs only when Databricks is the configured engine; on any other engine a publish is a no-op.
Each schema's and view's full name is recorded in the target's bindings (REQ-1389).
"""

# Requirements: REQ-2117

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from provisa.api.metadata_export.provider import AssetError, MetadataExport, PublishResult
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        DataProductAsset,
        GovernanceTag,
        MetadataSnapshot,
        TableAsset,
    )

GOVERNANCE_PREFIX = "provisa_governance:"
PRODUCT_PREFIX = "provisa_product:"
_TAG_VALUE_LIMIT = 256

Parts = tuple[str, str, str]


def lit(value: str) -> str:
    """A Databricks SQL string literal body: backslash escapes, a quote doubles."""
    return value.replace("\\", "\\\\").replace("'", "''")


def q(*parts: str) -> str:
    """A backquoted, dotted Unity Catalog name."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def view_name(table: "TableAsset") -> str:
    """The member's view: its business name."""
    return table.aliases[0] if table.aliases else table.name


def share_name(product: "DataProductAsset") -> str:
    return f"provisa_{product.id}"


def schema_comment(product: "DataProductAsset") -> str:
    """The product's prose fields, as the schema's comment."""
    parts = [product.description]
    for heading, text in (
        ("Limitations", product.limitations),
        ("Usage", product.usage),
        ("Service level", product.sla),
        ("Support", product.support),
        ("Support contact", product.support_contact),
        ("Documentation", product.documentation_url),
    ):
        if text:
            parts.append(f"{heading}: {text}")
    return "\n\n".join(p for p in parts if p)


def schema_tags(product: "DataProductAsset") -> dict[str, str]:
    """The product's short fields, as the schema's tags."""
    fields = {
        "owner": product.owner.id if product.owner else None,
        "team": product.team_role,
        "version": product.version,
        "status": product.status,
    }
    return {
        f"{PRODUCT_PREFIX}{k}": v[:_TAG_VALUE_LIMIT] for k, v in fields.items() if v is not None
    }


def view_statement(view: Parts, member: Parts, table: "TableAsset") -> str:
    """The view over ``member``: each column named and described, and the view described."""
    columns = ", ".join(
        f"{q(c.name)} COMMENT '{lit(c.description)}'" if c.description else q(c.name)
        for c in table.columns
    )
    select = ", ".join(q(c.name) for c in table.columns)
    comment = f" COMMENT '{lit(table.description)}'" if table.description else ""
    return (
        f"CREATE OR REPLACE VIEW {q(*view)} ({columns}){comment} "
        f"AS SELECT {select} FROM {q(*member)}"
    )


def governance_statements(
    view: Parts, table: "TableAsset", governance_tags: list["GovernanceTag"]
) -> list[str]:
    """The governance facts on the member and its columns, as tags on its view: which rule
    governs it, never the rule."""
    table_fqn = table.ref.fqn()
    columns = {c.ref.fqn(): c.name for c in table.columns}
    stmts = []
    for tag in governance_tags:
        key = f"{GOVERNANCE_PREFIX}{tag.signal.value}"
        value = lit(tag.rule_id[:_TAG_VALUE_LIMIT])
        fqn = tag.asset.fqn()
        if fqn == table_fqn:
            stmts.append(f"ALTER VIEW {q(*view)} SET TAGS ('{key}' = '{value}')")
        elif fqn in columns:
            stmts.append(
                f"ALTER VIEW {q(*view)} ALTER COLUMN {q(columns[fqn])} "
                f"SET TAGS ('{key}' = '{value}')"
            )
    return stmts


@register_provider
class UnityCatalogExport(MetadataExport):  # REQ-2117
    """Publishes each data product as a Unity Catalog schema of views over its members."""

    provider_name = "unity_catalog"
    # REQ-1912: which members the engine serves from a replica, as the reconcile decided it.
    needs_export_views = True

    def _runtime_or_none(self) -> Any:
        from provisa.federation.engine import configured_engine_url

        url = configured_engine_url()
        if not url or not url.startswith("databricks://"):
            return None
        from provisa.federation.databricks_runtime import DatabricksFederationRuntime

        return DatabricksFederationRuntime(url=url)

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        if not snapshot.data_products:
            return PublishResult(provider_name=self.provider_name)
        runtime = self._runtime_or_none()
        if runtime is None:
            return PublishResult(provider_name=self.provider_name)
        try:
            return await asyncio.to_thread(self._publish, runtime, snapshot)
        finally:
            runtime.close()

    def _member_parts(self, runtime: Any, snapshot: "MetadataSnapshot", ref: Any) -> Parts:
        """Where the engine keeps a member: its replica, or the external table it reads in
        place."""
        from provisa.federation.replica_address import replica_schema, replica_table_name

        source_id, schema_name, table_name = ref.parts
        if (source_id, schema_name, table_name) in self.export_views:
            return (
                runtime.ensure_materialize_attached(),
                replica_schema(snapshot.org_id),
                replica_table_name(source_id, schema_name, table_name),
            )
        source = SimpleNamespace(id=source_id, schema_name=schema_name, table_name=table_name)
        return runtime._phys_parts(source)

    def _publish(self, runtime: Any, snapshot: "MetadataSnapshot") -> PublishResult:
        from databricks.sql.exc import Error as DatabricksError

        from provisa.federation.runtime_support import close_cursor, open_cursor

        result = PublishResult(provider_name=self.provider_name)
        catalog = runtime.ensure_materialize_attached()
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        governance = list(snapshot.governance_tags)
        conn = runtime._open()
        cur = open_cursor(conn)
        try:
            for product in snapshot.data_products:
                try:
                    self._publish_product(
                        cur, runtime, snapshot, catalog, product, tables, governance, result
                    )
                except DatabricksError as exc:
                    # A refused statement is this product's outcome, reported by name; the org's
                    # other products still publish.
                    result.errors.append(AssetError(asset=product.ref, message=str(exc)))
        finally:
            close_cursor(cur)
            conn.close()
        return result

    def _rows(self, cur: Any, sql: str) -> list[tuple]:
        cur.execute(sql)
        return list(cur.fetchall())

    def _publish_product(
        self,
        cur: Any,
        runtime: Any,
        snapshot: "MetadataSnapshot",
        catalog: str,
        product: "DataProductAsset",
        tables: dict[str, "TableAsset"],
        governance: list["GovernanceTag"],
        result: PublishResult,
    ) -> None:
        schema = product.id
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {q(catalog, schema)}")
        cur.execute(f"COMMENT ON SCHEMA {q(catalog, schema)} IS '{lit(schema_comment(product))}'")
        wanted_tags = schema_tags(product)
        have_tags = {
            str(k)
            for (k,) in self._rows(
                cur,
                f"SELECT tag_name FROM {q(catalog)}.information_schema.schema_tags "
                f"WHERE schema_name = '{lit(schema)}'",
            )
            if str(k).startswith(PRODUCT_PREFIX)
        }
        for key in sorted(have_tags - set(wanted_tags)):
            cur.execute(f"ALTER SCHEMA {q(catalog, schema)} UNSET TAGS ('{lit(key)}')")
        if wanted_tags:
            pairs = ", ".join(f"'{lit(k)}' = '{lit(v)}'" for k, v in sorted(wanted_tags.items()))
            cur.execute(f"ALTER SCHEMA {q(catalog, schema)} SET TAGS ({pairs})")
        result.bindings[product.semantic_uri] = (f"{catalog}.{schema}", f"{catalog}.{schema}")

        views: dict[str, Parts] = {}
        members: dict[str, Parts] = {}
        for ref in product.members:
            table = tables[ref.fqn()]
            name = view_name(table)
            if name in views:
                result.errors.append(
                    AssetError(
                        asset=ref,
                        message=f"{ref.fqn()} has the business name {name!r} of another "
                        f"member of {product.id}; one view holds only one",
                    )
                )
                continue
            view = (catalog, schema, name)
            member = self._member_parts(runtime, snapshot, ref)
            cur.execute(view_statement(view, member, table))
            for stmt in governance_statements(view, table, governance):
                cur.execute(stmt)
            views[name] = view
            members[name] = member
            result.bindings[table.semantic_uri] = (".".join(view), ".".join(view))
        for (stale,) in self._rows(
            cur,
            f"SELECT table_name FROM {q(catalog)}.information_schema.views "
            f"WHERE table_schema = '{lit(schema)}'",
        ):
            if str(stale) not in views:
                cur.execute(f"DROP VIEW IF EXISTS {q(catalog, schema, str(stale))}")
        if self._config.delta_share:
            self._share(cur, product, views, members, result)
        result.published["data_products"] = result.published.get("data_products", 0) + 1
        result.published["views"] = result.published.get("views", 0) + len(views)

    def _share(
        self,
        cur: Any,
        product: "DataProductAsset",
        views: dict[str, Parts],
        members: dict[str, Parts],
        result: PublishResult,
    ) -> None:
        """The product's views as its Delta Share: each whose member is in Delta, the rest
        reported."""
        share = share_name(product)
        cur.execute(f"CREATE SHARE IF NOT EXISTS {q(share)}")
        shared = {str(row[0]) for row in self._rows(cur, f"SHOW ALL IN SHARE {q(share)}")}
        wanted: set[str] = set()
        for name, view in sorted(views.items()):
            catalog, schema, table = members[name]
            rows = self._rows(
                cur,
                f"SELECT data_source_format FROM {q(catalog)}.information_schema.tables "
                f"WHERE table_schema = '{lit(schema)}' AND table_name = '{lit(table)}'",
            )
            if not rows or str(rows[0][0]).upper() != "DELTA":
                result.errors.append(
                    AssetError(
                        asset=product.ref,
                        message=f"member {name!r} is not in Delta, so it cannot be shared",
                    )
                )
                continue
            full = ".".join(view)
            wanted.add(full)
            if full not in shared:
                cur.execute(f"ALTER SHARE {q(share)} ADD VIEW {q(*view)}")
        for stale in sorted(shared - wanted):
            cur.execute(f"ALTER SHARE {q(share)} REMOVE VIEW {q(*stale.split('.'))}")
        result.published["shares"] = result.published.get("shares", 0) + 1

    async def health(self) -> None:
        runtime = self._runtime_or_none()
        if runtime is None:
            raise RuntimeError("Unity Catalog publishes only when Databricks is the engine")
        try:
            await asyncio.to_thread(runtime.ensure_materialize_attached)
        finally:
            runtime.close()
