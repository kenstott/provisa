# Copyright (c) 2026 Kenneth Stott
# Canary: 0c4b8e5f-71a2-4d63-9b0e-2f8a15c37d64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governed config both metadata-egress e2e tests publish (REQ-1069).

One fixture, two targets: what the suite is checking is that the SAME snapshot lands correctly
in a real OpenLineage server and a real OpenMetadata server, so the two tests must not each
invent their own shape.

It carries every feature the adapters have to map — a stewarded domain and an unstewarded one,
a masked column, a column hidden from a role, an RLS rule, an approved relationship, and a view
whose column lineage is resolved from compiled SQL rather than inferred.
"""

# Requirements: REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

from provisa.api.metadata_egress import build_snapshot
from provisa.api.metadata_egress.model import MetadataSnapshot
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    ProvisaConfig,
    Relationship,
    RLSRule,
    Role,
    Source,
    SourceType,
    Table,
)

ORG_ID = "acme"
MASK_PATTERN = r"(\d{3})-(\d{2})-(\d{4})"
RLS_FILTER = "region_id = current_setting('provisa.region')"


def _table(name: str, columns: list[Column], view_sql: str | None = None) -> Table:
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=columns,
        view_sql=view_sql,
        materialize=view_sql is not None,
    )


def governed_config() -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[
            Domain(id="sales", description="Sales domain", steward="data-steward"),
            # Unstewarded on purpose: the catalog must show the governance gap.
            Domain(id="lab", description="Lab domain"),
        ],
        tables=[
            _table(
                "orders",
                [
                    Column(name="id", data_type="integer", visible_to=["analyst", "admin"]),
                    Column(
                        name="amount",
                        data_type="numeric",
                        visible_to=["analyst", "admin"],
                        description="Order total in cents",
                    ),
                    Column(
                        name="customer_id", data_type="integer", visible_to=["analyst", "admin"]
                    ),
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["analyst", "admin"],
                        mask_type="regex",
                        mask_pattern=MASK_PATTERN,
                        mask_replace="XXX-XX-\\3",
                        unmasked_to=["admin"],
                    ),
                    Column(name="margin", data_type="numeric", visible_to=["admin"]),
                ],
            ),
            _table(
                "customers",
                [
                    Column(name="id", data_type="integer", visible_to=["analyst", "admin"]),
                    Column(name="name", data_type="text", visible_to=["analyst", "admin"]),
                ],
            ),
            _table(
                "order_totals",
                [
                    Column(name="id", data_type="integer", visible_to=["analyst", "admin"]),
                    Column(name="net", data_type="numeric", visible_to=["analyst", "admin"]),
                ],
                view_sql="SELECT orders.id AS id, orders.amount * 2 AS net FROM orders",
            ),
        ],
        relationships=[
            Relationship(
                id="orders-to-customers",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
                owner="join-steward",
                version=3,
                needs_review=False,
            )
        ],
        roles=[
            Role(id="analyst", capabilities=[], domain_access=["*"]),
            Role(id="admin", capabilities=[], domain_access=["*"]),
        ],
        rls_rules=[RLSRule(table_id="orders", role_id="analyst", filter=RLS_FILTER)],
    )


def governed_snapshot() -> MetadataSnapshot:
    return build_snapshot(governed_config(), org_id=ORG_ID, dialect="postgres")
