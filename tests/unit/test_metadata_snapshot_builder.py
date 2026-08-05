# Copyright (c) 2026 Kenneth Stott
# Canary: 3a1e70bc-6d54-4c0f-8e9a-7b2c4d1f5e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The snapshot published to external catalogs is exactly what Provisa governs (REQ-1070).

These assert the projection itself — which assets appear, how they are addressed, and what a
missing steward or an unresolvable reference does. The published payload is the contract every
adapter maps from, so a change here changes what every downstream catalog sees.
"""

# Requirements: REQ-609, REQ-020, REQ-1070

from __future__ import annotations

import pytest

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.model import AssetKind
from provisa.api.metadata_export.refs import AmbiguousTableError, UnknownTableError
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    ProvisaConfig,
    Relationship,
    Source,
    SourceType,
    Table,
)


def _source(source_id: str = "wh", description: str = "warehouse") -> Source:
    return Source(id=source_id, type=SourceType.postgresql, description=description)


def _table(
    *,
    source_id: str = "wh",
    table_name: str,
    domain_id: str = "sales",
    columns: list[Column] | None = None,
    **kwargs,
) -> Table:
    return Table(
        source_id=source_id,
        domain_id=domain_id,
        schema_name="public",
        table_name=table_name,
        columns=columns
        if columns is not None
        else [Column(name="id", data_type="integer", visible_to=["admin"])],
        **kwargs,
    )


def _config(**kwargs) -> ProvisaConfig:
    base = {
        "sources": [_source()],
        "domains": [Domain(id="sales", description="Sales", steward="data-steward")],
        "tables": [_table(table_name="orders")],
        "roles": [],
    }
    base.update(kwargs)
    return ProvisaConfig(**base)


def test_sources_domains_tables_and_columns_are_published():
    config = _config(
        tables=[
            _table(
                table_name="orders",
                description="Order facts",
                alias="Order",
                columns=[
                    Column(
                        name="id",
                        data_type="integer",
                        visible_to=["admin"],
                        description="Order id",
                    ),
                    Column(
                        name="amount", data_type="numeric", visible_to=["admin"], alias="total"
                    ),
                ],
            )
        ]
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    assert snapshot.org_id == "acme"
    assert snapshot.asset_count() == {"source": 1, "domain": 1, "table": 1, "column": 2}
    assert snapshot.sources[0].ref.fqn() == "wh"
    assert snapshot.sources[0].source_type == "postgresql"
    table = snapshot.tables[0]
    assert table.ref.kind is AssetKind.TABLE
    assert table.ref.fqn() == "wh.public.orders"
    assert table.domain_id == "sales"
    assert table.description == "Order facts"
    assert table.aliases == ("Order",)
    amount = [c for c in snapshot.columns() if c.name == "amount"][0]
    assert amount.ref.fqn() == "wh.public.orders.amount"
    assert amount.data_type == "numeric"
    assert amount.aliases == ("total",)


def test_domain_without_steward_publishes_as_pending_not_dropped():
    # REQ-609: an unstewarded domain is a governance gap. Omitting it would make the external
    # catalog show a clean, incomplete picture.
    config = _config(
        domains=[
            Domain(id="sales", description="Sales", steward="data-steward"),
            Domain(id="lab", description="Unowned lab data"),
        ]
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    by_id = {d.id: d for d in snapshot.domains}
    assert set(by_id) == {"sales", "lab"}
    assert by_id["sales"].pending is False
    assert by_id["sales"].steward is not None
    assert by_id["sales"].steward.id == "data-steward"
    assert by_id["sales"].steward.kind == "steward"
    assert by_id["lab"].pending is True
    assert by_id["lab"].steward is None


def test_relationship_carries_owner_version_and_review_flag():
    config = _config(
        tables=[_table(table_name="orders"), _table(table_name="customers")],
        relationships=[
            Relationship(
                id="orders-to-customers",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
                alias="PLACED_BY",
                owner="join-steward",  # REQ-020
                version=4,
                needs_review=True,
            )
        ],
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    edge = snapshot.relationships[0]
    assert edge.source.fqn() == "wh.public.orders"
    assert edge.target is not None
    assert edge.target.fqn() == "wh.public.customers"
    assert edge.source_column == "customer_id"
    assert edge.cardinality == "many-to-one"
    assert edge.alias == "PLACED_BY"
    assert edge.owner is not None
    assert edge.owner.id == "join-steward"
    assert edge.owner.kind == "relationship_owner"
    assert edge.version == 4
    assert edge.needs_review is True


def test_computed_relationship_publishes_with_no_target():
    # REQ-019: a function-target relationship has no target table by design.
    config = _config(
        relationships=[
            Relationship(
                id="orders-to-score",
                source_table_id="orders",
                source_column="id",
                cardinality=Cardinality.many_to_one,
                target_function_name="score_order",
                function_arg="order_id",
            )
        ]
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    assert snapshot.relationships[0].target is None


def test_unknown_relationship_table_is_refused():
    config = _config(
        relationships=[
            Relationship(
                id="dangling",
                source_table_id="orders",
                target_table_id="ghost",
                source_column="id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
            )
        ]
    )

    with pytest.raises(UnknownTableError) as exc:
        build_snapshot(config, org_id="acme", dialect="postgres")
    assert exc.value.name == "ghost"
    assert "dangling" in str(exc.value)


def test_ambiguous_bare_table_name_is_refused_not_guessed():
    config = _config(
        sources=[_source("wh"), _source("crm")],
        tables=[
            _table(source_id="wh", table_name="orders"),
            _table(source_id="crm", table_name="orders"),
            _table(table_name="customers"),
        ],
        relationships=[
            Relationship(
                id="ambig",
                source_table_id="customers",
                target_table_id="orders",
                source_column="id",
                target_column="id",
                cardinality=Cardinality.one_to_many,
            )
        ],
    )

    with pytest.raises(AmbiguousTableError) as exc:
        build_snapshot(config, org_id="acme", dialect="postgres")
    assert sorted(exc.value.candidates) == ["crm.public.orders", "wh.public.orders"]


def test_qualified_reference_disambiguates_a_duplicated_table_name():
    config = _config(
        sources=[_source("wh"), _source("crm")],
        tables=[
            _table(source_id="wh", table_name="orders"),
            _table(source_id="crm", table_name="orders"),
            _table(table_name="customers"),
        ],
        relationships=[
            Relationship(
                id="explicit",
                source_table_id="customers",
                target_table_id="crm.public.orders",
                source_column="id",
                target_column="id",
                cardinality=Cardinality.one_to_many,
            )
        ],
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    target = snapshot.relationships[0].target
    assert target is not None
    assert target.fqn() == "crm.public.orders"


def test_snapshot_of_an_empty_config_publishes_nothing_rather_than_failing():
    snapshot = build_snapshot(
        ProvisaConfig(sources=[], domains=[], tables=[], roles=[]),
        org_id="acme",
        dialect="postgres",
    )
    assert snapshot.asset_count() == {"source": 0, "domain": 0, "table": 0, "column": 0}
    assert snapshot.relationships == []
    assert snapshot.lineage == []
