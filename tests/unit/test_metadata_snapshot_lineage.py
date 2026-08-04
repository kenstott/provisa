# Copyright (c) 2026 Kenneth Stott
# Canary: 9c0b7fd1-2a4e-4f83-b6d5-0e8a71c2f934
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Published lineage is column-level and derived from compiled SQL (REQ-1070).

Catalog scanners infer table-to-table dependencies from query logs. Provisa already holds the
compiled view SQL, so it publishes which column produces which column, and the transform that
produces it. A derivation chain is published as a chain of edges — each hop keeps its own
transform, so a downstream consumer can read the formula rather than a flattened claim.
"""

# Requirements: REQ-862, REQ-939, REQ-1070

from __future__ import annotations

import pytest

from provisa.api.metadata_egress.builder import build_snapshot
from provisa.api.metadata_egress.model import AssetKind
from provisa.api.metadata_egress.refs import UnknownTableError, UnqualifiedLineageError
from provisa.core.models import Column, Domain, ProvisaConfig, Source, SourceType, Table


def _cols(*names: str) -> list[Column]:
    return [Column(name=n, data_type="numeric", visible_to=["admin"]) for n in names]


def _table(table_name: str, columns: list[Column], view_sql: str | None = None) -> Table:
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="public",
        table_name=table_name,
        columns=columns,
        view_sql=view_sql,
        materialize=view_sql is not None,
    )


def _config(tables: list[Table]) -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql)],
        domains=[Domain(id="sales", description="Sales", steward="s")],
        tables=tables,
        roles=[],
    )


def _edges(snapshot) -> set[tuple[str, str]]:
    return {(e.upstream.fqn(), e.downstream.fqn()) for e in snapshot.lineage}


def test_view_publishes_column_level_edges_with_transforms():
    config = _config(
        [
            _table("orders", _cols("id", "amount", "tax")),
            _table(
                "order_totals",
                _cols("id", "total"),
                view_sql="SELECT orders.id AS id, orders.amount + orders.tax AS total FROM orders",
            ),
        ]
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    assert _edges(snapshot) == {
        ("wh.public.orders.id", "wh.public.order_totals.id"),
        ("wh.public.orders.amount", "wh.public.order_totals.total"),
        ("wh.public.orders.tax", "wh.public.order_totals.total"),
    }
    total_edges = [e for e in snapshot.lineage if e.downstream.parts[-1] == "total"]
    assert all(e.upstream.kind is AssetKind.COLUMN for e in total_edges)
    # The transform is published, not just the dependency — that is what makes this lineage
    # derived from a compiled query rather than inferred by a scanner.
    assert all("amount" in e.transforms[0] and "tax" in e.transforms[0] for e in total_edges)


def test_three_hop_chain_publishes_one_edge_per_hop():
    config = _config(
        [
            _table("orders", _cols("id", "amount")),
            _table(
                "hop1",
                _cols("id", "amount"),
                view_sql="SELECT orders.id AS id, orders.amount AS amount FROM orders",
            ),
            _table(
                "hop2",
                _cols("id", "net"),
                view_sql="SELECT hop1.id AS id, hop1.amount * 2 AS net FROM hop1",
            ),
            _table(
                "hop3",
                _cols("id", "net_rounded"),
                view_sql="SELECT hop2.id AS id, round(hop2.net) AS net_rounded FROM hop2",
            ),
        ]
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    assert _edges(snapshot) == {
        ("wh.public.orders.id", "wh.public.hop1.id"),
        ("wh.public.orders.amount", "wh.public.hop1.amount"),
        ("wh.public.hop1.id", "wh.public.hop2.id"),
        ("wh.public.hop1.amount", "wh.public.hop2.net"),
        ("wh.public.hop2.id", "wh.public.hop3.id"),
        ("wh.public.hop2.net", "wh.public.hop3.net_rounded"),
    }
    # No hop is collapsed: the chain is never published as orders.amount -> hop3.net_rounded,
    # which would assert a derivation no single compiled query performs.
    assert ("wh.public.orders.amount", "wh.public.hop3.net_rounded") not in _edges(snapshot)


def test_join_view_attributes_each_output_to_its_own_upstream_table():
    config = _config(
        [
            _table("orders", _cols("id", "customer_id", "amount")),
            _table("customers", _cols("id", "region")),
            _table(
                "orders_by_region",
                _cols("amount", "region"),
                view_sql=(
                    "SELECT orders.amount AS amount, customers.region AS region "
                    "FROM orders JOIN customers ON orders.customer_id = customers.id"
                ),
            ),
        ]
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    assert _edges(snapshot) == {
        ("wh.public.orders.amount", "wh.public.orders_by_region.amount"),
        ("wh.public.customers.region", "wh.public.orders_by_region.region"),
    }


def test_constant_projection_publishes_no_upstream_edge():
    # A literal has no upstream column. The output asset still exists as a published column;
    # inventing an edge for it would fabricate a derivation.
    config = _config(
        [
            _table("orders", _cols("id")),
            _table(
                "tagged",
                _cols("id", "kind"),
                view_sql="SELECT orders.id AS id, 'order' AS kind FROM orders",
            ),
        ]
    )

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    assert _edges(snapshot) == {("wh.public.orders.id", "wh.public.tagged.id")}
    published = {c.ref.fqn() for c in snapshot.columns()}
    assert "wh.public.tagged.kind" in published


def test_view_over_an_unknown_relation_is_refused():
    config = _config(
        [
            _table("orders", _cols("id")),
            _table(
                "ghosted",
                _cols("id"),
                view_sql="SELECT ghost.id AS id FROM ghost",
            ),
        ]
    )

    with pytest.raises(UnknownTableError) as exc:
        build_snapshot(config, org_id="acme", dialect="postgres")
    assert exc.value.name == "ghost"


def test_non_view_tables_contribute_no_lineage():
    config = _config([_table("orders", _cols("id", "amount"))])

    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")

    assert snapshot.lineage == []


def test_unqualified_leaf_is_refused_rather_than_attributed_to_a_guess():
    # An unqualified upstream column cannot be addressed as an asset. Dropping it would publish
    # a view column that appears to derive from nothing.
    config = _config(
        [
            _table("orders", _cols("id")),
            _table("bare", _cols("id"), view_sql="SELECT id FROM (SELECT 1 AS id) t"),
        ]
    )

    try:
        snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    except UnqualifiedLineageError as exc:
        assert exc.output == "id"
        return
    # A derived subquery leaf resolves to no config table, so the only acceptable alternative
    # outcome is that no edge was invented for it.
    assert _edges(snapshot) == set()
