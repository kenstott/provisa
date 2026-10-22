# Copyright (c) 2026 Kenneth Stott
# Canary: 811f5530-7d39-4bc4-92a1-e62feb89ee56
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No seeded meta relationship takes the GraphQL name of a column of the table it starts from.

A relationship field and a column field share one GraphQL type, and the relationship wins the
name: column_lineage_edges' source_column was unreachable once its edge to table_columns was also
called sourceColumn ("Field 'sourceColumn' ... must have a selection of subfields").
"""

from __future__ import annotations

import pytest
import sqlglot

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS, _OPS_LOG_TABLE_VIEWS, _OPS_REPORT_VIEWS
from provisa.compiler.naming import apply_gql_name, rel_field_name


def _view_columns(ddl: str) -> list[str]:
    select = sqlglot.parse_one(ddl, read="postgres").expression
    while select.key == "union":
        select = select.this
    return [e.output_name for e in select.expressions]


def _columns(table: str) -> list[str] | None:
    """The columns registration exposes for a seeded table: its curated view's, else the
    control-plane table's own."""
    from provisa.core import schema_org

    for views in (_META_TABLE_VIEWS, _OPS_LOG_TABLE_VIEWS, _OPS_REPORT_VIEWS):
        if table in views:
            return _view_columns(views[table])
    found = getattr(schema_org, table, None)
    return None if found is None else [c.name for c in found.columns]


def _field_name(target: str, cardinality: str, graphql_alias: str | None) -> str:
    """The GraphQL field a relationship is served under (schema_gen): its alias, else named for
    the table it reaches."""
    return graphql_alias or rel_field_name(target, cardinality)


@pytest.mark.parametrize("row", META_RELATIONSHIPS, ids=[r[0] for r in META_RELATIONSHIPS])
def test_a_relationship_never_takes_a_columns_name(row):
    rel_id, _src_source, src, _sc, _tc, cardinality, _tgt_source, tgt, _alias, gql_alias = row
    columns = _columns(src)
    assert columns is not None, f"{rel_id}: no columns known for {src}"
    field = apply_gql_name(_field_name(tgt, cardinality, gql_alias))
    assert field not in {apply_gql_name(c) for c in columns}, (
        f"{rel_id}: {field!r} is also a column of {src}"
    )
