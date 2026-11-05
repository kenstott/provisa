# Copyright (c) 2026 Kenneth Stott
# Canary: 0c4f8e27-6b3d-4a95-8d1e-73a2f9b5c614
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read's governed relations serve the outputs it reads, and a path column as its path (REQ-039).

Stage 2 put every served column of a table in its governed relation, read or not. The engine
statement grew tenfold, a live source was asked for every served column, and a column registered
at a path inside a document column (the perf bench's order_docs.customer_tier, at
customer_profile.tier) was projected by its own name, which the source does not have: Trino failed
federated_join with COLUMN_NOT_FOUND "Column 'd.customer_tier' cannot be resolved" though the
statement never reads it (J670)."""

# Requirements: REQ-039

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.governed_relation import read_through
from provisa.compiler.stage2 import GovernanceContext, apply_governance

_DOCS = [
    ("order_id", "integer"),
    ("status", "varchar"),
    ("customer_tier", "varchar"),
    ("channel", "varchar"),
]


def _gov(**kwargs) -> GovernanceContext:
    defaults = dict(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map={"order_docs": 3},
        all_columns={3: _DOCS},
        limit_ceiling=None,
        sample_size=None,
        column_paths={(3, "customer_tier"): "customer_profile.tier"},
    )
    defaults.update(kwargs)
    return GovernanceContext(**defaults)


def _outputs(governed: str) -> dict[str, str]:
    """The governed relation's outputs, by name, each as the SQL of its value."""
    tree = sqlglot.parse_one(governed, read="postgres")
    [relation] = [r for s in tree.find_all(exp.Subquery) if (r := read_through(s)) is not None]
    return {name: value.sql(dialect="postgres") for name, value in relation.columns.items()}


def test_the_relation_serves_only_the_outputs_the_read_reads():
    governed = apply_governance("SELECT d.status FROM order_docs d WHERE d.order_id = 7", _gov())
    assert set(_outputs(governed)) == {"order_id", "status"}


def test_an_unread_path_column_is_not_in_the_relation():
    governed = apply_governance("SELECT d.status FROM order_docs d", _gov())
    assert "customer_tier" not in governed and "customer_profile" not in governed


def test_a_read_path_column_is_its_path_inside_the_document_column():
    governed = apply_governance("SELECT d.customer_tier FROM order_docs d", _gov())
    assert _outputs(governed) == {"customer_tier": '"d"."customer_profile" ->> \'tier\''}


def test_a_one_key_path_is_a_key_inside_the_column_itself():
    gov = _gov(column_paths={(3, "customer_tier"): "tier"})
    governed = apply_governance("SELECT d.customer_tier FROM order_docs d", gov)
    assert _outputs(governed) == {"customer_tier": '"d"."customer_tier" ->> \'tier\''}


def test_a_star_reads_every_served_output():
    governed = apply_governance("SELECT * FROM order_docs d", _gov())
    assert set(_outputs(governed)) == {"order_id", "status", "customer_tier", "channel"}


def test_a_count_keeps_one_output():
    governed = apply_governance("SELECT count(*) FROM order_docs d", _gov())
    assert len(_outputs(governed)) == 1
