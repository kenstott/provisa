# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-846: a steward-assigned IR data-type persists through registration and every canonical IR
name maps to a GraphQL scalar (so a manually-typed column renders in the API)."""

from __future__ import annotations

from provisa.api.admin._table_ops import _build_column_models
from provisa.api.admin.types import ColumnInput
from provisa.compiler.type_map import column_type_to_graphql
from provisa.core.ir_types import IR_TYPES


def test_column_input_data_type_flows_to_model():
    cols = _build_column_models([ColumnInput(name="amount", visible_to=["*"], data_type="numeric")])
    assert cols[0].data_type == "numeric"  # assigned IR type carried into the Column model


def test_data_type_defaults_none_when_unassigned():
    cols = _build_column_models([ColumnInput(name="id", visible_to=["*"])])
    assert cols[0].data_type is None  # introspection fills it later; never silently defaulted


def test_every_ir_type_maps_to_a_graphql_scalar():
    # A persisted IR type must render in the GraphQL API — no IR name may raise (REQ-846). This is
    # the guard that caught the missing "float" mapping when IR types began reaching the schema.
    for ir in sorted(IR_TYPES):
        assert column_type_to_graphql(ir) is not None, f"IR type {ir!r} has no GraphQL scalar"
