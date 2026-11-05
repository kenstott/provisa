# Copyright (c) 2026 Kenneth Stott
# Canary: 4a7d2e90-6c18-4b53-9f3e-0d5b81c7a2e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1161: a caller reads what the role its request acts as is served, and nothing else."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.admin.served import served_columns, served_columns_for, served_in

STATE = SimpleNamespace(
    contexts={
        "analyst": SimpleNamespace(
            tables={"a": SimpleNamespace(table_id=1), "b": SimpleNamespace(table_id=2)},
            physical_to_sql={(1, "amount"): "amount", (1, "id"): "id"},
        )
    }
)


def _request(role: str | None) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(role=role))


def test_the_acting_roles_compiled_context_is_what_it_reads() -> None:
    assert served_columns(_request("analyst"), STATE) == {1: {"amount", "id"}, 2: set()}
    assert served_in(_request("analyst"), STATE, 1) == {"amount", "id"}
    assert served_in(_request("analyst"), STATE, 9) == set()


def test_no_acting_role_and_an_unknown_role_read_nothing() -> None:
    assert served_columns(_request(None), STATE) == {}
    assert served_columns_for(STATE, "nobody") == {}


def test_a_named_role_reads_without_a_request() -> None:
    assert served_columns_for(STATE, "analyst") == served_columns(_request("analyst"), STATE)
