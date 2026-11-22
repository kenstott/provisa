# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An approved view request is carried out on the approval's authority (REQ-1327(5)).

org_operator holds approve_view and not create_view. The view it approves is registered without
asking the approver's own right to author a view or its domain gates; a materialized view still
publishes only over approved relationships (REQ-1140) and is refused by name, not queued again.
"""

# Requirements: REQ-1327, REQ-1948, REQ-1140

from __future__ import annotations

import contextlib
import types
from typing import Any

import pytest

from provisa.api.admin import capabilities, schema_mutation_ops
from provisa.api.admin.types import TableInput
from provisa.mv import relationship_gate


class _Reached(Exception):
    """The registration got past every caller gate, to the store."""


def _view(**kw: Any) -> TableInput:
    return TableInput(
        **{
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "v_orders",
            "columns": [],
            "view_sql": "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id",
            **kw,
        }
    )


@pytest.fixture
def operator(monkeypatch):
    """The caller holds only approve_view (org_operator's view right); queueing is recorded and
    reaching the store stops the registration."""
    from provisa.api.admin import _dbt_table_registration

    queued: list[str] = []

    async def _queue(_info, request_type, _input):
        queued.append(request_type)
        return types.SimpleNamespace(success=True, code="schema.creation_request_queued")

    async def _not_dbt(_input):
        return None

    async def _store():
        raise _Reached

    identity = types.SimpleNamespace(user_id="opal", roles=["org_operator"])
    monkeypatch.setattr(_dbt_table_registration, "read_through_warehouse", _not_dbt)
    monkeypatch.setattr(schema_mutation_ops, "_queue_creation_request", _queue)
    monkeypatch.setattr(schema_mutation_ops, "_get_pool", _store)
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda _info: identity)
    monkeypatch.setattr(capabilities, "_resolved_capabilities", lambda _i, _s: {"approve_view"})
    monkeypatch.setattr(capabilities, "has_capability", lambda _info, _cap: False)
    return types.SimpleNamespace(queued=queued, info=types.SimpleNamespace(context={}))


async def test_without_approval_the_operator_cannot_author_a_view(operator):
    result = await schema_mutation_ops.register_table(operator.info, _view())
    assert operator.queued == ["view"]
    assert result.code == "schema.creation_request_queued"


async def test_an_approved_view_is_registered_without_the_approvers_create_right(operator):
    with pytest.raises(_Reached):
        await schema_mutation_ops.register_table(operator.info, _view(), approved=True)
    assert operator.queued == []


async def test_an_approved_materialized_view_over_unapproved_joins_is_refused_not_requeued(
    operator, monkeypatch
):
    from provisa.core.repositories import relationship as relationship_repo

    class _Pool:
        @contextlib.asynccontextmanager
        async def acquire(self):
            yield object()

    async def _pool():
        return _Pool()

    async def _none(_conn):
        return []

    dep = relationship_gate.JoinDep("orders", "cid", "customers", "id")
    missing = relationship_gate.MissingRelationship(dep, 1, 2)

    async def _gate(**_kw):
        return relationship_gate.GateDecision(deps=[dep], missing=[missing])

    monkeypatch.setattr(schema_mutation_ops, "_get_pool", _pool)
    monkeypatch.setattr(relationship_repo, "list_all", _none)
    monkeypatch.setattr(relationship_gate, "evaluate_gate", _gate)
    result = await schema_mutation_ops._apply_mv_relationship_gate(
        operator.info, _view(materialize=True), approved=True
    )
    assert result is not None and result.success is False
    assert result.code == "schema.view_relationships_unapproved"
    assert result.params == {"view": "v_orders", "relationships": "orders->customers"}
    assert operator.queued == []
