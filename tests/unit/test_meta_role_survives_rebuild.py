# Copyright (c) 2026 Kenneth Stott
# Canary: 1d4c91ac-48ba-42a7-9882-089b84d25561
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A model rebuild does not take a meta-role from under a request acting as it (#216).

A request that names several roles it holds acts as their meta-role. It resolves that role once
and then runs as it, one governed statement after another. A rebuild removed every meta-role,
"the next use builds it again" — but the request in flight does not resolve again: its next
statement found no role, an unhandled ``PermissionError`` and a 503.

A meta-role a request may still be acting as is now REBUILT by the rebuild, from its rebuilt
members, and its masks are published with theirs. One whose members are no longer all held, or
that nothing has resolved for longer than any request can live, is removed.
"""

from __future__ import annotations

import pytest

from provisa.api.role_surface import role_surface
from provisa.security import meta_role
from provisa.security.meta_role import ensure_meta_role, meta_role_id
from tests.unit.test_empty_domain_access_reaches_nothing import _built_roles

_A = {"id": "a", "description": "Test role.", "capabilities": ["usage"], "domain_access": ["sales"]}
_B = {
    "id": "b",
    "description": "Test role.",
    "capabilities": ["usage"],
    "domain_access": ["finance"],
}
_META = meta_role_id(["a", "b"])


@pytest.fixture
def multi_domain(monkeypatch):
    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")


@pytest.fixture
def state(multi_domain, monkeypatch):
    import provisa.api.app as appmod

    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(appmod.state._active_runtime(), name, {}, raising=False)
    return _built_roles(monkeypatch, [dict(_A), dict(_B)])


def _statement_of_a_request_in_flight(state) -> None:
    """What each governed statement does with the role the request resolved at its start
    (pgwire/_pipeline: the surface, the role, its masks)."""
    from provisa.security.rights import require_role

    assert role_surface(state, _META) is not None, f"No schema for role {_META!r}"
    require_role(state.roles, _META)


def test_a_request_acting_as_a_meta_role_survives_a_rebuild(state, monkeypatch):
    """THE DEFECT: resolve the meta-role, rebuild the model, run the request's next statement."""
    assert ensure_meta_role(state, ["a", "b"]) == _META
    before = role_surface(state, _META)
    _statement_of_a_request_in_flight(state)

    _built_roles_again(monkeypatch, [dict(_A), dict(_B)])

    _statement_of_a_request_in_flight(state)
    after = role_surface(state, _META)
    assert after is not before, "it is the rebuilt members' surface, not a leftover"
    assert state.meta_roles[_META] == ("a", "b")
    # The request's snapshot of the surface it started with is still whole, too.
    assert before.schema is not None and before.context is not None


def test_a_meta_role_whose_member_is_gone_is_removed_with_it(state, monkeypatch):
    ensure_meta_role(state, ["a", "b"])
    _built_roles_again(monkeypatch, [dict(_A)])  # b was deleted
    assert _META not in state.roles and role_surface(state, _META) is None
    assert _META not in state.meta_roles and _META not in state.meta_resolved_at


def test_a_meta_role_no_request_can_still_be_using_is_removed(state, monkeypatch):
    """Resolved longer ago than the longest request may live: nothing is acting as it. A later
    use builds it again."""
    from provisa.core import limits

    ensure_meta_role(state, ["a", "b"])
    state.meta_resolved_at[_META] -= limits.longest_request_timeout() + 1
    _built_roles_again(monkeypatch, [dict(_A), dict(_B)])
    assert _META not in state.roles and role_surface(state, _META) is None
    assert ensure_meta_role(state, ["a", "b"]) == _META
    assert role_surface(state, _META) is not None


def test_every_use_is_recorded_not_only_the_first(state, monkeypatch):
    from types import SimpleNamespace

    clock = iter([100.0, 200.0])
    # meta_role's own clock only: patching the shared ``time`` module handed these two readings
    # to any other thread of the worker that read the clock meanwhile.
    monkeypatch.setattr(meta_role, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    ensure_meta_role(state, ["a", "b"])
    assert state.meta_resolved_at[_META] == 100.0
    ensure_meta_role(state, ["b", "a"])  # the kept one: no build, but a use
    assert state.meta_resolved_at[_META] == 200.0


def test_the_longest_request_timeout_is_the_largest_any_transport_allows():
    from provisa.core import limits

    assert limits.longest_request_timeout() == max(
        limits.request_timeout_for(t) for t in limits.REQUEST_TRANSPORTS
    )


# --- its masks are published with its members' -------------------------------------------------------


def test_a_rebuilds_masking_rules_carry_the_meta_roles_masks():
    """A rebuild publishes the masks of a meta-role requests act as with its members' -- the same
    assignment -- read from its own chain (REQ-2059): a column whose list unmasks one member is
    unmasked for it, and a meta-role whose member is gone gets none."""
    from types import SimpleNamespace

    from provisa.security.meta_role import with_meta_masks

    state = SimpleNamespace(
        meta_roles={_META: ("a", "b"), "meta:a+gone": ("a", "gone")}, meta_qualifiers={}
    )
    masked_to_all = [{"to": ["*"], "mask": {"type": "constant", "value": "MASKED"}}]
    unmasked_to_b = [{"to": ["b"], "mask": None}, *masked_to_all]
    masked = [
        (1, "ssn", masked_to_all, "varchar"),
        (1, "email", unmasked_to_b, "varchar"),
        (2, "phone", unmasked_to_b, "varchar"),
    ]
    rules = {(1, "a"): {"ssn": "kept as given"}}
    published = with_meta_masks(state, dict(rules), masked, {"a": ["a"], "b": ["b"]})
    assert list(published[(1, _META)]) == ["ssn"]
    assert (2, _META) not in published
    assert not any(role == "meta:a+gone" for (_table, role) in published)  # a member is not held
    assert published[(1, "a")] == {"ssn": "kept as given"}


def test_the_masking_load_publishes_them_in_its_one_assignment():
    import inspect

    from provisa.api import app_loaders

    source = inspect.getsource(app_loaders._load_masking_rules)
    assert source.count("state.masking_rules =") == 1
    assert "state.masking_rules = with_meta_masks(state, rules," in source


def _built_roles_again(monkeypatch, roles: list[dict]) -> None:
    _rebuild(monkeypatch, roles)


def _rebuild(monkeypatch, roles: list[dict]) -> None:
    """The role build of a rebuild, on the same runtime (what _built_roles does, without
    emptying the runtime first)."""
    from provisa.api.app_loaders import _build_and_register_schemas
    from provisa.compiler.introspect import ColumnMetadata
    from tests.unit.test_empty_domain_access_reaches_nothing import DOMAIN_OF, _tables

    _build_and_register_schemas(
        roles=roles,
        tables=_tables(),
        relationships=[],
        col_types_converted={
            tid: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for tid in DOMAIN_OF
        },
        naming_rules=[],
        domains=[{"id": d} for d in DOMAIN_OF.values()],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )


def _governed_read_as_the_meta_role(state) -> str:
    """One governed statement of a request acting as the meta-role, as the pipeline governs it:
    the role's surface, its role entry, and the masking rules the state holds NOW."""
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    from provisa.security.rights import require_role
    from tests.unit.test_empty_domain_access_reaches_nothing import _tables

    surface = role_surface(state, _META)
    gov = build_governance_context(
        _META,
        surface.rls,
        state.masking_rules,
        surface.context,
        _tables(),
        role=require_role(state.roles, _META),
    )
    return apply_governance("SELECT id FROM orders", gov)


def test_a_meta_role_has_its_masks_at_every_step_of_a_rebuild(state, monkeypatch):
    """A rebuild publishes its masking rules, reads on, and only then builds the roles. A
    request acting as a meta-role is governed with its masks before the rebuild, BETWEEN those
    two steps, and after it: a column masked for every member is masked for it throughout."""
    import asyncio
    from unittest.mock import MagicMock

    from provisa.api import app_loaders
    from tests.unit.test_empty_domain_access_reaches_nothing import SALES
    from tests.unit.test_masking_rules_published_whole import _mask_row

    class _ControlPlane:
        async def execute_core(self, _stmt):
            result = MagicMock()
            # orders.id is masked for every role: its list unmasks nobody.
            result.fetchall.return_value = [
                MagicMock(_mapping={**_mask_row("id", unmasked_to=[]), "table_id": SALES})
            ]
            return result

    def _the_masking_load_of_a_rebuild() -> None:
        asyncio.run(app_loaders._load_masking_rules(_ControlPlane(), {}, [dict(_A), dict(_B)]))

    _the_masking_load_of_a_rebuild()
    ensure_meta_role(state, ["a", "b"])
    assert "MASKED" in _governed_read_as_the_meta_role(state)

    # A rebuild begins: the masking rules are published first...
    _the_masking_load_of_a_rebuild()
    # ...and a request acting as the meta-role runs a statement before the roles are built.
    assert "MASKED" in _governed_read_as_the_meta_role(state), (
        "between the masking load and the role build the meta-role was governed without its mask"
    )
    _rebuild(monkeypatch, [dict(_A), dict(_B)])
    assert "MASKED" in _governed_read_as_the_meta_role(state)
