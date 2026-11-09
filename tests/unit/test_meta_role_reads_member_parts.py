# Copyright (c) 2026 Kenneth Stott
# Canary: c3d70d63-5647-4992-a861-7e8cc61f38a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A meta-role is folded from what its members read, and builds no member's schema (REQ-1882).

The UI acts as every role a user holds (Role: All), so every rebuild re-folds that meta-role from
its members (meta_role.rebuild_meta_roles). The fold reads each member's context and row filters
alone, and it built each member's WHOLE surface for them -- its GraphQL schema and proto too: on
the core lane's runner (run 42440880718, py-spy) that was two thirds of a rebuild's CPU, and the
next request's model commit waited on the GIL behind it. A member's reading parts are now built
alone (LazySurface.reading), and the member's whole surface, when something asks for it, reuses
the context they built.

The fold also asked, for every table, whether each member reads it by scanning the member's
context: quadratic in the model. Each member's parts are now taken once per fold.
"""

# Requirements: REQ-1882

from __future__ import annotations

import pytest

from provisa.api.role_surface import LazySurface, RoleSurface, RoleSurfaceUnbuilt, role_surface
from provisa.security import meta_role
from provisa.security.meta_role import ensure_meta_role, meta_role_id
from tests.unit.test_empty_domain_access_reaches_nothing import _built_roles
from tests.unit.test_meta_role_survives_rebuild import _A, _B, _built_roles_again

_META = meta_role_id(["a", "b"])


@pytest.fixture
def state(monkeypatch):
    import provisa.api.app as appmod

    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")
    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(appmod.state._active_runtime(), name, {}, raising=False)
    return _built_roles(monkeypatch, [dict(_A), dict(_B)])


def test_a_meta_role_is_folded_without_building_its_members_surfaces(state):
    assert ensure_meta_role(state, ["a", "b"]) == _META
    assert not state.surfaces["a"].built and not state.surfaces["b"].built
    # The meta-role itself is whole when a request asks for it.
    served = role_surface(state, _META)
    assert served is not None and served.schema is not None and served.context is not None


def test_a_rebuild_refolds_a_meta_role_without_building_its_members_surfaces(state, monkeypatch):
    ensure_meta_role(state, ["a", "b"])
    _built_roles_again(monkeypatch, [dict(_A), dict(_B)])
    assert state.meta_roles[_META] == ("a", "b")
    assert not state.surfaces["a"].built and not state.surfaces["b"].built
    assert role_surface(state, _META) is not None


def test_a_members_whole_surface_reuses_the_context_its_reading_parts_built(state):
    ensure_meta_role(state, ["a", "b"])
    reading = state.surfaces["a"].reading()
    assert reading.schema is None and reading.proto is None
    whole = role_surface(state, "a")
    assert whole is not None and whole.schema is not None and whole.proto is not None
    assert whole.context is reading.context and whole.rls is reading.rls


def test_the_meta_roles_row_filters_are_its_members_as_before(state):
    """The fold over the reading parts gives what the fold over whole surfaces gave: each table
    read by a member, filtered as that member's own filter -- here none, so no rule."""
    ensure_meta_role(state, ["a", "b"])
    a, b = role_surface(state, "a"), role_surface(state, "b")
    assert a is not None and b is not None
    folded = role_surface(state, _META)
    assert folded is not None
    read_by_members = {tm.table_id for s in (a, b) for tm in s.context.tables.values()}
    assert set(folded.rls.member_reads) == read_by_members
    assert folded.rls.rules == {}


def test_each_member_is_read_once_per_fold_not_once_per_table(state, monkeypatch):
    asked: list[object] = []
    real = meta_role._member_surface

    def _counting(held):
        asked.append(held)
        return real(held)

    monkeypatch.setattr(meta_role, "_member_surface", _counting)
    members = [state.roles["a"], state.roles["b"]]
    meta_role._rls(state, members, state.role_build_inputs["tables"])
    assert len(asked) == len(members)


# --- LazySurface.reading --------------------------------------------------------------------------


def test_reading_builds_the_reading_parts_alone_and_leaves_the_surface_unbuilt():
    parts = RoleSurface(context=object(), rls=object())
    lazy = LazySurface("r", lambda: pytest.fail("the whole surface was built"), lambda: parts)
    assert lazy.reading() is parts
    assert not lazy.built


def test_reading_a_built_surface_is_the_built_surface():
    whole = RoleSurface(schema=object(), context=object(), rls=object())
    lazy = LazySurface("r", lambda: whole, lambda: pytest.fail("the reading parts were built"))
    assert lazy.resolve() is whole
    assert lazy.reading() is whole


def test_reading_a_surface_given_no_reading_parts_builds_it_whole():
    whole = RoleSurface(schema=object(), context=object(), rls=object())
    lazy = LazySurface("r", lambda: whole)
    assert lazy.reading() is whole
    assert lazy.built


def test_a_failed_reading_is_held_for_every_reader_of_the_role():
    def _fails() -> RoleSurface:
        raise ValueError("no such column")

    lazy = LazySurface("r", lambda: pytest.fail("built after a held failure"), _fails)
    with pytest.raises(RoleSurfaceUnbuilt) as first:
        lazy.reading()
    with pytest.raises(RoleSurfaceUnbuilt) as again:
        lazy.resolve()
    assert again.value is first.value
    assert lazy.built
