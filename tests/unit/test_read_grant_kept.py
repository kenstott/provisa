# Copyright (c) 2026 Kenneth Stott
# Canary: 3c6f3e33-823d-4162-a46c-1f8397eb122b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2154: a role cannot remove its own read on a column -- to keep a column's values from
itself it adds a mask rule for itself, which keeps the column's name so it can still administer
it -- and the last role holding read on a column cannot lose it, so every column has a reader."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.admin._hiding_guard import grant_edit_refusal


def _model(**grants: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        columns=[SimpleNamespace(name=n, visible_to=list(g)) for n, g in grants.items()]
    )


def _stored(**grants: list[str]) -> dict[str, dict]:
    return {n: {"column_name": n, "visible_to": list(g)} for n, g in grants.items()}


def test_another_roles_read_may_be_removed():
    stored = _stored(email=["steward", "viewer"])
    assert grant_edit_refusal(stored, _model(email=["steward"]), "steward") is None


def test_a_role_cannot_remove_its_own_read():
    stored = _stored(email=["steward", "viewer"])
    refused = grant_edit_refusal(stored, _model(email=["viewer"]), "steward")
    assert refused is not None
    assert "email" in refused and "steward" in refused and "mask" in refused


def test_the_last_reader_of_a_column_cannot_lose_it():
    stored = _stored(email=["viewer"])
    refused = grant_edit_refusal(stored, _model(email=[]), "steward")
    assert refused is not None and "email" in refused and "last" in refused


def test_a_new_column_and_an_unchanged_one_are_not_this_checks_concern():
    stored = _stored(email=["viewer"])
    assert grant_edit_refusal(stored, _model(email=["viewer"], phone=[]), "steward") is None


def test_a_set_of_roles_acting_together_keeps_each_members_read():
    stored = _stored(email=["a", "b", "viewer"])
    refused = grant_edit_refusal(stored, _model(email=["b", "viewer"]), "meta:a+b")
    assert refused is not None and "'a'" in refused
