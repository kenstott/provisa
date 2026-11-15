# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0c7a1d-9b2f-4c63-8d14-a7f3e6b0c952
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Accept all and Reject all: one request handles every listed suggestion as its row's own action
would, and a suggestion that fails is reported with its reason while the rest go on (REQ-413)."""

# Requirements: REQ-413

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError

from provisa.api.admin import discovery

_REQUEST = cast(Any, object())


class _Pool:
    @asynccontextmanager
    async def acquire(self):
        yield object()


class _Repo:
    """The candidate store, recording what each suggestion was asked; ids in ``failing`` raise as
    the store does for a suggestion that is no longer pending."""

    def __init__(self, failing: set[int] | None = None) -> None:
        self.failing = failing or set()
        self.accepted: list[tuple[int, str | None]] = []
        self.rejected: list[tuple[int, str]] = []

    async def accept(self, conn, candidate_id: int, rel_id: str | None = None) -> dict:
        if candidate_id in self.failing:
            raise ValueError(f"Candidate {candidate_id} not found or not in suggested status")
        self.accepted.append((candidate_id, rel_id))
        return {"relationship_id": rel_id or f"disc-{candidate_id}"}

    async def reject(self, conn, candidate_id: int, reason: str) -> None:
        if candidate_id in self.failing:
            raise ValueError(f"Candidate {candidate_id} not found or not in suggested status")
        self.rejected.append((candidate_id, reason))


@pytest.fixture
def gate(monkeypatch) -> list[str]:
    asked: list[str] = []
    monkeypatch.setattr(
        discovery, "require_capability_request", lambda request, cap: asked.append(cap)
    )
    monkeypatch.setattr(discovery, "state", SimpleNamespace(model_db=_Pool()))
    return asked


def _repo(monkeypatch, failing: set[int] | None = None) -> _Repo:
    repo = _Repo(failing)
    monkeypatch.setattr(discovery.candidates_repo, "accept", repo.accept)
    monkeypatch.setattr(discovery.candidates_repo, "reject", repo.reject)
    return repo


def _accept_all(*pairs: tuple[int, str]) -> discovery.AcceptAllRequest:
    return discovery.AcceptAllRequest.model_validate(
        {"candidates": [{"id": i, "name": n} for i, n in pairs]}
    )


async def test_accept_all_accepts_every_suggestion_with_its_name(monkeypatch, gate):
    repo = _repo(monkeypatch)
    body = _accept_all((1, "purchase-history"), (2, "pet-owner"), (3, "visit-vet"))
    found = await discovery.accept_candidates(_REQUEST, body)
    assert repo.accepted == [(1, "purchase-history"), (2, "pet-owner"), (3, "visit-vet")]
    assert found["results"] == [
        {"id": 1, "ok": True, "relationship_id": "purchase-history"},
        {"id": 2, "ok": True, "relationship_id": "pet-owner"},
        {"id": 3, "ok": True, "relationship_id": "visit-vet"},
    ]
    assert gate == ["create_relationship"]


async def test_accept_all_reports_a_failure_per_suggestion_and_accepts_the_rest(monkeypatch, gate):
    repo = _repo(monkeypatch, failing={2})
    body = _accept_all((1, "a"), (2, "b"), (3, "c"))
    found = await discovery.accept_candidates(_REQUEST, body)
    assert repo.accepted == [(1, "a"), (3, "c")]
    assert found["results"][0] == {"id": 1, "ok": True, "relationship_id": "a"}
    assert found["results"][1] == {
        "id": 2,
        "ok": False,
        "error": "Candidate 2 not found or not in suggested status",
    }
    assert found["results"][2] == {"id": 3, "ok": True, "relationship_id": "c"}


async def test_reject_all_rejects_every_suggestion_with_the_one_reason(monkeypatch, gate):
    repo = _repo(monkeypatch)
    body = discovery.RejectAllRequest.model_validate({"ids": [4, 5], "reason": "Rejected by user"})
    found = await discovery.reject_candidates(_REQUEST, body)
    assert repo.rejected == [(4, "Rejected by user"), (5, "Rejected by user")]
    assert found["results"] == [{"id": 4, "ok": True}, {"id": 5, "ok": True}]
    assert gate == ["create_relationship"]


async def test_reject_all_reports_a_failure_per_suggestion_and_rejects_the_rest(monkeypatch, gate):
    repo = _repo(monkeypatch, failing={4})
    body = discovery.RejectAllRequest.model_validate({"ids": [4, 5], "reason": "no"})
    found = await discovery.reject_candidates(_REQUEST, body)
    assert repo.rejected == [(5, "no")]
    assert found["results"] == [
        {"id": 4, "ok": False, "error": "Candidate 4 not found or not in suggested status"},
        {"id": 5, "ok": True},
    ]


async def test_the_whole_set_is_one_request_gated_like_the_row_actions(monkeypatch):
    def refuse(request, cap):
        raise PermissionError(cap)

    monkeypatch.setattr(discovery, "require_capability_request", refuse)
    repo = _repo(monkeypatch)
    with pytest.raises(PermissionError, match="create_relationship"):
        await discovery.accept_candidates(_REQUEST, _accept_all((1, "a")))
    with pytest.raises(PermissionError, match="create_relationship"):
        await discovery.reject_candidates(
            _REQUEST, discovery.RejectAllRequest.model_validate({"ids": [1], "reason": "r"})
        )
    assert repo.accepted == [] and repo.rejected == []


def test_an_empty_set_is_refused():
    with pytest.raises(ValidationError):
        discovery.AcceptAllRequest.model_validate({"candidates": []})
    with pytest.raises(ValidationError):
        discovery.RejectAllRequest.model_validate({"ids": [], "reason": "r"})


def test_the_routes_take_the_whole_set_in_one_request():
    paths = {(r.path, tuple(sorted(r.methods))) for r in discovery.router.routes}  # type: ignore[attr-defined]
    assert ("/admin/discover/candidates/accept", ("POST",)) in paths
    assert ("/admin/discover/candidates/reject", ("POST",)) in paths
