# Copyright (c) 2026 Kenneth Stott
# Canary: 6e5ad2aa-8ea2-4212-9982-fce78d9c77e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A BigQuery replica build opens its write stream on the build table it has just created.

The build table is dropped and re-created under one name for every build, and BigQuery's Storage
Write API learns of a re-created table a little after the DDL returns. A stream asked for in
that window was answered "404 Requested entity was not found" and the build failed, for a table
that existed."""

from __future__ import annotations

import pytest
from google.api_core.exceptions import NotFound, PermissionDenied

from provisa.federation import replica_target_warehouse as rtw


class _Writer:
    def __init__(self, answers: list) -> None:
        self.answers = answers
        self.asked: list[str] = []

    def create_write_stream(self, *, parent, write_stream):
        self.asked.append(parent)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture(autouse=True)
def _no_waiting(monkeypatch):
    monkeypatch.setattr(rtw, "_STREAM_OPEN_INTERVAL", 0.0)


def test_a_table_the_write_api_has_not_seen_yet_is_asked_for_again_until_it_opens():
    writer = _Writer([NotFound("Requested entity was not found."), NotFound("again"), "stream"])
    assert rtw._open_pending_stream(writer, "projects/p/datasets/d/tables/t", object()) == "stream"  # noqa: SLF001
    assert writer.asked == ["projects/p/datasets/d/tables/t"] * 3


def test_a_table_that_never_appears_fails_with_that_answer_when_the_bound_passes(monkeypatch):
    monkeypatch.setattr(rtw, "_STREAM_OPEN_SECONDS", 0.0)
    writer = _Writer([NotFound("Requested entity was not found.")])
    with pytest.raises(NotFound):
        rtw._open_pending_stream(writer, "projects/p/datasets/d/tables/t", object())  # noqa: SLF001
    assert len(writer.asked) == 1


def test_any_other_answer_is_not_waited_on():
    writer = _Writer([PermissionDenied("no"), "stream"])
    with pytest.raises(PermissionDenied):
        rtw._open_pending_stream(writer, "projects/p/datasets/d/tables/t", object())  # noqa: SLF001
    assert len(writer.asked) == 1


def test_a_request_past_its_deadline_stops_asking(monkeypatch):
    from provisa.core import request_deadline

    def _passed() -> None:
        raise request_deadline.DeadlinePassed()

    monkeypatch.setattr(request_deadline, "check", _passed)
    writer = _Writer([NotFound("not yet"), "stream"])
    with pytest.raises(request_deadline.DeadlinePassed):
        rtw._open_pending_stream(writer, "projects/p/datasets/d/tables/t", object())  # noqa: SLF001
    assert len(writer.asked) == 1
