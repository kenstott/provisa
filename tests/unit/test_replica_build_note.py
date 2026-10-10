# Copyright (c) 2026 Kenneth Stott
# Canary: 85ccb0f2-9349-485b-a9c5-13b9359a226c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a completed build has to say of the copy it made (REQ-1915): a source may state a note
once its rows are read to the end, and the build's outcome carries it."""

# Requirements: REQ-1915
from __future__ import annotations

import pyarrow as pa
import pytest

from provisa.federation.data_replicator import BuildNote, Method, ReplicaJob
from provisa.federation.replica_source import CursorSource

pytestmark = pytest.mark.asyncio

COLUMNS = [("id", "text")]
NOTE = BuildNote("replication.unreadable_messages", {"count": 1, "ids": ["m9"], "more": 0})


class _Target:
    caps = None

    def __init__(self) -> None:
        self.swapped = self.aborted = False

    async def begin(self) -> None: ...

    async def write(self, batch, rows) -> None: ...

    async def swap(self) -> None:
        self.swapped = True

    async def abort(self) -> None:
        self.aborted = True


class _Engine:
    caps = None

    async def after_swap(self) -> None: ...


async def _progress(_copied: int) -> None: ...


def _source(notes=None, *, ids=("a", "b")):
    read: list[str] = []

    async def rows(_batch_rows: int):
        for row_id in ids:
            read.append(row_id)
            yield [{"id": row_id}]

    source = CursorSource(rows, COLUMNS, notes=notes)
    source.read = read  # type: ignore[attr-defined]
    return source


async def _build(source, *, prior_hash=None):
    target = _Target()
    job = ReplicaJob(
        Method.STREAM_BATCHES, source, target, _Engine(), batch_rows=10, prior_hash=prior_hash
    )
    return await job.run(_progress), target


async def test_a_build_carries_what_its_source_says_of_the_read():
    outcome, target = await _build(_source(lambda: [NOTE]))
    assert outcome.notes == (NOTE,)
    assert (outcome.rows_copied, target.swapped) == (2, True)


async def test_the_source_is_asked_once_its_rows_are_read_to_the_end():
    source = _source()
    asked_after: list[int] = []
    source._notes = lambda: asked_after.append(len(source.read)) or [NOTE]  # type: ignore[attr-defined]
    await _build(source)
    assert asked_after == [2]


async def test_a_source_with_nothing_to_say_leaves_no_note():
    assert (await _build(_source(lambda: [])))[0].notes == ()
    assert (await _build(_source()))[0].notes == ()


async def test_a_source_that_states_no_notes_at_all_leaves_none():
    class Plain:
        caps = None

        async def batches(self, _batch_rows: int):
            yield pa.RecordBatch.from_pylist([{"id": "a"}])

    assert (await _build(Plain()))[0].notes == ()


async def test_a_copy_equal_to_the_last_build_still_carries_the_note():
    first, _ = await _build(_source(lambda: [NOTE]))
    again, target = await _build(_source(lambda: [NOTE]), prior_hash=first.content_hash)
    assert (again.changed, target.swapped, target.aborted) == (False, False, True)
    assert again.notes == (NOTE,)


async def test_a_build_carries_every_note_its_source_has_in_the_order_given():
    from provisa.federation.data_replicator import noted

    other = BuildNote("replication.mailboxes_left_out", {"count": 1})
    outcome, _ = await _build(_source(lambda: noted(NOTE, None, other)))
    assert outcome.notes == (NOTE, other)
    assert noted(None, None) == []
