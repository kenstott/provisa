# Copyright (c) 2026 Kenneth Stott
# Canary: 6e8b2d14-0f7c-4a93-b5d1-c9a47e3f2b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The requests a semantic discovery run makes (REQ-413).

For each type group, the source columns and the target columns are packed into windows by the
length in characters of what is sent, sized from the chosen model's input limit at
:data:`provisa.llm.limits.CHARS_PER_TOKEN` characters per token (REQ-464); a request holds one
source window and one target window of one group, and asks for at most as many suggestions as the
model's output limit holds at :data:`TOKENS_PER_SUGGESTION` tokens each. A pair of windows holding
no two columns that are compared (:func:`provisa.discovery.columns.comparable`) is not asked.
:func:`plan` is the run's work list. :func:`bound` is the count shown before Run: the most
requests :func:`plan` can give for the same tables, computed from their registration alone.
"""

# Requirements: REQ-413, REQ-464

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

from provisa.discovery import context
from provisa.discovery.columns import GROUPS, ColumnFacts, TableFacts, comparable
from provisa.llm.limits import CHARS_PER_TOKEN

#: REQ-413: each request asks for at most as many suggestions as the output limit holds at this
#: many tokens per suggestion.
TOKENS_PER_SUGGESTION = 150

# The headers context.prompt puts before the windows, the longer pair.
_HEADERS = len("\n## SOURCE window\n") + len("\n## TARGET window\n")


class NoRoomError(ValueError):
    """The model's limits hold no request: its output limit holds no suggestion, or its input
    limit no window beside the instructions."""


@dataclass(frozen=True)
class Entry:
    """One table's columns in a window."""

    table: TableFacts
    columns: tuple[ColumnFacts, ...]

    @property
    def weight(self) -> int:
        return context.block_weight(self.table, self.columns)


@dataclass(frozen=True)
class Window:
    entries: tuple[Entry, ...]

    @property
    def weight(self) -> int:
        return context.window_weight([e.weight for e in self.entries])

    def blocks(self) -> list[dict]:
        return [context.block(e.table, e.columns) for e in self.entries]

    def columns(self) -> list[tuple[TableFacts, ColumnFacts]]:
        return [(e.table, c) for e in self.entries for c in e.columns]

    @property
    def splittable(self) -> bool:
        return len(self.columns()) > 1

    def halves(self) -> tuple["Window", "Window"]:
        """The window's columns split in two, in order, a table's context going with each half
        that holds one of its columns."""
        cols = self.columns()
        cut = len(cols) // 2
        return _window(cols[:cut]), _window(cols[cut:])


def _window(cols: Sequence[tuple[TableFacts, ColumnFacts]]) -> Window:
    entries: list[Entry] = []
    for table, column in cols:
        if entries and entries[-1].table.table_id == table.table_id:
            last = entries[-1]
            entries[-1] = Entry(table, (*last.columns, column))
        else:
            entries.append(Entry(table, (column,)))
    return Window(tuple(entries))


@dataclass(frozen=True)
class Request:
    """One model request: a source window and a target window of one group. ``target`` None: the
    source window compared with itself (the target is the same as the source)."""

    group: str
    source: Window
    target: Window | None
    either_direction: bool

    def prompt(self, at_most: int, threshold: float) -> str:
        """The request as sent, stating the run's ``threshold`` (REQ-413)."""
        return context.prompt(
            self.group,
            at_most,
            self.source.blocks(),
            None if self.target is None else self.target.blocks(),
            threshold=threshold,
            either_direction=self.either_direction,
        )

    def halves(self) -> list["Request"]:
        """The request split in half (REQ-413): its heavier splittable window in two, every pair
        of columns it held still asked once."""
        if self.target is None:
            a, b = self.source.halves()
            return [
                Request(self.group, a, None, self.either_direction),
                Request(self.group, a, b, self.either_direction),
                Request(self.group, b, None, self.either_direction),
            ]
        split_source = self.source.splittable and (
            not self.target.splittable or self.source.weight >= self.target.weight
        )
        if split_source:
            a, b = self.source.halves()
            return [Request(self.group, w, self.target, self.either_direction) for w in (a, b)]
        a, b = self.target.halves()
        return [Request(self.group, self.source, w, self.either_direction) for w in (a, b)]

    @property
    def splittable(self) -> bool:
        return self.source.splittable or (self.target is not None and self.target.splittable)


def at_most(output_tokens: int) -> int:
    """How many suggestions one request asks for: what the output limit holds at
    :data:`TOKENS_PER_SUGGESTION` tokens each."""
    k = output_tokens // TOKENS_PER_SUGGESTION
    if k < 1:
        raise NoRoomError(
            f"the model's output limit of {output_tokens} tokens holds no suggestion "
            f"({TOKENS_PER_SUGGESTION} tokens each)"
        )
    return k


def window_chars(
    input_tokens: int, group: str, k: int, *, either_direction: bool, not_read: bool = False
) -> int:
    """The characters one window may hold: the input limit in characters, less the instructions
    and headers, shared by the two windows of a request; the instructions are weighed at the
    widest threshold they can state (:func:`provisa.discovery.context.instructions_chars`).
    ``not_read``: a table of the run is never read, so a request's instructions may carry the
    text saying so."""
    fixed = (
        context.instructions_chars(group, k, either_direction=either_direction, not_read=not_read)
        + _HEADERS
    )
    room = (input_tokens * CHARS_PER_TOKEN - fixed) // 2
    if room < 1:
        raise NoRoomError(
            f"the model's input limit of {input_tokens} tokens holds no window beside the "
            "instructions"
        )
    return room


def pack(tables: Sequence[TableFacts], group: str, room: int) -> list[Window]:
    """The group's columns of ``tables`` packed, in order, into windows of at most ``room``
    characters; a table whose columns do not fit one window is spread over several, its context
    with each. A single column heavier than ``room`` is a window alone."""
    windows: list[Window] = []
    current: list[tuple[TableFacts, ColumnFacts]] = []
    for table in tables:
        for column in table.columns:
            if group not in column.groups:
                continue
            trial = [*current, (table, column)]
            if current and _window(trial).weight > room:
                windows.append(_window(current))
                current = [(table, column)]
            else:
                current = trial
    if current:
        windows.append(_window(current))
    return windows


def _meets(a: Window, b: Window, group: str) -> bool:
    """Whether some column of ``a`` and a different column of ``b`` are compared in ``group``."""
    return any(
        comparable(ca, cb, group) and (ta.table_id, ca.name) != (tb.table_id, cb.name)
        for ta, ca in a.columns()
        for tb, cb in b.columns()
    )


def _ordered(tables: Sequence[TableFacts]) -> list[TableFacts]:
    return sorted(tables, key=lambda t: (t.domain_id, t.table_name, t.table_id))


def plan(
    source: Sequence[TableFacts],
    target: Sequence[TableFacts] | None,
    input_tokens: int,
    output_tokens: int,
) -> tuple[int, list[Request]]:
    """``(k, requests)``: the suggestions each request asks for, and the run's requests in order,
    by group. ``target`` None: the target is the same as the source."""
    k = at_most(output_tokens)
    either = target is None
    src = _ordered(source)
    tgt = src if target is None else _ordered(target)
    not_read = any(t.reduced for t in [*src, *tgt])
    requests: list[Request] = []
    for group in GROUPS:
        room = window_chars(input_tokens, group, k, either_direction=either, not_read=not_read)
        s_windows = pack(src, group, room)
        if either:
            for i, a in enumerate(s_windows):
                for b in s_windows[i:]:
                    if _meets(a, b, group):
                        same = b is a
                        requests.append(Request(group, a, None if same else b, True))
            continue
        t_windows = pack(tgt, group, room)
        for a in s_windows:
            for b in t_windows:
                if _meets(a, b, group):
                    requests.append(Request(group, a, b, False))
    return k, requests


def _widest(tables: Sequence[TableFacts]) -> list[TableFacts]:
    return [replace(t, columns=tuple(context.widest(c) for c in t.columns)) for t in tables]


def bound(
    source: Sequence[TableFacts],
    target: Sequence[TableFacts] | None,
    input_tokens: int,
    output_tokens: int,
) -> tuple[int, dict[str, int]]:
    """``(k, requests by group)``: the suggestions each request asks for, and an upper bound on
    the requests :func:`plan` gives for these tables whatever their values and classes turn out
    to be, from their registration alone (REQ-413: the count before Run reads no table).

    Each column is weighed at the most it can send (:func:`provisa.discovery.context.widest`).
    Packing in order never gives fewer windows for heavier columns, so a group has at least as
    many windows here as in the run; and every pair of windows is counted, as if some two of
    their columns were always compared. A group with no two different columns to compare has no
    request."""
    k = at_most(output_tokens)
    either = target is None
    src = _ordered(_widest(source))
    tgt = src if target is None else _ordered(_widest(target))
    not_read = any(t.reduced for t in [*src, *tgt])
    out: dict[str, int] = {}
    for group in GROUPS:
        s_columns = {(t.table_id, c.name) for t in src for c in t.columns if group in c.groups}
        t_columns = {(t.table_id, c.name) for t in tgt for c in t.columns if group in c.groups}
        if not s_columns or not t_columns or len(s_columns | t_columns) < 2:
            continue
        room = window_chars(input_tokens, group, k, either_direction=either, not_read=not_read)
        s_windows = len(pack(src, group, room))
        if either:
            out[group] = s_windows * (s_windows + 1) // 2
        else:
            out[group] = s_windows * len(pack(tgt, group, room))
    return k, out
