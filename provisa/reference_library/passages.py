# Copyright (c) 2026 Kenneth Stott
# Canary: a891d81e-d01e-4f40-8ef6-11a05a5e23e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A library document cut into passages (REQ-2185).

A passage is whole paragraphs of one section, up to :data:`TARGET` characters, so it never runs
across a heading or a page and cites one place. A paragraph longer than :data:`LIMIT` is cut at
its sentences, and a sentence longer than that at :data:`LIMIT` characters.
"""

# Requirements: REQ-2185

from __future__ import annotations

import re
from dataclasses import dataclass

from provisa.reference_library.extract import Document

#: About how long a passage is, in characters: whole paragraphs are gathered up to it.
TARGET = 1000
#: The longest a passage is.
LIMIT = 1500


@dataclass(frozen=True)
class Passage:
    ordinal: int
    section: str | None
    page: int | None
    text: str


_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")


def _pieces(paragraph: str) -> list[str]:
    """``paragraph`` as pieces of at most :data:`LIMIT` characters, cut at its sentences."""
    if len(paragraph) <= LIMIT:
        return [paragraph]
    pieces: list[str] = []
    current = ""
    for sentence in _SENTENCE_END.split(paragraph):
        while len(sentence) > LIMIT:
            if current:
                pieces.append(current)
                current = ""
            pieces.append(sentence[:LIMIT])
            sentence = sentence[LIMIT:]
        if current and len(current) + 1 + len(sentence) > LIMIT:
            pieces.append(current)
            current = sentence
        else:
            current = f"{current} {sentence}" if current else sentence
    if current:
        pieces.append(current)
    return pieces


def cut(document: Document) -> list[Passage]:
    """The document's passages, in order."""
    passages: list[Passage] = []
    for section in document.sections:
        current = ""
        for paragraph in section.paragraphs:
            for piece in _pieces(paragraph):
                if current and len(current) + 2 + len(piece) > TARGET:
                    passages.append(Passage(len(passages), section.heading, section.page, current))
                    current = piece
                else:
                    current = f"{current}\n\n{piece}" if current else piece
        if current:
            passages.append(Passage(len(passages), section.heading, section.page, current))
    return passages
