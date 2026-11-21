# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8b2c71-9d4e-4a06-b1c5-7e2d9a0f6b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1387: the relationship assistant's answer cut off at its output limit.

The bulk suggestion asks for the whole glossary at once. An answer that stops at its output limit
keeps its complete relationships and the rest is asked for again; a remainder that still cannot be
answered is reported by name in plain words, never as a parser error.
"""

# Requirements: REQ-1387

from __future__ import annotations

import pytest

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.llm.client import Completion

pytestmark = pytest.mark.asyncio

TERMS = [
    {"name": n, "definition": "", "role": "property"}
    for n in ("permit", "permit type", "building permit status", "inspection")
]

FIRST = (
    '[{"from": "permit type", "to": "permit", "rel_type": "PART_OF"},\n'
    ' {"from": "building permit status", "to": "permit", "rel_type": "PART_OF"},\n'
    ' {"from": "inspection", "to": "permit", "rel_type": "RELATED_TO", '
    '"resolve": {"common_whole": ["permit applica'
)


def _answer(text: str, cut_off: bool) -> Completion:
    return Completion(text, "anthropic", "m", None, cut_off)


class _Model:
    def __init__(self, *answers: Completion) -> None:
        self.answers = list(answers)
        self.prompts: list[str] = []

    async def __call__(self, prompt: str) -> Completion:
        self.prompts.append(prompt)
        return self.answers.pop(0)


async def test_cut_off_answer_keeps_complete_elements_and_asks_for_the_rest():
    model = _Model(
        _answer(FIRST, cut_off=True),
        _answer(
            '[{"from": "permit type", "to": "permit", "rel_type": "PART_OF"},'
            ' {"from": "inspection", "to": "permit", "rel_type": "RELATED_TO"}]',
            cut_off=False,
        ),
    )
    gathered = await glossary_router.gather_relationships(TERMS, model, 2048)
    assert [(p["from"], p["to"], p["rel_type"]) for p in gathered.proposals] == [
        ("permit type", "permit", "PART_OF"),
        ("building permit status", "permit", "PART_OF"),
        ("inspection", "permit", "RELATED_TO"),
    ]
    assert gathered.unanswered is None
    # The second request names what was already proposed, so only the rest comes back.
    assert len(model.prompts) == 2
    assert "already proposed" in model.prompts[1]
    assert '"from": "building permit status"' in model.prompts[1]
    assert "already proposed" not in model.prompts[0]


async def test_remainder_that_adds_nothing_is_named_in_plain_words():
    model = _Model(
        _answer(FIRST, cut_off=True),
        _answer('[{"from": "permit type", "to": "perm', cut_off=True),
    )
    gathered = await glossary_router.gather_relationships(TERMS, model, 2048)
    assert len(gathered.proposals) == 2
    assert gathered.unanswered == (
        "The model's answer to the rest of the ontology relationship suggestion stopped at its "
        "limit of 2048 output tokens before a further relationship was complete."
    )


async def test_cut_off_before_any_relationship_is_reported_by_name():
    model = _Model(_answer('[{"from": "permit type", "to": "perm', cut_off=True))
    with pytest.raises(ApiError) as err:
        await glossary_router.gather_relationships(TERMS, model, 2048)
    assert err.value.status_code == 502
    assert err.value.detail == (
        "The model's answer to the ontology relationship suggestion stopped at its limit of 2048 "
        "output tokens before a further relationship was complete."
    )


async def test_unreadable_answer_is_reported_without_a_parser_error():
    model = _Model(_answer("I cannot help with that.", cut_off=False))
    with pytest.raises(ApiError) as err:
        await glossary_router.gather_relationships(TERMS, model, 2048)
    assert err.value.detail == (
        "The model's answer to the ontology relationship suggestion was not a list of "
        "relationships."
    )
    assert "line" not in err.value.detail and "char" not in err.value.detail
