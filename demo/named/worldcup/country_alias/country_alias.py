# Copyright (c) 2026 Kenneth Stott
# Canary: f797d95b-6cb1-4c7d-8291-e318c0b5fa16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""country_alias: the FIFA member a team's name stands for today, matched by an LLM."""

import json
from dataclasses import dataclass

import anthropic

ASK = (
    "Which FIFA member does this national team's name stand for, as FIFA names it today: the "
    "member itself, or the one its results passed to (West Germany is Germany)? A team whose "
    "results passed to no member keeps its own name (East Germany). Give the three-letter FIFA "
    "code. Null for both when the name is not a national team. Name: "
)
ANSWER = {
    "type": "object",
    "properties": {
        "canonical_country": {"type": ["string", "null"]},
        "fifa_code": {"type": ["string", "null"]},
    },
    "required": ["canonical_country", "fifa_code"],
    "additionalProperties": False,
}


@dataclass
class Alias:
    name: str
    canonical_country: str | None
    fifa_code: str | None


@dataclass
class AliasInputs:
    #: The team's name, as a tournament's records spell it.
    name: str


def country_alias(inputs: AliasInputs) -> list[Alias]:
    """The FIFA member a team's name stands for: West Germany and Germany FR are Germany.

    @table
    """
    name = inputs.name
    reply = anthropic.Anthropic().messages.create(
        model="claude-sonnet-5-5",
        max_tokens=2000,
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": ANSWER}},
        messages=[{"role": "user", "content": ASK + name}],
    )
    match = json.loads(next(b.text for b in reply.content if b.type == "text"))
    return [Alias(name, match["canonical_country"], match["fifa_code"])]
