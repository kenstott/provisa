# Copyright (c) 2026 Kenneth Stott
# Canary: d142cd74-c085-4579-9908-4c94b61eeeae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""insider_signals: what a message says of a watch-list deal.

One @columns function, deal_signals, a batch column transform (REQ-2131): for each (message,
deal) row of a batch, the signals that fired --

- legal_name  the company's name, fuzzily: a run of the message's words a letter or two off
- alias       one of the names it also goes by
- ticker      its ticker, as a word, in its case
- code_name   the deal's project code name, as a word
- meaning     the message read close to the deal's description by meaning, the cosine of their
              embeddings (embed() in the statement, the demo's local model): a paraphrase that
              avoids every name still scores

-- its strongest match by name and its match by meaning. A strength is from 0 to 1; a match of
meaning below MEANING_FLOOR is no signal. The matching runs here, not in SQL, because a match is
no join along a relationship, and no SQL function matches a name a letter or two off.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from difflib import SequenceMatcher

# Below this, a message's meaning is not close enough to a deal's description to be a signal.
MEANING_FLOOR = 0.40
# A run of the message's words this alike to the company's name is a match of the name.
NAME_FLOOR = 0.85
# Words a legal name carries that say nothing of which company it is.
_LEGAL_WORDS = {"inc", "corp", "corporation", "ltd", "llc", "co", "group", "plc", "the"}


@dataclass
class DealSignals:
    name_match: float
    meaning_match: float
    signals: str


def _words(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _name_match(legal_name: str, words: list[str]) -> float:
    """How alike the message's closest run of words is to the company's name, without its legal
    words ("Inc.", "Corp."); 0 below NAME_FLOOR."""
    core = [w for w in _words(legal_name) if w not in _LEGAL_WORDS]
    target = " ".join(core)
    best = 0.0
    for start in range(len(words) - len(core) + 1):
        best = max(
            best, SequenceMatcher(None, " ".join(words[start : start + len(core)]), target).ratio()
        )
    return round(best, 3) if best >= NAME_FLOOR else 0.0


def _as_word(term: str, text: str, *, case: bool = False) -> bool:
    flags = 0 if case else re.IGNORECASE
    return re.search(rf"(?<![A-Za-z0-9]){re.escape(term)}(?![A-Za-z0-9])", text, flags) is not None


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def signals(deal: dict, message: dict) -> list[tuple[str, float]]:
    """What ``message`` says of ``deal``: (signal, strength) for each signal that fired."""
    text = f"{message['subject']} {message['body']}"
    found = []
    name = _name_match(deal["legal_name"], _words(text))
    if name:
        found.append(("legal_name", name))
    if any(_as_word(alias, text) for alias in deal["aliases"].split(";")):
        found.append(("alias", 1.0))
    if _as_word(deal["ticker"], text, case=True):
        found.append(("ticker", 1.0))
    if _as_word(deal["code_name"], text):
        found.append(("code_name", 1.0))
    meaning = round(_cosine(message["vector"], deal["vector"]), 3)
    if meaning >= MEANING_FLOOR:
        found.append(("meaning", meaning))
    return found


def deal_signals(
    subject: list[str],
    body: list[str],
    legal_name: list[str],
    aliases: list[str],
    ticker: list[str],
    code_name: list[str],
    message_vector: list[list[float]],
    deal_vector: list[list[float]],
) -> list[DealSignals]:
    """For each message and deal of the batch, its strongest match by name, its match by meaning
    (0 below the floor) and the signals that fired, in order -- empty when none did.

    @columns
    """
    out = []
    for row in zip(
        subject, body, legal_name, aliases, ticker, code_name, message_vector, deal_vector
    ):
        s, b, name, alias, tick, code, m_vec, d_vec = row
        found = signals(
            {
                "legal_name": name,
                "aliases": alias,
                "ticker": tick,
                "code_name": code,
                "vector": d_vec,
            },
            {"subject": s, "body": b, "vector": m_vec},
        )
        names = [strength for signal, strength in found if signal != "meaning"]
        meaning = next((strength for signal, strength in found if signal == "meaning"), 0.0)
        out.append(
            DealSignals(max(names, default=0.0), meaning, ", ".join(signal for signal, _s in found))
        )
    return out
