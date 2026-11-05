# Copyright (c) 2026 Kenneth Stott
# Canary: 9ecd2ab8-8929-439d-8dbe-68499c9a25a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The insider demo's match_deals command (REQ-2056): every message matched to every watch-list
deal, what it says of each written to bank.deal_match.

A python command (``impl_kind: python``), called as ``match_deals(payload, session)``. It reads
the watch list (the control room's workbook) and the mail (the bank's Microsoft 365 source)
through Provisa as the caller, the texts embedded there by the demo's
own local model (``embed()``), and writes what it finds through Provisa as the caller:

- legal_name  the company's name, fuzzily: its words read in the message, a letter or two off
- alias       one of the names it also goes by
- ticker      its ticker, as a word
- code_name   the deal's project code name, as a word
- meaning     the message read close to the deal's description by meaning: a paraphrase that
              avoids every name still scores

A strength is from 0 to 1; a match of meaning below MEANING_FLOOR is no signal. A message and deal
with any signal is one deal_match row: the flagged entity (the deal's company), its strongest name
match and its meaning match, the signals, whether its sender was on that deal's team, whether it
went to one of the deal's own counterparties and whether it was sent before the deal was announced.
The review queue reads these, never the watch list, so a reviewer granted the queue is granted no
row of the list. The matching runs here, not in SQL, because a match is no join along a
relationship: the review queue joins deal_match only along the demo's relationships.
"""

from __future__ import annotations

import math
import re
from datetime import date
from difflib import SequenceMatcher
from typing import Any

from provisa.api.errors import ApiError
from provisa.compiler.sql_literals import sql_literal

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
# Below this, a message's meaning is not close enough to a deal's description to be a signal.
MEANING_FLOOR = 0.40
# A run of the message's words this alike to the company's name is a match of the name.
NAME_FLOOR = 0.85
# Words a legal name carries that say nothing of which company it is.
_LEGAL_WORDS = {"inc", "corp", "corporation", "ltd", "llc", "co", "group", "plc", "the"}
_BATCH = 200


def literal(value: Any) -> str:
    """``value`` as a literal of the SQL Provisa is sent (Postgres)."""
    return sql_literal(value, "postgres")


async def _run(sql: str, role_id: str) -> Any:
    from provisa.api.app import state
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    return await _execute_plan(await _govern_and_route(sql, role_id), state)


async def _rows(sql: str, role_id: str) -> list[dict]:
    result = await _run(sql, role_id)
    return [dict(zip(result.column_names, row)) for row in result.rows]


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


async def match_deals(_payload: dict, session: Any) -> list[dict]:
    if session.role_id is None:
        raise ValueError("match_deals runs as a role; none is bound")
    role_id = session.role_id
    [held] = await _rows('SELECT COUNT(*) AS n FROM "bank"."deal_match"', role_id)
    if held["n"]:
        raise ApiError(
            409,
            "insider.match_deals_refused",
            f"match_deals: deal_match already holds {held['n']} rows",
        )
    model = literal(MODEL)
    deals = await _rows(
        "SELECT deal_id, code_name, legal_name, aliases, ticker, announced_on, "
        f'embed({model}, description) AS vector FROM "bank"."deal"',
        role_id,
    )
    # The bank's mail, from its Microsoft 365 source: each message's sender is the employee
    # whose address sent it.
    sender_of = {
        r["email"]: r["employee_id"]
        for r in await _rows('SELECT employee_id, email FROM "bank"."employee"', role_id)
    }
    messages = [
        {**m, "sender_id": sender_of[m["from_address"]]}
        for m in await _rows(
            "SELECT id AS message_id, from_address, sent_at, subject, body_text AS body, "
            f'embed({model}, CONCAT(subject, \'. \', body_text)) AS vector FROM "bank"."messages"',
            role_id,
        )
    ]
    team = {
        (r["deal_id"], r["employee_id"])
        for r in await _rows('SELECT deal_id, employee_id FROM "bank"."deal_team"', role_id)
    }
    counterparty = {
        (r["deal_id"], r["domain"])
        for r in await _rows('SELECT deal_id, domain FROM "bank"."deal_counterparty"', role_id)
    }
    domains: dict[str, set[str]] = {}
    for r in await _rows(
        'SELECT message_id, address FROM "bank"."message_recipients" '
        "WHERE kind IN ('to', 'cc', 'bcc')",
        role_id,
    ):
        domains.setdefault(r["message_id"], set()).add(r["address"].split("@", 1)[1])
    values = []
    for m in messages:
        for d in deals:
            found = signals(d, m)
            if not found:
                continue
            names = [strength for signal, strength in found if signal != "meaning"]
            meaning = next((strength for signal, strength in found if signal == "meaning"), 0.0)
            on_team = int((d["deal_id"], m["sender_id"]) in team)
            to_counterparty = int(
                any((d["deal_id"], dom) in counterparty for dom in domains[m["message_id"]])
            )
            # The workbook holds a date as its ISO-8601 text; empty while the deal is not public.
            announced = date.fromisoformat(d["announced_on"]) if d["announced_on"] else None
            before = int(announced is None or m["sent_at"].date() < announced)
            values.append(
                f"({literal(m['message_id'])}, {literal(d['deal_id'])}, {literal(d['legal_name'])}, "
                f"{max(names, default=0.0)}, {meaning}, "
                f"{literal(', '.join(signal for signal, _s in found))}, {on_team}, "
                f"{to_counterparty}, {before})"
            )
    for start in range(0, len(values), _BATCH):
        await _run(
            'INSERT INTO "bank"."deal_match" (message_id, deal_id, flagged_entity, name_match, '
            "meaning_match, signals, sender_on_team, to_counterparty, before_announcement) VALUES "
            + ", ".join(values[start : start + _BATCH]),
            role_id,
        )
    return [{"rows_affected": len(values)}]
