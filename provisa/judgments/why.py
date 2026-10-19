# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A judgment's ``why`` (REQ-2094): one shape for every kind of judgment.

The decider gives ``reason`` (the facts relied on), ``purpose`` (the outcome wanted), ``bases``
(named bases cited, by id) and ``review_on`` (a review-by date), all optional. The server sets
``decided_by`` (the change's actor), ``decided_at`` and ``evidence`` at each save that changes
the decision or its why; they are never typed, and given ones -- an exported model's -- are
recomputed on import.
"""

# Requirements: REQ-2094

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

GIVEN = ("reason", "purpose", "bases", "review_on")
SERVER = ("decided_by", "decided_at", "evidence")


class WhyRefused(ValueError):
    """A why that cannot be recorded, said by field."""


def parse(raw: Mapping[str, Any] | None, where: str) -> dict[str, Any]:
    """The given part of ``raw``, checked: text reason and purpose, a list of basis ids, an ISO
    review-by date. The server's fields are dropped (an export carries them read-only); any other
    key is refused by name."""
    if raw is None:
        return {}
    unknown = sorted(set(raw) - set(GIVEN) - set(SERVER))
    if unknown:
        raise WhyRefused(f"{where}: why has no field {', '.join(repr(u) for u in unknown)}")
    out: dict[str, Any] = {}
    for text in ("reason", "purpose"):
        value = raw.get(text)
        if value is None or value == "":
            continue
        if not isinstance(value, str):
            raise WhyRefused(f"{where}: why.{text} is text")
        out[text] = value
    bases = raw.get("bases")
    if bases:
        if not isinstance(bases, (list, tuple)) or not all(isinstance(b, str) for b in bases):
            raise WhyRefused(f"{where}: why.bases is a list of basis ids")
        out["bases"] = sorted(set(bases))
    review_on = raw.get("review_on")
    if review_on:
        try:
            dt.date.fromisoformat(str(review_on))
        except ValueError as exc:
            raise WhyRefused(f"{where}: why.review_on is an ISO date, not {review_on!r}") from exc
        out["review_on"] = str(review_on)
    return out


async def check_bases(conn: Any, given: Mapping[str, Any], where: str) -> None:
    """Refuse a cited basis the org does not hold, by name: bases are a short list, never free
    text."""
    from sqlalchemy import select

    from provisa.core.schema_org import judgment_bases

    cited = set(given.get("bases") or ())
    if not cited:
        return
    held = {
        r[0]
        for r in (
            await conn.execute_core(
                select(judgment_bases.c.id).where(judgment_bases.c.id.in_(sorted(cited)))
            )
        ).fetchall()
    }
    missing = sorted(cited - held)
    if missing:
        raise WhyRefused(f"{where}: no basis {', '.join(repr(m) for m in missing)}")


def given_part(why: Mapping[str, Any] | None) -> dict[str, Any]:
    return {k: why[k] for k in GIVEN if why and why.get(k) not in (None, "", [])}


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def stamp(
    previous: Mapping[str, Any] | None,
    given: Mapping[str, Any],
    *,
    changed: bool,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """The why to store: ``given`` with the server's fields. When neither the decision
    (``changed``) nor the given part moved, the previous decider, time and evidence stand;
    otherwise they are this change's actor, now, and ``evidence``."""
    from provisa.core import model_change

    if previous and not changed and given_part(previous) == given_part(given):
        return {**given_part(given), **{k: previous.get(k) for k in SERVER}}
    return {
        **given_part(given),
        "decided_by": model_change.actor(),
        "decided_at": now().isoformat(),
        "evidence": dict(evidence),
    }


async def stamp_for(
    conn: Any,
    previous: Mapping[str, Any] | None,
    given_raw: Mapping[str, Any] | None,
    *,
    changed: bool,
    where: str,
    evidence: Any,
) -> dict[str, Any]:
    """The why to store for a judgment being saved. ``given_raw`` None keeps the stored given
    part (a save that does not restate the why, such as an editor or a model file that leaves it
    out); an empty mapping clears it. ``evidence`` is awaited for the evidence only when the
    decider, time and evidence move."""
    given = given_part(previous) if given_raw is None else parse(given_raw, where)
    await check_bases(conn, given, where)
    if previous and not changed and given_part(previous) == given_part(given):
        return {**given_part(given), **{k: previous.get(k) for k in SERVER}}
    return stamp(previous, given, changed=True, evidence=await evidence())
