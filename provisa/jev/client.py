# Copyright (c) 2026 Kenneth Stott
# Canary: 12ad836d-886c-4d8a-a9ce-b651f100d4bc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""TypeSafe Jev (System One) API client.

Wraps the single ``POST /v1/systemone`` endpoint: a batch of typed decision
questions (noul/choice/score) evaluated against shared ``state`` in one call,
each answer carrying a probability distribution and calibrated confidence.
Reference: https://docs.typesafe.ai/api.md
"""

from __future__ import annotations

from typing import Any

import httpx

BASE_URL = "https://api.typesafe.ai/v1"
DEFAULT_MODEL = "jev-latest"

_QUESTION_TYPES = {"noul", "choice", "score"}


class JevApiError(Exception):
    """The Jev API returned a non-2xx response."""

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"Jev API HTTP {status_code}: {body[:300]}")


def _build_questions(questions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """List of {id, type, instructions, criteria} -> the API's {question_id: {...}} map."""
    out: dict[str, dict[str, Any]] = {}
    for q in questions:
        qid = q.get("id")
        qtype = q.get("type")
        if not qid:
            raise ValueError("each question requires a non-empty id")
        if qtype not in _QUESTION_TYPES:
            raise ValueError(f"question {qid!r}: type must be one of {sorted(_QUESTION_TYPES)}")
        if qid in out:
            raise ValueError(f"duplicate question id {qid!r}")
        out[qid] = {
            "type": qtype,
            "instructions": q.get("instructions", ""),
            "criteria": q.get("criteria", {}),
        }
    return out


async def evaluate(
    api_key: str,
    state: Any,
    questions: list[dict[str, Any]],
    *,
    model: str = DEFAULT_MODEL,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """``POST /v1/systemone`` — evaluate every question against ``state`` in one call.

    ``questions`` is a list of ``{"id", "type", "instructions", "criteria"}`` dicts
    (noul/choice/score, per docs.typesafe.ai/primitives). Returns the raw response:
    ``{"model", "answers": {question_id: {...}}, "usage": {...}}``.
    """
    if not api_key or not api_key.strip():
        raise ValueError("api_key is required")
    if not questions:
        raise ValueError("at least one question is required")
    payload = {
        "state": state,
        "model": model,
        "questions": _build_questions(questions),
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(f"{BASE_URL}/systemone", json=payload, headers=headers)
        if resp.status_code != 200:
            raise JevApiError(resp.status_code, resp.text)
        return resp.json()
