# Copyright (c) 2026 Kenneth Stott
# Canary: 7e1a9c4b-2f6d-4b8e-9a3c-5d7f1e8b4c6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.jev.client — the TypeSafe Jev (System One) API wrapper."""

import httpx
import pytest
import respx

from provisa.jev.client import JevApiError, evaluate

API_KEY = "ts_test_key"


@respx.mock
async def test_evaluate_sends_bearer_and_builds_questions_map():
    route = respx.post("https://api.typesafe.ai/v1/systemone").mock(
        return_value=httpx.Response(
            200,
            json={
                "model": "jev-latest",
                "answers": {"q1": {"type": "noul", "noul": 0.92, "confidence": 0.81}},
                "usage": {"input_tokens": 10, "output_tokens": 2},
            },
        )
    )
    result = await evaluate(
        API_KEY,
        {"text": "hello"},
        [{"id": "q1", "type": "noul", "instructions": "is this a greeting?"}],
    )
    assert result["answers"]["q1"]["noul"] == 0.92
    request = route.calls.last.request
    assert request.headers["Authorization"] == f"Bearer {API_KEY}"
    import json

    body = json.loads(request.content)
    assert body["model"] == "jev-latest"
    assert body["questions"]["q1"]["type"] == "noul"
    assert body["state"] == {"text": "hello"}


@respx.mock
async def test_evaluate_raises_on_error_status():
    respx.post("https://api.typesafe.ai/v1/systemone").mock(
        return_value=httpx.Response(422, text="bad criteria")
    )
    with pytest.raises(JevApiError):
        await evaluate(API_KEY, "state", [{"id": "q1", "type": "noul"}])


async def test_evaluate_requires_api_key():
    with pytest.raises(ValueError):
        await evaluate("", "state", [{"id": "q1", "type": "noul"}])


async def test_evaluate_requires_questions():
    with pytest.raises(ValueError):
        await evaluate(API_KEY, "state", [])


async def test_evaluate_rejects_unknown_question_type():
    with pytest.raises(ValueError):
        await evaluate(API_KEY, "state", [{"id": "q1", "type": "bogus"}])


async def test_evaluate_rejects_duplicate_question_id():
    with pytest.raises(ValueError):
        await evaluate(
            API_KEY,
            "state",
            [
                {"id": "q1", "type": "noul"},
                {"id": "q1", "type": "score"},
            ],
        )


async def test_evaluate_rejects_missing_id():
    with pytest.raises(ValueError):
        await evaluate(API_KEY, "state", [{"type": "choice"}])
