# Copyright (c) 2026 Kenneth Stott
# Canary: 3a5c8f31-0229-43c4-98f3-6cdc866f0448
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Natural Language Query endpoints (Phase AV, REQ-354–359).

POST /query/nl         — submit NL query, return { job_id }
GET  /query/nl/{id}   — poll for result
GET  /query/nl/{id}/stream — SSE stream, one event per branch result
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from provisa.nl.job import NlJob, make_job_store, new_job_id

log = logging.getLogger(__name__)

router = APIRouter()

# Module-level job store (one per process; shared across requests)
_job_store = make_job_store()


class NlRequest(BaseModel):
    q: str
    role: str = "default"


@router.post("/query/nl")
async def submit_nl_query(body: NlRequest, request: Request) -> JSONResponse:
    """Submit an NL query. Returns job_id immediately; processing is async."""
    from provisa.api.app import state

    job_id = new_job_id()
    job = NlJob(job_id=job_id, nl_query=body.q, role=body.role)
    await _job_store.put(job)

    llm = _get_llm()
    asyncio.create_task(_run_job(job_id, body.q, body.role, state, llm))

    return JSONResponse(status_code=202, content={"job_id": job_id})


@router.get("/query/nl/{job_id}")
async def get_nl_result(job_id: str) -> JSONResponse:
    """Poll for NL job result."""
    job = await _job_store.get(job_id)
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    return JSONResponse(content=job.to_dict())


@router.get("/query/nl/{job_id}/stream")
async def stream_nl_result(job_id: str) -> StreamingResponse:
    """SSE stream: emits each branch result as it completes."""
    return StreamingResponse(
        _sse_generator(job_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _run_job(job_id: str, nl_query: str, role: str, app_state: object, llm: object) -> None:
    from provisa.nl.runner import run_nl_job
    try:
        await run_nl_job(job_id, nl_query, role, app_state, _job_store, llm)
    except Exception as exc:
        log.exception("NL job %s failed: %s", job_id, exc)
        await _job_store.set_state(job_id, "failed")


async def _sse_generator(job_id: str) -> AsyncGenerator[str, None]:
    """Poll job store and emit SSE events for each completed branch."""
    emitted: set[str] = set()
    poll_interval = 0.5
    max_polls = 120  # 60 seconds

    for _ in range(max_polls):
        job = await _job_store.get(job_id)
        if job is None:
            yield f"event: error\ndata: {json.dumps({'error': 'Job not found'})}\n\n"
            return

        for target, branch in job.branches.items():
            if target not in emitted:
                emitted.add(target)
                payload = {
                    "target": target,
                    "query": branch.query,
                    "result": branch.result,
                    "error": branch.error,
                }
                yield f"event: branch\ndata: {json.dumps(payload)}\n\n"

        if job.state in ("complete", "failed"):
            yield f"event: done\ndata: {json.dumps({'state': job.state})}\n\n"
            return

        await asyncio.sleep(poll_interval)

    yield f"event: timeout\ndata: {{}}\n\n"


def _get_llm() -> object:
    """Build an LLM client from environment config."""
    from provisa.nl.loop import LLMClient

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        return _AnthropicLLM(api_key)
    return _NoopLLM()


class _AnthropicLLM:
    """Thin wrapper over the Anthropic SDK."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def complete(self, prompt: str) -> str:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=self._api_key)
        message = await client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text


class _NoopLLM:
    """Returns empty string — used when no API key is configured."""

    async def complete(self, prompt: str) -> str:
        return ""
