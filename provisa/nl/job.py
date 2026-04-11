# Copyright (c) 2026 Kenneth Stott
# Canary: a72649b6-1876-4577-943f-6c7ccdedf665
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""NlJob dataclass and job store (Phase AV, REQ-354).

States: pending → running → complete | failed

The job store is Redis-backed when REDIS_URL is set; falls back to an
in-process dict otherwise (useful for tests and single-process dev).
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

NlJobState = Literal["pending", "running", "complete", "failed"]
NlTarget = Literal["cypher", "graphql", "sql"]

_JOB_TTL = 3600  # seconds


@dataclass
class BranchResult:
    """Result for a single generation branch."""

    query: str | None = None
    result: Any = None
    error: str | None = None


@dataclass
class NlJob:
    """Represents a natural-language query job."""

    job_id: str
    nl_query: str
    role: str
    state: NlJobState = "pending"
    created_at: float = field(default_factory=time.time)
    branches: dict[NlTarget, BranchResult] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "nl_query": self.nl_query,
            "role": self.role,
            "state": self.state,
            "created_at": self.created_at,
            "branches": {
                k: {"query": v.query, "result": v.result, "error": v.error}
                for k, v in self.branches.items()
            },
        }

    @classmethod
    def from_dict(cls, d: dict) -> "NlJob":
        job = cls(
            job_id=d["job_id"],
            nl_query=d["nl_query"],
            role=d["role"],
            state=d["state"],
            created_at=d.get("created_at", time.time()),
        )
        for k, v in d.get("branches", {}).items():
            job.branches[k] = BranchResult(  # type: ignore[index]
                query=v.get("query"),
                result=v.get("result"),
                error=v.get("error"),
            )
        return job


def new_job_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Job store
# ---------------------------------------------------------------------------

class InMemoryJobStore:
    """In-process job store for dev/test."""

    def __init__(self) -> None:
        self._jobs: dict[str, NlJob] = {}

    async def put(self, job: NlJob) -> None:
        self._jobs[job.job_id] = job

    async def get(self, job_id: str) -> NlJob | None:
        return self._jobs.get(job_id)

    async def update_branch(self, job_id: str, target: NlTarget, branch: BranchResult) -> None:
        job = self._jobs.get(job_id)
        if job is not None:
            job.branches[target] = branch

    async def set_state(self, job_id: str, state: NlJobState) -> None:
        job = self._jobs.get(job_id)
        if job is not None:
            job.state = state


class RedisJobStore:
    """Redis-backed job store."""

    def __init__(self, redis_url: str) -> None:
        import redis.asyncio as aioredis
        self._redis = aioredis.from_url(redis_url)

    def _key(self, job_id: str) -> str:
        return f"nl:job:{job_id}"

    async def put(self, job: NlJob) -> None:
        await self._redis.setex(self._key(job.job_id), _JOB_TTL, json.dumps(job.to_dict()))

    async def get(self, job_id: str) -> NlJob | None:
        raw = await self._redis.get(self._key(job_id))
        if raw is None:
            return None
        return NlJob.from_dict(json.loads(raw))

    async def update_branch(self, job_id: str, target: NlTarget, branch: BranchResult) -> None:
        job = await self.get(job_id)
        if job is not None:
            job.branches[target] = branch
            await self.put(job)

    async def set_state(self, job_id: str, state: NlJobState) -> None:
        job = await self.get(job_id)
        if job is not None:
            job.state = state
            await self.put(job)


def make_job_store(redis_url: str | None = None) -> InMemoryJobStore | RedisJobStore:
    """Return Redis-backed store if URL given, else in-memory."""
    import os
    url = redis_url or os.environ.get("REDIS_URL", "")
    if url:
        return RedisJobStore(url)
    return InMemoryJobStore()
