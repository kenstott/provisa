# Copyright (c) 2026 Kenneth Stott
# Canary: e8a14f06-27c3-4d91-b5f2-1a93c60d7e45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""dbt Cloud, reached with the dbt source's API token (REQ-2228).

The token is the source's credential, held as any source's credential is and resolved when it is
used: to read a job's latest manifest through the API, and to run a command through the jobs API.
"""

# Requirements: REQ-2228

from __future__ import annotations

from typing import Any

import httpx
from sqlalchemy import select

from provisa.core.schema_org import sources
from provisa.core.secrets_store import resolved_credential
from provisa.security import sandbox_egress

MANIFEST_ARTIFACT = "manifest.json"
RUN_RESULTS_ARTIFACT = "run_results.json"
_TIMEOUT_S = 30.0
#: The status dbt Cloud gives a run that succeeded.
_RUN_SUCCESS = 10


class NoCloudRun(LookupError):
    """The dbt Cloud job has no successful run to read a manifest from."""


def token_headers(token: str, scheme: str = "Token") -> dict[str, str]:
    return {"Authorization": f"{scheme} {token}"}


async def authorization(state: Any, source_id: str, scheme: str) -> dict[str, str]:
    """The header a command of ``source_id`` calls dbt Cloud with: its credential, resolved now."""
    async with state.model_db.acquire() as conn:
        row = (
            await conn.execute_core(select(sources.c.password_ref).where(sources.c.id == source_id))
        ).fetchone()
    if row is None or not row.password_ref:
        raise LookupError(f"dbt source {source_id!r} holds no dbt Cloud token")
    return token_headers(await resolved_credential(row.password_ref), scheme)


async def latest_manifest(host: str, account_id: str, job_id: str, token: str) -> bytes:
    """The manifest of the job's latest successful run."""
    return await _latest_artifact(host, account_id, job_id, token, MANIFEST_ARTIFACT)


async def latest_run_results(host: str, account_id: str, job_id: str, token: str) -> bytes:
    """The run results of the job's latest successful run."""
    return await _latest_artifact(host, account_id, job_id, token, RUN_RESULTS_ARTIFACT)


async def _latest_artifact(host: str, account_id: str, job_id: str, token: str, name: str) -> bytes:
    base = f"{host.rstrip('/')}/api/v2/accounts/{account_id}"
    async with httpx.AsyncClient(
        timeout=_TIMEOUT_S,
        headers=token_headers(token),
        event_hooks=sandbox_egress.http_event_hooks(),
    ) as client:
        runs = await client.get(
            f"{base}/runs/",
            params={
                "job_definition_id": job_id,
                "status": _RUN_SUCCESS,
                "order_by": "-finished_at",
                "limit": 1,
            },
        )
        runs.raise_for_status()
        latest = runs.json().get("data") or []
        if not latest:
            raise NoCloudRun(f"dbt Cloud job {job_id} has no successful run")
        artifact = await client.get(f"{base}/runs/{latest[0]['id']}/artifacts/{name}")
        artifact.raise_for_status()
        return artifact.content
