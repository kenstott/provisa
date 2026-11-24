# Copyright (c) 2026 Kenneth Stott
# Canary: e3b7d1a9-2f46-4c08-9a5e-61c4f8b2d7e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The organisation's default Spark cluster, as its administrator sets it (REQ-2225).

``/admin/orgs/{org_id}/spark-cluster`` is the organisation's own, opened as its registries are
(``org_settings`` held in THAT organisation):

- ``GET``: the default as entered (its credential a secret reference, so it is shown as it is),
  and the cluster a source naming none would use now: the default, the local test cluster of a
  demo or local install, or none.
- ``PUT`` ``{address, credential}``: keep the default. ``DELETE``: remove it.
- ``POST /test`` ``{address, credential}``: call the cluster as entered (the form's values, saved or
  not; left empty, the cluster a source naming none would use) and answer its Spark version or
  the server's own words.
"""

# Requirements: REQ-2225
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.secrets_router import _admin_pool, _org_guard
from provisa.api.errors import ApiError
from provisa.core import secrets_store, spark_cluster
from provisa.core.spark_cluster import SparkClusterRefused

router = APIRouter(prefix="/admin/orgs/{org_id}/spark-cluster", tags=["admin"])


class ClusterBody(BaseModel):
    address: str
    #: A ${secret:NAME} reference; empty or left out: the cluster takes no token.
    credential: str | None = None


def _refusal(refused: SparkClusterRefused) -> ApiError:
    return ApiError(refused.status, refused.code, str(refused), None, **refused.params)


async def _in_use(org_id: str) -> dict:
    """What a source naming no cluster runs on now. The local test cluster is shown, never
    started: viewing the tab does not download Spark."""
    if await spark_cluster.read(_admin_pool(), org_id) is None:
        local = spark_cluster.local_test_target()
        if local is not None:
            return {
                "origin": spark_cluster.ORIGIN_LOCAL,
                "label": spark_cluster.LOCAL_TEST_LABEL,
                "target": local,
            }
    try:
        found = await spark_cluster.resolved(_admin_pool(), org_id, None)
    except SparkClusterRefused as refused:
        return {"origin": None, "detail": str(refused), "code": refused.code}
    label = spark_cluster.LOCAL_TEST_LABEL if found.origin == spark_cluster.ORIGIN_LOCAL else None
    # The address is shown without a token it was given.
    shown = spark_cluster.parse(found.address)
    return {
        "origin": found.origin,
        "label": label,
        "target": f"{shown.host}:{shown.port}",
    }


@router.get("")
async def get_cluster(request: Request, org_id: str) -> dict:
    """The default as entered, and the cluster a source naming none uses."""
    await _org_guard(request, org_id)
    kept = await spark_cluster.read(_admin_pool(), org_id)
    return {
        "cluster": None
        if kept is None
        else {"address": kept.address, "credential": kept.credential},
        "in_use": await _in_use(org_id),
    }


@router.put("")
async def put_cluster(request: Request, org_id: str, body: ClusterBody) -> dict:
    """Keep the default, replacing any the organisation had."""
    actor = await _org_guard(request, org_id)
    try:
        kept = await spark_cluster.put(
            _admin_pool(), org_id, address=body.address, credential=body.credential, actor=actor
        )
    except SparkClusterRefused as refused:
        raise _refusal(refused) from None
    return {
        "cluster": {"address": kept.address, "credential": kept.credential},
        "in_use": await _in_use(org_id),
    }


@router.delete("")
async def delete_cluster(request: Request, org_id: str) -> dict:
    """Remove the default. A secret it named is the administrator's and is not touched."""
    await _org_guard(request, org_id)
    removed = await spark_cluster.forget(_admin_pool(), org_id)
    return {"removed": removed, "in_use": await _in_use(org_id)}


async def _address_to_test(org_id: str, body: ClusterBody | None) -> str:
    from provisa.core.secrets import resolve_secrets

    if body is None or not body.address.strip():
        return (await spark_cluster.resolved(_admin_pool(), org_id, None)).address
    address = body.address.strip()
    spark_cluster.parse(address)
    credential = (body.credential or "").strip()
    if not credential:
        return address
    async with secrets_store.bound(_admin_pool(), org_id):
        try:
            token = resolve_secrets(credential)
        except (KeyError, ValueError) as unresolved:
            reason = unresolved.args[0] if unresolved.args else type(unresolved).__name__
            raise SparkClusterRefused(
                "unresolved", f"The credential could not be resolved: {reason}", status=409
            ) from None
    return spark_cluster.with_token(address, token)


@router.post("/test")
async def test_cluster(request: Request, org_id: str, body: ClusterBody | None = None) -> dict:
    """Call the cluster and answer its Spark version, or why it did not answer."""
    await _org_guard(request, org_id)
    try:
        address = await _address_to_test(org_id, body)
        # REQ-2006: held to this organisation's egress rule, refused by name before any dial.
        version = await asyncio.to_thread(spark_cluster.probe, address, org_id=org_id)
    except SparkClusterRefused as refused:
        raise _refusal(refused) from None
    shown = spark_cluster.parse(address)
    return {"ok": True, "spark_version": version, "target": f"{shown.host}:{shown.port}"}
