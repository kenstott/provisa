# Copyright (c) 2026 Kenneth Stott
# Canary: 95464bc1-295f-441f-bc08-d0f7b21bb351
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The OpenAPI source kept beside each ServiceNow source (REQ-1954): ``<id>-api``, its
specification ``servicenow:<id>`` (``openapi/servicenow``). It is written whenever the ServiceNow
source is saved, from the source as stored (its address and the credential reference it keeps),
keeping the APIs and import sets its steward added; it is removed with the source.
"""

# Requirements: REQ-1954

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provisa.openapi.servicenow import MAPPING_KEY, companion_id, companion_spec_path

if TYPE_CHECKING:
    from provisa.core.database import Connection


def held(mapping: dict | None) -> dict:
    """What a companion's mapping keeps of its instance and its steward's choices."""
    return dict((mapping or {}).get(MAPPING_KEY) or {})


async def sync_companion(conn: "Connection", source_id: str) -> None:
    """Write ServiceNow source ``source_id``'s companion from the source as stored: its address,
    the username and the password reference it keeps, and the APIs and import sets already added
    to the companion."""
    from provisa.api_source.openapi_endpoint import register_openapi_source, store_openapi_auth
    from provisa.core.models import Source, SourceType
    from provisa.core.repositories import source as source_repo

    row = await source_repo.get(conn, source_id)
    if row is None or row["type"] != SourceType.servicenow.value:
        raise LookupError(f"{source_id!r} is not a ServiceNow source")
    source = source_repo.source_from_row(row)
    cid = companion_id(source_id)
    existing = await source_repo.get(conn, cid)
    mapping: dict[str, Any] = dict(existing["mapping"] or {}) if existing is not None else {}
    kept = held(mapping)
    instance_url = source.base_url or source.host
    mapping[MAPPING_KEY] = {
        "source": source_id,
        "instance_url": instance_url,
        "username": source.username,
        "password": source.password,
        "apis": kept.get("apis", []),
        "import_sets": kept.get("import_sets", []),
    }
    companion = Source(
        id=cid,
        type=SourceType.openapi,
        host="",
        port=0,
        database="",
        username="",
        path=companion_spec_path(source_id),
        description=f"The APIs of ServiceNow source {source_id}",
        mapping=mapping,
    )
    if existing is not None:
        # Its own Load Management and Timeliness settings are its steward's: kept.
        companion = source_repo.source_from_row(existing).model_copy(
            update={
                "path": companion.path,
                "mapping": mapping,
                "description": companion.description,
            }
        )
    await source_repo.upsert(conn, companion)
    await register_openapi_source(conn, cid, instance_url)
    await store_openapi_auth(
        conn, cid, {"type": "basic", "username": source.username, "password": source.password}
    )
    from provisa.api.app import state

    # Held by this process as every saved source is, so its operations are offered at once.
    state.source_types[cid] = SourceType.openapi.value
    _forget_spec(cid)


async def change_companion(conn: "Connection", source_id: str, **changes: Any) -> None:
    """Set the companion's ``apis`` or ``import_sets`` to what ``changes`` gives."""
    from provisa.core.repositories import source as source_repo

    cid = companion_id(source_id)
    existing = await source_repo.get(conn, cid)
    if existing is None:
        raise LookupError(f"ServiceNow source {source_id!r} has no companion {cid!r}")
    mapping = dict(existing["mapping"] or {})
    mapping[MAPPING_KEY] = {**held(mapping), **changes}
    await source_repo.upsert(
        conn, source_repo.source_from_row(existing).model_copy(update={"mapping": mapping})
    )
    _forget_spec(cid)


def _forget_spec(cid: str) -> None:
    """Drop the companion's document from this process, so its next use assembles it afresh."""
    from provisa.api.app import state

    getattr(state, "openapi_specs", {}).pop(cid, None)


# -- what the ServiceNow page's APIs section reads and changes ----------------------------------


async def _servicenow_source(source_id: str):
    from provisa.api.admin.introspect import _adapter_source

    source = await _adapter_source(source_id)
    if source.type.value != "servicenow":
        raise LookupError(f"{source_id!r} is not a ServiceNow source")
    return source


async def _held(source_id: str) -> dict:
    from provisa.api.admin.schema import _get_pool
    from provisa.core.repositories import source as source_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await source_repo.get(conn, companion_id(source_id))
    if row is None:
        raise LookupError(f"ServiceNow source {source_id!r} has no companion")
    return held(row["mapping"])


async def instance_apis(source_id: str) -> tuple[list[Any], dict]:
    """Every API the instance publishes, and what the companion holds."""
    import asyncio

    from provisa.core.secrets_store import bound_to_request_org
    from provisa.openapi.servicenow import list_instance_apis

    source = await _servicenow_source(source_id)
    async with bound_to_request_org():
        apis = await asyncio.to_thread(
            list_instance_apis, source.base_url or source.host, source.username, source.password
        )
    return apis, await _held(source_id)


async def set_api(source_id: str, namespace: str, name: str, added: bool) -> None:
    """Add one of the instance's APIs to the companion, or remove it. An API the instance does not
    publish is refused by name."""
    from provisa.api.admin.schema import _get_pool

    apis, kept = await instance_apis(source_id)
    published = next((a for a in apis if (a.namespace, a.name) == (namespace, name)), None)
    if added and published is None:
        raise LookupError(f"the instance of {source_id!r} publishes no API {namespace}/{name}")
    others = [a for a in kept.get("apis", []) if (a["namespace"], a["name"]) != (namespace, name)]
    wanted = others + (
        [{"namespace": namespace, "name": name, "version": "latest"}] if added else []
    )
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await change_companion(conn, source_id, apis=wanted)


async def set_import_set(source_id: str, table_name: str, staging: str, added: bool) -> None:
    """Add import set ``staging`` to the companion, typed from its staging table's columns, or
    remove it. Only the import set of one of the instance's active transform maps into
    ``table_name`` is added; any other is refused by name."""
    from provisa.api.admin.schema import _get_pool
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.federation.pgwire_replica import (
        schema_name,
        servicenow_columns,
        servicenow_transform_maps,
    )

    kept = await _held(source_id)
    others = [s for s in kept.get("import_sets", []) if s["staging"] != staging]
    wanted = others
    if added:
        source = await _servicenow_source(source_id)
        schema = schema_name(source)
        async with bound_to_request_org():
            maps = await servicenow_transform_maps(source, schema, table_name)
            if staging not in {s for _name, s in maps}:
                raise LookupError(
                    f"{staging!r} is not the import set of an active transform map into "
                    f"{table_name!r}"
                )
            columns = await servicenow_columns(source, schema, staging)
        wanted = others + [
            {"staging": staging, "table": table_name, "columns": [list(c) for c in columns]}
        ]
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await change_companion(conn, source_id, import_sets=wanted)
