# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e1b7a-3d52-4f86-b0a9-2e7f6c18d453
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The services a code source's worker reads besides its secrets and the LLM (REQ-2106).

Each is under ``$PROVISA_SERVICES`` (provisa/codesource/services.py), answered by the egress proxy
for the worker holding the token, as JSON:

- ``settings[/<name>]`` GET: the source's declared non-secret settings, as its steward edits them.
- ``cache/<key>`` GET, PUT ``{value, ttl}``, DELETE: the source's own cache, shared by every
  worker of it in every process (``code_source_cache``). A value is JSON, at most 1 MB.
- ``vectors/embed`` ``{texts, model}``, ``vectors/upsert`` ``{items, model}``, ``vectors/search``
  ``{query, k, model}``, ``vectors/delete`` ``{ids}``, all POST: the source's own vector
  collection (``code_source_vectors``), which its code builds from its own reference data and
  searches; embedding runs through the org's vector models as the LLM service's does (REQ-2105).
  It is never a governed table's data: that reaches code only through its parameters.
- ``http`` POST ``{method, url, headers, body | body_base64, timeout}``: a fetch Provisa makes for
  the code, to a host on the egress allow-list the worker's own egress is held to, through the
  deployment's outbound proxy and CA bundle (``HTTPS_PROXY``, ``SSL_CERT_FILE``,
  ``REQUESTS_CA_BUNDLE``), recorded in the org's trail.
- ``log`` POST ``{level, message, fields}`` and ``metric`` POST ``{name, value, kind,
  attributes}``: a log record and a metric point attached to the request the call serves.
"""

# Requirements: REQ-2106
from __future__ import annotations

import base64
import json
import logging
import math
import os
import re
import threading
from datetime import UTC, datetime, timedelta
from typing import Any, TypeGuard
from urllib.parse import unquote, urlsplit

from provisa.codesource.services import ServiceGrant, ServiceRefused

log = logging.getLogger(__name__)
#: The logger a code source's own log lines are written to.
code_log = logging.getLogger("provisa.codesource.code")

MAX_CACHE_VALUE = 1024 * 1024
MAX_VECTORS = 50_000
MAX_UPSERT = 1000
MAX_HTTP_BODY = 32 * 1024 * 1024
HTTP_TIMEOUT_S = 60.0
HTTP_CALL = "code_source.http"
_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}
_METRIC_NAME = re.compile(r"^[a-z][a-z0-9_.]{0,63}$")
_METRIC_KINDS = ("counter", "gauge", "histogram")


def _json(body: bytes) -> dict:
    try:
        request = json.loads(body or b"{}")
    except ValueError as exc:
        raise ServiceRefused(400, f"the request body is not JSON: {exc}") from None
    if not isinstance(request, dict):
        raise ServiceRefused(400, "the request body is a JSON object")
    return request


def _answer(value: Any) -> bytes:
    return json.dumps(value, default=str).encode()


def _method(method: str, *allowed: str) -> None:
    if method not in allowed:
        raise ServiceRefused(405, f"this service takes {' or '.join(allowed)}, not {method}")


# -- settings -----------------------------------------------------------------------------------


def settings(grant: ServiceGrant, method: str, name: str, span: Any) -> bytes:
    _method(method, "GET")
    if not name:
        return _answer(dict(grant.settings))
    name = unquote(name)
    span.set_attribute("codesource.setting", name)
    if name not in grant.settings:
        raise ServiceRefused(404, f"the source {grant.source_id!r} declares no setting {name!r}")
    return _answer({"value": grant.settings[name]})


# -- cache --------------------------------------------------------------------------------------


async def cache(grant: ServiceGrant, method: str, key: str, body: bytes, span: Any) -> bytes:
    from provisa.codesource.llm_service import org_bound

    key = unquote(key)
    if not key:
        raise ServiceRefused(400, "a cache entry is named: cache/<key>")
    _method(method, "GET", "PUT", "DELETE")
    async with org_bound(grant) as (model_db, _cfg):
        if method == "GET":
            found = await _cache_get(model_db, grant.source_id, key)
            span.set_attribute("codesource.cache", "hit" if found is not None else "miss")
            if found is None:
                raise ServiceRefused(404, f"the cache holds no {key!r}")
            return _answer({"value": json.loads(found)})
        if method == "DELETE":
            await _cache_delete(model_db, grant.source_id, key)
            span.set_attribute("codesource.cache", "delete")
            return _answer({})
        request = _json(body)
        if "value" not in request:
            raise ServiceRefused(400, "a cache entry is set with its value: {value, ttl}")
        text = json.dumps(request["value"])
        if len(text.encode()) > MAX_CACHE_VALUE:
            raise ServiceRefused(413, f"the cache keeps values of at most {MAX_CACHE_VALUE} bytes")
        ttl = request.get("ttl")
        if ttl is not None and (
            isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or ttl <= 0
        ):
            raise ServiceRefused(400, "a cache entry's ttl is a number of seconds above 0")
        expires = None if ttl is None else datetime.now(UTC) + timedelta(seconds=float(ttl))
        await _cache_put(model_db, grant.source_id, key, text, expires)
        span.set_attribute("codesource.cache", "set")
        return _answer({})


async def _cache_get(model_db: Any, source_id: str, key: str) -> str | None:
    from sqlalchemy import select

    from provisa.core.schema_org import code_source_cache as t

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(t.c.value).where(
                (t.c.source_id == source_id)
                & (t.c.key == key)
                & (t.c.expires_at.is_(None) | (t.c.expires_at > datetime.now(UTC)))
            )
        )
        row = result.fetchone()
    return None if row is None else row[0]


async def _cache_put(
    model_db: Any, source_id: str, key: str, text: str, expires: datetime | None
) -> None:
    from sqlalchemy.dialects.postgresql import insert

    from provisa.core.schema_org import code_source_cache as t

    stmt = insert(t).values(source_id=source_id, key=key, value=text, expires_at=expires)
    async with model_db.acquire() as conn:
        await conn.execute_core(
            stmt.on_conflict_do_update(
                index_elements=[t.c.source_id, t.c.key],
                set_={"value": stmt.excluded.value, "expires_at": stmt.excluded.expires_at},
            )
        )


async def _cache_delete(model_db: Any, source_id: str, key: str) -> None:
    from sqlalchemy import delete

    from provisa.core.schema_org import code_source_cache as t

    async with model_db.acquire() as conn:
        await conn.execute_core(delete(t).where((t.c.source_id == source_id) & (t.c.key == key)))


# -- vectors ------------------------------------------------------------------------------------


async def vectors(grant: ServiceGrant, method: str, op: str, body: bytes, span: Any) -> bytes:
    from provisa.codesource.llm_service import embed, org_bound

    _method(method, "POST")
    if op not in ("embed", "upsert", "search", "delete"):
        raise ServiceRefused(404, f"vectors offers embed, upsert, search and delete, not {op!r}")
    request = _json(body)
    span.set_attribute("codesource.vectors", op)
    async with org_bound(grant) as (model_db, cfg):

        async def embedded(texts: list[str]) -> list[list[float]]:
            model = request.get("model")
            if not isinstance(model, str):
                raise ServiceRefused(400, "embedding a text names the org's vector model: model")
            span.set_attribute("codesource.llm.model", model)
            return await embed(grant, model_db, cfg, model, texts)

        if op == "embed":
            texts = request.get("texts")
            if not _texts(texts):
                raise ServiceRefused(400, "texts is a list of strings")
            return _answer({"vectors": await embedded(texts)})
        if op == "delete":
            ids = request.get("ids")
            if not _texts(ids):
                raise ServiceRefused(400, "ids is a list of strings")
            await _vectors_delete(model_db, grant.source_id, ids)
            return _answer({})
        if op == "upsert":
            return _answer(await _upsert(grant, model_db, request, embedded))
        return _answer(await _search(grant, model_db, request, embedded))


def _texts(value: Any) -> TypeGuard[list[str]]:
    return isinstance(value, list) and bool(value) and all(isinstance(v, str) for v in value)


def _vector(value: Any) -> TypeGuard[list[float]]:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in value)
    )


async def _upsert(grant: ServiceGrant, model_db: Any, request: dict, embedded: Any) -> dict:
    items = request.get("items")
    if not isinstance(items, list) or not items:
        raise ServiceRefused(400, "items is a list of {id, text | vector, metadata}")
    if len(items) > MAX_UPSERT:
        raise ServiceRefused(413, f"an upsert takes at most {MAX_UPSERT} items")
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            raise ServiceRefused(400, "each item has an id, a string")
        if not (isinstance(item.get("text"), str) or _vector(item.get("vector"))):
            raise ServiceRefused(400, f"the item {item['id']!r} has a text or a vector")
    to_embed = [i for i in items if not _vector(i.get("vector"))]
    if to_embed:
        for item, vec in zip(to_embed, await embedded([i["text"] for i in to_embed])):
            item["vector"] = vec
    dimension = await _dimension(model_db, grant.source_id)
    for item in items:
        width = len(item["vector"])
        if dimension is not None and width != dimension:
            raise ServiceRefused(
                400,
                f"the item {item['id']!r} has {width} dimensions; the collection's have {dimension}",
            )
        dimension = width
    held = await _vector_count(model_db, grant.source_id)
    if held + len(items) > MAX_VECTORS:
        raise ServiceRefused(
            413, f"the source {grant.source_id!r}'s collection holds at most {MAX_VECTORS} items"
        )
    await _vectors_put(model_db, grant.source_id, items)
    return {"upserted": len(items)}


async def _search(grant: ServiceGrant, model_db: Any, request: dict, embedded: Any) -> dict:
    query = request.get("query")
    k = request.get("k", 5)
    if isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= 1000:
        raise ServiceRefused(400, "k is a whole number from 1 to 1000")
    if isinstance(query, str):
        query = (await embedded([query]))[0]
    if not _vector(query):
        raise ServiceRefused(400, "query is a text or a vector")
    rows = await _vectors_all(model_db, grant.source_id)
    scored = []
    for item_id, vector_text, text, metadata in rows:
        vec = json.loads(vector_text)
        if len(vec) != len(query):
            raise ServiceRefused(
                400, f"the query has {len(query)} dimensions; the collection's have {len(vec)}"
            )
        scored.append((_cosine(query, vec), item_id, text, metadata))
    scored.sort(key=lambda s: s[0], reverse=True)
    return {
        "matches": [
            {"id": i, "score": score, "text": text, "metadata": json.loads(metadata)}
            for score, i, text, metadata in scored[:k]
        ]
    }


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return 0.0 if norm == 0 else dot / norm


async def _dimension(model_db: Any, source_id: str) -> int | None:
    from sqlalchemy import select

    from provisa.core.schema_org import code_source_vectors as t

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(t.c.vector).where(t.c.source_id == source_id).limit(1)
        )
        row = result.fetchone()
    return None if row is None else len(json.loads(row[0]))


async def _vector_count(model_db: Any, source_id: str) -> int:
    from sqlalchemy import func, select

    from provisa.core.schema_org import code_source_vectors as t

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(func.count()).select_from(t).where(t.c.source_id == source_id)
        )
        return int(result.fetchone()[0])


async def _vectors_all(model_db: Any, source_id: str) -> list[tuple]:
    from sqlalchemy import select

    from provisa.core.schema_org import code_source_vectors as t

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(t.c.item_id, t.c.vector, t.c.text, t.c.item_metadata).where(
                t.c.source_id == source_id
            )
        )
        return [tuple(r) for r in result.fetchall()]


async def _vectors_put(model_db: Any, source_id: str, items: list[dict]) -> None:
    from sqlalchemy.dialects.postgresql import insert

    from provisa.core.schema_org import code_source_vectors as t

    stmt = insert(t).values(
        [
            {
                "source_id": source_id,
                "item_id": i["id"],
                "vector": json.dumps(i["vector"]),
                "text": i.get("text"),
                "item_metadata": json.dumps(i.get("metadata") or {}),
            }
            for i in items
        ]
    )
    async with model_db.acquire() as conn:
        await conn.execute_core(
            stmt.on_conflict_do_update(
                index_elements=[t.c.source_id, t.c.item_id],
                set_={
                    "vector": stmt.excluded.vector,
                    "text": stmt.excluded.text,
                    "item_metadata": stmt.excluded.item_metadata,
                },
            )
        )


async def _vectors_delete(model_db: Any, source_id: str, ids: list[str]) -> None:
    from sqlalchemy import delete

    from provisa.core.schema_org import code_source_vectors as t

    async with model_db.acquire() as conn:
        await conn.execute_core(
            delete(t).where((t.c.source_id == source_id) & t.c.item_id.in_(ids))
        )


# -- http ---------------------------------------------------------------------------------------


def _ca_bundle() -> str | bool:
    """The deployment's CA bundle for an outbound fetch: REQUESTS_CA_BUNDLE, else SSL_CERT_FILE,
    else the system's."""
    return os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE") or True


async def http(grant: ServiceGrant, method: str, body: bytes, span: Any) -> bytes:
    import httpx

    from provisa.codesource.egress import admitted
    from provisa.codesource.llm_service import org_bound
    from provisa.core.org_membership import record_admin_action

    _method(method, "POST")
    request = _json(body)
    verb = str(request.get("method") or "GET").upper()
    if verb not in ("GET", "POST"):
        raise ServiceRefused(400, f"http fetches with GET or POST, not {verb}")
    url = request.get("url")
    if not isinstance(url, str):
        raise ServiceRefused(400, "url is an absolute http:// or https:// address")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ServiceRefused(400, "url is an absolute http:// or https:// address")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    span.set_attribute("codesource.http.host", parts.hostname)
    if not admitted(grant.egress, parts.hostname, port):
        raise ServiceRefused(
            403, f"egress to {parts.hostname}:{port} is not on the egress allow-list"
        )
    headers = request.get("headers") or {}
    if not isinstance(headers, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in headers.items()
    ):
        raise ServiceRefused(400, "headers is a mapping of name to text")
    content = (
        base64.b64decode(request["body_base64"])
        if isinstance(request.get("body_base64"), str)
        else (request.get("body") or "").encode()
    )
    timeout = request.get("timeout") or HTTP_TIMEOUT_S
    try:
        async with httpx.AsyncClient(
            trust_env=True, verify=_ca_bundle(), timeout=float(timeout)
        ) as client:
            resp = await client.request(verb, url, headers=headers, content=content or None)
    except httpx.HTTPError as exc:
        raise ServiceRefused(
            502, f"the fetch of {parts.hostname} failed: {type(exc).__name__}"
        ) from None
    span.set_attribute("codesource.http.status", resp.status_code)
    async with org_bound(grant) as (model_db, _cfg):
        await record_admin_action(
            model_db,
            action=HTTP_CALL,
            actor_id=f"source:{grant.source_id}",
            subject_id=parts.hostname,
            detail={"source": grant.source_id, "method": verb, "status": resp.status_code},
        )
    raw = resp.content
    if len(raw) > MAX_HTTP_BODY:
        raise ServiceRefused(
            413, f"the answer from {parts.hostname} is larger than {MAX_HTTP_BODY} bytes"
        )
    answered: dict[str, Any] = {"status": resp.status_code, "headers": dict(resp.headers)}
    try:
        answered["body"] = raw.decode("utf-8")
    except UnicodeDecodeError:
        answered["body_base64"] = base64.b64encode(raw).decode()
    return _answer(answered)


# -- log and metric -----------------------------------------------------------------------------


def log_line(grant: ServiceGrant, method: str, body: bytes, span: Any) -> bytes:
    from provisa.otel_compat import request_event

    _method(method, "POST")
    request = _json(body)
    level = str(request.get("level") or "info").lower()
    if level not in _LEVELS:
        raise ServiceRefused(400, f"a log level is one of {', '.join(_LEVELS)}, not {level!r}")
    message = request.get("message")
    if not isinstance(message, str):
        raise ServiceRefused(400, "a log line has a message, a string")
    fields = request.get("fields") or {}
    if not isinstance(fields, dict):
        raise ServiceRefused(400, "a log line's fields are a mapping")
    code_log.log(
        _LEVELS[level],
        "%s: %s",
        grant.source_id,
        message,
        extra={"codesource_source": grant.source_id, "codesource_fields": fields},
    )
    request_event("codesource.log", source=grant.source_id, level=level, message=message)
    span.set_attribute("codesource.log_level", level)
    return _answer({})


_instruments: dict[tuple[str, str], Any] = {}
_instruments_lock = threading.Lock()


def _instrument(kind: str, name: str) -> Any:
    from provisa.otel_compat import get_meter

    with _instruments_lock:
        held = _instruments.get((kind, name))
        if held is None:
            meter = get_meter("provisa.codesource")
            full = f"code_source.{name}"
            held = _instruments[(kind, name)] = {
                "counter": meter.create_counter,
                "gauge": meter.create_gauge,
                "histogram": meter.create_histogram,
            }[kind](full)
        return held


def metric(grant: ServiceGrant, method: str, body: bytes, span: Any) -> bytes:
    from provisa.otel_compat import ORG_ATTRIBUTE

    _method(method, "POST")
    request = _json(body)
    name, value = request.get("name"), request.get("value")
    kind = request.get("kind") or "counter"
    if not isinstance(name, str) or not _METRIC_NAME.match(name):
        raise ServiceRefused(400, "a metric name is lower-case letters, digits, _ and ., up to 64")
    if kind not in _METRIC_KINDS:
        raise ServiceRefused(400, f"a metric is a {', '.join(_METRIC_KINDS)}, not {kind!r}")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ServiceRefused(400, f"the metric {name!r}'s value is a number")
    attributes = request.get("attributes") or {}
    if not isinstance(attributes, dict) or not all(
        isinstance(v, (str, int, float, bool)) for v in attributes.values()
    ):
        raise ServiceRefused(400, "a metric's attributes are a mapping of name to a plain value")
    point = {
        **attributes,
        "provisa.codesource.source": grant.source_id,
        ORG_ATTRIBUTE: grant.org_id,
    }
    instrument = _instrument(kind, name)
    if kind == "counter":
        if value < 0:
            raise ServiceRefused(400, f"the counter {name!r} only counts up")
        instrument.add(value, point)
    elif kind == "gauge":
        instrument.set(value, point)
    else:
        instrument.record(value, point)
    span.set_attribute("codesource.metric", name)
    return _answer({})
