# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8d2e6f-1a93-4c7e-85b0-d3f9a6c2e417
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's services to a code source's code (REQ-2104, REQ-2105, REQ-2106).

Always present in a Python code source's worker: the host puts this module on the path. Outside
Provisa it is not there, and code that imports it is tested by mocking it.

    import provisa_services as ps

    key = ps.secrets.get("openrouter")
    region = ps.settings.get("region")
    ps.cache.set("rates", rates, ttl=3600); ps.cache.get("rates")
    ps.vectors.upsert([{"id": "a1", "text": "..."}], model="text-embedding-3-small")
    ps.vectors.search("late payment", k=5, model="text-embedding-3-small")
    ps.http.get("https://api.example.com/rates").json()
    ps.llm.chat("provisa:sql_generation", [{"role": "user", "content": "..."}])
    ps.log("scored", level="info", accounts=12); ps.metric("scored", 12)

Each call is made to ``$PROVISA_SERVICES`` through the worker's egress proxy, naming the call it
serves, so what it does is recorded against the request that called the code. A refusal is raised
as :class:`ServiceError`, naming the service; with no ``$PROVISA_SERVICES`` every call raises it.
The same calls, by the same names, are Node's ``@provisa/services`` (provisa/node/services.mjs).
"""

# Requirements: REQ-2104, REQ-2105, REQ-2106
from __future__ import annotations

import base64
import contextvars
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

__all__ = [
    "ServiceError",
    "cache",
    "dq",
    "http",
    "llm",
    "log",
    "metric",
    "secrets",
    "settings",
    "vectors",
]

#: The call this code is serving: its id, and the request's traceparent (set by the host).
_call: contextvars.ContextVar[tuple[int, str | None] | None] = contextvars.ContextVar(
    "provisa_services_call", default=None
)
_MISSING = object()

#: REQ-1165: where the calls go when this module runs inside Provisa itself (a view's preflight) rather
#: than in a worker: ``(method, path, body, headers) -> (status, body)``, set by the server for the
#: call it serves. Unset, the calls go to $PROVISA_SERVICES.
_transport: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "provisa_services_transport", default=None
)


class ServiceError(Exception):
    """A service refused or failed: ``service`` names it, ``status`` is its HTTP status (0 when
    the services are not reachable at all), and the message says why."""

    def __init__(self, service: str, status: int, message: str) -> None:
        super().__init__(f"{service}: {message}")
        self.service = service
        self.status = status


def _bind(call_id: int, traceparent: str | None) -> None:
    """The host's: the call this context serves."""
    _call.set((call_id, traceparent))


def _base(service: str) -> str:
    base = os.environ.get("PROVISA_SERVICES")
    if not base:
        raise ServiceError(
            service,
            0,
            "Provisa's services are reached only from a Provisa code source's worker "
            "($PROVISA_SERVICES is not set)",
        )
    if not base.startswith("http://"):
        raise ServiceError(service, 0, f"$PROVISA_SERVICES is not an http:// address: {base!r}")
    return base


def _send(service: str, method: str, path: str, payload: Any = None) -> tuple[int, bytes]:
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    held = _call.get()
    if held is not None:
        headers["X-Provisa-Call"] = str(held[0])
        if held[1]:
            headers["traceparent"] = held[1]
    data = None if payload is None else json.dumps(payload).encode()
    inside = _transport.get()
    if inside is not None:
        return inside(method, path, data, headers)
    request = urllib.request.Request(
        f"{_base(service)}/{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        # nosec B310 - the URL is $PROVISA_SERVICES's, checked to be http:// in _base
        with urllib.request.urlopen(request, timeout=300) as answered:  # nosec B310
            return answered.status, answered.read()
    except urllib.error.HTTPError as refused:
        return refused.code, refused.read()


def _json(service: str, method: str, path: str, payload: Any = None) -> Any:
    status, body = _send(service, method, path, payload)
    if status != 200:
        raise ServiceError(service, status, _reason(body))
    return json.loads(body)


def _reason(body: bytes) -> str:
    try:
        return json.loads(body)["error"]["message"]
    except (ValueError, KeyError, TypeError):
        return body.decode("utf-8", "replace")


def _q(name: str) -> str:
    return urllib.parse.quote(name, safe="")


class secrets:  # noqa: N801 -- a namespace, named as the service
    @staticmethod
    def get(name: str) -> str:
        """The declared secret ``name``, read from the org's vault now."""
        status, body = _send("secrets", "GET", f"secrets/{_q(name)}")
        if status != 200:
            raise ServiceError("secrets", status, body.decode("utf-8", "replace"))
        return body.decode()


class settings:  # noqa: N801
    @staticmethod
    def get(name: str, default: Any = _MISSING) -> Any:
        """The source's setting ``name``; ``default`` when it declares none (else raised)."""
        status, body = _send("settings", "GET", f"settings/{_q(name)}")
        if status == 404 and default is not _MISSING:
            return default
        if status != 200:
            raise ServiceError("settings", status, _reason(body))
        return json.loads(body)["value"]

    @staticmethod
    def all() -> dict:
        return _json("settings", "GET", "settings")


class cache:  # noqa: N801
    @staticmethod
    def get(key: str, default: Any = None) -> Any:
        """The cached value of ``key``, or ``default`` when none is cached."""
        status, body = _send("cache", "GET", f"cache/{_q(key)}")
        if status == 404:
            return default
        if status != 200:
            raise ServiceError("cache", status, _reason(body))
        return json.loads(body)["value"]

    @staticmethod
    def set(key: str, value: Any, ttl: float | None = None) -> None:
        """Cache ``value`` (JSON) under ``key`` for every worker of this source, for ``ttl``
        seconds (None: until deleted or replaced)."""
        _json("cache", "PUT", f"cache/{_q(key)}", {"value": value, "ttl": ttl})

    @staticmethod
    def delete(key: str) -> None:
        _json("cache", "DELETE", f"cache/{_q(key)}")


class vectors:  # noqa: N801
    """The source's own vector collection."""

    @staticmethod
    def embed(texts: list[str], model: str) -> list[list[float]]:
        return _json("vectors", "POST", "vectors/embed", {"texts": texts, "model": model})[
            "vectors"
        ]

    @staticmethod
    def upsert(items: list[dict], model: str | None = None) -> int:
        """Add or replace ``items`` -- each ``{id, text | vector, metadata}``; a text is embedded
        by ``model``, one of the org's vector models."""
        return _json("vectors", "POST", "vectors/upsert", {"items": items, "model": model})[
            "upserted"
        ]

    @staticmethod
    def search(query: str | list[float], k: int = 5, model: str | None = None) -> list[dict]:
        """The ``k`` items nearest ``query`` (a text, embedded by ``model``, or a vector):
        ``{id, score, text, metadata}``, nearest first."""
        return _json("vectors", "POST", "vectors/search", {"query": query, "k": k, "model": model})[
            "matches"
        ]

    @staticmethod
    def delete(ids: list[str]) -> None:
        _json("vectors", "POST", "vectors/delete", {"ids": ids})


class Response:
    """What a fetch through Provisa answered."""

    def __init__(self, answered: dict) -> None:
        self.status: int = answered["status"]
        self.headers: dict = answered["headers"]
        self.content: bytes = (
            base64.b64decode(answered["body_base64"])
            if "body_base64" in answered
            else answered["body"].encode()
        )

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", "replace")

    def json(self) -> Any:
        return json.loads(self.content)


class http:  # noqa: N801
    """A fetch Provisa makes for the code: to a host on the egress allow-list, recorded."""

    @staticmethod
    def get(url: str, headers: dict | None = None, timeout: float | None = None) -> Response:
        return Response(
            _json(
                "http",
                "POST",
                "http",
                {"method": "GET", "url": url, "headers": headers or {}, "timeout": timeout},
            )
        )

    @staticmethod
    def post(
        url: str,
        body: str | bytes | None = None,
        json_body: Any = None,
        headers: dict | None = None,
        timeout: float | None = None,
    ) -> Response:
        sent = dict(headers or {})
        request: dict[str, Any] = {"method": "POST", "url": url, "timeout": timeout}
        if json_body is not None:
            sent.setdefault("Content-Type", "application/json")
            request["body"] = json.dumps(json_body)
        elif isinstance(body, bytes):
            request["body_base64"] = base64.b64encode(body).decode()
        elif body is not None:
            request["body"] = body
        request["headers"] = sent
        return Response(_json("http", "POST", "http", request))


class llm:  # noqa: N801
    """Provisa's OpenAI-compatible LLM service; any OpenAI SDK also works at :meth:`base_url`."""

    @staticmethod
    def base_url() -> str:
        return f"{_base('llm')}/llm/v1"

    @staticmethod
    def chat(model: str, messages: list[dict], **params: Any) -> dict:
        """A chat completion: ``model`` is ``provisa:<role>`` or a model the org assigns."""
        return _json(
            "llm",
            "POST",
            "llm/v1/chat/completions",
            {"model": model, "messages": messages, **params},
        )

    @staticmethod
    def embeddings(model: str, input: str | list[str]) -> dict:  # noqa: A002 -- the API's name
        return _json("llm", "POST", "llm/v1/embeddings", {"model": model, "input": input})


class dq:  # noqa: N801
    """Everything about the data quality of a dataset or a data product, as the meta domain shows it
    to the caller (REQ-2133)."""

    @staticmethod
    def dataset(name: str | int) -> dict:
        """A dataset's checker tables -- each one's description, contract, latest run (its outcome,
        when it ran, its failing checks) and the product it is assigned to -- and its profiles,
        with each one's latest run. ``name`` is the dataset's ``domain.table``, or its table id."""
        return _json("dq", "GET", f"dq/dataset/{_q(str(name))}")

    @staticmethod
    def product(product_id: str) -> dict:
        """A data product's promises, expectations, partner DQ and orphans, each with its latest
        run (REQ-1443)."""
        return _json("dq", "GET", f"dq/product/{_q(product_id)}")


def log(message: str, level: str = "info", **fields: Any) -> None:
    """A log line, recorded against the request this call serves."""
    _json("log", "POST", "log", {"level": level, "message": message, "fields": fields})


def metric(name: str, value: float, kind: str = "counter", **attributes: Any) -> None:
    """A metric point (``counter``, ``gauge`` or ``histogram``), named ``code_source.<name>``."""
    _json(
        "metric",
        "POST",
        "metric",
        {"name": name, "value": value, "kind": kind, "attributes": attributes},
    )
