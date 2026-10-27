# Copyright (c) 2026 Kenneth Stott
# Canary: 0a41bf54-ea0d-491c-abb1-5c3ed5746d9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL over HTTP: the shape of a request to /data/graphql and of its answer (REQ-2167).

What the GraphQL-over-HTTP specification asks of the transport, and the graphql-http audit checks:

* the request's parameters -- ``query``, ``operationName``, ``variables``, ``extensions`` -- read
  from a POST's JSON body or a GET's URL, each refused with 400 when it is of the wrong JSON type;
  a body that is missing, not JSON or not an object is refused the same way;
* the answer's media type: application/graphql-response+json when Accept lists it before
  application/json, else application/json, which stays the answer to every client that does not
  ask (no Accept, ``*/*``);
* a document that does not parse or validate, or whose variables do not coerce, answered with its
  errors and no data: 200 under application/json, 400 under application/graphql-response+json --
  and 400 as before to a client whose Accept asks for a file format (CSV, Arrow, ...), which reads
  any answer that is not an error as the file.
"""

# Requirements: REQ-2167

from __future__ import annotations

import functools
import json
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request
from fastapi.responses import Response

from provisa.observability.request_facts import TimedJSONResponse as JSONResponse  # REQ-1910

from provisa.api.errors import ApiError

GRAPHQL_RESPONSE = "application/graphql-response+json"

#: Each parameter of a GraphQL request, and the JSON type it must have when present (null is absent).
_PARAMETER_TYPES: dict[str, tuple[type, str]] = {
    "query": (str, "a string"),
    "operationName": (str, "a string"),
    "variables": (dict, "an object"),
    "extensions": (dict, "an object"),
    "role": (str, "a string"),
}


class GraphQLDocumentError(Exception):
    """The request's document did not parse or validate, or its variables did not coerce: a
    GraphQL error, answered as the response's media type asks (:func:`document_error_response`)."""

    def __init__(self, messages: list[str]) -> None:
        super().__init__("; ".join(messages))
        self.messages = messages


def wants_graphql_response(accept: str | None) -> bool:
    """Whether the answer is application/graphql-response+json: Accept lists it, and before
    application/json when it lists both."""
    if not accept:
        return False
    listed = [part.split(";", 1)[0].strip().lower() for part in accept.split(",")]
    if GRAPHQL_RESPONSE not in listed:
        return False
    return "application/json" not in listed or listed.index(GRAPHQL_RESPONSE) < listed.index(
        "application/json"
    )


def graphql_parameters(payload: Any) -> dict[str, Any]:
    """The request's parameters from a decoded body or URL, a null one left out; 400 for a body
    that is not an object or a parameter of the wrong JSON type."""
    if not isinstance(payload, dict):
        raise ApiError(
            400,
            "data.graphql_request_not_object",
            "A GraphQL request is a JSON object with a query.",
        )
    params: dict[str, Any] = {}
    for name, (kind, described) in _PARAMETER_TYPES.items():
        value = payload.get(name)
        if value is None:
            continue
        # bool is an int in Python, and neither is a string or a mapping here.
        if not isinstance(value, kind):
            raise ApiError(
                400,
                "data.graphql_parameter_type",
                f"The GraphQL request's {name} must be {described}.",
                parameter=name,
                expected=described,
            )
        params[name] = value
    return params


async def post_parameters(raw_request: Request) -> dict[str, Any]:
    """A POST's parameters, from its JSON body."""
    body = await raw_request.body()
    if not body.strip():
        raise ApiError(
            400,
            "data.graphql_body_missing",
            "The request has no body: a GraphQL request is a JSON object with a query.",
        )
    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise ApiError(
            400,
            "data.graphql_body_not_json",
            f"The request body is not JSON: {exc}",
            error=str(exc),
        ) from exc
    return graphql_parameters(payload)


def get_parameters(raw_request: Request) -> dict[str, Any]:
    """A GET's parameters, URL-encoded: ``variables`` and ``extensions`` each a JSON string."""
    query = raw_request.query_params
    payload: dict[str, Any] = {
        name: query[name] for name in ("query", "operationName", "role") if name in query
    }
    for name in ("variables", "extensions"):
        if name not in query:
            continue
        try:
            payload[name] = json.loads(query[name])
        except ValueError as exc:
            raise ApiError(
                400,
                "data.graphql_parameter_type",
                f"The GraphQL request's {name} must be an object.",
                parameter=name,
                expected="an object",
            ) from exc
    return graphql_parameters(payload)


def document_error_response(exc: GraphQLDocumentError, accept: str | None) -> JSONResponse:
    """The answer to a document that did not parse, validate or coerce: its errors, no data."""
    from provisa.api.data.endpoint_helpers import _parse_accept

    body = {"errors": [{"message": m} for m in exc.messages]}
    if wants_graphql_response(accept):
        return JSONResponse(body, status_code=400, media_type=GRAPHQL_RESPONSE)
    if _parse_accept(accept) != "json":
        return JSONResponse(body, status_code=400)
    return JSONResponse(body, status_code=200)


def as_graphql_response(response: Any) -> Any:
    """``response`` under application/graphql-response+json when it is a JSON answer; any other
    (a file format Accept also asked for, an event stream) as it is."""
    if isinstance(response, (dict, list)):
        return JSONResponse(response, media_type=GRAPHQL_RESPONSE)
    if isinstance(response, Response) and (response.media_type or "") == "application/json":
        response.media_type = GRAPHQL_RESPONSE
        response.headers["content-type"] = f"{GRAPHQL_RESPONSE}; charset=utf-8"
    return response


def negotiated(handler: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """``handler`` answering in the media type its ``accept`` asks for, and a document error as
    that media type answers one. Its signature is kept, so FastAPI reads its parameters."""

    @functools.wraps(handler)
    async def answer(*args: Any, **kwargs: Any) -> Any:
        accept = kwargs.get("accept")
        try:
            response = await handler(*args, **kwargs)
        except GraphQLDocumentError as exc:
            return document_error_response(exc, accept)
        return as_graphql_response(response) if wants_graphql_response(accept) else response

    return answer
