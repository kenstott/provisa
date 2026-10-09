# Copyright (c) 2026 Kenneth Stott
# Canary: 7c18321c-cb63-45c8-9bcd-6e28059efd01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading Microsoft Graph: one GET, a collection followed page by page, and a batch of GETs.

Graph answers a collection as ``{"value": [...], "@odata.nextLink": "<address of the next
page>"}``; the next page is that address used as it is. A refusal carries
``{"error": {"code", "message"}}``. A throttled call (429, or 503 naming a wait) carries
``Retry-After``; it is waited out as asked while the waits of one call stay within
``wait_seconds``, and past that the read fails by name -- never a shorter answer.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

BASE_URL = "https://graph.microsoft.com/v1.0"
#: Ids that stay the same when an item moves to another folder.
IMMUTABLE_IDS = 'IdType="ImmutableId"'
#: Graph takes at most this many requests in one batch.
BATCH_SIZE = 20
#: How long the waits a throttled call is asked for may add up to before the read fails.
WAIT_SECONDS = 60.0
_TIMEOUT = 30.0


class GraphRefused(RuntimeError):
    """Graph refused a call. ``code`` is Graph's own (``ErrorItemNotFound``, ``MailboxNotEnabledForRESTAPI``)."""

    def __init__(self, status: int, code: str, message: str, path: str) -> None:
        super().__init__(f"Microsoft Graph refused {path}: {status} {code}: {message}")
        self.status = status
        self.code = code


class GraphThrottled(RuntimeError):
    """Graph asked the caller to wait for longer than a read waits."""

    def __init__(self, path: str, asked: str, waited: float, bound: float) -> None:
        super().__init__(
            f"Microsoft Graph is throttling {path} (Retry-After: {asked}); the read waited "
            f"{waited:g}s of the {bound:g}s it may and was still refused"
        )
        self.retry_after = asked


@dataclass(frozen=True)
class Answer:
    status: int
    headers: Mapping[str, str]
    body: Any  # the parsed JSON, or None for an empty answer


#: Sends one request: (method, address, query parameters, headers, JSON body) -> Answer.
Send = Callable[[str, str, Mapping[str, str] | None, Mapping[str, str], Any], Answer]


def http_send(client: httpx.Client) -> Send:
    def send(method, url, params, headers, body) -> Answer:
        reply = client.request(
            method, url, params=params, headers=dict(headers), json=body, timeout=_TIMEOUT
        )
        return Answer(reply.status_code, reply.headers, reply.json() if reply.content else None)

    return send


def _path(url: str) -> str:
    """The address without its query, which may hold a paging token."""
    return url.split("?", 1)[0].removeprefix(BASE_URL)


class Graph:
    """Graph as one credential reads it. ``token`` gives the access token for each call."""

    def __init__(
        self,
        send: Send,
        token: Callable[[], str],
        *,
        wait_seconds: float = WAIT_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._send = send
        self._token = token
        self._wait_seconds = wait_seconds
        self._sleep = sleep

    def _call(
        self,
        method: str,
        url: str,
        params: Mapping[str, str] | None,
        prefer: tuple[str, ...],
        body: Any = None,
    ) -> Any:
        waited = 0.0
        while True:
            headers = {"Authorization": f"Bearer {self._token()}"}
            if prefer:
                headers["Prefer"] = ", ".join(prefer)
            answer = self._send(method, url, params, headers, body)
            asked = answer.headers.get("Retry-After")
            if answer.status == 429 or (answer.status == 503 and asked is not None):
                if asked is None or not asked.strip().isdigit():
                    raise GraphThrottled(_path(url), str(asked), waited, self._wait_seconds)
                wait = float(asked)
                if waited + wait > self._wait_seconds:
                    raise GraphThrottled(_path(url), asked, waited, self._wait_seconds)
                self._sleep(wait)
                waited += wait
                continue
            if answer.status >= 400:
                error = (answer.body or {}).get("error") or {}
                raise GraphRefused(
                    answer.status,
                    str(error.get("code")),
                    str(error.get("message")),
                    _path(url),
                )
            return answer.body

    def get(self, path: str, params: Mapping[str, str] | None = None, *, prefer=()) -> dict:
        return self._call("GET", BASE_URL + path, params, tuple(prefer))

    def find(self, path: str, params: Mapping[str, str] | None = None) -> dict | None:
        """One item, or None where Graph says there is no such item."""
        try:
            return self.get(path, params)
        except GraphRefused as refused:
            if refused.status == 404 and refused.code == "ErrorItemNotFound":
                return None
            raise

    def pages(
        self, path: str, params: Mapping[str, str] | None = None, *, prefer=()
    ) -> Iterator[list[dict]]:
        """A collection, one page of items at a time, to its end."""
        url, query = BASE_URL + path, params
        while url is not None:
            page = self._call("GET", url, query, tuple(prefer))
            yield page["value"]
            # The next page is the address Graph gave, whole: it carries the query already.
            url, query = page.get("@odata.nextLink"), None

    def batch(self, paths: list[str], *, prefer=()) -> list[dict]:
        """The answers to GETs of ``paths``, in their order. A refusal of any one fails all."""
        answers = self.batch_each(paths, prefer=prefer)
        for answer in answers:
            if isinstance(answer, GraphRefused):
                raise answer
        return answers  # type: ignore[return-value]  # no refusal is left in it

    def batch_each(self, paths: list[str], *, prefer=()) -> "list[dict | GraphRefused]":
        """The answer to a GET of each of ``paths``, in their order: its body, or the refusal
        Graph gave that one. A throttled request is asked again; throttling past the bound
        fails the whole call."""
        answers: list[dict | GraphRefused] = []
        for start in range(0, len(paths), BATCH_SIZE):
            chunk = paths[start : start + BATCH_SIZE]
            headers = {"Prefer": ", ".join(prefer)} if prefer else {}
            requests = [
                {"id": str(n), "method": "GET", "url": path, "headers": headers}
                if headers
                else {"id": str(n), "method": "GET", "url": path}
                for n, path in enumerate(chunk)
            ]
            pending = {r["id"]: r for r in requests}
            got: dict[str, dict | GraphRefused] = {}
            waited = 0.0
            while pending:
                reply = self._call(
                    "POST", BASE_URL + "/$batch", None, (), {"requests": list(pending.values())}
                )
                wait = 0.0
                for item in reply["responses"]:
                    path = pending[item["id"]]["url"]
                    if item["status"] == 429:
                        asked = (item.get("headers") or {}).get("Retry-After")
                        if asked is None or not str(asked).strip().isdigit():
                            raise GraphThrottled(path, str(asked), waited, self._wait_seconds)
                        wait = max(wait, float(asked))
                        continue
                    if item["status"] >= 400:
                        error = (item.get("body") or {}).get("error") or {}
                        got[item["id"]] = GraphRefused(
                            item["status"],
                            str(error.get("code")),
                            str(error.get("message")),
                            path.split("?", 1)[0],
                        )
                    else:
                        got[item["id"]] = item["body"]
                    del pending[item["id"]]
                if pending:
                    if waited + wait > self._wait_seconds:
                        first = next(iter(pending.values()))["url"]
                        raise GraphThrottled(
                            first.split("?", 1)[0], f"{wait:g}", waited, self._wait_seconds
                        )
                    self._sleep(wait)
                    waited += wait
            answers += [got[str(n)] for n in range(len(chunk))]
        return answers
