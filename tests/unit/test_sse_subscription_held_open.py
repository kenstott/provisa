# Copyright (c) 2026 Kenneth Stott
# Canary: a6bf7613-182c-4d5b-84f0-9cf5885d08a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A held SSE subscription stays open until its block exits (REQ-369).

The role-cap e2e holds one subscription while it asks for a second. Read past its first line
by a throwaway line iterator, the first stream was closed on the spot: the server saw the
disconnect, released the slot, admitted the second, and the test read that endless stream
until pytest-timeout ended it at 900 s."""

from __future__ import annotations

import httpx

from tests.integration.sse_subscription import open_subscription


class _Stream(httpx.SyncByteStream):
    """An endless SSE body that records when the client lets go of it."""

    def __init__(self) -> None:
        self.closed = False

    def __iter__(self):
        try:
            yield b": connected\n\n"
            while True:
                yield b": keepalive\n\n"
        finally:
            self.closed = True


def _client(stream: _Stream, status: int = 200) -> httpx.Client:
    def handler(_request: httpx.Request) -> httpx.Response:
        if status != 200:
            return httpx.Response(status, json={"detail": "refused"})
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_the_stream_is_open_after_its_first_line_and_closed_when_the_block_exits():
    stream = _Stream()
    with _client(stream) as client:
        with open_subscription(client, "http://t/data/subscribe/orders", {}) as first:
            assert first.status_code == 200
            assert not stream.closed, "the subscription was closed while it was held"
        assert stream.closed


def test_a_refused_subscription_answers_at_once_with_its_body():
    with _client(_Stream(), status=429) as client:
        with open_subscription(client, "http://t/data/subscribe/orders", {}) as refused:
            assert refused.status_code == 429
            assert refused.json() == {"detail": "refused"}
