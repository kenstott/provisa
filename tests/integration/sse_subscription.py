# Copyright (c) 2026 Kenneth Stott
# Canary: 85d7f82a-8d2a-4abb-ba8b-2486668525ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An SSE subscription a test holds open, as a client holds one (REQ-369)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

import httpx


@contextmanager
def open_subscription(
    client: httpx.Client, url: str, headers: dict[str, str]
) -> Generator[httpx.Response]:
    """The subscription's response, open until the block exits.

    An admitted stream's first line, ``: connected``, is read before the block runs; a refused
    one is read whole. The body is never read past that first line, so an admitted stream
    answers at once rather than streaming forever.

    The line iterator is held for the life of the block: a throwaway ``next(r.iter_lines())``
    is collected as soon as it returns, and closing it closes the connection, so the server
    sees the subscription end while the test still believes it holds it.
    """
    with client.stream("GET", url, headers=headers) as response:
        if response.status_code == 200:
            lines = response.iter_lines()
            assert next(lines) == ": connected"
        else:
            response.read()
            lines = None
        yield response
        del lines
