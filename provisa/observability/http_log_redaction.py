# Copyright (c) 2026 Kenneth Stott
# Canary: dc3e0b65-bca7-42f8-9795-56454b15b1e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The HTTP client's request log carries no query string.

httpx logs every request at INFO as ``HTTP Request: GET <url> ...``. A source that takes its
credential in the query (``?api_key=...``) would have it written to every log the record reaches.
The filter on the ``httpx`` logger drops the query (and fragment) from each URL in the record,
keeping scheme, host and path. httpcore logs no URL (its trace lines show ``<Request [b'GET']>``
and host/port), so it needs none. Importing :mod:`provisa` installs it, so every Provisa process
(server, workers, CLI) has it before its first request, whatever level is configured."""

from __future__ import annotations

import logging
import re

# A URL's query and fragment: from '?' or '#' to the next whitespace or quote.
_URL_QUERY = re.compile(r"""(\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s?#"'<>]*)[?#][^\s"'<>]*""")


def strip_url_queries(text: str) -> str:
    """``text`` with the query string and fragment removed from every URL in it."""
    return _URL_QUERY.sub(r"\1", text)


class UrlQueryFilter(logging.Filter):
    """Rewrites a record's message so no URL in it carries a query string."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        stripped = strip_url_queries(message)
        if stripped != message:
            record.msg, record.args = stripped, None
        return True


URL_QUERY_FILTER = UrlQueryFilter()

_LOGGERS = ("httpx",)


def install() -> None:
    """Put the filter on the HTTP client's logger. Idempotent."""
    for name in _LOGGERS:
        logger = logging.getLogger(name)
        if URL_QUERY_FILTER not in logger.filters:
            logger.addFilter(URL_QUERY_FILTER)
