# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1d9b42-7c3a-4f16-8d20-b6a4c9e07f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's own error from a catalog macro (REQ-2167, CLIENT-CALLED SERVER FUNCTIONS ANSWER
LIKE POSTGRESQL): a macro raises it with ``raising``, in DuckDB's error(), whose message carries
the SQLSTATE; ``from_engine`` reads that back as the error PostgreSQL reports."""

# Requirements: REQ-2167

from __future__ import annotations

import re

_MARK = "postgresql-error"
_MARKED = re.compile(rf"{_MARK} (?P<sqlstate>[0-9A-Z]{{5}}): (?P<message>.*)", re.DOTALL)


class PostgresError(Exception):
    """An error with the SQLSTATE PostgreSQL reports it with."""

    def __init__(self, sqlstate: str, message: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def raising(sqlstate: str, message_sql: str) -> str:
    """SQL raising PostgreSQL's error ``sqlstate`` with the text of ``message_sql`` (an SQL
    expression), usable where the engine expects a value of any type."""
    return f"error('{_MARK} {sqlstate}: ' || {message_sql})"


def from_engine(error: Exception) -> PostgresError | None:
    """The PostgreSQL error a ``raising`` call raised; None for any other engine error."""
    marked = _MARKED.search(str(error))
    return None if marked is None else PostgresError(marked["sqlstate"], marked["message"])
