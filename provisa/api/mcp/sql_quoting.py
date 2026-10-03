# Copyright (c) 2026 Kenneth Stott
# Canary: e4022077-7f72-4930-9883-c00aa17d961e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Render SQL with every identifier quoted, as the SQL surfaces read it.

Used on the SQL Polly hands on (provisa/api/mcp/chat.py): a model writes reserved words as
column names (``order``, ``user``) unquoted, and the query then fails. Unquoted names are folded to
lower case first, as Postgres folds them, so quoting changes no name's meaning.
"""

from __future__ import annotations

import sqlglot
from sqlglot.errors import ParseError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

_DIALECT = "postgres"


def quote_identifiers(sql: str) -> tuple[str, str | None]:
    """``sql`` with every identifier quoted, and None; or ``sql`` unchanged and the parse error.

    SQL that does not parse is never altered: the caller hands it on as written and says why.
    """
    try:
        statements = [s for s in sqlglot.parse(sql, read=_DIALECT) if s is not None]
    except ParseError as exc:
        return sql, str(exc)
    return (
        ";\n".join(
            normalize_identifiers(s, dialect=_DIALECT).sql(dialect=_DIALECT, identify=True)
            for s in statements
        ),
        None,
    )
