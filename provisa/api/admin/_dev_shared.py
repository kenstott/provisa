# Copyright (c) 2026 Kenneth Stott
# Canary: 7a8b9c0d-1e2f-3a4b-5c6d-7e8f9a0b1c2d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared helpers used by dev_queries.py and endpoint_dev.py."""

from __future__ import annotations

import re
from typing import Literal


def detect_target(query: str) -> Literal["graphql", "sql", "cypher"]:
    stripped = query.strip()
    first = stripped.split()[0].lower() if stripped.split() else ""
    if first in ("query", "mutation", "subscription", "fragment") or stripped.startswith("{"):
        return "graphql"
    if first in ("match", "optional", "call") or re.search(r"\([\w]*:", stripped):
        return "cypher"
    return "sql"


def extract_operation_name(query_text: str) -> str | None:
    from graphql import parse as gql_parse
    from graphql.language.ast import OperationDefinitionNode
    try:
        doc = gql_parse(query_text)
        for defn in doc.definitions:
            if isinstance(defn, OperationDefinitionNode) and defn.name:
                return defn.name.value
    except Exception:
        pass
    return None
