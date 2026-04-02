# Copyright (c) 2025 Kenneth Stott
# Canary: f7594a56-e994-4966-b158-a12d10439b55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Approved query ceiling enforcement (REQ-005).

Clients may restrict within the approved query (fewer columns, additional filters)
but cannot exceed the approved ceiling (no new columns, no removed filters).
"""

from __future__ import annotations

from graphql import DocumentNode, FieldNode, parse


class CeilingViolationError(Exception):
    """Raised when a client query exceeds the approved ceiling."""

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(f"Query exceeds approved ceiling: {detail}")


def _extract_field_names(document: DocumentNode) -> set[str]:
    """Extract all leaf field names from a GraphQL document."""
    names: set[str] = set()
    for defn in document.definitions:
        if hasattr(defn, "selection_set") and defn.selection_set:
            _collect_fields(defn.selection_set, names)
    return names


def _collect_fields(selection_set, names: set[str]) -> None:
    for sel in selection_set.selections:
        if isinstance(sel, FieldNode):
            names.add(sel.name.value)
            if sel.selection_set:
                _collect_fields(sel.selection_set, names)


def check_ceiling(
    approved_query_text: str,
    client_query_text: str,
) -> None:
    """Verify that the client query doesn't exceed the approved query's ceiling.

    The client may:
    - Select fewer columns than approved
    - Add additional WHERE filters
    - Reduce limit

    The client may NOT:
    - Select columns not in the approved query
    - Remove filters present in the approved query (enforced at RLS level, not here)

    Raises CeilingViolationError if exceeded.
    """
    approved_doc = parse(approved_query_text)
    client_doc = parse(client_query_text)

    approved_fields = _extract_field_names(approved_doc)
    client_fields = _extract_field_names(client_doc)

    extra_fields = client_fields - approved_fields
    if extra_fields:
        raise CeilingViolationError(
            f"Fields not in approved query: {', '.join(sorted(extra_fields))}"
        )
