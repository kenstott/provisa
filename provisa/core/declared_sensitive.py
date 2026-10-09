# Copyright (c) 2026 Kenneth Stott
# Canary: 99b00d8c-8026-4de7-8b63-66ca6b9a837e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Columns a source kind declares sensitive (REQ-1943).

Some tables hold personal data by what they are: a mailbox's messages, whoever registers them.
A source kind that produces such tables declares which of their columns are sensitive, and when
one of those tables is first registered the system itself, not the registrar, hides them:

- a ``HIDDEN`` column (what a message says) is served to no role: scope ``restricted`` with no
  grant (REQ-1959);
- a ``MASKED`` column (who it is from and to) is served as NULL to every role;
- both carry the ``pii`` tag, so from then on their grants, masks and the tag itself are under
  the ``sensitive_data`` right like any sensitive column's.

The declaration is the only input. It is keyed on the canonical tables every mail source
produces (docs/arch/canonical-mail-schema.md), so two providers' tables are hidden alike; a
source kind opts in by name (:func:`declare_canonical_mail`) and states no list of its own.
"""

# Requirements: REQ-1943, REQ-1959, REQ-1923
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.core.column_scope import SCOPE_RESTRICTED

#: The tag a declared column carries: the built-in one with the Sensitive data option.
TAG = "pii"


@dataclass(frozen=True)
class Declared:
    """What one table's declaration hides, by column name."""

    hidden: frozenset[str] = frozenset()
    masked: frozenset[str] = frozenset()

    @property
    def columns(self) -> frozenset[str]:
        return self.hidden | self.masked


#: The canonical mail tables: what a message says is hidden, who it is between is masked.
CANONICAL_MAIL: dict[str, Declared] = {
    "messages": Declared(
        hidden=frozenset({"subject", "snippet", "body_text", "body_html", "headers"}),
        masked=frozenset(
            {
                "from_address",
                "from_name",
                "sender_address",
                "to_addresses",
                "cc_addresses",
                "bcc_addresses",
                "reply_to_addresses",
            }
        ),
    ),
    "message_recipients": Declared(masked=frozenset({"address", "name"})),
    "threads": Declared(hidden=frozenset({"subject", "snippet"})),
    "attachments": Declared(hidden=frozenset({"name"})),
}

_BY_SOURCE_TYPE: dict[str, dict[str, Declared]] = {}


def declare_canonical_mail(source_type: str) -> None:
    """``source_type`` produces the canonical mail tables, hidden as :data:`CANONICAL_MAIL` says."""
    _BY_SOURCE_TYPE[source_type] = CANONICAL_MAIL


def declared_for(source_type: str | None, table_name: str) -> Declared | None:
    """What ``source_type`` declares of its table ``table_name``, or None when it declares
    nothing of it."""
    return _BY_SOURCE_TYPE.get(source_type or "", {}).get(table_name)


def reason(source_type: str) -> str:
    """What the tag assignment records of where it came from."""
    return f"declared sensitive by the {source_type} source kind"


def hide(columns: list[Any], declared: Declared) -> frozenset[str]:
    """Set how each declared column among ``columns`` is hidden, whatever it was given as, and
    answer which columns those were. Every other column, and every other field, is untouched."""
    hidden_now: set[str] = set()
    for column in columns:
        if column.name in declared.hidden:
            column.scope = SCOPE_RESTRICTED
            column.visible_to = []
        elif column.name in declared.masked:
            column.mask_type = "constant"
            column.mask_value = None  # NULL, whatever the column's type
            column.mask_pattern = column.mask_replace = column.mask_precision = None
            column.unmasked_to = []
        else:
            continue
        hidden_now.add(column.name)
    return frozenset(hidden_now)
