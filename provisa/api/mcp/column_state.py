# Copyright (c) 2026 Kenneth Stott
# Canary: 69e18bca-ce8a-4af1-9191-7550cbcd984f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How the caller's role reads one column, for deciding whether a condition on it can be
evaluated: ``plain``, ``masked``, ``faked`` or ``hidden``.

Read from the role's governance context -- the one the query guard checks a statement against --
never from the declared mask and grant lists, so the answer is what a query would do:

- ``hidden``: the role is not served the column; a statement naming it is refused (V003).
- ``masked``: a mask other than a fake applies; a condition on it is refused (V005).
- ``faked``: a condition on it is allowed but reads the column's fake values, not the real ones.
- ``plain``: none of these; a condition on it reads the real values.

A meta-role's governance context is its resolved rights, so its member reads the state of those.
"""

from __future__ import annotations

from typing import Any, Literal

ColumnState = Literal["plain", "masked", "faked", "hidden"]

# What describe_table says of itself, on every surface that lists it (Polly's chat, the MCP
# server, the MCP status page).
DESCRIBE_TABLE = (
    "Describe a table: columns (name, type, description, state) and foreign keys. A column's "
    "state is how your role reads it: a condition on a plain column can be evaluated, and one on "
    "a masked, faked (fake values) or hidden (not served, so not listed) column cannot be "
    "evaluated."
)


def column_state(gov: Any, table_id: int, name: str) -> ColumnState:
    """The state of column ``name`` (its SQL or registered name) of table ``table_id`` for the
    role whose governance context is ``gov``."""
    from provisa.compiler.stage2 import registered_name
    from provisa.security.masking import MaskType

    physical = registered_name(gov, table_id, name)
    # As V003: no governance for the table refuses every column of it (fail closed); a set of
    # served columns refuses one outside it; None serves them all.
    if table_id not in gov.visible_columns:
        return "hidden"
    served = gov.visible_columns[table_id]
    if served is not None and physical not in served:
        return "hidden"
    entry = gov.masking_rules.get((table_id, physical))
    if entry is None:
        return "plain"
    return "faked" if entry[0].mask_type == MaskType.fake else "masked"
