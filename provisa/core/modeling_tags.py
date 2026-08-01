# Copyright (c) 2026 Kenneth Stott
# Canary: 7e6c6226-7ecd-42f5-adb3-69f7b2dbe51c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1320: shared formatting for the star/vault modeling-role tag.

Every catalog surface (GraphQL introspection, pg_description, and the Flight/MCP
catalog description) appends the same "[fact]" / "[dimension, scd2]" suffix so
callers see the star shape regardless of which surface they query.
"""

from __future__ import annotations


def append_modeling_tag(description: str | None, role: str | None, history: str | None) -> str:
    """Append the bracketed modeling-role tag to a description.

    Returns the tag alone (no leading space) when description is empty/None,
    and the description unchanged (or "") when role is unset.
    """
    if not role:
        return description or ""
    tags = [role]
    if history:
        tags.append(history)
    suffix = f"[{', '.join(tags)}]"
    return f"{description} {suffix}" if description else suffix
