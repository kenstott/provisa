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
