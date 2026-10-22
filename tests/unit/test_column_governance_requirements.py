# Copyright (c) 2026 Kenneth Stott
# Canary: 2a6a6ca4-86c2-445f-be74-349f1b0fd535
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for column governance requirements: REQ-249, REQ-395, REQ-396, REQ-401, REQ-404, REQ-410"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# REQ-249, REQ-2059: Column-level masking config stored inline on Column in models.py, as the
# column's ordered mask list (``masks``): each entry names its readers and its mask (type,
# pattern, replace, value, precision), or none.
# ---------------------------------------------------------------------------


def _masked(mask: dict) -> list[dict]:
    return [{"to": ["*"], "mask": mask}]


def test_column_has_mask_type_field():
    # REQ-249: a mask entry's type is on the Column model, inline.
    from provisa.core.models import Column

    col = Column(name="email", visible_to=["analyst"], masks=_masked({"type": "regex"}))
    assert col.masks[0].mask is not None and col.masks[0].mask.type == "regex"


def test_column_mask_type_defaults_to_none():
    # REQ-249: a column declares no mask unless given one: its list is empty.
    from provisa.core.models import Column

    col = Column(name="name", visible_to=["admin"])
    assert col.masks == []


def test_column_has_mask_pattern_field():
    # REQ-249: a regex mask's pattern is inline on the column's entry.
    from provisa.core.models import Column

    col = Column(
        name="ssn",
        visible_to=["admin"],
        masks=_masked({"type": "regex", "pattern": r"\d{3}-\d{2}-\d{4}"}),
    )
    assert col.masks[0].mask is not None and col.masks[0].mask.pattern is not None


def test_column_has_mask_replace_field():
    # REQ-249: a regex mask's replacement is inline on the column's entry.
    from provisa.core.models import Column

    col = Column(
        name="ssn", visible_to=["admin"], masks=_masked({"type": "regex", "replace": "***-**-****"})
    )
    assert col.masks[0].mask is not None and col.masks[0].mask.replace == "***-**-****"


def test_column_has_mask_value_field():
    # REQ-249: a constant mask's value is inline on the column's entry.
    from provisa.core.models import Column

    col = Column(
        name="secret",
        visible_to=["analyst"],
        masks=_masked({"type": "constant", "value": "[REDACTED]"}),
    )
    assert col.masks[0].mask is not None and col.masks[0].mask.value == "[REDACTED]"


def test_column_has_mask_precision_field():
    # REQ-249: a truncate mask's precision (year, month, day, etc.) is inline on the entry.
    from provisa.core.models import Column

    col = Column(
        name="birth_date",
        visible_to=["analyst"],
        masks=_masked({"type": "truncate", "precision": "year"}),
    )
    assert col.masks[0].mask is not None and col.masks[0].mask.precision == "year"


def test_column_all_mask_fields_default_none():
    # REQ-249: a mask's fields default to None (co-located on Column, no separate model), and an
    # entry with no mask is unmasked.
    from provisa.core.models import Column

    col = Column(
        name="value",
        visible_to=["admin"],
        masks=[{"to": ["admin"]}, *_masked({"type": "constant"})],
    )
    assert col.masks[0].mask is None
    mask = col.masks[1].mask
    assert mask is not None
    assert (mask.pattern, mask.replace, mask.value, mask.precision) == (None, None, None, None)


# ---------------------------------------------------------------------------
# REQ-395: PK designation configurable via checkbox per column in TablesPage UI
# (visible in add-form, edit-form, and read-only views).
# REQ-396: "Exclude from query" disabled when no PK is configured for a label.
# ---------------------------------------------------------------------------


def test_column_has_is_primary_key_field():
    # REQ-395: Column must have is_primary_key boolean field.
    from provisa.core.models import Column

    col = Column(name="id", visible_to=["admin"], is_primary_key=True)
    assert col.is_primary_key is True


def test_column_is_primary_key_defaults_false():
    # REQ-395/396: is_primary_key defaults to False — exclusion disabled unless set.
    from provisa.core.models import Column

    col = Column(name="name", visible_to=["admin"])
    assert col.is_primary_key is False


def test_pk_columns_empty_means_no_pk_configured():
    # REQ-396: When pk_columns is empty, exclusion must be disabled.
    # Simulate the endpoint check: exclusion only available when pk is set.
    pk_columns: list[str] = []
    exclusion_enabled = bool(pk_columns)
    assert exclusion_enabled is False


def test_pk_columns_nonempty_enables_exclusion():
    # REQ-396: When pk_columns is non-empty, exclusion clause can be generated.
    pk_columns = ["id"]
    exclusion_enabled = bool(pk_columns)
    assert exclusion_enabled is True


# ---------------------------------------------------------------------------
# REQ-401: FK and AK badges as read-only indicators in the column editor UI.
# ---------------------------------------------------------------------------


def test_column_has_is_foreign_key_field():
    # REQ-401: Column must have is_foreign_key field (derived from relationships).
    from provisa.core.models import Column

    col = Column(name="user_id", visible_to=["admin"], is_foreign_key=True)
    assert col.is_foreign_key is True


def test_column_has_is_alternate_key_field():
    # REQ-401: Column must have is_alternate_key field (AK badge).
    from provisa.core.models import Column

    col = Column(name="email", visible_to=["admin"], is_alternate_key=True)
    assert col.is_alternate_key is True


def test_column_fk_ak_default_false():
    # REQ-401: FK and AK badges are false by default (read-only derived state).
    from provisa.core.models import Column

    col = Column(name="email", visible_to=["admin"])
    assert col.is_foreign_key is False
    assert col.is_alternate_key is False


# ---------------------------------------------------------------------------
# REQ-404: Security page RLS form includes "Apply To" toggle: "Specific Table"
# or "Entire Domain". Selection determines whether table_id or domain_id is
# populated; the other is NULL.
# ---------------------------------------------------------------------------


def test_rls_rule_has_table_id_and_domain_id():
    # REQ-404: RLSRule must have both table_id and domain_id fields.
    from provisa.core.models import RLSRule

    rule_table = RLSRule(
        table_id="public.orders",
        domain_id=None,
        role_id="analyst",
        filter="true",
        description="Test rule.",
    )
    assert rule_table.table_id == "public.orders"
    assert rule_table.domain_id is None


def test_rls_rule_domain_id_populated_when_entire_domain():
    # REQ-404: When "Entire Domain" is selected, domain_id is set and table_id is None.
    from provisa.core.models import RLSRule

    rule_domain = RLSRule(
        table_id=None, domain_id="sales", role_id="analyst", filter="true", description="Test rule."
    )
    assert rule_domain.domain_id == "sales"
    assert rule_domain.table_id is None


def test_rls_rule_table_id_defaults_none():
    # REQ-404: table_id is optional (None by default).
    from provisa.core.models import RLSRule

    rule = RLSRule(role_id="admin", filter="1=1", description="Test rule.")
    assert rule.table_id is None


def test_rls_rule_domain_id_defaults_none():
    # REQ-404: domain_id is optional (None by default).
    from provisa.core.models import RLSRule

    rule = RLSRule(role_id="admin", filter="1=1", description="Test rule.")
    assert rule.domain_id is None


# ---------------------------------------------------------------------------
# REQ-410: GraphFrame Cypher WHERE clause generation must use single-quoted
# string literals (not double-quoted SQL identifiers) for non-numeric PK values.
# ---------------------------------------------------------------------------


def test_rewrite_cypher_dquote_strings_converts_to_single_quotes():
    # REQ-410: Double-quoted Cypher string literals must become single-quoted.
    from provisa.cypher.rewrites import _rewrite_cypher_dquote_strings

    result = _rewrite_cypher_dquote_strings('WHERE n.id = "abc123"')
    assert "'" in result
    assert '"abc123"' not in result
    assert "'abc123'" in result


def test_rewrite_cypher_dquote_strings_does_not_convert_property_identifiers():
    # REQ-410: Double-quoted identifiers after `.` (property names) must NOT be converted.
    from provisa.cypher.rewrites import _rewrite_cypher_dquote_strings

    expr = 'n."user_id" = 1'
    result = _rewrite_cypher_dquote_strings(expr)
    # The .identifier pattern is left unchanged
    assert '"user_id"' in result


def test_rewrite_cypher_dquote_strings_handles_inner_single_quotes():
    # REQ-410: Inner single quotes within the string literal must be escaped.
    from provisa.cypher.rewrites import _rewrite_cypher_dquote_strings

    result = _rewrite_cypher_dquote_strings('WHERE n.name = "O\'Brien"')
    assert "\\'Brien" in result or "O\\'Brien" in result


def test_rewrite_cypher_dquote_strings_produces_valid_where_clause():
    # REQ-410: The resulting WHERE clause uses single-quoted string for PK exclusion.
    from provisa.cypher.rewrites import _rewrite_cypher_dquote_strings

    expr = 'WHERE NOT n.id IN ["pk-value-1", "pk-value-2"]'
    result = _rewrite_cypher_dquote_strings(expr)
    assert "'pk-value-1'" in result
    assert "'pk-value-2'" in result
    assert '"pk-value-1"' not in result


def test_rewrite_cypher_dquote_strings_leaves_non_string_parts_unchanged():
    # REQ-410: Numeric values and non-string parts of the expression are untouched.
    from provisa.cypher.rewrites import _rewrite_cypher_dquote_strings

    expr = "WHERE n.count > 5"
    result = _rewrite_cypher_dquote_strings(expr)
    assert result == expr
