# Copyright (c) 2026 Kenneth Stott
# Canary: a9b8c7d6-e5f4-3210-fedc-ba9876543210
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for ceiling enforcement (REQ-005) in the context of the
execution pipeline.

test_ceiling.py (unit) already covers:
- exact match passes
- fewer columns passes
- single column passes
- extra column rejected (basic)
- extra nested field rejected (basic)
- additional filter argument passes
- error detail lists all offending fields

This file adds integration-marker tests for pipeline-context scenarios and
edge cases that are not individually covered in the unit suite:
- Empty client query (no fields selected) is always a subset
- Violation when the extra field is deeply nested
- Multiple violations are all named in the error message
- Approved query with multiple root-level types; client violates one of them
- Alias on a field in client does NOT bypass ceiling (alias name is checked)
- CeilingViolationError.detail attribute contains the offending field names
- Ceiling passes when client uses query arguments (with-args form)
- Ceiling passes when client uses field aliases pointing to approved names
  (alias VALUE maps to the approved name via name.value, so alias itself
  appears in error — this tests the actual behaviour described in the code)
"""

from __future__ import annotations

import pytest

from provisa.registry.ceiling import CeilingViolationError, check_ceiling

# These tests do not require a live database connection — check_ceiling is a
# pure GraphQL-parsing function.  We mark them @pytest.mark.integration to
# match the integration test suite conventions for ceiling enforcement in the
# execution pipeline context.
pytestmark = pytest.mark.integration


class TestCeilingEnforcementIntegration:
    # ------------------------------------------------------------------
    # Empty / minimal client queries
    # ------------------------------------------------------------------

    def test_empty_selection_set_not_possible_but_no_fields_passes(self):
        """Client querying only the root object with no leaf fields is a subset."""
        # GraphQL requires at least one field in a selection set, so we use
        # a single approved field and the client selects only that one field —
        # this confirms the zero-extra-fields path works for a minimal query.
        approved = "{ shipments { tracking_number } }"
        client = "{ shipments { tracking_number } }"
        # Should not raise
        check_ceiling(approved, client)

    def test_client_with_only_one_of_many_approved_fields_passes(self):
        """Client selecting a strict subset of a large approved set passes."""
        approved = "{ products { id sku title description price stock_count } }"
        client = "{ products { sku price } }"
        check_ceiling(approved, client)

    # ------------------------------------------------------------------
    # Deeply nested violations
    # ------------------------------------------------------------------

    def test_deeply_nested_extra_field_raises_violation(self):
        """An extra leaf field inside a multi-level nested selection is caught."""
        approved = "{ invoices { id line_items { quantity unit_price } } }"
        client = "{ invoices { id line_items { quantity unit_price product_code } } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert "product_code" in str(exc_info.value)

    def test_violation_at_third_nesting_level_raises(self):
        """Violation at a third level of nesting is still caught."""
        approved = "{ org { departments { teams { id } } } }"
        client = "{ org { departments { teams { id manager_email } } } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert "manager_email" in str(exc_info.value)

    # ------------------------------------------------------------------
    # Multiple simultaneous violations
    # ------------------------------------------------------------------

    def test_multiple_extra_fields_all_appear_in_error(self):
        """When multiple unapproved fields are requested, all are in the error."""
        approved = "{ accounts { id balance } }"
        client = "{ accounts { id balance ssn credit_score routing_number } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        error_text = str(exc_info.value)
        assert "ssn" in error_text
        assert "credit_score" in error_text
        assert "routing_number" in error_text

    # ------------------------------------------------------------------
    # CeilingViolationError detail attribute
    # ------------------------------------------------------------------

    def test_violation_error_detail_attribute_contains_field_name(self):
        """CeilingViolationError.detail attribute names the offending field."""
        approved = "{ employees { id department } }"
        client = "{ employees { id department salary } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert "salary" in exc_info.value.detail

    def test_violation_error_detail_is_string(self):
        """CeilingViolationError.detail is a plain string."""
        approved = "{ tasks { id } }"
        client = "{ tasks { id assignee_email } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert isinstance(exc_info.value.detail, str)

    def test_violation_error_str_includes_prefix_text(self):
        """str(CeilingViolationError) includes the standard prefix message."""
        approved = "{ nodes { id } }"
        client = "{ nodes { id weight } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert "ceiling" in str(exc_info.value).lower()

    # ------------------------------------------------------------------
    # Multiple root types in the approved query
    # ------------------------------------------------------------------

    def test_multiple_root_types_no_violation_passes(self):
        """Client querying a subset across two approved root types passes."""
        approved = "{ orders { id amount } customers { id name email } }"
        client = "{ orders { id } customers { name } }"
        check_ceiling(approved, client)

    def test_multiple_root_types_violation_in_second_type_raises(self):
        """Extra field in the second root type is caught even when first is clean."""
        approved = "{ orders { id amount } customers { id name email } }"
        client = "{ orders { id } customers { id name phone_number } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert "phone_number" in str(exc_info.value)

    def test_multiple_root_types_violation_in_first_type_raises(self):
        """Extra field in the first root type is caught even when second is clean."""
        approved = "{ orders { id amount } customers { id name } }"
        client = "{ orders { id amount discount_code } customers { id } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert "discount_code" in str(exc_info.value)

    # ------------------------------------------------------------------
    # Field aliases
    # ------------------------------------------------------------------

    def test_field_alias_on_approved_field_passes(self):
        """An alias for an approved field does not introduce a violation.

        _collect_fields uses sel.name.value (the actual field name), so an
        alias like `total: amount` contributes 'amount' to the field set —
        which is approved.
        """
        approved = "{ orders { id amount } }"
        # 'total' is an alias; sel.name.value is still 'amount'
        client = "{ orders { id total: amount } }"
        check_ceiling(approved, client)

    def test_field_alias_pointing_to_unapproved_field_raises(self):
        """An alias on an unapproved field is still caught because name.value
        is the actual field name (e.g. `disguised: secret` → name.value = 'secret').
        """
        approved = "{ orders { id amount } }"
        # alias 'disguised' maps to field 'secret' — name.value = 'secret'
        client = "{ orders { id disguised: secret } }"
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        # The code uses sel.name.value, so 'secret' appears in the violation
        assert "secret" in str(exc_info.value)

    # ------------------------------------------------------------------
    # Query argument variations (WHERE-style arguments are always allowed)
    # ------------------------------------------------------------------

    def test_approved_fields_with_complex_arguments_pass(self):
        """Client can add complex argument expressions without violating ceiling."""
        approved = "{ orders { id amount region } }"
        client = (
            '{ orders(where: { region: { _in: ["us-east", "eu-west"] } }, '
            'limit: 10, offset: 0) { id amount } }'
        )
        check_ceiling(approved, client)

    def test_field_with_argument_but_unapproved_still_raises(self):
        """Arguments on an unapproved field don't make it pass the ceiling."""
        approved = "{ orders { id amount } }"
        client = '{ orders { id amount internal_cost(format: "cents") } }'
        with pytest.raises(CeilingViolationError) as exc_info:
            check_ceiling(approved, client)
        assert "internal_cost" in str(exc_info.value)

    # ------------------------------------------------------------------
    # Idempotency / multiple calls
    # ------------------------------------------------------------------

    def test_check_ceiling_is_idempotent(self):
        """Calling check_ceiling repeatedly with the same inputs is safe."""
        approved = "{ widgets { id color } }"
        client = "{ widgets { id } }"
        for _ in range(3):
            check_ceiling(approved, client)  # should not raise on any iteration

    def test_violation_raised_consistently_across_calls(self):
        """check_ceiling always raises for the same violating input."""
        approved = "{ widgets { id color } }"
        client = "{ widgets { id color size } }"
        for _ in range(3):
            with pytest.raises(CeilingViolationError):
                check_ceiling(approved, client)
