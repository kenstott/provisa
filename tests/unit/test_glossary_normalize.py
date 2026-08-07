# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1e5b28-4a9c-4e3f-b6d2-8c0a5f7e9b11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1387: deterministic physical-name → term normalization.

The rules under test are the dedup engine: case folding, separator and camelCase
tokenization, abbreviation expansion, and trailing-proxy stripping must all land the
key/id/camel spellings of one concept on the SAME phrase, or the glossary fragments.
"""

# Requirements: REQ-1387

from __future__ import annotations

import pytest

from provisa.core.glossary import normalize_term


@pytest.mark.parametrize(
    "physical",
    ["cust_id", "customerId", "CUSTOMER_KEY", "Customer-Id", "customer id", "CUSTOMER_SK"],
)
def test_customer_spellings_converge(physical):
    assert normalize_term(physical) == "customer"


def test_phrase_is_the_unit_not_tokens():
    assert normalize_term("order_line_amt") == "order line amount"


def test_camel_case_and_acronyms_tokenize():
    assert normalize_term("XMLParserRef") == "xml parser"
    assert normalize_term("orderDt") == "order date"


def test_trailing_proxy_tokens_strip_only_from_the_tail():
    assert normalize_term("status_code") == "status"
    assert normalize_term("customer_ref_id") == "customer"
    # An interior proxy token is part of the concept, not a dangling stand-in.
    assert normalize_term("code_page") == "code page"


def test_lone_proxy_token_survives():
    assert normalize_term("id") == "identifier"
    assert normalize_term("ref") == "reference"


def test_abbreviation_expansion():
    assert normalize_term("emp_dob") == "employee date of birth"
    assert normalize_term("txn_qty") == "transaction quantity"


def test_activity_by_columns_keep_the_full_phrase():
    assert normalize_term("created_by") == "created by"
    assert normalize_term("approved_by") == "approved by"


def test_unknown_short_forms_stay_as_written():
    assert normalize_term("st_min_no") == "st min no"


def test_untokenizable_name_case_folds_raw():
    assert normalize_term("___") == "___"


def test_determinism():
    assert normalize_term("Cust_ID") == normalize_term("cust_id")


def test_generic_phrase_qualifies_with_the_table_concept():
    # A bare name/date/id column names an attribute of its table's concept; one shared
    # "name" term for employees.name and products.name would over-merge unrelated meanings.
    assert normalize_term("first_name", table_context="employees") == "employee first name"
    assert normalize_term("name", table_context="products") == "product name"
    assert normalize_term("id", table_context="orders") == "order identifier"
    assert normalize_term("guid", table_context="shipments") == "shipment identifier"
    assert normalize_term("dt", table_context="orders") == "order date"
    # Audit-trail phrases sit on every table, so they qualify too.
    assert normalize_term("created_by", table_context="orders") == "order created by"
    assert normalize_term("created_dt", table_context="orders") == "order created date"
    assert normalize_term("submitted_at", table_context="claims") == "claim submitted at"
    # A phrase with its own non-generic modifier is a concept already — no qualification.
    assert normalize_term("ship_dt", table_context="orders") == "ship date"


def test_non_generic_phrase_ignores_the_table_context():
    # cust_id means customer wherever it appears — qualification is only for phrases
    # that carry no concept of their own.
    assert normalize_term("cust_id", table_context="orders") == "customer"


def test_table_concept_singularizes_the_head_noun():
    assert normalize_term("name", table_context="order_lines") == "order line name"
    # -ss nouns keep their form (inflect's false singular 'addres' is rejected).
    assert normalize_term("name", table_context="address") == "address name"
