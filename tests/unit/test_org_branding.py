# Copyright (c) 2026 Kenneth Stott
# Canary: aa640d2e-478d-4492-b588-763767a2fd35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1486: what an org may store as its branding, and what it may not.

Validation runs at the edit, so these are the rules every reader afterwards relies on: the field
set is closed, colors are hex, text is capped, and an emptied field is absent rather than blank.
The logo rules matter for the same reason — the bytes are served back from the platform's own
origin, so the type is decided here and never sniffed from the content.
"""

# Requirements: REQ-1486

from __future__ import annotations

import pytest

from provisa.core.org_branding import (
    MAX_LOGO_BYTES,
    BrandingError,
    parse_branding,
    serialize_branding,
    validate_branding,
    validate_logo,
)


def test_a_full_document_survives_validation_unchanged_except_for_color_case():
    document = validate_branding(
        {
            "display_name": "Acme Analytics",
            "primary_color": "#4F46E5",
            "accent_color": "#10b981",
            "welcome_message": "Sign in with your Acme account.",
            "invite_message": "Ping #data-platform if you have questions.",
        }
    )
    assert document == {
        "display_name": "Acme Analytics",
        "primary_color": "#4f46e5",
        "accent_color": "#10b981",
        "welcome_message": "Sign in with your Acme account.",
        "invite_message": "Ping #data-platform if you have questions.",
    }


@pytest.mark.parametrize("value", ["", "   "])
def test_an_emptied_field_clears_rather_than_storing_blank(value):
    """A blank display name would render as an org with no name; absence renders as the org's own
    id, which is the design."""
    assert validate_branding({"display_name": value}) == {}


def test_a_cleared_field_is_absent_not_null():
    assert validate_branding({"display_name": "Acme", "welcome_message": None}) == {
        "display_name": "Acme"
    }


def test_a_misspelled_field_is_refused_and_named():
    """Dropping it would look identical to storing a value that has no effect."""
    with pytest.raises(BrandingError, match="primaryColor") as exc:
        validate_branding({"primaryColor": "#4f46e5"})
    assert exc.value.field == "primaryColor"


@pytest.mark.parametrize("value", ["4f46e5", "#4f46e", "#gggggg", "red", "#4f46e5aa"])
def test_a_color_that_is_not_a_six_digit_hex_is_refused(value):
    with pytest.raises(BrandingError) as exc:
        validate_branding({"primary_color": value})
    assert exc.value.field == "primary_color"


def test_a_non_string_value_is_refused():
    with pytest.raises(BrandingError) as exc:
        validate_branding({"display_name": 7})
    assert exc.value.field == "display_name"


@pytest.mark.parametrize(
    ("field", "limit"),
    [("display_name", 80), ("welcome_message", 400), ("invite_message", 1000)],
)
def test_each_text_field_is_capped_at_the_surface_it_renders_into(field, limit):
    assert validate_branding({field: "x" * limit}) == {field: "x" * limit}
    with pytest.raises(BrandingError) as exc:
        validate_branding({field: "x" * (limit + 1)})
    assert exc.value.field == field


def test_an_org_that_set_nothing_stores_null_and_reads_back_empty():
    assert serialize_branding({}) is None
    assert parse_branding(None) == {}


def test_a_stored_document_round_trips():
    document = validate_branding({"display_name": "Acme", "accent_color": "#10B981"})
    assert parse_branding(serialize_branding(document)) == document


def test_a_stored_value_that_is_not_an_object_raises_rather_than_reading_as_unset():
    """Every value in this column was written through validate_branding, so a scalar is corruption
    — reporting it as "no branding" would hide that."""
    with pytest.raises(BrandingError):
        parse_branding('"acme"')


@pytest.mark.parametrize(
    "media_type",
    ["image/png", "image/jpeg", "image/svg+xml", "image/webp", "IMAGE/PNG", "image/png; x=1"],
)
def test_an_accepted_logo_type_is_normalized(media_type):
    assert validate_logo(b"bytes", media_type) == media_type.split(";")[0].strip().lower()


@pytest.mark.parametrize("media_type", ["", "text/html", "application/pdf", "image/gif"])
def test_a_logo_type_the_page_cannot_safely_render_is_refused(media_type):
    with pytest.raises(BrandingError) as exc:
        validate_logo(b"bytes", media_type)
    assert exc.value.field == "logo"


def test_an_empty_logo_is_refused():
    with pytest.raises(BrandingError):
        validate_logo(b"", "image/png")


def test_a_logo_over_the_cap_is_refused_at_the_write():
    validate_logo(b"x" * MAX_LOGO_BYTES, "image/png")
    with pytest.raises(BrandingError, match="256 KiB"):
        validate_logo(b"x" * (MAX_LOGO_BYTES + 1), "image/png")
