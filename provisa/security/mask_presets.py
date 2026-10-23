# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2d9a41-5c03-4b8f-a1e6-3f90b4c8d257
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The regex masks the column editor offers ready-made (REQ-2142), each in the universal dialect
(provisa.security.universal_regex). A value a preset's pattern does not match is left as it is,
as with any regex mask."""

# Requirements: REQ-2142

from __future__ import annotations

from typing import Any

PRESETS: list[dict[str, Any]] = [
    {
        "id": "ssn_last4",
        "label": "SSN: last 4 digits",
        "pattern": r"^\d{3}-?\d{2}-?(\d{4})$",
        "replace": "***-**-$1",
        "example": {"input": "123-45-6789", "output": "***-**-6789"},
    },
    {
        "id": "card_last4",
        "label": "Card number: last 4 digits",
        "pattern": r"^[\d -]*(\d{4})$",
        "replace": "**** **** **** $1",
        "example": {"input": "4111 1111 1111 1234", "output": "**** **** **** 1234"},
    },
    {
        "id": "phone_last4",
        "label": "Phone number: last 4 digits",
        "pattern": r"^.*(\d{4})$",
        "replace": "***-***-$1",
        "example": {"input": "(555) 867-5309", "output": "***-***-5309"},
    },
    {
        "id": "email_initial_domain",
        "label": "Email: first letter and domain",
        "pattern": r"^(\w)[^@]*(@.*)$",
        "replace": "$1***$2",
        "example": {"input": "jane.doe@example.com", "output": "j***@example.com"},
    },
    {
        "id": "all_but_last4",
        "label": "All but the last 4 characters",
        "pattern": r"^.*(.{4})$",
        "replace": "****$1",
        "example": {"input": "ACCT-0042-7781", "output": "****7781"},
    },
    {
        "id": "digits_keep_separators",
        "label": "Every digit, separators kept",
        "pattern": r"\d",
        "replace": "#",
        "example": {"input": "555-867-5309", "output": "###-###-####"},
    },
    {
        "id": "zip3",
        "label": "ZIP code: first 3 digits",
        "pattern": r"^(\d{3})\d{2}(-\d{4})?$",
        "replace": "$1**",
        "example": {"input": "94110-1234", "output": "941**"},
    },
    {
        "id": "birth_year",
        "label": "Date of birth: year only",
        "pattern": r"^(\d{4})-\d{2}-\d{2}.*$",
        "replace": "$1",
        "example": {"input": "1984-07-12", "output": "1984"},
    },
    {
        "id": "iban_last4",
        "label": "IBAN: country and last 4",
        "pattern": r"^([A-Z]{2})[0-9A-Z ]*([0-9A-Z] ?[0-9A-Z] ?[0-9A-Z] ?[0-9A-Z])$",
        "replace": "$1** **** $2",
        "example": {"input": "GB82 WEST 1234 5698 7654 32", "output": "GB** **** 54 32"},
    },
    {
        "id": "keep_first_last",
        "label": "Keep the first and last characters",
        "params": ["first", "last"],
        "pattern_template": r"^(.{{{first}}}).*(.{{{last}}})$",
        "replace": "$1***$2",
        "example": {
            "params": {"first": 2, "last": 2},
            "input": "Alexandra",
            "output": "Al***ra",
        },
    },
]


def preset_pattern(preset: dict[str, Any], **params: int) -> str:
    """A preset's pattern; for a parameterised one, its template with ``params`` filled in."""
    if "pattern" in preset:
        return preset["pattern"]
    return preset["pattern_template"].format(**params)
