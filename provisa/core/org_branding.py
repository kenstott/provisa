# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-org branding (REQ-1486).

An org_admin may put their own mark on the surfaces their people meet before and just after
sign-in: a display name, a logo, two colors, a welcome line, and the sentence the invitation adds
to Provisa's own copy. It is deliberately light — a tenant styles its instance, it does not
re-skin the product — so the field set is fixed here and every value is validated before it is
stored, not when it is rendered.

Storage is one JSON document on the org row plus the logo bytes beside it (the logo is served
from the control plane, so nothing on the page depends on a tenant-supplied remote URL).

Validation is strict and total: a value that does not parse is refused at the edit, so every
reader can render what it reads. There is no defaulting here — an unset field is absent from the
document, and what an unset field looks like is the UI's own design, not a substituted value.
"""

from __future__ import annotations

import json
import re
from typing import Any

# The editable field set. Fixed: branding is a tenant mark on Provisa's UI, not a theme engine.
FIELDS = (
    "display_name",
    "primary_color",
    "accent_color",
    "welcome_message",
    "invite_message",
)

_HEX_COLOR = re.compile(r"^#[0-9a-fA-F]{6}$")

# Caps sized to the surface each value lands on: a header slot, a login sub-heading, a paragraph
# in an email. A longer value would not be rejected by the renderer, it would just break it.
_MAX_LEN = {
    "display_name": 80,
    "welcome_message": 400,
    "invite_message": 1000,
}

# Logo formats every browser and mail client renders, and a size that stays inline in a JSON-free
# byte response. SVG is accepted because a wordmark is usually vector; it is served with a
# Content-Security-Policy that keeps it from executing (see the branding router).
LOGO_MEDIA_TYPES = ("image/png", "image/jpeg", "image/svg+xml", "image/webp")
MAX_LOGO_BYTES = 256 * 1024


class BrandingError(ValueError):
    """A branding value that cannot be stored. Carries the field so the API can name it."""

    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field


def validate_branding(payload: dict[str, Any]) -> dict[str, str]:
    """The branding document as it will be stored, or raise.

    Unknown keys are refused rather than dropped: an org_admin who misspells a field must be told,
    because silently storing nothing looks identical to storing a value that has no effect.
    An explicit ``None`` clears the field, which is how the UI's "reset" works.
    """
    unknown = sorted(set(payload) - set(FIELDS))
    if unknown:
        raise BrandingError(unknown[0], f"Unknown branding field(s): {', '.join(unknown)}")
    document: dict[str, str] = {}
    for field in FIELDS:
        value = payload.get(field)
        if value is None:
            continue
        if not isinstance(value, str):
            raise BrandingError(field, f"{field} must be a string")
        value = value.strip()
        if not value:
            continue  # an emptied input clears the field, same as omitting it
        if field.endswith("_color"):
            if not _HEX_COLOR.match(value):
                raise BrandingError(field, f"{field} must be a hex color like #4f46e5")
            document[field] = value.lower()
            continue
        limit = _MAX_LEN[field]
        if len(value) > limit:
            raise BrandingError(field, f"{field} must be {limit} characters or fewer")
        document[field] = value
    return document


def serialize_branding(document: dict[str, str]) -> str | None:
    """The column value for a validated document. An empty document stores NULL — "this org has
    set no branding" is the absence of a row value, not an empty object to be parsed."""
    return json.dumps(document, sort_keys=True) if document else None


def parse_branding(column_value: str | None) -> dict[str, str]:
    """The document a stored column holds. NULL means no branding was ever set.

    A non-NULL value that does not parse is a corrupted row, not a missing one, so it raises:
    every value in this column was written through :func:`validate_branding`.
    """
    if column_value is None:
        return {}
    document = json.loads(column_value)
    if not isinstance(document, dict):
        raise BrandingError("branding", "Stored branding is not an object")
    return document


def validate_logo(data: bytes, media_type: str) -> str:
    """The normalized media type for an accepted logo, or raise."""
    normalized = media_type.split(";")[0].strip().lower()
    if normalized not in LOGO_MEDIA_TYPES:
        raise BrandingError(
            "logo",
            f"Logo must be one of: {', '.join(LOGO_MEDIA_TYPES)} (got {normalized or 'nothing'})",
        )
    if not data:
        raise BrandingError("logo", "Logo is empty")
    if len(data) > MAX_LOGO_BYTES:
        raise BrandingError(
            "logo", f"Logo must be {MAX_LOGO_BYTES // 1024} KiB or smaller ({len(data)} bytes)"
        )
    return normalized
