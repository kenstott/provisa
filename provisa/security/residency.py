# Copyright (c) 2026 Kenneth Stott
# Canary: e2a36ebd-d054-4c80-b233-1687da83fc77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Who may say where data lives (REQ-1921, "a region governs where data may be, a domain governs
who owns it").

Setting, changing or removing the region of a table, a source or a materialized view needs the
``data_residency`` right, beside the domain ownership the edit needs anyway. A grant lists the
values it covers: the org's regions and "no region" (:data:`NO_REGION`). A change from P to Q
needs both covered; a new object needs only Q; while the object is draft, a change needs only Q
(the destination holder claims it). A refusal names the value not covered. None of this exists
when the platform declares no regions.
"""

# Requirements: REQ-1921

from __future__ import annotations

#: The grant value that stands for "no region". Not a region id: those are ``[a-z][a-z0-9]+``.
NO_REGION = "no_region"


def value_of(region: str | None) -> str:
    """The grant value a region is covered by."""
    return NO_REGION if region is None else region


class ResidencyRefused(PermissionError):
    """A change of where data lives that the caller's data_residency grant does not cover."""

    def __init__(self, what: str, value: str, *, holds_right: bool) -> None:
        # holds_right False: the caller holds no data_residency grant at all.
        self.code = (
            "security.data_residency_refused" if holds_right else "security.data_residency_missing"
        )
        self.params = {"object": what, "value": value}
        shown = "no region" if value == NO_REGION else f"region {value!r}"
        whose = (
            "the caller's data_residency grant does not"
            if holds_right
            else "no grant of the caller's"
        )
        super().__init__(
            f"the region of {what} cannot be set: {whose} cover {shown}; a data_residency grant "
            f"listing {shown} allows it"
        )


#: Before-value of an object being created: there was no region to change from.
CREATED = object()


def require_change(
    covered: set[str] | None,
    what: str,
    before: object,
    after: str | None,
    *,
    draft: bool = False,
) -> None:
    """Refuse ``what``'s region going from ``before`` (``CREATED`` for a new object) to
    ``after`` unless ``covered`` (the caller's grant values; None = no data_residency right)
    covers what the change needs. No change needs nothing."""
    if before is not CREATED and before == after:
        return
    needed = [value_of(after)]
    if before is not CREATED and not draft:
        assert before is None or isinstance(before, str)
        needed.insert(0, value_of(before))
    for value in needed:
        if covered is None or value not in covered:
            raise ResidencyRefused(what, value, holds_right=covered is not None)
