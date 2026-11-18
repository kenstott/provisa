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
needs both covered; a new object needs only Q. A move between regions may pass through
withdrawn instead, by two holders: withdrawing a table needs its region P covered, and putting it
back into a region Q needs only Q (:func:`require_covers`). A refusal names the value not
covered. None of this exists when the platform declares no regions.
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
) -> None:
    """Refuse ``what``'s region going from ``before`` (``CREATED`` for a new object) to
    ``after`` unless ``covered`` (the caller's grant values; None = no data_residency right)
    covers what the change needs. No change needs nothing."""
    if before is not CREATED and before == after:
        return
    needed = [value_of(after)]
    if before is not CREATED:
        assert before is None or isinstance(before, str)
        needed.insert(0, value_of(before))
    for value in needed:
        if covered is None or value not in covered:
            raise ResidencyRefused(what, value, holds_right=covered is not None)


class WithdrawRefused(ResidencyRefused):
    """Withdrawing an object from a region the caller's data_residency grant does not cover: only
    a holder for the region it is in takes it out (REQ-1921)."""

    def __init__(self, what: str, value: str) -> None:
        super().__init__(what, value, holds_right=True)
        self.code = "security.withdraw_residency_refused"
        shown = "no region" if value == NO_REGION else f"region {value!r}"
        PermissionError.__init__(
            self,
            f"{what} cannot be withdrawn: no data_residency grant of the caller's covers {shown}, "
            f"where it is kept; a data_residency grant listing {shown} allows it",
        )


def require_covers(
    covered: set[str] | None, what: str, region: str | None, *, withdrawing: bool
) -> None:
    """Refuse a step of a move between regions in ``region`` unless ``covered`` (the caller's
    grant values; None = no data_residency right) covers it (REQ-1921): withdrawing ``what``
    from the region it is in (:class:`WithdrawRefused`), or putting it back into the region
    named (:class:`ResidencyRefused`, as any setting of a region)."""
    value = value_of(region)
    if covered is not None and value in covered:
        return
    if withdrawing:
        raise WithdrawRefused(what, value)
    raise ResidencyRefused(what, value, holds_right=covered is not None)
