# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d8a52-3c47-4e9b-a0d6-2b7e9c5f1a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247: a Syncing entry's standing choices, per entry, direction and kind of difference.

A standing choice is take Provisa's value, take the other system's, or hold. It is kept per entry,
per direction (Export, Import) and per kind, set and cleared from the page, and the review shows
each kind's choice for both directions beside its differences.

What is asserted is what a wrong store gets wrong silently: a choice leaking to another entry,
another direction or another kind; a clear that leaves the choice behind; a set that keeps the old
choice; and a value outside the three choices being kept.
"""

# Requirements: REQ-2247

from __future__ import annotations

import pytest

from provisa.api.errors import ApiError
from provisa.api.metadata_export import sync_values as sv
from tests.unit.test_syncing_admin_surface import (  # noqa: F401 - the shared fixtures
    _catalog,
    _request,
    surface,
)
from tests.unit.test_syncing_review import _catalog_values, entry  # noqa: F401 - fixtures


async def _put(entry_id: str, direction: str, kind: str, choice: str | None) -> dict:
    from provisa.api.admin.syncing_router import set_standing_choice

    return await set_standing_choice(
        _request({"direction": direction, "kind": kind, "choice": choice}), entry_id
    )


async def _get(entry_id: str) -> dict:
    from provisa.api.admin.syncing_router import get_standing_choices

    return await get_standing_choices(_request(), entry_id)


@pytest.fixture
def two_entries(surface):  # noqa: F811 - the imported fixture, requested
    surface.settings["catalogs"] = [
        _catalog(id="mz", provider="openmetadata", role="steward"),
        _catalog(id="dh", provider="datahub", role="steward"),
    ]
    return surface


@pytest.mark.asyncio
async def test_a_choice_is_kept_per_entry_direction_and_kind(two_entries):
    await _put("mz", "export", sv.DESCRIPTION, "provisa")
    await _put("mz", "import", sv.DESCRIPTION, "catalog")
    await _put("mz", "import", sv.TAG, "hold")

    assert (await _get("mz"))["standing_choices"] == {
        "export": {sv.DESCRIPTION: "provisa"},
        "import": {sv.DESCRIPTION: "catalog", sv.TAG: "hold"},
    }
    # Another entry holds none of them.
    assert (await _get("dh"))["standing_choices"] == {"export": {}, "import": {}}


@pytest.mark.asyncio
async def test_setting_again_replaces_and_clearing_removes(two_entries):
    await _put("mz", "export", sv.OWNER, "provisa")
    await _put("mz", "export", sv.OWNER, "hold")
    assert (await _get("mz"))["standing_choices"]["export"] == {sv.OWNER: "hold"}

    body = await _put("mz", "export", sv.OWNER, None)
    assert body["standing_choices"] == {"export": {}, "import": {}}
    assert (await _get("mz"))["standing_choices"] == {"export": {}, "import": {}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("direction", "kind", "choice", "code"),
    [
        ("both", sv.DESCRIPTION, "provisa", "syncing.unknown_direction"),
        ("export", "colour", "provisa", "syncing.unknown_kind"),
        ("export", sv.DESCRIPTION, "maybe", "syncing.unknown_choice"),
    ],
)
async def test_a_value_outside_the_choices_is_refused_by_name(
    two_entries, direction, kind, choice, code
):
    with pytest.raises(ApiError) as caught:
        await _put("mz", direction, kind, choice)
    assert caught.value.code == code
    assert (await _get("mz"))["standing_choices"] == {"export": {}, "import": {}}


@pytest.mark.asyncio
async def test_an_entry_that_does_not_exist_is_refused_by_name(two_entries):
    with pytest.raises(ApiError) as caught:
        await _put("nope", "export", sv.DESCRIPTION, "provisa")
    assert caught.value.code == "syncing.not_found"


@pytest.mark.asyncio
async def test_the_review_shows_each_kinds_choice_for_both_directions(entry):  # noqa: F811
    from provisa.api.admin.syncing_router import review_entry

    await _put("mz", "import", sv.DESCRIPTION, "catalog")
    await _put("mz", "export", sv.COLUMN_DESCRIPTION, "hold")
    entry.catalog.values = _catalog_values(
        description__wh_public_orders="Orders, as the catalog says",
        column_description__wh_public_orders_id="The id",
    )
    body = await review_entry(_request(), "mz")

    assert {k["kind"]: k["standing_choice"] for k in body["kinds"]} == {
        sv.DESCRIPTION: {"export": None, "import": "catalog"},
        sv.COLUMN_DESCRIPTION: {"export": "hold", "import": None},
    }
