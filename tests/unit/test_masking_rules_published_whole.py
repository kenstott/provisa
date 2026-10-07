# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0e9b47-3f52-4a18-b7c9-4e1a8d2f5c63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A schema rebuild never leaves a worker without its masking rules (REQ-1914, REQ-040).

Every worker rebuilds its model when the config stamp changes, while requests run on their own
threads. The masking rules used to be cleared at the start of a rebuild and refilled near its
end; a request in between was answered unmasked. The rules a request finds are now always a
whole set: the previous one until the new one is published in a single assignment."""

# Requirements: REQ-1914, REQ-040

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock

from provisa.api import app as app_module
from provisa.api import app_loaders
from provisa.security.masking import MaskType


def _mask_row(column: str, unmasked_to: list[str]) -> dict:
    return {
        "table_id": 7,
        "column_name": column,
        "unmasked_to": unmasked_to,
        "mask_type": "constant",
        "mask_pattern": None,
        "mask_replace": None,
        "mask_value": "MASKED",
        "mask_precision": None,
        "fake": None,
        "fake_stable": False,
        "fake_stable_version": None,
    }


def test_the_previous_rules_stay_in_force_until_the_new_set_is_published(monkeypatch):
    previous = {(7, "analyst"): {"name": ("the previous rule", "varchar")}}
    state = SimpleNamespace(
        masking_rules=previous,
        # prod: no data mode, so nothing is faked (REQ-1942)
        _active_runtime=lambda: SimpleNamespace(data_mode=None, env="prod", data_refusal=None),
    )
    monkeypatch.setattr(app_module, "state", state)
    seen_during_the_read: list[object] = []

    class _Conn:
        async def execute_core(self, _stmt):
            # What a request on another thread finds while this build reads the control plane.
            seen_during_the_read.append(state.masking_rules)
            result = MagicMock()
            result.fetchall.return_value = [
                MagicMock(_mapping=_mask_row("email", unmasked_to=["org_admin"]))
            ]
            return result

    roles = [{"id": "analyst"}, {"id": "org_admin"}]
    asyncio.run(app_loaders._load_masking_rules(_Conn(), {}, roles))

    assert seen_during_the_read == [previous]
    assert seen_during_the_read[0] == {(7, "analyst"): {"name": ("the previous rule", "varchar")}}
    # The new set replaces the previous one whole: the mask that was removed is gone, the new one
    # applies to the role that is not exempt, and the exempt role has none.
    assert state.masking_rules is not previous
    assert set(state.masking_rules) == {(7, "analyst")}
    rule, data_type = state.masking_rules[(7, "analyst")]["email"]
    assert (rule.mask_type, rule.value, data_type) == (MaskType.constant, "MASKED", "varchar")


def test_a_rebuild_does_not_clear_the_rules_before_it_reads_them():
    source = inspect.getsource(app_module._rebuild_schemas_impl)
    assert "state.masking_rules = {}" not in source
    assert "_load_masking_rules(" in source
