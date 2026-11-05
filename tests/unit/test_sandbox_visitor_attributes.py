# Copyright (c) 2026 Kenneth Stott
# Canary: 17a2b16f-27f7-4d78-a1ee-e154189a2242
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2151: a published demo declares the attributes its sandbox visitor carries (demo.yaml
``visitor: attributes:``), and opening a visitor's sandbox records them as the org's attributes for
that visitor -- the user-attribute record an administrator writes -- so a derived role the tour acts
as (retail's region__eu, REQ-2057) is held from an attribute, as for any member. Retail's
reveal step was refused ("not permitted") for a visitor who carried none."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.api.demo_sandbox import record_visitor_attributes
from provisa.api.demo_templates import demo_visitor_attributes
from provisa.auth.derived_roles import held_derived_roles

_NAMED = Path(__file__).resolve().parents[2] / "demo" / "named"


def _demo(tmp_path: Path, spec: dict) -> Path:
    (tmp_path / "demo.yaml").write_text(yaml.safe_dump(spec))
    return tmp_path


def test_retails_visitor_carries_region_eu():
    assert demo_visitor_attributes(_NAMED / "retail") == {"region": "eu"}


def test_a_demo_that_declares_none_gives_its_visitor_none(tmp_path):
    assert demo_visitor_attributes(_demo(tmp_path, {"published": True})) == {}


@pytest.mark.parametrize(
    "visitor",
    [{"attributes": {"region": 3}}, {"attributes": ["region"]}, {"attributes": {"region": None}}],
)
def test_a_value_that_is_not_text_refuses_the_demo_by_name(tmp_path, visitor):
    directory = _demo(tmp_path, {"published": True, "visitor": visitor})
    with pytest.raises(ValueError, match="demo.yaml"):
        demo_visitor_attributes(directory)


async def test_opening_records_the_visitors_attributes_so_it_holds_the_derived_role(
    control_plane,
):
    db = control_plane.org()
    async with db.acquire() as conn:
        held = await record_visitor_attributes(conn, "sandbox-visitor-x", _NAMED / "retail")
    assert held["sandbox-visitor-x"] == {"region": "eu"}
    assert "region__eu" in held_derived_roles(["region"], held["sandbox-visitor-x"])
