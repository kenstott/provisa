# Copyright (c) 2026 Kenneth Stott
# Canary: c6c46779-89b5-4f15-93e5-a3c804110121
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The roadmap renders the release plan (docs/arch/release-plan.json)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location(
        "gen_roadmap", REPO / "scripts" / "gen_roadmap.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _plan() -> dict:
    return {
        "capacity_per_release": 30,
        "capacity_unit": "missing product rules",
        "basis": "About 15 rules a day.",
        "releases": [
            {
                "version": "513",
                "date": "2026-11-27",
                "status": "converging",
                "topics": [
                    {
                        "topic": "Sources",
                        "features": [
                            {
                                "req": "REQ-1986",
                                "feature": "Parameterized tables",
                                "benefit": "Read an endpoint as a table.",
                                "priority": "MUST",
                                "state": "complete",
                            },
                            {
                                "req": "REQ-2219",
                                "feature": "Per-person entitlements "
                                "(delivered rules; the rest is REQ-2348)",
                                "benefit": "Roles stay few.",
                                "priority": "SHOULD",
                                "state": "complete",
                            },
                        ],
                    }
                ],
            },
            {
                "version": "514",
                "date": "2026-11-29",
                "status": "on main (cut from main)",
                "notes": ["The GitHub organization becomes simpleishard-llc."],
                "topics": [
                    {
                        "topic": "Install & ops",
                        "features": [
                            {
                                "req": "REQ-2269",
                                "feature": "One-script host install",
                                "benefit": "Install with one command.",
                                "priority": "SHOULD",
                                "state": "in-progress",
                            }
                        ],
                    }
                ],
            },
            {
                "version": "515",
                "date": "2026-12-01",
                "status": "planned",
                "planned_rules": 30,
                "topics": [
                    {
                        "topic": "Sources",
                        "features": [
                            {
                                "req": "REQ-2290",
                                "feature": "Break-glass password",
                                "benefit": "No leaked admin password works.",
                                "priority": "MUST",
                                "state": "accepted",
                                "rules": 3,
                            }
                        ],
                    }
                ],
            },
        ],
        "decisions": [{"req": "REQ-1536", "decision": "Superseded by REQ-2014."}],
        "already_shipped": [
            {
                "req": "REQ-116",
                "title": "Kafka Schema Registry integration",
                "evidence": "tests present at v0.1.0-alpha.512",
                "state_after_edit": "complete",
                "remainder": "REQ-2317",
            },
            {
                "req": "REQ-259",
                "title": "Apollo Federation v2 subgraph",
                "evidence": "tests present at v0.1.0-alpha.512",
                "state_after_edit": "in-progress",
                "remainder": None,
            },
        ],
        "off_train": ["REQ-818", "REQ-1027"],
    }


def test_release_asset_lists_each_release_with_its_topics_and_features():
    text = _load().render_internal(_plan())
    assert "## v0.1.0-alpha.513: 2026-11-27 (converging)\n\n### Sources\n\n" in text
    assert (
        "- **REQ-1986** Parameterized tables [MUST] [complete] — Read an endpoint as a table."
        in text
    )
    assert (
        text.index("v0.1.0-alpha.513:")
        < text.index("v0.1.0-alpha.514:")
        < text.index("v0.1.0-alpha.515:")
    )
    assert "| 513 | 2026-11-27 | converging | 2 | shipped |" in text
    assert "| 515 | 2026-12-01 | planned | 1 | 30 |" in text


def test_release_asset_carries_notes_decisions_appendix_and_basis():
    text = _load().render_internal(_plan())
    assert "## v0.1.0-alpha.514: 2026-11-29 (on main (cut from main))\n\n> The GitHub" in text
    assert "- **REQ-1536** Superseded by REQ-2014." in text
    assert "REQ-818, REQ-1027." in text
    assert (
        "- **REQ-116** Kafka Schema Registry integration"
        " (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2317)" in text
    )
    assert (
        "- **REQ-259** Apollo Federation v2 subgraph"
        " (tests present at v0.1.0-alpha.512; now in-progress)" in text
    )
    assert text.rstrip().endswith("About 15 rules a day.")


def test_public_page_lists_releases_topics_and_benefits_without_ids_or_status():
    text = _load().render_public(_plan())
    assert "## v0.1.0-alpha.513 (2026-11-27)\n\n### Sources\n\n" in text
    assert "- **Parameterized tables.** Read an endpoint as a table." in text
    assert "- **Per-person entitlements.** Roles stay few." in text
    for word in ("REQ-", "MUST", "SHOULD", "complete", "in-progress", "accepted", "planned"):
        assert word not in text
    assert "simpleishard-llc" not in text  # release notes stay in the release asset
    assert "Kafka" not in text  # the shipped appendix stays in the release asset
    assert "may shift as priorities develop" in text


def test_public_page_refuses_a_benefit_that_names_a_requirement():
    plan = _plan()
    plan["releases"][0]["topics"][0]["features"][0]["benefit"] = "Finishes REQ-12."
    with pytest.raises(ValueError, match="REQ-12"):
        _load().render_public(plan)


def test_checked_in_pages_match_the_checked_in_plan():
    module = _load()
    plan = module.load_plan(REPO / "docs" / "arch" / "release-plan.json")
    assert (REPO / "docs" / "exports" / "roadmap.md").read_text() == module.render_internal(plan)
    assert (REPO / "docs" / "roadmap.md").read_text() == module.render_public(plan)
