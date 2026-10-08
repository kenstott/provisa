# Copyright (c) 2026 Kenneth Stott
# Canary: 355b33a9-019c-4c7e-9e59-420d84aed473
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every feature file is bound by a step module, and every binding names something that exists.

tests/features holds only .feature files; pytest collects none of them. A scenario runs because a
module under tests/steps binds its feature (``scenarios(...)`` or ``@scenario(...)``), and those
modules run in the integration suite's lanes. A feature no step module binds is a specification
nothing executes, and nothing said so: the job meant to run the scenarios collected nothing and
reported success."""

from __future__ import annotations

import re
from pathlib import Path

_TESTS = Path(__file__).resolve().parents[1]
_FEATURES = _TESTS / "features"
_STEPS = _TESTS / "steps"

_FEATURE_NAME = re.compile(r"""["'/]([\w.\-]+\.feature)["']""")
# @scenario("<path>/<file>.feature", "<scenario name>")
_ONE_SCENARIO = re.compile(
    r"""@scenario\(\s*[^,]*?([\w.\-]+\.feature)["']\s*\)?\s*,\s*["']([^"']+)["']""", re.S
)
_SCENARIO_TITLE = re.compile(r"^\s*Scenario(?: Outline| Template)?:\s*(.+?)\s*$", re.M)


def _step_sources() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(_STEPS.glob("*.py"))}


def _bound() -> dict[str, set[str]]:
    """Feature file name -> the step modules that name it."""
    bound: dict[str, set[str]] = {}
    for module, source in _step_sources().items():
        for feature in _FEATURE_NAME.findall(source):
            bound.setdefault(feature, set()).add(module)
    return bound


def test_there_are_features_and_step_modules_to_check():
    assert len(list(_FEATURES.glob("*.feature"))) > 300
    assert len(_step_sources()) > 100


def test_every_feature_file_is_bound_by_a_step_module():
    bound = _bound()
    unbound = sorted(p.name for p in _FEATURES.glob("*.feature") if p.name not in bound)
    assert unbound == [], f"{len(unbound)} feature files no step module binds"


def test_every_feature_a_step_module_binds_exists():
    existing = {p.name for p in _FEATURES.glob("*.feature")}
    missing = {
        feature: sorted(modules)
        for feature, modules in sorted(_bound().items())
        if feature not in existing
    }
    assert missing == {}


def test_every_scenario_bound_by_name_exists_in_its_feature():
    titles = {
        p.name: set(_SCENARIO_TITLE.findall(p.read_text(encoding="utf-8")))
        for p in _FEATURES.glob("*.feature")
    }
    absent = sorted(
        (module, feature, name)
        for module, source in _step_sources().items()
        for feature, name in _ONE_SCENARIO.findall(source)
        if feature in titles and name not in titles[feature]
    )
    assert absent == []
