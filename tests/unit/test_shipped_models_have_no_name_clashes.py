# Copyright (c) 2026 Kenneth Stott
# Canary: 1170ac42-abe4-46db-99f0-03db352246bf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-07): every model Provisa ships or deploys builds its names table under the
default clash policy, refuse. A shipped model that clashes would fail its own org build, so it fails
here first (the survey J929 wrote, scripts/survey_names_clash.py)."""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "survey_names_clash.py"
_spec = importlib.util.spec_from_file_location("survey_names_clash", _SCRIPT)
assert _spec is not None and _spec.loader is not None, _SCRIPT
survey_names_clash = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(survey_names_clash)

_MODELS = survey_names_clash.default_configs()


def test_the_survey_finds_the_shipped_models():
    assert len(_MODELS) >= 30, [str(p) for p in _MODELS]


@pytest.mark.parametrize("config", _MODELS, ids=lambda p: str(p.relative_to(_SCRIPT.parents[1])))
def test_a_shipped_model_builds_its_names_table_without_a_clash(config):
    result = survey_names_clash.survey(config)
    assert result["clashes"] == [], result["clashes"]
