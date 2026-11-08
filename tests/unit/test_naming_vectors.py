# Copyright (c) 2026 Kenneth Stott
# Canary: 0e8aa8cf-8546-41f2-a255-dfa0febc65ed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194: the naming vectors naming.ts is held to are naming.py's own answers.

provisa-ui/src/__fixtures__/naming-vectors.json is read by src/__tests__/namingVectors.test.ts,
which holds naming.ts to it. This holds the file to naming.py: a rule changed in naming.py fails
here until `scripts/gen_naming_vectors.py` is run again, and naming.ts then fails until it follows.
"""

from __future__ import annotations

import json

from scripts.gen_naming_vectors import FIXTURE, render, vectors

from provisa.compiler import naming


def test_the_committed_vectors_are_what_naming_py_answers():
    assert FIXTURE.read_text(encoding="utf-8") == render()


def test_the_vectors_name_every_convention_naming_py_accepts():
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert data["presets"] == list(naming.PRESET_CONVENTIONS)
    assert set(data["valid_conventions"]) == naming.VALID_CONVENTIONS
    for entry in data["names"]:
        assert set(entry["convention"]) == naming.VALID_CONVENTIONS


def test_the_vectors_carry_the_cases_the_ui_drifted_on():
    by_input = {e["input"]: e for e in vectors()["names"]}
    assert by_input["Order_ID"]["camel"] == "orderID"
    assert by_input["userID"]["camel"] == "userID"
    assert by_input["foo_123"]["camel"] == "foo_123"
