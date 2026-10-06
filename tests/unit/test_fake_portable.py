# Copyright (c) 2026 Kenneth Stott
# Canary: 76ba7c89-39cd-47c7-9695-6034f329ca71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The portable definition of stable fakes (REQ-1494, A STABLE FAKE): every stable method is
computed from the digest by version 1's frozen lists and templates; the version's files and its
values are pinned, so no release changes an existing stable column's fakes silently; the Trino
plugin's PortableFakes is held to the same values by its own test and by the engine parity tests."""

from __future__ import annotations

import hashlib
import re
import uuid
from pathlib import Path

import pytest

from provisa.fakes.methods import STABLE_METHODS
from provisa.fakes.digest import definition_hash, seed
from provisa.fakes.portable import compute, definition, methods, stable_fake

_V1 = Path(__file__).resolve().parents[2] / "provisa/fakes/portable/v1.json"
_JAVA_TEST = (
    Path(__file__).resolve().parents[2]
    / "trino-functions/src/test/java/dev/provisa/trino/functions/PortableFakesTest.java"
)
_SEEDS = [d * 7919 * 104729 for d in range(-500, 500)]


def test_version_1_is_frozen():
    # A change to the lists or templates is a new version beside this one, never an edit.
    assert hashlib.sha256(_V1.read_bytes()).hexdigest() == (
        "86b4dd63b8b053600891db626f9e4eb9ba58c6f3fecb97c79168496af4ca6ef5"
    )


def test_version_1_s_values_are_pinned():
    h = hashlib.sha256()
    for m in sorted(STABLE_METHODS):
        for d in _SEEDS:
            h.update(f"{m}|{d}|{compute(m, 1, d)}\n".encode())
    assert h.hexdigest() == "c08d638fcd1498f70841a2d06ec285c95b0a051fcdd5985bb9e5a4a79f873e2e"


def test_every_stable_method_is_computed_by_version_1():
    assert STABLE_METHODS <= methods(1)
    for m in STABLE_METHODS:
        assert all(compute(m, 1, d) for d in _SEEDS[:50]), m


def test_a_stable_fake_is_a_function_of_the_digest_and_null_for_null():
    h = definition_hash("name", {}, 1, "varchar")
    assert (
        stable_fake("name", 1, 42, h)
        == stable_fake("name", 1, 42, h)
        == compute("name", 1, seed(42, h))
    )
    assert len({stable_fake("name", 1, d, h) for d in _SEEDS}) > 900
    assert stable_fake("name", 1, None, h) is None


def test_two_columns_faked_from_one_value_differ_by_their_definitions():
    """REQ-1494: the seed mixes the definition hash in, so name() and first_name() of one value,
    or one method pinned to two versions, do not draw alike."""
    d = 123456789
    draws = {
        (m, v, t): seed(d, definition_hash(m, {}, v, t))
        for m, v, t in [
            ("name", 1, "text"),
            ("first_name", 1, "text"),
            ("name", 2, "text"),
            ("name", None, "text"),
            ("name", 1, "char(40)"),  # the same family: the same draw
        ]
    }
    assert len(set(draws.values())) == 4
    assert draws[("name", 1, "text")] == draws[("name", 1, "char(40)")]


def test_shapes():
    for d in _SEEDS[:200]:
        u = uuid.UUID(compute("uuid4", 1, d))
        assert u.version == 4 and u.variant == uuid.RFC_4122
        assert re.fullmatch(r"\d{5}", compute("postcode", 1, d))
        assert re.fullmatch(r"[a-z0-9._]+@[a-z.]+", compute("email", 1, d))
        sentence = compute("sentence", 1, d)
        assert sentence[0].isupper() and sentence.endswith(".") and 4 <= len(sentence.split()) <= 9
        assert compute("state", 1, d) in definition(1)["lists"]["state"]


def test_an_unknown_version_or_method_is_refused_by_name():
    with pytest.raises(ValueError, match="no version 2 of the portable fake definition"):
        compute("name", 2, 1)
    with pytest.raises(ValueError, match=r"has no ipv4\(\)"):
        compute("ipv4", 1, 1)


def test_the_trino_plugin_s_test_holds_the_same_values():
    """PortableFakesTest pins the Java side to these values for one digest."""
    src = _JAVA_TEST.read_text()
    found = re.search(r"SEED = (-?\d+)L;", src)
    assert found is not None
    java_seed = int(found[1])
    pinned = dict(re.findall(r'\{"(\w+)", "([^"]*)"\}', src))
    assert set(pinned) == STABLE_METHODS
    assert pinned == {m: compute(m, 1, java_seed) for m in STABLE_METHODS}
