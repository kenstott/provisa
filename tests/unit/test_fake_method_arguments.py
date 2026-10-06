# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0f1cf2-c0d2-449f-bc3b-99b120eafe37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A fake method's arguments are the ones every engine computes (REQ-1494): the Python side's list
is the Trino plugin's, each names a real parameter of its method, and any other argument is
refused by name when declared."""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from provisa.fakes.kinds import FakeRefused, Method, parse
from provisa.fakes.methods import ARGUMENTS, _generator, check_method

_JAVA = (
    Path(__file__).resolve().parents[2]
    / "trino-functions/src/main/java/dev/provisa/trino/functions/FakeMethods.java"
)


def _java_arguments() -> dict[str, tuple[str, ...]]:
    src = _JAVA.read_text()
    block = src[src.index("ARGUMENTS = Map.ofEntries(") : src.index("METHODS = methods();")]
    return {
        name: tuple(re.findall(r'"(\w+)"', args))
        for name, args in re.findall(r'Map\.entry\("(\w+)", List\.of\(([^)]*)\)\)', block)
    }


def test_the_python_side_and_the_trino_plugin_take_the_same_arguments():
    assert ARGUMENTS == _java_arguments()


@pytest.mark.parametrize("method", sorted(ARGUMENTS))
def test_each_argument_is_a_parameter_of_its_method(method):
    params = set(inspect.signature(getattr(_generator(), method)).parameters)
    assert set(ARGUMENTS[method]) <= params


def test_a_range_argument_is_taken_and_another_refused_by_name():
    check_method(parse("date_between(start_date='-5y', end_date='today')"), "date")  # type: ignore[arg-type]
    check_method(parse("pyfloat(left_digits=3, right_digits=2, positive=true)"), "numeric")  # type: ignore[arg-type]
    with pytest.raises(FakeRefused, match="date_of_birth\\(\\) takes no argument 'tzinfo'"):
        check_method(Method("date_of_birth", (("tzinfo", "UTC"),)), "date")
    with pytest.raises(FakeRefused, match="email\\(\\) takes no argument 'domain'.*it takes none"):
        check_method(Method("email", (("domain", "x.com"),)), "text")
