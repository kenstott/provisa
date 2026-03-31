# Copyright (c) 2025 Kenneth Stott
# Canary: d404b48b-19bf-4ee0-b501-d45704be9835
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for ParamCollector — variable binding with positional placeholders."""

from provisa.compiler.params import ParamCollector


class TestParamCollector:
    def test_empty_collector(self):
        c = ParamCollector()
        assert c.params == []

    def test_single_param(self):
        c = ParamCollector()
        placeholder = c.add("hello")
        assert placeholder == "$1"
        assert c.params == ["hello"]

    def test_multiple_params(self):
        c = ParamCollector()
        p1 = c.add("a")
        p2 = c.add(42)
        p3 = c.add(True)
        assert p1 == "$1"
        assert p2 == "$2"
        assert p3 == "$3"
        assert c.params == ["a", 42, True]

    def test_params_returns_copy(self):
        c = ParamCollector()
        c.add("x")
        params = c.params
        params.append("mutated")
        assert c.params == ["x"]

    def test_none_param(self):
        c = ParamCollector()
        p = c.add(None)
        assert p == "$1"
        assert c.params == [None]

    def test_list_param(self):
        c = ParamCollector()
        p = c.add([1, 2, 3])
        assert p == "$1"
        assert c.params == [[1, 2, 3]]
