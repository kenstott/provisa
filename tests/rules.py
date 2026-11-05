# Copyright (c) 2026 Kenneth Stott
# Canary: 96f7e98d-9d24-40ae-b1a2-d48cc31aa4b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a test expects of a row rule, now that a rule is an expression (REQ-913).

A test states the rule it expects as an administrator would write it; it is read as every
rule entering the pipeline is (``compiler.rls.read_rule``), and compared as a tree: the same
predicate, however it would be written out.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from provisa.compiler.rls import read_rule


def rule_of(text: str) -> Any:
    """The rule ``text`` states, as an expression."""
    return read_rule(text)


def rules_of(by_key: Mapping[Any, str]) -> dict[Any, Any]:
    """Each rule of ``by_key`` as an expression."""
    return {key: read_rule(text) for key, text in by_key.items()}


def holds(whole: Any, part: str) -> bool:
    """Whether the predicate ``part`` states is one of the terms of ``whole`` (anywhere in it)."""
    wanted = read_rule(part)
    return any(node == wanted for node in whole.walk())


def mentions(whole: Any, value: Any) -> bool:
    """Whether a literal of ``whole`` carries ``value``: a number equal to it, or a string
    holding it (a domain written into ``|a|b|``, say)."""
    import sqlglot.expressions as exp

    wanted = str(value)
    return any(
        isinstance(node, exp.Literal)
        and (wanted in node.this if node.is_string else node.this == wanted)
        for node in whole.walk()
    )
