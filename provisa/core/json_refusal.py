# Copyright (c) 2026 Kenneth Stott
# Canary: e4a1c7d9-2b56-4f3a-8c0e-7d9b1a6f5e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement on a JSON column that cannot be answered correctly, refused by name (REQ-1965).

An operation a backend cannot perform correctly is refused, never approximated. Every such
refusal is a :class:`JsonRefused`: ``code`` is its stable identifier and ``params`` what it is
about. A surface answers the family in one place, as it does a read refusal.
"""

from __future__ import annotations

#: The name a backend raises with when a float element is not a number or is infinite. The
#: datasource cast writes it into the statement; the backend's error carries it back.
NON_FINITE = "json.non_finite_number"


class JsonRefused(ValueError):
    """A statement on a JSON column refused because it cannot be answered correctly."""

    code: str = "query.json_refused"
    params: dict[str, str]


class NonFiniteNumber(JsonRefused):
    """A natively nested column holds a float that is not a number or is infinite: it has no
    JSON form, and nothing is substituted for it."""

    code = NON_FINITE

    def __init__(self) -> None:
        self.params = {}
        super().__init__(
            f"{NON_FINITE}: a value read as JSON holds a floating-point element that is not a "
            "number or is infinite, which JSON cannot carry"
        )


def non_finite_refusal(exc: BaseException) -> NonFiniteNumber | None:
    """The refusal a backend's error stands for, or None when it is some other error. The cast
    makes each backend raise with the rule's name in its own message (DuckDB ``error``, Trino
    ``fail``, PostgreSQL a cast that cannot succeed); the name is how the error is recognised."""
    return NonFiniteNumber() if NON_FINITE in str(exc) else None


class JsonValueRefused(JsonRefused):
    """REQ-1867: a JSON value an answer would hold that JSON cannot carry exactly -- NaN or
    Infinity, a lone surrogate, text in a JSON-typed column that is not JSON, nesting past what a
    response can hold. The answer is refused naming the field; nothing is substituted."""

    code = "json.value_refused"

    def __init__(self, field: str, reason: str) -> None:
        self.params = {"field": field, "reason": reason}
        super().__init__(f"{self.code}: the value of {field!r} {reason}")


class JsonResponseRefused(JsonRefused):
    """REQ-1867: a response body JSON cannot carry -- nested past the encoder's depth, or holding a
    lone surrogate -- refused by name rather than failed."""

    code = "json.response_refused"

    def __init__(self, reason: str) -> None:
        self.params = {"reason": reason}
        super().__init__(f"{self.code}: the response {reason}")
