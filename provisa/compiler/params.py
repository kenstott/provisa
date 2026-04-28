# Copyright (c) 2026 Kenneth Stott
# Canary: bf3c93ad-012b-42f5-ab8f-4dd7d96cb416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL variables to parameterized SQL. Never interpolates values."""


class ParamCollector:
    """Collects parameter values and returns positional placeholders ($1, $2, ...)."""

    def __init__(self) -> None:
        self._params: list = []

    def add(self, value: object) -> str:
        """Add a parameter value and return its placeholder string."""
        self._params.append(value)
        return f"${len(self._params)}"

    @property
    def params(self) -> list:
        return list(self._params)
