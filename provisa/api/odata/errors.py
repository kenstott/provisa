# Copyright (c) 2026 Kenneth Stott
# Canary: e2075795-4ff2-4b16-a6da-ae7358609e5a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the OData service answers when it does not take an option (REQ-2193)."""

# Requirements: REQ-2193

from __future__ import annotations


class QueryOptionRefused(ValueError):
    """A system query option the service does not take; ``status`` is the answer."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
