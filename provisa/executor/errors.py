# Copyright (c) 2026 Kenneth Stott
# Canary: c6ccd9a6-21e0-4ebe-9eac-8c0410435836
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from __future__ import annotations

from typing import Optional

# Requirements: REQ-027, REQ-028, REQ-031


class FederationError(Exception):  # REQ-027, REQ-028, REQ-031
    """Federation-layer query error — wraps underlying engine errors."""

    def __init__(
        self,
        error_type: Optional[str],
        error_name: Optional[str],
        message: str,
        query_id: Optional[str] = None,
    ) -> None:
        self.error_type = error_type
        self.error_name = error_name
        self.message = message
        self.query_id = query_id

    def __repr__(self) -> str:
        return 'FederationError(type={}, name={}, message="{}", query_id={})'.format(
            self.error_type,
            self.error_name,
            self.message,
            self.query_id,
        )

    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def from_trino(cls, exc: Exception) -> "FederationError":  # REQ-028
        return cls(
            error_type=getattr(exc, "error_type", None),
            error_name=getattr(exc, "error_name", None),
            message=getattr(exc, "message", str(exc)),
            query_id=getattr(exc, "query_id", None),
        )
