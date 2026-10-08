# Copyright (c) 2026 Kenneth Stott
# Canary: 17fff03e-f7f5-458b-90d0-0a89fa251a80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from __future__ import annotations

import re
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
    def from_engine_error(cls, exc: Exception) -> "FederationError":  # REQ-028
        """Build a FederationError from any engine driver exception — duck-typed on the standard
        ``error_type``/``error_name``/``message``/``query_id`` attributes, so no engine-specific.

        One case is told apart: the engine not finding one of Provisa's own system catalogs
        (:class:`SystemCatalogUnavailable`)."""
        fields = {
            "error_type": getattr(exc, "error_type", None),
            "error_name": getattr(exc, "error_name", None),
            "message": getattr(exc, "message", str(exc)),
            "query_id": getattr(exc, "query_id", None),
        }
        catalog = _missing_system_catalog(fields["error_name"], fields["message"])
        if catalog is not None:
            return SystemCatalogUnavailable(catalog, **fields)
        return cls(**fields)


class SystemCatalogUnavailable(FederationError):
    """The engine does not, at this moment, have one of the catalogs Provisa itself registers on
    it (``provisa_admin``, ``otel``, ``results``).

    Those catalogs are created once and re-created only when their spec changes
    (``provisa/core/trino_system_catalogs.py``); a statement that arrives while one is between
    its drop and its create finds no catalog. That is the deployment's state, not the caller's
    mistake, and it passes: the statement is retried, and if the catalog is still absent when
    the request's time is up the answer is 503 with this code, not the engine's USER_ERROR. A
    SOURCE's catalog that is missing is not this: that stays the engine's own error."""

    code = "data.system_catalog_unavailable"

    def __init__(self, catalog: str, **fields: Optional[str]) -> None:
        super().__init__(
            fields["error_type"], fields["error_name"], fields["message"] or "", fields["query_id"]
        )
        self.catalog = catalog
        self.params = {"catalog": catalog}


# Trino: ``line 1:22: Catalog 'provisa_admin' not found``.
_CATALOG_NOT_FOUND = re.compile(r"Catalog '([^']+)' not found")


def _missing_system_catalog(error_name: Optional[str], message: str) -> Optional[str]:
    """The system catalog ``message`` says the engine lacks, or None for any other error."""
    if error_name != "CATALOG_NOT_FOUND":
        return None
    found = _CATALOG_NOT_FOUND.search(message)
    if found is None:
        return None
    from provisa.core.trino_system_catalogs import SYSTEM_CATALOGS

    return found.group(1) if found.group(1) in SYSTEM_CATALOGS else None
