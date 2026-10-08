# Copyright (c) 2026 Kenneth Stott
# Canary: c98a1d59-64f2-4611-aac5-75be7e97c652
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The engine not finding one of Provisa's own catalogs is the deployment's state, to be retried;
not finding a SOURCE's catalog stays the engine's own error (REQ-1429)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.executor.errors import FederationError, SystemCatalogUnavailable
from provisa.executor.trino import _is_retryable


def _engine_error(message: str, name: str = "CATALOG_NOT_FOUND") -> Exception:
    return SimpleNamespace(error_type="USER_ERROR", error_name=name, message=message, query_id="q1")  # type: ignore[return-value]


@pytest.mark.parametrize("catalog", ["provisa_admin", "otel", "results"])
def test_a_missing_system_catalog_is_named_and_retried(catalog):
    wrapped = FederationError.from_engine_error(
        _engine_error(f"line 1:22: Catalog '{catalog}' not found")
    )
    assert isinstance(wrapped, SystemCatalogUnavailable)
    assert (wrapped.code, wrapped.params) == (
        "data.system_catalog_unavailable",
        {"catalog": catalog},
    )
    assert wrapped.error_name == "CATALOG_NOT_FOUND" and wrapped.query_id == "q1"
    assert _is_retryable(wrapped)


def test_a_missing_source_catalog_stays_the_engines_own_error():
    wrapped = FederationError.from_engine_error(
        _engine_error("line 1:15: Catalog 'org_acme__sales_pg' not found")
    )
    assert type(wrapped) is FederationError
    assert not _is_retryable(wrapped)


def test_another_error_that_mentions_a_system_catalog_is_not_this():
    wrapped = FederationError.from_engine_error(
        _engine_error("Table 'provisa_admin.public.t' does not exist", name="TABLE_NOT_FOUND")
    )
    assert type(wrapped) is FederationError
