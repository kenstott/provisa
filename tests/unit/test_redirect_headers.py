# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8b2d61-3a9c-4e17-b5d0-8c6e1a7f2b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A forced redirect asked for over HTTP (REQ-1194): the X-Provisa-Redirect headers.

The format a request names is read as a format name or its media type and refused by name when it
is neither. It used to go through the Accept-header parser, which knows only media types and
answers ``json`` for anything else, so ``X-Provisa-Redirect-Format: parquet`` asked for a JSON
delivery the results store cannot write."""

# Requirements: REQ-029, REQ-1194

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.errors import ApiError
from provisa.executor.redirect import RedirectFormatUnknown, parse_redirect_format


@pytest.mark.parametrize(
    ("named", "fmt"),
    [
        ("parquet", "parquet"),
        ("PARQUET", "parquet"),
        ("application/vnd.apache.parquet", "parquet"),
        ("orc", "orc"),
        ("application/x-orc", "orc"),
    ],
)
def test_a_format_is_read_by_name_or_media_type(named, fmt):
    assert parse_redirect_format(named) == fmt


def test_a_format_that_is_none_is_refused_by_name():
    with pytest.raises(RedirectFormatUnknown, match="'parquett'"):
        parse_redirect_format("parquett")


def _delivery(headers: dict):
    from provisa.api.redirect_headers import delivery_from_headers

    return delivery_from_headers(headers, "analyst")


def test_the_headers_force_a_delivery_in_the_format_they_name(monkeypatch):
    monkeypatch.setattr(
        "provisa.executor.redirect.request_redirect_config",
        lambda threshold: SimpleNamespace(default_format=None, threshold=threshold),
    )
    delivery = _delivery({"x-provisa-redirect": "true", "x-provisa-redirect-format": "orc"})
    assert delivery is not None and (delivery.output_format, delivery.role) == ("orc", "analyst")
    assert _delivery({}) is None
    assert _delivery({"x-provisa-redirect-format": "parquet"}) is None  # no force, no delivery


@pytest.mark.parametrize(
    ("headers", "code"),
    [
        (
            {"x-provisa-redirect": "true", "x-provisa-redirect-format": "xlsx"},
            "invalid_redirect_format",
        ),
        (
            {"x-provisa-redirect": "true", "x-provisa-redirect-threshold": "many"},
            "invalid_redirect_threshold",
        ),
    ],
)
def test_a_header_that_cannot_be_read_is_refused(headers, code):
    with pytest.raises(ApiError) as refused:
        _delivery(headers)
    assert (refused.value.status_code, refused.value.code) == (400, f"data.{code}")


def test_graphql_reads_the_format_header_by_name_too():
    from provisa.api.data.endpoint_helpers import _build_redirect_params

    directives = SimpleNamespace(redirect_format=None, redirect_threshold=None)
    fmt, threshold, force = _build_redirect_params("true", None, "parquet", directives)
    assert (fmt, threshold, force) == ("parquet", None, True)
    with pytest.raises(ApiError):
        _build_redirect_params("true", None, "xlsx", directives)
