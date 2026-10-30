# Copyright (c) 2026 Kenneth Stott
# Canary: 77973dd5-f2fe-4f2a-acc3-3cbe6a259e0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-987: an external link reuses an existing Unity Catalog external location only when that
location is a parent directory of the linked path -- never one whose URL merely starts with the
same characters -- and a listing Unity Catalog does not answer is an error, never "not covered".

Run 40869045831: a location left at ``.../provisa_ext_link_e2e`` was taken to cover
``.../provisa_ext_link_e2e_852309f0/orders.parquet``; no location was created, and the external
table's CREATE failed with NO_PARENT_EXTERNAL_LOCATION_FOR_PATH. Driver-free: the Unity Catalog
REST client is faked."""

# Requirements: REQ-987

from __future__ import annotations

from http import HTTPStatus

import pytest

from provisa.federation import databricks_uc
from provisa.federation.databricks_uc import ExternalLinkError, ensure_external_link

_ROOT = "r2://pubs@acct.r2.cloudflarestorage.com"
_PATH = f"{_ROOT}/provisa_ext_link_e2e_852309f0/orders.parquet"
_CRED = {"access_key_id": "k", "secret_access_key": "s", "account_id": "a"}


class _FakeUC:
    """Unity Catalog's REST API over ``pages`` of external locations (each a list of URLs)."""

    def __init__(self, pages: list[list[str]], *, list_status: int = HTTPStatus.OK) -> None:
        self.pages, self.list_status = pages, list_status
        self.calls: list[tuple[str, str]] = []

    def get(self, path: str):
        self.calls.append(("GET", path))
        if path.startswith("/api/2.1/unity-catalog/external-locations"):
            if self.list_status != HTTPStatus.OK:
                return self.list_status, {"error_code": "REQUEST_LIMIT_EXCEEDED"}
            index = int(path.split("page_token=")[1]) if "page_token=" in path else 0
            body: dict[str, object] = {
                "external_locations": [
                    {"name": f"loc{index}_{i}", "url": url}
                    for i, url in enumerate(self.pages[index])
                ]
            }
            if index + 1 < len(self.pages):
                body["next_page_token"] = str(index + 1)
            return HTTPStatus.OK, body
        return HTTPStatus.OK, {"name": path.rsplit("/", 1)[-1]}  # the storage credential exists

    def post(self, path: str, body: dict):
        self.calls.append(("POST", path))
        return HTTPStatus.OK, body


def _with(monkeypatch, uc: _FakeUC) -> _FakeUC:
    monkeypatch.setattr(databricks_uc, "_UC", lambda _host, _token: uc)
    return uc


def _posted(uc: _FakeUC) -> list[str]:
    return [path for method, path in uc.calls if method == "POST"]


def test_a_location_whose_url_only_shares_a_name_stem_does_not_cover_the_path(monkeypatch):
    uc = _with(monkeypatch, _FakeUC([[f"{_ROOT}/provisa_ext_link_e2e"]]))
    governed = ensure_external_link("h", "t", location=_PATH, credential=_CRED)
    assert governed == f"{_ROOT}/provisa_ext_link_e2e_852309f0/"
    assert _posted(uc) == ["/api/2.1/unity-catalog/external-locations"]


@pytest.mark.parametrize(
    "url",
    [f"{_ROOT}/provisa_ext_link_e2e_852309f0", f"{_ROOT}/provisa_ext_link_e2e_852309f0/", _ROOT],
)
def test_a_parent_directory_location_covers_the_path(monkeypatch, url):
    uc = _with(monkeypatch, _FakeUC([[url]]))
    assert ensure_external_link("h", "t", location=_PATH, credential=_CRED) == url
    assert _posted(uc) == []


def test_a_covering_location_on_a_later_page_is_found(monkeypatch):
    parent = f"{_ROOT}/provisa_ext_link_e2e_852309f0"
    uc = _with(monkeypatch, _FakeUC([[f"{_ROOT}/elsewhere"], [parent]]))
    assert ensure_external_link("h", "t", location=_PATH, credential=_CRED) == parent
    assert _posted(uc) == []


def test_a_listing_unity_catalog_does_not_answer_raises(monkeypatch):
    uc = _with(monkeypatch, _FakeUC([[]], list_status=HTTPStatus.TOO_MANY_REQUESTS))
    with pytest.raises(ExternalLinkError, match="429"):
        ensure_external_link("h", "t", location=_PATH, credential=_CRED)
    assert _posted(uc) == []
