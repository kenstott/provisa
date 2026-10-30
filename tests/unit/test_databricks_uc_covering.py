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


class _Resp:
    def __init__(self, status: int, headers: dict | None = None) -> None:
        self.status_code, self.headers = status, headers or {}

    def json(self):
        return {"external_locations": []}


def _uc_over(monkeypatch, statuses: list, slept: list):
    """A real ``_UC`` whose HTTP layer answers ``statuses`` in turn; sleeps are recorded."""
    import requests

    answers = iter(statuses)
    calls: list[tuple[str, str]] = []

    def fake_request(method, url, **kw):
        calls.append((method, url))
        return next(answers)

    monkeypatch.setattr(requests, "request", fake_request)
    return databricks_uc._UC("h", "t", sleep=slept.append), calls


def test_a_429_with_retry_after_waits_that_long_then_succeeds(monkeypatch):
    slept: list[float] = []
    uc, calls = _uc_over(monkeypatch, [_Resp(429, {"Retry-After": "7"}), _Resp(200)], slept)
    assert uc.get("/x")[0] == HTTPStatus.OK
    assert slept == [7.0] and len(calls) == 2


def test_a_429_without_retry_after_backs_off_then_succeeds(monkeypatch):
    slept: list[float] = []
    uc, _ = _uc_over(monkeypatch, [_Resp(429), _Resp(429), _Resp(200)], slept)
    assert uc.post("/x", {})[0] == HTTPStatus.OK
    assert slept == [1.0, 2.0]


def test_exhausted_retries_raise_naming_the_call(monkeypatch):
    slept: list[float] = []
    uc, _ = _uc_over(monkeypatch, [_Resp(429)] * 6, slept)
    with pytest.raises(ExternalLinkError, match="GET /api/x"):
        uc.get("/api/x")
    assert slept == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_a_non_429_error_is_returned_at_once(monkeypatch):
    slept: list[float] = []
    uc, calls = _uc_over(monkeypatch, [_Resp(500)], slept)
    assert uc.get("/x")[0] == 500
    assert slept == [] and len(calls) == 1


def test_a_throttled_page_is_waited_out_and_every_page_is_read(monkeypatch):
    class Page(_Resp):
        def __init__(self, url: str, token: str | None) -> None:
            super().__init__(200)
            self.body = {"external_locations": [{"name": "l", "url": url}]}
            if token:
                self.body["next_page_token"] = token

        def json(self):
            return self.body

    covering = f"{_ROOT}/provisa_ext_link_e2e_852309f0"
    slept: list[float] = []
    uc, calls = _uc_over(
        monkeypatch,
        [Page("r2://other@a/x", "t1"), _Resp(429), Page(covering, None)],
        slept,
    )
    assert databricks_uc._covering_location(uc, _PATH) == covering
    assert slept == [1.0] and "page_token=t1" in calls[-1][1]
