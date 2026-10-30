# Copyright (c) 2026 Kenneth Stott
# Canary: 0e330a02-4afe-42d8-ae2a-b305a9a6d52b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Dataplex e2e's teardown (tests/integration/dataplex_catalog.py) against a fake Dataplex
that answers each delete as the real API does: an operation for products, assets and glossaries,
the deleted resource itself for entry links (REQ-254)."""

# Requirements: REQ-254, REQ-2118

from __future__ import annotations

import httpx
import pytest

from tests.integration import dataplex_catalog

PARENT = "projects/p1/locations/us"
GLOSSARY = f"{PARENT}/glossaries/provisa-org"
TERM = f"{GLOSSARY}/terms/order"
CATEGORY = f"{GLOSSARY}/categories/classes"
LINK = "projects/906/locations/us/entryGroups/@bigquery/entryLinks/provisa-8c39"
LINK2 = "projects/906/locations/us/entryGroups/@bigquery/entryLinks/provisa-77aa"
PRODUCT = f"{PARENT}/dataProducts/provisa-e2e-1"
ASSET = f"{PRODUCT}/dataAssets/orders"
# Deleted through an operation; an entry link is not.
_BY_OPERATION = ("/dataProducts/", "/glossaries/")


class FakeDataplex:
    """Resources by name; ``linger`` names stay after their delete; ``refuse`` answers a delete
    with that status; ``op_error`` ends a delete's operation in an error; ``throttle`` answers the
    first that many reads of an operation 429, with ``retry_after`` as its Retry-After."""

    def __init__(
        self,
        names,
        *,
        linger=(),
        refuse=None,
        op_error=None,
        link_page=1,
        throttle=0,
        retry_after=None,
    ):
        self.names = set(names)
        self.throttle = throttle
        self.retry_after = retry_after
        self.linger = set(linger)
        self.refuse = refuse
        self.op_error = op_error
        self.link_page = link_page
        self.operations: dict[str, int] = {}
        self.calls: list[tuple[str, str]] = []

    def _children(self, prefix: str) -> list[dict]:
        return [{"name": n} for n in sorted(self.names) if n.startswith(prefix)]

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        if request.url.host == "cloudresourcemanager.googleapis.com":
            return httpx.Response(200, json={"name": "projects/906"})
        name = path.removeprefix("/v1/")
        if request.method == "DELETE":
            if self.refuse:
                return httpx.Response(self.refuse, json={"error": {"code": self.refuse}})
            if name not in self.names:
                return httpx.Response(404, json={"error": {"code": 404}})
            if name not in self.linger:
                self.names.discard(name)
            if "/entryLinks/" in name:
                return httpx.Response(200, json={"name": name, "entryLinkType": "definition"})
            if (
                any(k in name for k in _BY_OPERATION)
                and "/terms/" not in name
                and ("/categories/" not in name)
            ):
                op = f"{PARENT}/operations/op-{len(self.operations)}"
                self.operations[op] = 0
                return httpx.Response(200, json={"name": op, "done": False})
            return httpx.Response(200, json={})
        if name in self.operations:
            if self.throttle:
                self.throttle -= 1
                headers = {"Retry-After": self.retry_after} if self.retry_after else {}
                return httpx.Response(429, headers=headers, json={"error": {"code": 429}})
            self.operations[name] += 1
            body: dict = {"name": name, "done": self.operations[name] >= 2}
            if body["done"] and self.op_error:
                body["error"] = {"code": 9, "message": self.op_error}
            return httpx.Response(200, json=body)
        if name.endswith(":lookupEntryLinks"):
            links = self._children("projects/906/locations/us/entryGroups/@bigquery/entryLinks/")
            start = int(request.url.params.get("pageToken", "0"))
            page = links[start : start + self.link_page]
            body = {"entryLinks": page}
            if start + self.link_page < len(links):
                body["nextPageToken"] = str(start + self.link_page)
            return httpx.Response(200, json=body)
        for listing, key in (
            ("/terms", "terms"),
            ("/categories", "categories"),
            ("/dataAssets", "dataAssets"),
        ):
            if name.endswith(listing):
                parent = name.removesuffix(listing)
                if parent not in self.names:
                    return httpx.Response(404, json={})
                return httpx.Response(200, json={key: self._children(name + "/")})
        if name in self.names:
            return httpx.Response(200, json={"name": name})
        return httpx.Response(404, json={"error": {"code": 404}})

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def deletes(self) -> list[str]:
        return [p.removeprefix("/v1/") for m, p in self.calls if m == "DELETE"]


def _no_sleep(_s: float) -> None:
    return None


@pytest.fixture(autouse=True)
def _instant_polls(monkeypatch):
    monkeypatch.setattr(dataplex_catalog.time, "sleep", _no_sleep)


@pytest.fixture
def waits(monkeypatch) -> list[float]:
    """Every wait but the operation poll's own interval, in order."""
    seen: list[float] = []

    def record(seconds: float) -> None:
        if seconds != dataplex_catalog._POLL_S:
            seen.append(seconds)

    monkeypatch.setattr(dataplex_catalog.time, "sleep", record)
    return seen


def test_an_entry_link_answered_with_itself_is_read_back_gone_not_polled():
    """Run 40910302654: entryLinks.delete answers with the deleted EntryLink. Its name is the
    link's own, not an operation's; polling it found the link gone (404) and failed the teardown
    of a link that had been deleted."""
    fake = FakeDataplex([LINK])
    with fake.client() as http:
        dataplex_catalog.delete(http, LINK)
    assert fake.calls == [("DELETE", f"/v1/{LINK}"), ("GET", f"/v1/{LINK}")]
    assert LINK not in fake.names


def test_a_delete_answered_with_an_operation_waits_for_it_then_reads_back():
    fake = FakeDataplex([PRODUCT])
    with fake.client() as http:
        dataplex_catalog.delete(http, PRODUCT)
    assert [m for m, _ in fake.calls] == ["DELETE", "GET", "GET", "GET"]
    assert fake.calls[-1] == ("GET", f"/v1/{PRODUCT}")


def test_an_operation_that_ends_in_an_error_raises():
    fake = FakeDataplex([PRODUCT], op_error="FAILED_PRECONDITION")
    with fake.client() as http, pytest.raises(RuntimeError, match="FAILED_PRECONDITION"):
        dataplex_catalog.delete(http, PRODUCT)


def test_a_resource_still_there_after_its_delete_raises():
    fake = FakeDataplex([LINK], linger=[LINK])
    with fake.client() as http, pytest.raises(RuntimeError, match="still exists"):
        dataplex_catalog.delete(http, LINK)


@pytest.mark.parametrize("status", [404, 403])
def test_a_refused_delete_raises_whatever_its_status(status):
    """Teardown deletes only what it found; a delete answered with an error, 404 included, is a
    failure, never the state the delete was for."""
    fake = FakeDataplex([LINK], refuse=status)
    with fake.client() as http, pytest.raises(httpx.HTTPStatusError, match=str(status)):
        dataplex_catalog.delete(http, LINK)


def test_a_glossary_that_does_not_exist_is_not_deleted():
    fake = FakeDataplex([])
    with fake.client() as http:
        dataplex_catalog.remove_glossary(http, PARENT, GLOSSARY)
    assert fake.deletes() == []


def test_the_glossary_goes_with_every_link_on_its_terms_then_its_terms_and_categories():
    """Links are read page by page: one past the first page is deleted too."""
    fake = FakeDataplex([GLOSSARY, TERM, CATEGORY, LINK, LINK2], link_page=1)
    with fake.client() as http:
        dataplex_catalog.remove_glossary(http, PARENT, GLOSSARY)
    assert fake.deletes() == [LINK2, LINK, TERM, CATEGORY, GLOSSARY]
    assert fake.names == set()


def test_the_term_entry_looked_up_names_the_project_by_number():
    fake = FakeDataplex([GLOSSARY, TERM])
    seen: list[str] = []
    handle = fake.handle

    def recording(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(":lookupEntryLinks"):
            seen.append(request.url.params["entry"])
        return handle(request)

    with httpx.Client(transport=httpx.MockTransport(recording)) as http:
        dataplex_catalog.remove_glossary(http, PARENT, GLOSSARY)
    assert seen == [
        "projects/906/locations/us/entryGroups/@dataplex/entries/"
        "projects/906/locations/us/glossaries/provisa-org/terms/order"
    ]


def test_a_product_that_does_not_exist_is_not_deleted():
    fake = FakeDataplex([])
    with fake.client() as http:
        dataplex_catalog.remove_product(http, PRODUCT)
    assert fake.deletes() == []


def test_a_product_goes_after_its_assets():
    fake = FakeDataplex([PRODUCT, ASSET])
    with fake.client() as http:
        dataplex_catalog.remove_product(http, PRODUCT)
    assert fake.deletes() == [ASSET, PRODUCT]
    assert fake.names == set()


def test_a_throttled_operation_poll_waits_as_long_as_dataplex_says_then_finishes(waits):
    """Run 40970287308: the teardown's operation poll answered 429 and failed the teardown of a
    delete that was under way. A 429 is Dataplex asking the caller to wait (Retry-After)."""
    fake = FakeDataplex([PRODUCT], throttle=2, retry_after="3")
    with fake.client() as http:
        dataplex_catalog.delete(http, PRODUCT)
    assert waits == [3.0, 3.0]
    assert PRODUCT not in fake.names


def test_a_throttled_read_without_retry_after_backs_off_1_2_4_8_16_then_raises(waits):
    fake = FakeDataplex([PRODUCT], throttle=99)
    with fake.client() as http, pytest.raises(httpx.HTTPStatusError, match="429"):
        dataplex_catalog.delete(http, PRODUCT)
    assert waits == [1.0, 2.0, 4.0, 8.0, 16.0]
