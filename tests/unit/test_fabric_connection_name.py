# Copyright (c) 2026 Kenneth Stott
# Canary: be01d6c6-c7d8-4412-8e74-def3c383b317
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Fabric connection Provisa keeps for an S3-compatible endpoint is its workspace's own.

A connection's name is unique across a tenant, and a listing shows a principal only the
connections it can use. Named for the endpoint alone, a connection made under another principal
was not in the listing and collided on create: ``409 DuplicateConnectionName`` (warehouse run
37777052154, where the lane's service principal met a connection made from a developer's login).
The listing was also read one page deep."""

from __future__ import annotations

from http import HTTPStatus

import pytest

from provisa.federation import fabric_shortcuts as fs

_WS_A, _WS_B = "11111111-aaaa-4aaa-8aaa-111111111111", "22222222-bbbb-4bbb-8bbb-222222222222"
_ENDPOINT = "https://acct.r2.cloudflarestorage.com"


class _Api(fs._Fabric):  # noqa: SLF001
    """Fabric's REST API, answering from a script; the requests it was sent."""

    def __init__(self, answers: dict) -> None:  # no credential is fetched
        self.answers = answers
        self.sent: list[tuple[str, str, dict | None]] = []

    def _req(self, method, path, body=None):
        self.sent.append((method, path, body))
        answer = self.answers[(method, path)]
        return answer.pop(0) if isinstance(answer, list) else answer


def test_the_name_carries_the_endpoint_and_the_workspace():
    a, b = fs.connection_name(_WS_A, _ENDPOINT), fs.connection_name(_WS_B, _ENDPOINT)
    assert a != b  # two workspaces reaching one endpoint do not share a name
    assert a.startswith("provisa_acct_r2_cloudflarestorage_com_")
    assert a == fs.connection_name(_WS_A, _ENDPOINT)  # and it is the same on every call
    assert len(a.rsplit("_", 1)[1]) == 8


def test_a_long_endpoint_keeps_its_workspace_digest_within_the_limit():
    name = fs.connection_name(_WS_A, "https://" + "very-long-endpoint-name." * 8 + "example.com")
    other = fs.connection_name(_WS_B, "https://" + "very-long-endpoint-name." * 8 + "example.com")
    assert len(name) == 64 and name != other


def test_a_connection_this_workspace_already_has_is_reused():
    name = fs.connection_name(_WS_A, _ENDPOINT)
    api = _Api(
        {("GET", "/connections"): (HTTPStatus.OK, {"value": [{"displayName": name, "id": "c1"}]})}
    )
    assert api.ensure_connection(_WS_A, _ENDPOINT, "key", "secret") == "c1"
    assert [m for m, _p, _b in api.sent] == ["GET"]  # nothing was created


def test_the_listing_is_read_to_its_last_page():
    name = fs.connection_name(_WS_A, _ENDPOINT)
    api = _Api(
        {
            ("GET", "/connections"): (
                HTTPStatus.OK,
                {"value": [{"displayName": "other", "id": "x"}], "continuationToken": "p 2/+"},
            ),
            ("GET", "/connections?continuationToken=p%202%2F%2B"): (
                HTTPStatus.OK,
                {"value": [{"displayName": name, "id": "c2"}]},
            ),
        }
    )
    assert api.ensure_connection(_WS_A, _ENDPOINT, "key", "secret") == "c2"
    assert [m for m, _p, _b in api.sent] == ["GET", "GET"]


def test_another_workspaces_connection_for_the_endpoint_is_not_this_ones():
    """The other workspace's connection is in the listing, under its own name: this workspace
    creates its own rather than colliding with it."""
    api = _Api(
        {
            ("GET", "/connections"): (
                HTTPStatus.OK,
                {"value": [{"displayName": fs.connection_name(_WS_B, _ENDPOINT), "id": "theirs"}]},
            ),
            ("POST", "/connections"): (HTTPStatus.CREATED, {"id": "ours"}),
        }
    )
    assert api.ensure_connection(_WS_A, _ENDPOINT, "key", "secret") == "ours"
    _method, _path, body = api.sent[-1]
    assert body["displayName"] == fs.connection_name(_WS_A, _ENDPOINT)
    assert body["connectionDetails"]["parameters"][0]["value"] == _ENDPOINT


def test_a_name_someone_else_holds_is_refused_by_name():
    api = _Api(
        {
            ("GET", "/connections"): (HTTPStatus.OK, {"value": []}),
            ("POST", "/connections"): (
                HTTPStatus.CONFLICT,
                {"errorCode": "DuplicateConnectionName", "message": "already being used"},
            ),
        }
    )
    with pytest.raises(fs.FabricShortcutError) as refused:
        api.ensure_connection(_WS_A, _ENDPOINT, "key", "secret")
    message = str(refused.value)
    assert fs.connection_name(_WS_A, _ENDPOINT) in message
    assert "cannot use it" in message and "share that connection" in message


def test_any_other_refusal_to_create_is_reported_as_fabric_said_it():
    api = _Api(
        {
            ("GET", "/connections"): (HTTPStatus.OK, {"value": []}),
            ("POST", "/connections"): (HTTPStatus.BAD_REQUEST, {"errorCode": "InvalidInput"}),
        }
    )
    with pytest.raises(fs.FabricShortcutError, match=r"create failed \(400\).*InvalidInput"):
        api.ensure_connection(_WS_A, _ENDPOINT, "key", "secret")


def test_the_lakehouse_listing_is_read_to_its_last_page_too():
    items = f"/workspaces/{_WS_A}/items"
    api = _Api(
        {
            ("GET", items): (
                HTTPStatus.OK,
                {
                    "value": [{"type": "Warehouse", "displayName": "w", "id": "w1"}],
                    "continuationToken": "next",
                },
            ),
            ("GET", f"{items}?continuationToken=next"): (
                HTTPStatus.OK,
                {"value": [{"type": "Lakehouse", "displayName": "provisa_shortcuts", "id": "lh"}]},
            ),
        }
    )
    assert api.ensure_lakehouse(_WS_A) == "lh"
