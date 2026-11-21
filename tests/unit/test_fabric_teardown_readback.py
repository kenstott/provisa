# Copyright (c) 2026 Kenneth Stott
# Canary: f4f382aa-0d89-42ce-9ed3-4532b58b0b67
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fabric teardown item read-back: 401/403 count as gone only once the workspace answers 404."""

import pytest

from tests.integration import fabric_capacity as fc


class _Resp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


class _Fabric:
    def __init__(self, workspace, item):
        self._by_suffix = {"/ws": workspace, "/ws/warehouses/wh": item}

    def get(self, url):
        return _Resp(*self._by_suffix[url.removeprefix(fc._FABRIC_API)])


def _read(workspace, item):
    return fc._item_gone(_Fabric(workspace, item), "warehouse", "/ws", "/ws/warehouses/wh")


def test_workspace_404_item_401_is_gone():
    assert _read((404,), (401, '{"errorCode":"Unauthorized"}')) is True


def test_workspace_404_item_403_is_gone():
    assert _read((404,), (403,)) is True


def test_item_404_is_gone():
    assert _read((200,), (404,)) is True


def test_item_200_is_not_gone():
    assert _read((200,), (200,)) is False


def test_workspace_200_item_401_raises():
    with pytest.raises(RuntimeError, match="HTTP 401.*workspace read back: HTTP 200"):
        _read((200,), (401, '{"errorCode":"Unauthorized"}'))


def test_other_status_raises():
    with pytest.raises(RuntimeError, match="HTTP 500"):
        _read((404,), (500, "boom"))
