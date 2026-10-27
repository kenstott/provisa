# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3d7e21-4c8a-4f6b-9e2d-7a5c8b1f6e93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1819: _stage_kaggle_if_needed — the one place a Kaggle-derived source's files get
downloaded, called from inside create_source itself so every creation path (the admin form's
createSource call, MCP chat's create_source_now, and a queued propose_source request executed on
Requests-page approval) is guaranteed staged files, never a caller-trusted path."""

from __future__ import annotations

import json

import pytest

from provisa.api.admin.schema_common import _stage_kaggle_if_needed
from provisa.api.admin.types import SourceInput

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


def _source(
    federation_hints_json: str | None,
    path: str | None = "whatever-the-caller-sent",
    password: str = "${secret:my_kaggle}",
) -> SourceInput:
    # The source's credential field holds the Kaggle token as the operator entered it: here a
    # reference they typed, resolved at use.
    return SourceInput(
        id="kaggle_src",
        type="files",
        path=path,
        password=password,
        federation_hints_json=federation_hints_json,
    )


class TestNoKaggleHints:
    async def test_no_hints_is_a_noop(self, monkeypatch):
        called = False

        async def fake_stage_dataset(token, owner, ref):
            nonlocal called
            called = True
            raise AssertionError("stage_dataset must not be called without kaggle hints")

        monkeypatch.setattr("provisa.kaggle.downloader.stage_dataset", fake_stage_dataset)

        source = _source(federation_hints_json=None)
        result = await _stage_kaggle_if_needed(source)

        assert result is None
        assert not called
        assert source.path == "whatever-the-caller-sent"

    async def test_hints_without_kaggle_fields_is_a_noop(self, monkeypatch):
        source = _source(federation_hints_json=json.dumps({"account": "xyz"}))
        result = await _stage_kaggle_if_needed(source)
        assert result is None
        assert source.path == "whatever-the-caller-sent"


class TestKaggleStaging:
    async def test_stages_and_overwrites_path_regardless_of_caller_input(self, monkeypatch):
        seen = {}

        async def fake_stage_dataset(token, owner, ref):
            seen["token"] = token
            seen["owner"] = owner
            seen["ref"] = ref
            from pathlib import Path

            return Path("/data/kaggle/camnugent/sandp500")

        monkeypatch.setattr("provisa.kaggle.downloader.stage_dataset", fake_stage_dataset)
        resolved: list[str] = []

        def fake_resolve(value):
            resolved.append(value)
            return "KGAT_the_real_token"

        monkeypatch.setattr("provisa.core.secrets.resolve_secrets", fake_resolve)

        source = _source(
            federation_hints_json=json.dumps(
                {"kaggle_owner": "camnugent", "kaggle_ref": "sandp500"}
            ),
            path="a-path-the-model-guessed-per-old-staging-convention",
        )
        result = await _stage_kaggle_if_needed(source)

        assert result is None
        assert seen == {
            "token": "KGAT_the_real_token",
            "owner": "camnugent",
            "ref": "sandp500",
        }
        # The source's own token, as entered: never the operator setting's.
        assert resolved == ["${secret:my_kaggle}"]
        # The caller's own path is NEVER trusted for a Kaggle-hinted source — this is the whole
        # point (REQ-1819): no path can skip staging by pre-filling a plausible-looking directory.
        assert source.path == "/data/kaggle/camnugent/sandp500"

    async def test_unsupported_dataset_fails_without_creating_the_source(self, monkeypatch):
        from provisa.kaggle.downloader import UnsupportedKaggleDataset

        async def fake_stage_dataset(token, owner, ref):
            raise UnsupportedKaggleDataset("bundle contains a .sqlite file")

        monkeypatch.setattr("provisa.kaggle.downloader.stage_dataset", fake_stage_dataset)
        monkeypatch.setattr("provisa.core.secrets.resolve_secrets", lambda ref: "token")

        source = _source(federation_hints_json=json.dumps({"kaggle_owner": "x", "kaggle_ref": "y"}))
        result = await _stage_kaggle_if_needed(source)

        assert result is not None
        assert result.success is False
        assert "sqlite" in result.message
        assert source.path == "whatever-the-caller-sent"

    async def test_unexpected_staging_error_fails_closed(self, monkeypatch):
        async def fake_stage_dataset(token, owner, ref):
            raise RuntimeError("Kaggle API returned 401")

        monkeypatch.setattr("provisa.kaggle.downloader.stage_dataset", fake_stage_dataset)
        monkeypatch.setattr("provisa.core.secrets.resolve_secrets", lambda ref: "token")

        source = _source(federation_hints_json=json.dumps({"kaggle_owner": "x", "kaggle_ref": "y"}))
        result = await _stage_kaggle_if_needed(source)

        assert result is not None
        assert result.success is False
        assert "401" in result.message
        assert source.path == "whatever-the-caller-sent"

    async def test_a_kaggle_source_with_no_token_is_refused_by_name(self, monkeypatch):
        async def fake_stage_dataset(token, owner, ref):
            raise AssertionError("nothing is downloaded without the source's token")

        monkeypatch.setattr("provisa.kaggle.downloader.stage_dataset", fake_stage_dataset)
        source = _source(
            federation_hints_json=json.dumps({"kaggle_owner": "x", "kaggle_ref": "y"}), password=""
        )
        result = await _stage_kaggle_if_needed(source)

        assert result is not None and result.success is False
        assert result.code == "schema.kaggle_token_missing"
        assert result.params == {"owner": "x", "ref": "y"}
        assert source.path == "whatever-the-caller-sent"


def test_the_kaggle_token_setting_names_no_secret_by_default(monkeypatch):
    """Only the assistant's tool, which has no form, reads the setting; unset unless the operator
    sets it, it names no secret nobody created."""
    from provisa.core import settings_registry
    from provisa.kaggle import downloader

    monkeypatch.delenv("PROVISA_KAGGLE_API_TOKEN", raising=False)
    assert settings_registry.setting("kaggle.api_token").default is None
    if settings_registry.value("kaggle.api_token") is None:
        with pytest.raises(downloader.KaggleTokenUnset, match="kaggle.api_token"):
            downloader.api_token()


def _refresh(monkeypatch, password_ref: str | None):
    """refreshKaggleSource for a Kaggle source whose credential field holds ``password_ref``."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock

    from provisa.api.admin import schema_mutation

    row = {
        "id": "kg",
        "path": None,
        "password_ref": password_ref,
        "federation_hints": {"kaggle_owner": "o", "kaggle_ref": "r"},
    }

    @asynccontextmanager
    async def _acquire():
        yield MagicMock()

    pool = MagicMock()
    pool.acquire = _acquire
    monkeypatch.setattr(schema_mutation, "_get_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr("provisa.api.admin.capabilities.require_capability", lambda *a: None)
    monkeypatch.setattr("provisa.core.repositories.source.get", AsyncMock(return_value=row))
    monkeypatch.setattr("provisa.federation.pgwire_replica.stop_endpoint", lambda sid: None)

    @asynccontextmanager
    async def _bound():
        yield

    monkeypatch.setattr("provisa.core.secrets_store.bound_to_request_org", _bound)
    seen: dict = {}

    async def fake_stage_dataset(token, owner, ref):
        seen["token"] = token

    monkeypatch.setattr("provisa.kaggle.downloader.stage_dataset", fake_stage_dataset)
    monkeypatch.setattr("provisa.core.secrets.resolve_secrets", lambda value: f"resolved({value})")
    info = MagicMock()
    info.context = {}
    return schema_mutation.Mutation(), info, seen


async def test_a_refresh_downloads_with_the_source_s_own_token(monkeypatch):
    mutation, info, seen = _refresh(monkeypatch, "${secret:my_kaggle}")
    result = await mutation.refresh_kaggle_source(info, "kg")
    assert result.success, result.message
    assert seen == {"token": "resolved(${secret:my_kaggle})"}


async def test_a_refresh_of_a_source_with_no_token_is_refused_by_name(monkeypatch):
    mutation, info, seen = _refresh(monkeypatch, "")
    result = await mutation.refresh_kaggle_source(info, "kg")
    assert result.success is False and result.code == "schema.kaggle_token_missing"
    assert "Sources form" in result.message
    assert seen == {}
