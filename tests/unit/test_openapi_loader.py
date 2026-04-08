# Copyright (c) 2026 Kenneth Stott
# Canary: 424a42c2-14bd-4441-a66d-4f794fbb6b54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.openapi.loader."""

import json
import pathlib
import pytest
import httpx
import respx

from provisa.openapi.loader import load_spec

SAMPLE_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Test API", "version": "1.0.0"},
    "paths": {},
}

SAMPLE_YAML = "openapi: \"3.0.0\"\ninfo:\n  title: Test API\n  version: \"1.0.0\"\npaths: {}\n"
SAMPLE_JSON = json.dumps(SAMPLE_SPEC)


def test_load_local_yaml(tmp_path: pathlib.Path):
    f = tmp_path / "spec.yaml"
    f.write_text(SAMPLE_YAML)
    result = load_spec(str(f))
    assert result["openapi"] == "3.0.0"
    assert result["info"]["title"] == "Test API"


def test_load_local_json(tmp_path: pathlib.Path):
    f = tmp_path / "spec.json"
    f.write_text(SAMPLE_JSON)
    result = load_spec(str(f))
    assert result["openapi"] == "3.0.0"


@respx.mock
def test_load_remote_yaml():
    url = "https://example.com/api/spec.yaml"
    respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_YAML))
    result = load_spec(url)
    assert result["openapi"] == "3.0.0"


@respx.mock
def test_load_remote_json():
    url = "https://example.com/api/spec.json"
    respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_JSON))
    result = load_spec(url)
    assert result["openapi"] == "3.0.0"


def test_missing_local_path_raises():
    with pytest.raises(FileNotFoundError):
        load_spec("/nonexistent/path/to/spec.yaml")


@respx.mock
def test_http_404_raises():
    url = "https://example.com/api/missing.json"
    respx.get(url).mock(return_value=httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError):
        load_spec(url)
