# Copyright (c) 2025 Kenneth Stott
# Canary: 772c0581-f552-4b0e-9aef-64b30ff1b88c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for secrets provider."""

import os

import pytest

from provisa.core.secrets import resolve_secrets, resolve_secrets_in_dict


class TestResolveSecrets:
    def test_env_var_resolved(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "hunter2")
        assert resolve_secrets("${env:MY_SECRET}") == "hunter2"

    def test_env_var_in_string(self, monkeypatch):
        monkeypatch.setenv("DB_PASS", "s3cret")
        result = resolve_secrets("password=${env:DB_PASS}")
        assert result == "password=s3cret"

    def test_multiple_refs_resolved(self, monkeypatch):
        monkeypatch.setenv("USER", "admin")
        monkeypatch.setenv("PASS", "pw")
        result = resolve_secrets("${env:USER}:${env:PASS}")
        assert result == "admin:pw"

    def test_missing_env_var_raises(self):
        with pytest.raises(KeyError, match="NONEXISTENT_VAR_12345"):
            resolve_secrets("${env:NONEXISTENT_VAR_12345}")

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown secrets provider: vault"):
            resolve_secrets("${vault:secret/path}")

    def test_no_pattern_passthrough(self):
        assert resolve_secrets("plain_text") == "plain_text"

    def test_empty_string_passthrough(self):
        assert resolve_secrets("") == ""


class TestResolveSecretsInDict:
    def test_nested_dict(self, monkeypatch):
        monkeypatch.setenv("PW", "secret")
        data = {
            "host": "localhost",
            "auth": {"password": "${env:PW}"},
            "port": 5432,
        }
        result = resolve_secrets_in_dict(data)
        assert result["auth"]["password"] == "secret"
        assert result["host"] == "localhost"
        assert result["port"] == 5432

    def test_list_in_dict(self, monkeypatch):
        monkeypatch.setenv("TOKEN", "abc")
        data = {"tokens": ["${env:TOKEN}", "literal"]}
        result = resolve_secrets_in_dict(data)
        assert result["tokens"] == ["abc", "literal"]

    def test_list_of_dicts(self, monkeypatch):
        monkeypatch.setenv("PW", "x")
        data = {"sources": [{"password": "${env:PW}"}]}
        result = resolve_secrets_in_dict(data)
        assert result["sources"][0]["password"] == "x"
