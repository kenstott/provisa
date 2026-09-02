# Copyright (c) 2026 Kenneth Stott
# Canary: 113c5533-03e3-4fc8-84b2-401ad333c798
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Warehouse/federation connection configuration knobs.

Requirements:
  REQ-1076 — Databricks federation TLS trust is env-configurable (for TLS-intercepting proxies).
  REQ-1077 — MSSQL (Synapse/Fabric) login timeout is env-configurable (serverless pool wake-up).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from provisa.federation.databricks_tls import databricks_tls_kwargs

_TLS_ENV = (
    "DATABRICKS_TLS_NO_VERIFY",
    "DATABRICKS_TLS_CA_FILE",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_FILE",
)


@pytest.fixture
def _clean_tls_env(monkeypatch):
    for k in _TLS_ENV:
        monkeypatch.delenv(k, raising=False)
    return monkeypatch


@pytest.fixture
def _bundles(tmp_path):
    """Three CA bundle paths that exist — the resolver rejects a path that names no file."""
    paths = {}
    for name in ("explicit", "requests", "ssl"):
        p = tmp_path / f"{name}.pem"
        p.write_text("")
        paths[name] = str(p)
    return paths


class TestDatabricksTlsKwargs:  # REQ-1076
    def test_default_is_connector_default(self, _clean_tls_env):
        # No env set → no TLS overrides → connector keeps its default verified behavior.
        assert databricks_tls_kwargs() == {}

    def test_no_verify_disables_verification(self, _clean_tls_env):
        _clean_tls_env.setenv("DATABRICKS_TLS_NO_VERIFY", "1")
        assert databricks_tls_kwargs() == {"_tls_no_verify": True}

    def test_no_verify_wins_over_ca_file(self, _clean_tls_env):
        # Highest precedence: NO_VERIFY short-circuits even when a CA file is also set.
        _clean_tls_env.setenv("DATABRICKS_TLS_NO_VERIFY", "1")
        _clean_tls_env.setenv("DATABRICKS_TLS_CA_FILE", "/ca.pem")
        assert databricks_tls_kwargs() == {"_tls_no_verify": True}

    def test_ca_file_beats_conventional_bundles(self, _clean_tls_env, _bundles):
        _clean_tls_env.setenv("DATABRICKS_TLS_CA_FILE", _bundles["explicit"])
        _clean_tls_env.setenv("REQUESTS_CA_BUNDLE", _bundles["requests"])
        _clean_tls_env.setenv("SSL_CERT_FILE", _bundles["ssl"])
        assert databricks_tls_kwargs() == {"_tls_trusted_ca_file": _bundles["explicit"]}

    def test_requests_ca_bundle_is_next(self, _clean_tls_env, _bundles):
        _clean_tls_env.setenv("REQUESTS_CA_BUNDLE", _bundles["requests"])
        _clean_tls_env.setenv("SSL_CERT_FILE", _bundles["ssl"])
        assert databricks_tls_kwargs() == {"_tls_trusted_ca_file": _bundles["requests"]}

    def test_ssl_cert_file_is_last_resort(self, _clean_tls_env, _bundles):
        _clean_tls_env.setenv("SSL_CERT_FILE", _bundles["ssl"])
        assert databricks_tls_kwargs() == {"_tls_trusted_ca_file": _bundles["ssl"]}

    def test_a_bundle_path_that_does_not_exist_names_itself(self, _clean_tls_env, tmp_path):
        # The connector loads the path inside its HTTPS transport and raises a FileNotFoundError
        # naming nothing, so a stale bundle path took every Databricks test down with an
        # unexplained connect failure. The variable and the path both have to be in the message.
        missing = str(tmp_path / "gone.pem")
        _clean_tls_env.setenv("DATABRICKS_TLS_CA_FILE", missing)
        with pytest.raises(FileNotFoundError) as exc:
            databricks_tls_kwargs()
        assert missing in str(exc.value)
        assert "DATABRICKS_TLS_CA_FILE" in str(exc.value)

    def test_no_verify_only_on_exact_1(self, _clean_tls_env):
        # A truthy-looking but non-"1" value does not disable verification (fail safe).
        _clean_tls_env.setenv("DATABRICKS_TLS_NO_VERIFY", "true")
        assert databricks_tls_kwargs() == {}


class TestMssqlLoginTimeout:  # REQ-1077
    async def _connect_capture_timeout(self, monkeypatch, env_value):
        """Drive the driver's connect() with pyodbc mocked; return the login timeout it passed."""
        from provisa.executor.drivers.mssql_warehouse import MssqlWarehouseDriver

        if env_value is None:
            monkeypatch.delenv("PROVISA_MSSQL_LOGIN_TIMEOUT", raising=False)
        else:
            monkeypatch.setenv("PROVISA_MSSQL_LOGIN_TIMEOUT", env_value)

        driver = MssqlWarehouseDriver()
        monkeypatch.setattr(driver, "_token", lambda: "fake-token")
        fake_pyodbc = MagicMock()
        with patch.dict("sys.modules", {"pyodbc": fake_pyodbc}):
            await driver.connect("host", 1433, "db", "user", "pw")
        return fake_pyodbc.connect.call_args.kwargs["timeout"]

    @pytest.mark.asyncio
    async def test_defaults_to_120s(self, monkeypatch):
        assert await self._connect_capture_timeout(monkeypatch, None) == 120

    @pytest.mark.asyncio
    async def test_env_overrides_login_timeout(self, monkeypatch):
        assert await self._connect_capture_timeout(monkeypatch, "300") == 300
