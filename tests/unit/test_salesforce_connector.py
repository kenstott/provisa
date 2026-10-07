# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1f0a7e-93d4-4b6a-8e21-7f4d2c9b0a16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the Salesforce source type (REQ-1946): the pgwire server's model operand for
each credential set and each refusal, the Trino catalog properties, the per-engine connectors,
the bundle name and the federation strategy."""

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_base import Mechanism
from provisa.federation.engine import (
    build_clickhouse_engine,
    build_duckdb_engine,
    build_pg_engine,
    live_source_types,
    reachable_source_types,
)
from provisa.federation.strategy import Strategy, engine_attaches, federate
from provisa.federation.trino_connectors import TRINO_CONNECTORS, trino_connector_name
from provisa.runtime_deps import pgwire_bundles as rd

LOGIN_URL = "https://acme.my.salesforce.com"


def _source(**kw) -> Source:
    return Source(
        **{
            "id": "sf-sales",
            "type": SourceType.salesforce,
            "base_url": LOGIN_URL,
            "username": "consumer-key",  # clientId
            "password": "consumer-secret",  # clientSecret
            **kw,
        }
    )


def _operand(source: Source, state_dir: Path) -> dict:
    return pr.build_model_json(source, state_dir=state_dir)["schemas"][0]["operand"]


# -- the pgwire server's model ---------------------------------------------------


def test_model_names_the_salesforce_schema_factory(tmp_path):
    model = pr.build_model_json(_source(), state_dir=tmp_path)
    schema = model["schemas"][0]
    assert schema["factory"] == "org.apache.calcite.adapter.salesforce.SalesforceSchemaFactory"
    assert schema["name"] == model["defaultSchema"] == "sf_sales"


def test_client_credentials_operand(tmp_path):
    operand = _operand(_source(), tmp_path)
    assert operand["loginUrl"] == LOGIN_URL
    assert operand["clientId"] == "consumer-key"
    assert operand["clientSecret"] == "consumer-secret"
    assert "username" not in operand and "accessToken" not in operand


def test_login_url_may_ride_in_host(tmp_path):
    operand = _operand(_source(base_url=None, host=LOGIN_URL), tmp_path)
    assert operand["loginUrl"] == LOGIN_URL


@pytest.mark.parametrize(
    "login_url",
    ["acme.my.salesforce.com", "http://acme.my.salesforce.com", "https://"],
)
def test_a_login_url_that_is_not_the_full_https_url_is_refused_by_both_readers(tmp_path, login_url):
    # The adapter appends the token path to the URL as written; a bare host fails there as an
    # unknown URL. The scheme is never added for the steward.
    source = _source(base_url=login_url)
    with pytest.raises(pr.MissingConnectorConfig, match=r"loginUrl must be the full https://"):
        _operand(source, tmp_path)
    with pytest.raises(pr.MissingConnectorConfig, match=r"loginUrl must be the full https://"):
        _props(source)


def test_a_missing_login_url_is_refused_by_both_readers(tmp_path):
    source = _source(base_url=None)
    with pytest.raises(pr.MissingConnectorConfig, match="requires loginUrl"):
        _operand(source, tmp_path)
    with pytest.raises(pr.MissingConnectorConfig, match="requires loginUrl"):
        _props(source)


def test_username_password_operand_carries_the_optional_security_token(tmp_path):
    src = _source(
        mapping={
            "auth_type": "USERNAME_PASSWORD",
            "sf_username": "ops@acme.com",
            "sf_password": "pw",
            "security_token": "tok",
        }
    )
    operand = _operand(src, tmp_path)
    assert operand["username"] == "ops@acme.com"
    assert operand["password"] == "pw"
    assert operand["securityToken"] == "tok"
    assert operand["clientId"] == "consumer-key"
    assert operand["clientSecret"] == "consumer-secret"


def test_username_password_operand_without_a_security_token(tmp_path):
    src = _source(
        mapping={"auth_type": "USERNAME_PASSWORD", "sf_username": "u", "sf_password": "p"}
    )
    assert "securityToken" not in _operand(src, tmp_path)


def test_access_token_operand_carries_no_connected_app(tmp_path):
    src = _source(
        username="",
        password="",
        mapping={
            "auth_type": "ACCESS_TOKEN",
            "access_token": "00D...",
            "instance_url": "https://acme.my.salesforce.com",
        },
    )
    operand = _operand(src, tmp_path)
    assert operand["accessToken"] == "00D..."
    assert operand["instanceUrl"] == "https://acme.my.salesforce.com"
    assert "clientId" not in operand and "clientSecret" not in operand


def test_api_version_is_carried_only_when_set(tmp_path):
    assert "apiVersion" not in _operand(_source(), tmp_path)
    assert _operand(_source(mapping={"api_version": "v61.0"}), tmp_path)["apiVersion"] == "v61.0"


def test_each_sobject_is_one_table_under_its_own_name(tmp_path):
    # The adapter also registers a lower-case alias per sObject unless told not to; an engine that
    # imports the whole schema would then hold every sObject twice.
    assert _operand(_source(), tmp_path)["lowercaseAliases"] is False


def test_describe_cache_lives_in_the_servers_own_state_directory(tmp_path):
    operand = _operand(_source(), tmp_path)
    assert operand["describeCacheDirectory"] == str(tmp_path / "describe-cache")


def test_a_salesforce_model_without_a_state_directory_is_refused():
    with pytest.raises(pr.MissingConnectorConfig, match="state directory"):
        pr.build_model_json(_source())


def test_secrets_are_resolved_through_the_secrets_contract(tmp_path, monkeypatch):
    monkeypatch.setenv("SF_TEST_SECRET", "from-env")
    operand = _operand(_source(password="${env:SF_TEST_SECRET}"), tmp_path)
    assert operand["clientSecret"] == "from-env"


@pytest.mark.parametrize(
    ("overrides", "names"),
    [
        ({"base_url": None, "host": ""}, "loginUrl"),
        ({"username": ""}, "consumer key"),
        ({"password": ""}, "consumer secret"),
        ({"mapping": {"auth_type": "USERNAME_PASSWORD", "sf_password": "p"}}, "username"),
        ({"mapping": {"auth_type": "USERNAME_PASSWORD", "sf_username": "u"}}, "password"),
        (
            {
                "mapping": {
                    "auth_type": "USERNAME_PASSWORD",
                    "sf_username": "u",
                    "sf_password": "p",
                },
                "password": "",
            },
            "consumer secret",
        ),
        ({"mapping": {"auth_type": "ACCESS_TOKEN", "instance_url": "https://x"}}, "access token"),
        ({"mapping": {"auth_type": "ACCESS_TOKEN", "access_token": "t"}}, "instance URL"),
        ({"mapping": {"auth_type": "SAML"}}, "auth_type"),
    ],
)
def test_an_incomplete_credential_set_is_refused_by_name(tmp_path, overrides, names):
    with pytest.raises(pr.MissingConnectorConfig, match=names):
        pr.build_model_json(_source(**overrides), state_dir=tmp_path)


def test_the_running_server_is_given_its_own_state_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    bundle = tmp_path / "bundle"
    (bundle / "bin").mkdir(parents=True)

    class _Resolver:
        def resolve(self, spec):
            return bundle

    spawned: list[Path] = []

    class _Proc:
        def terminate(self): ...

        def wait(self, timeout): ...

        def exit_code(self):
            return None

    def _spawn(command, cwd):
        spawned.append(cwd)
        return _Proc()

    replica = pr.ConnectorReplica(
        _source(),
        resolver=_Resolver(),  # pyright: ignore[reportArgumentType]
        spawn=_spawn,
        health_check=lambda host, port: True,
        port_is_free=lambda port: True,
    )
    replica.endpoint()
    state = spawned[0]
    assert tmp_path / "data" in state.parents
    import json

    operand = json.loads((state / "model" / "model.json").read_text())["schemas"][0]["operand"]
    assert operand["describeCacheDirectory"] == str(state / "describe-cache")
    replica.close()


# -- bundle ----------------------------------------------------------------------


def test_bundle_is_pgwire_salesforce():
    assert rd.bundle_spec_for("salesforce").artifact_name == "pgwire-salesforce"
    assert "salesforce" in pr.PGWIRE_REPLICA_TYPES


# -- Trino catalog -----------------------------------------------------------------


def _props(source: Source) -> dict[str, str]:
    return _build_catalog_properties(source, "")


def test_trino_connector_is_registered():
    assert "salesforce" in TRINO_CONNECTORS
    assert trino_connector_name("salesforce") == "salesforce"


def test_trino_client_credentials_props():
    props = _props(_source())
    assert props["login-url"] == LOGIN_URL
    assert props["client-id"] == "consumer-key"
    assert props["client-secret"] == "consumer-secret"
    assert props["schema"] == "sf_sales"


def test_trino_always_matches_names_case_insensitively():
    # The connector refuses to start without it: sObject names are mixed case.
    assert _props(_source())["case-insensitive-name-matching"] == "true"


def test_trino_keeps_each_sources_describe_results_on_the_volume_trino_keeps():
    # The plugin's own default is under the Trino user's home, lost with the container: every
    # restart would describe the whole org again, out of its daily API allowance.
    from provisa.federation.trino_connectors import SALESFORCE_DESCRIBE_CACHE_ROOT

    assert SALESFORCE_DESCRIBE_CACHE_ROOT.startswith("/data/trino/cache/")
    one = _props(_source())["describe-cache-directory"]
    assert one == f"{SALESFORCE_DESCRIBE_CACHE_ROOT}/sf_sales"
    other = _source()
    other.id = "sf-service"
    assert (
        _props(other)["describe-cache-directory"] == f"{SALESFORCE_DESCRIBE_CACHE_ROOT}/sf_service"
    )


def test_trino_username_password_props():
    props = _props(
        _source(
            mapping={
                "auth_type": "USERNAME_PASSWORD",
                "sf_username": "ops@acme.com",
                "sf_password": "pw",
                "security_token": "tok",
                "api_version": "v61.0",
            }
        )
    )
    assert props["username"] == "ops@acme.com"
    assert props["password"] == "pw"
    assert props["security-token"] == "tok"
    assert props["client-id"] == "consumer-key"
    assert props["api-version"] == "v61.0"


def test_trino_access_token_props():
    props = _props(
        _source(
            username="",
            password="",
            mapping={
                "auth_type": "ACCESS_TOKEN",
                "access_token": "00D...",
                "instance_url": "https://acme.my.salesforce.com",
            },
        )
    )
    assert props["access-token"] == "00D..."
    assert props["instance-url"] == "https://acme.my.salesforce.com"
    assert "client-id" not in props and "client-secret" not in props


def test_trino_reads_only():
    connector = TRINO_CONNECTORS["salesforce"]
    assert connector.mechanism in (Mechanism.ATTACH_R, Mechanism.SCAN)
    assert not connector.capability().write


# -- per-engine connectors -------------------------------------------------------------


@pytest.mark.parametrize(
    "engine", [build_duckdb_engine(), build_pg_engine(), build_clickhouse_engine()]
)
def test_the_engine_attaches_the_sources_pgwire_server(engine):
    assert engine_attaches(engine, "salesforce")
    assert engine.connectors["salesforce"].mechanism == Mechanism.ATTACH_R


def test_reachable_live_on_every_engine():
    for key in ("trino", "duckdb", "pg", "clickhouse"):
        assert "salesforce" in reachable_source_types(key), key
        assert "salesforce" in live_source_types(key), key


def test_strategy_is_virtual_where_attached_and_a_replica_elsewhere():
    from provisa.federation.engine import build_sqlalchemy_engine

    assert federate(_source(), build_duckdb_engine()) is Strategy.VIRTUAL
    assert federate(_source(), build_sqlalchemy_engine("mysql://h/db")) is Strategy.MATERIALIZED


def test_duckdb_attach_points_at_the_sources_endpoint(monkeypatch):
    ports = pr.PortPair(pgwire_port=5999, calcite_child_host="127.0.0.1", calcite_child_port=6099)
    monkeypatch.setattr(pr, "ensure_endpoint", lambda source, **kw: ports)
    details = build_duckdb_engine().connectors["salesforce"].details(_source())
    assert "port=5999" in details["attach"]
    assert details["remote_schema"] == "sf_sales"
