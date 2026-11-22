# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8f1d62-0c47-4e95-a6d3-9e2f7a1c5b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the cloud inventory source type (REQ-1947): the pgwire server's model operand
for each cloud and each refusal, the Trino catalog properties, the per-engine connectors, the
bundle name, and that the source reads only."""

from __future__ import annotations

import pytest

from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import Source, SourceType
from provisa.executor.writable import is_written_through_pgwire_server, resolve_write_path
from provisa.executor.write_capability import table_write_ops, table_write_returns_rows
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_base import Mechanism
from provisa.federation.engine import (
    build_clickhouse_engine,
    build_duckdb_engine,
    build_pg_engine,
    build_sqlalchemy_engine,
    build_trino_engine,
    live_source_types,
    reachable_source_types,
)
from provisa.federation.strategy import Strategy, engine_attaches, federate
from provisa.federation.trino_connectors import TRINO_CONNECTORS, trino_connector_name
from provisa.runtime_deps import pgwire_bundles as rd

AZURE = {
    "azure_tenant_id": "tenant-1",
    "azure_client_id": "app-1",
    "azure_client_secret": "az-secret",
    "azure_subscription_ids": "sub-1,sub-2",
}
AWS = {
    "aws_access_key_id": "AKIA1",
    "aws_secret_access_key": "aws-secret",
    "aws_region": "us-east-1",
    "aws_account_ids": "111111111111",
}
GCP = {"gcp_credentials_path": "/etc/provisa/gcp-key.json", "gcp_project_ids": "proj-1"}


def _source(mapping: dict) -> Source:
    return Source(id="cloud-estate", type=SourceType.cloudops, mapping=mapping)


def _operand(mapping: dict) -> dict:
    return pr.build_model_json(_source(mapping))["schemas"][0]["operand"]


# -- the pgwire server's model ---------------------------------------------------------------------


def test_model_names_the_cloudops_schema_factory():
    model = pr.build_model_json(_source(AWS))
    schema = model["schemas"][0]
    assert schema["factory"] == "org.apache.calcite.adapter.ops.CloudOpsSchemaFactory"
    assert schema["name"] == model["defaultSchema"] == "cloud_estate"


def test_azure_operand():
    assert _operand(AZURE) == {
        "providers": "azure",
        "azure.tenantId": "tenant-1",
        "azure.clientId": "app-1",
        "azure.clientSecret": "az-secret",
        "azure.subscriptionIds": "sub-1,sub-2",
    }


def test_aws_operand_and_its_optional_role():
    assert _operand(AWS) == {
        "providers": "aws",
        "aws.accessKeyId": "AKIA1",
        "aws.secretAccessKey": "aws-secret",
        "aws.region": "us-east-1",
        "aws.accountIds": "111111111111",
    }
    role = "arn:aws:iam::111111111111:role/CloudOpsRole"
    assert _operand({**AWS, "aws_role_arn": role})["aws.roleArn"] == role


def test_gcp_operand():
    assert _operand(GCP) == {
        "providers": "gcp",
        "gcp.credentialsPath": "/etc/provisa/gcp-key.json",
        "gcp.projectIds": "proj-1",
    }


def test_the_providers_are_exactly_the_clouds_the_source_names():
    # The adapter reads a cloud's credentials from the process environment when the model does
    # not carry them; naming the providers keeps it to the clouds this source configured.
    operand = _operand({**AZURE, **AWS, **GCP})
    assert operand["providers"] == "azure,aws,gcp"
    assert _operand({**AZURE, **GCP})["providers"] == "azure,gcp"


def test_cache_ttl_is_carried_only_when_set():
    assert "cache.ttlMinutes" not in _operand(AWS)
    assert _operand({**AWS, "cache_ttl_minutes": 30})["cache.ttlMinutes"] == 30


def test_secrets_are_resolved_through_the_secrets_contract(monkeypatch):
    monkeypatch.setenv("CLOUDOPS_TEST_SECRET", "from-env")
    operand = _operand({**AWS, "aws_secret_access_key": "${env:CLOUDOPS_TEST_SECRET}"})
    assert operand["aws.secretAccessKey"] == "from-env"


def test_a_source_naming_no_cloud_is_refused():
    with pytest.raises(pr.MissingConnectorConfig, match="at least one of Azure, AWS or GCP"):
        pr.build_model_json(_source({}))


@pytest.mark.parametrize(
    ("cloud", "missing"),
    [
        (AZURE, "azure_client_secret"),
        (AZURE, "azure_subscription_ids"),
        (AZURE, "azure_tenant_id"),
        (AWS, "aws_secret_access_key"),
        (AWS, "aws_region"),
        (AWS, "aws_account_ids"),
        (GCP, "gcp_project_ids"),
        (GCP, "gcp_credentials_path"),
    ],
)
def test_a_partly_named_cloud_is_refused_naming_what_is_missing(cloud, missing):
    mapping = {k: v for k, v in cloud.items() if k != missing}
    with pytest.raises(pr.MissingConnectorConfig, match=missing):
        pr.build_model_json(_source(mapping))


def test_a_complete_cloud_does_not_excuse_a_partial_one():
    with pytest.raises(pr.MissingConnectorConfig, match="azure_client_secret"):
        pr.build_model_json(_source({**AWS, "azure_tenant_id": "tenant-1"}))


def test_a_relative_gcp_credentials_path_is_refused():
    with pytest.raises(pr.MissingConnectorConfig, match="absolute"):
        pr.build_model_json(_source({**GCP, "gcp_credentials_path": "keys/gcp.json"}))


# -- bundle --------------------------------------------------------------------------------------


def test_bundle_is_pgwire_cloudops():
    assert rd.bundle_spec_for("cloudops").artifact_name == "pgwire-cloudops"
    assert "cloudops" in pr.PGWIRE_REPLICA_TYPES


# -- Trino catalog ---------------------------------------------------------------------------------


def _props(mapping: dict) -> dict[str, str]:
    return _build_catalog_properties(_source(mapping), "")


def test_trino_connector_is_registered_and_reads_only():
    assert trino_connector_name("cloudops") == "cloudops"
    connector = TRINO_CONNECTORS["cloudops"]
    assert connector.mechanism == Mechanism.ATTACH_R
    assert not connector.capability().write


def test_trino_props_for_every_cloud():
    props = _props({**AZURE, **AWS, **GCP, "aws_role_arn": "arn:role", "cache_ttl_minutes": 30})
    assert props["providers"] == "azure,aws,gcp"
    assert props["azure.tenant-id"] == "tenant-1"
    assert props["azure.client-id"] == "app-1"
    assert props["azure.client-secret"] == "az-secret"
    assert props["azure.subscription-ids"] == "sub-1,sub-2"
    assert props["aws.access-key-id"] == "AKIA1"
    assert props["aws.secret-access-key"] == "aws-secret"
    assert props["aws.account-ids"] == "111111111111"
    assert props["aws.region"] == "us-east-1"
    assert props["aws.role-arn"] == "arn:role"
    assert props["gcp.credentials-path"] == "/etc/provisa/gcp-key.json"
    assert props["gcp.project-ids"] == "proj-1"
    assert props["cache.ttl-minutes"] == "30"
    assert props["schema"] == "cloud_estate"


def test_trino_props_carry_only_the_named_clouds():
    props = _props(AWS)
    assert props["providers"] == "aws"
    assert not [k for k in props if k.startswith(("azure.", "gcp."))]


def test_trino_always_matches_names_case_insensitively():
    # The connector refuses to start without it.
    assert _props(AWS)["case-insensitive-name-matching"] == "true"


# -- per-engine connectors -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "engine", [build_duckdb_engine(), build_pg_engine(), build_clickhouse_engine()]
)
def test_the_engine_attaches_the_sources_pgwire_server(engine):
    assert engine_attaches(engine, "cloudops")
    assert engine.connectors["cloudops"].mechanism == Mechanism.ATTACH_R


def test_reachable_live_on_every_engine():
    for key in ("trino", "duckdb", "pg", "clickhouse"):
        assert "cloudops" in reachable_source_types(key), key
        assert "cloudops" in live_source_types(key), key


def test_strategy_is_virtual_where_attached_and_a_replica_elsewhere():
    assert federate(_source(AWS), build_duckdb_engine()) is Strategy.VIRTUAL
    assert federate(_source(AWS), build_sqlalchemy_engine("mysql://h/db")) is Strategy.MATERIALIZED


# -- it reads only -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "build", [build_trino_engine, build_duckdb_engine, build_pg_engine, build_clickhouse_engine]
)
def test_a_cloud_inventory_table_takes_no_writes(build):
    engine = build()
    assert not is_written_through_pgwire_server("cloudops")
    assert resolve_write_path("cloudops", engine) is None
    assert (
        table_write_ops({"table_name": "compute_resources"}, "cloudops", engine, source_path=None)
        == frozenset()
    )
    assert (
        table_write_returns_rows(
            {"table_name": "compute_resources"}, "cloudops", engine, source_path=None
        )
        is False
    )


def test_its_cloud_secrets_are_fields_of_the_source_kept_as_typed(monkeypatch):
    """A cloud's secret is a field of the source's mapping: it holds what the operator entered
    (a value, or a reference they wrote) and is resolved by the one resolver where it is used."""
    from provisa.core.secrets import resolve_secrets_in_dict

    monkeypatch.setenv("PROVISA_TEST_AZURE_SECRET", "from-the-environment")
    typed = {
        "azure_client_secret": "${env:PROVISA_TEST_AZURE_SECRET}",
        "aws_secret_access_key": "typed-as-a-value",
    }
    assert resolve_secrets_in_dict(typed) == {
        "azure_client_secret": "from-the-environment",
        "aws_secret_access_key": "typed-as-a-value",
    }


# -- its server gets credentials only for the clouds it names ---------------------------------------

# Credential variables the deployment's own environment may hold for each cloud: what the cloud
# SDKs and the adapter's fallbacks read (maintainer 2026-11-21, job CLOUDOPS-ENV).
_DEPLOYMENT_CREDENTIALS = {
    "azure": {"AZURE_TENANT_ID": "lane-tenant", "AZURE_CLIENT_SECRET": "lane-az-secret"},
    "aws": {"AWS_ACCESS_KEY_ID": "lane-akia", "AWS_SECRET_ACCESS_KEY": "lane-aws-secret"},
    "gcp": {
        "GCP_PROJECT_IDS": "lane-proj",
        "GOOGLE_APPLICATION_CREDENTIALS": "/lane/gcp-key.json",
    },
}
_MAPPINGS = {"azure": AZURE, "aws": AWS, "gcp": GCP}


def _launched_environment(tmp_path, monkeypatch, clouds: tuple[str, ...]) -> dict[str, str]:
    """The environment a cloudops source naming ``clouds`` launches its server with, the
    deployment's environment holding every cloud's credential variables."""
    from types import SimpleNamespace

    from tests.helpers import ready_org_folder

    for names in _DEPLOYMENT_CREDENTIALS.values():
        for name, value in names.items():
            monkeypatch.setenv(name, value)
    monkeypatch.setenv("PROVISA_TEST_UNRELATED", "kept")
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    ready_org_folder()  # REQ-1992: the org's folder, as its runtime makes it ready
    bundle = tmp_path / "bundle"
    (bundle / "bin").mkdir(parents=True)
    mapping = {k: v for cloud in clouds for k, v in _MAPPINGS[cloud].items()}
    spawned: list[dict[str, str]] = []

    def spawn(command, cwd, env=None):
        spawned.append(env)
        return SimpleNamespace(exit_code=lambda: None)

    replica = pr.ConnectorReplica(
        _source(mapping),
        resolver=SimpleNamespace(resolve=lambda spec: bundle),  # type: ignore[arg-type]
        allocator=pr.PortAllocator(is_free=lambda port: True),
        spawn=spawn,
        health_check=lambda host, port: True,
        port_is_free=lambda port: True,
        prepare_catalog=lambda ports, password: None,
    )
    replica.start()
    assert len(spawned) == 1
    return spawned[0]


@pytest.mark.parametrize(
    "clouds",
    [
        ("azure",),
        ("aws",),
        ("gcp",),
        ("azure", "aws"),
        ("azure", "gcp"),
        ("aws", "gcp"),
        ("azure", "aws", "gcp"),
    ],
)
def test_its_server_is_launched_with_credentials_only_for_the_clouds_it_names(
    tmp_path, monkeypatch, clouds
):
    env = _launched_environment(tmp_path, monkeypatch, clouds)
    # Compared by name: a failure must not print the environment (it holds real secrets).
    expected = {
        name: value
        for cloud, names in _DEPLOYMENT_CREDENTIALS.items()
        if cloud in clouds
        for name, value in names.items()
    }
    every = {name for names in _DEPLOYMENT_CREDENTIALS.values() for name in names}
    assert sorted(name for name in every if name in env) == sorted(expected)
    assert all(env[name] == value for name, value in expected.items())
    assert env.get("PROVISA_TEST_UNRELATED") == "kept"


def test_the_spawn_hands_the_server_exactly_the_environment_it_is_given(tmp_path, monkeypatch):
    """The real spawn does not add this process's environment back: what was withheld stays
    withheld."""
    recorded: dict = {}

    class _Popen:
        def __init__(self, command, **kwargs):
            recorded.update(kwargs)

    monkeypatch.setenv("AZURE_CLIENT_SECRET", "lane-az-secret")
    monkeypatch.setattr(pr.subprocess, "Popen", _Popen)
    pr._spawn_process(["launcher"], tmp_path, {"ONLY": "this"})
    # Compared by name: a failure must not print the environment (it holds real secrets).
    assert sorted(recorded["env"]) == ["ONLY"]
    assert recorded["env"]["ONLY"] == "this"
