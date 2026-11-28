# Copyright (c) 2026 Kenneth Stott
# Canary: c83a671b-66fc-443c-be80-533991e0b57f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2114 (amended 2026-11-24): Polly knows every source type from the product's own registry.

The descriptor (provisa/api/admin/source_types.py) offers every SourceType a user can pick, every
brand and every code type; marks every secret; and is the file the UI's picker reads
(provisa-ui/src/generated/sourceTypes.json). Polly reads it with her source_types tool, and her
prompt never has her take a secret in the chat.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest
import yaml

from provisa.api.admin import source_types as st
from provisa.codesource.languages import LANGUAGES
from provisa.core.models import BUILTIN_CONNECTORS, SourceType

_REPO = Path(__file__).resolve().parents[2]


def _by_name() -> dict[str, st.SourceTypeDescriptor]:
    return {d.name: d for d in st.source_types()}


def _generator():
    spec = importlib.util.spec_from_file_location(
        "gen_source_types", _REPO / "scripts/gen_source_types.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestEveryTypeIsOffered:
    def test_every_source_type_a_user_can_pick_is_in_the_descriptor(self):
        offered = {d.backend_type for d in st.source_types()}
        members = {m.value for m in SourceType}
        assert members - offered == st.WRAPPER_STUBS

    def test_the_stubs_are_source_types_and_none_is_offered(self):
        members = {m.value for m in SourceType}
        assert st.WRAPPER_STUBS <= members
        assert not st.WRAPPER_STUBS & set(_by_name())

    def test_every_backend_type_is_a_source_type(self):
        members = {m.value for m in SourceType}
        for d in st.source_types():
            assert d.backend_type in members, d.name

    def test_picker_names_are_unique(self):
        names = [d.name for d in st.source_types()]
        assert len(names) == len(set(names))

    def test_aliases_are_offered_under_their_picker_names(self):
        types = _by_name()
        assert types["graphql"].backend_type == "graphql_remote"
        assert types["grpc"].backend_type == "grpc_remote"
        assert st.source_type("graphql_remote").name == "graphql"

    def test_every_brand_of_every_registry_is_offered_as_its_carrier(self):
        from provisa.file_source.wikipedia import BRAND as WIKIPEDIA
        from provisa.graphql_remote.brands import BRANDS as GRAPHQL
        from provisa.openapi.brands import BRANDS as OPENAPI
        from provisa.sparql.wikidata import BRAND as WIKIDATA

        types = _by_name()
        for brand in GRAPHQL:
            assert (types[brand].brand, types[brand].backend_type) == (brand, "graphql_remote")
            assert types[brand].label == GRAPHQL[brand].label
        for brand in OPENAPI:
            assert (types[brand].brand, types[brand].backend_type) == (brand, "openapi")
            assert types[brand].label == OPENAPI[brand].label
        assert (types[WIKIPEDIA].brand, types[WIKIPEDIA].backend_type) == (WIKIPEDIA, "files")
        assert (types[WIKIDATA].brand, types[WIKIDATA].backend_type) == (WIKIDATA, "sparql")

    def test_stripe_and_shopify_are_brands_only(self):
        types = _by_name()
        assert "stripe" not in SourceType.__members__
        assert "shopify" not in SourceType.__members__
        assert types["stripe"].backend_type == "openapi"
        assert types["shopify"].backend_type == "graphql_remote"

    def test_every_code_language_is_offered_with_its_language(self):
        types = _by_name()
        for name, language in LANGUAGES.items():
            assert types[name].category == "Code"
            assert types[name].code["runtimes"] == list(language.runtimes)
            assert types[name].code["builtin_host"] is (name in BUILTIN_CONNECTORS)

    def test_spark_is_a_built_in_code_type_joined_by_its_host(self):
        spark = _by_name()["spark"]
        assert spark.code["builtin_host"] is True
        join = next(f for f in spark.fields if f.name == "connector_join")
        assert join.value == {"connector": "spark"}

    def test_spark_takes_what_its_host_requires(self):
        """The Spark host's requires, as its connector file states them."""
        manifest = yaml.safe_load((_REPO / "connectors/spark/connector.yaml").read_text())
        spark = _by_name()["spark"]
        settings = {
            f.name.removeprefix("settings."): f
            for f in spark.fields
            if f.name.startswith("settings.")
        }
        assert set(settings) == set(manifest["requires"])
        for name, spec in manifest["requires"].items():
            assert settings[name].required is not bool(spec.get("org_default")), name

    def test_kaggle_is_made_on_its_form(self):
        kaggle = _by_name()["kaggle"]
        assert kaggle.create.via == "form"
        assert kaggle.create.link.startswith("/sources?new=kaggle")
        assert kaggle.backend_type == "files"

    def test_an_unknown_type_is_refused_by_name(self):
        with pytest.raises(KeyError, match="nosuchtype"):
            st.source_type("nosuchtype")


class TestHowEachIsCreated:
    def test_a_rest_route_body_is_its_request_model(self):
        from provisa.api.admin.grpc_remote_router import GrpcRemoteRegisterRequest

        grpc = _by_name()["grpc"]
        assert (grpc.create.via, grpc.create.path) == (
            "admin_request",
            "/admin/grpc-remote/register",
        )
        assert [f.name for f in grpc.fields] == list(GrpcRemoteRegisterRequest.model_fields)
        required = {f.name for f in grpc.fields if f.required}
        assert required == {"source_id", "proto_path", "server_address"}

    def test_each_brand_posts_to_its_carriers_route(self):
        types = _by_name()
        assert types["github"].create.path == "/admin/sources/graphql-remote"
        assert types["stripe"].create.path == "/admin/openapi/register"
        assert types["wikidata"].create.via == "admin_mutation"

    def test_a_brands_setup_is_its_registrys(self):
        from provisa.graphql_remote.brands import BRANDS

        shopify = _by_name()["shopify"]
        setup = {f.name: f for f in shopify.fields if f.at == "setup"}
        assert set(setup) == {f.name for f in BRANDS["shopify"].setup_fields}
        assert setup["store"].required is True
        assert setup["api_version"].default == BRANDS["shopify"].setup_fields[1].default

    def test_a_plain_type_is_created_with_create_source(self):
        pg = _by_name()["postgresql"]
        assert (pg.create.via, pg.create.mutation) == ("admin_mutation", "createSource")
        assert {f.name for f in pg.fields if f.required} == {"host", "port", "database"}

    def test_mapping_and_hint_keys_are_listed(self):
        types = _by_name()
        assert "connector_join" in types["spark"].mapping_json
        assert "http_path" in types["databricks"].federation_hints
        assert set(st.ATHENA_HINTS) <= set(types["athena"].federation_hints)

    def test_types_whose_tables_provisa_produces_have_no_setup_flow(self):
        # REQ-2234 (amended 2026-11-27): their tables are not registered through the setup flow.
        types = _by_name()
        without = {name for name, d in types.items() if not d.setup_flow}
        assert without == {"data_profiler", "great_expectations", "soda"}
        assert types["postgresql"].setup_flow is True
        assert st.as_json(types["data_profiler"])["setup_flow"] is False


_CREDENTIAL = re.compile(r"(^|_)(password|secret|token|api_key|account_key|secret_key)$")
# Field names that match but carry no credential: a flag choosing token sign-in.
_NOT_CREDENTIALS = {"use_token"}
# A REST body's auth object carries its credential; a hint or mapping "auth" names a kind.
_BODY_AUTH = {"auth", "auth_config"}


class TestSecretsAreMarked:
    def test_every_credential_field_is_a_secret(self):
        for d in st.source_types():
            for f in d.fields:
                base = f.name.removeprefix("settings.")
                credential = _CREDENTIAL.search(base) and base not in _NOT_CREDENTIALS
                if credential or (f.at == "body" and f.name in _BODY_AUTH):
                    assert f.secret, f"{d.name}.{f.name} carries a credential and is not secret"

    def test_known_secrets(self):
        types = _by_name()

        def secret(type_: str, name: str) -> bool:
            return next(f for f in types[type_].fields if f.name == name).secret

        assert secret("postgresql", "password")
        assert secret("github", "auth")
        assert secret("stripe", "token")
        assert secret("cloudops", "azure_client_secret")
        assert secret("cloudops", "aws_secret_access_key")
        assert secret("iceberg", "iceberg_catalog_password")
        assert secret("csv", "secret_access_key")
        assert not secret("postgresql", "host")

    def test_a_secret_is_written_as_a_reference(self):
        assert st.SECRET_REFERENCE == "${secret:NAME}"


class TestTheUiReadsTheDescriptor:
    def test_the_generated_file_matches_the_descriptor(self):
        gen = _generator()
        assert gen.OUT.read_text(encoding="utf-8") == gen.render(), (
            "provisa-ui/src/generated/sourceTypes.json is stale: run "
            "PYTHONPATH=. .venv/bin/python3 scripts/gen_source_types.py"
        )

    def test_brand_credentials_are_the_registries(self):
        assert st.brand_auth("github") == "token"
        assert st.brand_auth("datahub") == "token"
        assert st.brand_auth("wikipedia") == "none"
        with pytest.raises(KeyError):
            st.brand_auth("postgresql")


class TestTheFormBuildersShape:
    """What the UI's builders read from the descriptor: choices, defaults, per-choice fields."""

    def _field(self, type_name: str, name: str, when: str = "") -> st.Field:
        return next(
            f
            for f in _by_name()[type_name].fields
            if f.name == name and (not when or f.when == when)
        )

    def test_value_sets_are_the_modules_that_read_them(self):
        from provisa.kafka.security import SASL_MECHANISMS, SECURITY_PROTOCOLS
        from provisa.profiler.source import DRIFT_SEASONS
        from provisa.quickbooks.settings import ENVIRONMENTS
        from provisa.sparql.reasoning import REASONERS
        from provisa.sparql.source import AUTH_KINDS

        assert self._field("kafka", "kafka_security_protocol").choices == SECURITY_PROTOCOLS
        assert self._field("kafka", "kafka_sasl_mechanism").choices == SASL_MECHANISMS
        assert self._field("data_profiler", "drift_season").choices == DRIFT_SEASONS
        assert self._field("quickbooks", "environment").choices == ENVIRONMENTS
        assert self._field("sparql", "reasoner").choices == REASONERS
        assert self._field("sparql", "auth").choices == AUTH_KINDS
        assert self._field("salesforce", "auth_type").choices == tuple(st.SALESFORCE_AUTH)

    def test_each_iceberg_catalog_states_its_own_fields(self):
        for catalog, entries in st.ICEBERG_CATALOG.items():
            when = f"iceberg_catalog_type={catalog}"
            stated = [(f.name, f.required) for f in _by_name()["iceberg"].fields if f.when == when]
            assert stated == list(entries), catalog

    def test_a_code_source_lists_every_key_its_reader_takes(self):
        for name in ("node", "python", "go", "rust"):
            keys = set(_by_name()[name].mapping_json)
            assert {"mode", "remote", "content_hash", "connector"} <= keys, name

    def test_a_mail_source_lists_an_earlier_approval_as_a_secret(self):
        for name in ("google_workspace", "microsoft_365"):
            assert self._field(name, "refresh_token").secret, name
