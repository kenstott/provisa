# Copyright (c) 2026 Kenneth Stott
# Canary: 13ef6824-8ef9-4b1d-8ec6-e437314275d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Avro messages are read through the source's schema registry (REQ-1951).

The registry here is an httpx transport that answers as a Confluent-compatible registry does; the
messages are real Avro in the registry's wire format. The integration lane reads the same through
a registry container (tests/integration/test_debezium_avro_registry.py)."""

from __future__ import annotations

import io
import json
import struct

import fastavro
import httpx
import pytest

from provisa.kafka import avro_registry as sr
from provisa.kafka.avro_registry import (
    RegistrySettingRefused,
    RegistrySettings,
    SchemaRegistry,
    SchemaRegistryRefusal,
    validate_settings,
)

pytestmark = pytest.mark.asyncio

_V1 = {"type": "record", "name": "Order", "fields": [{"name": "id", "type": "int"}]}
_V2 = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "region", "type": ["null", "string"], "default": None},
    ],
}


def _framed(schema_id: int, schema: dict, value: dict) -> bytes:
    out = io.BytesIO()
    out.write(struct.pack(">bI", 0, schema_id))
    fastavro.schemaless_writer(out, fastavro.parse_schema(schema), value)
    return out.getvalue()


class _Registry:
    """A registry's answers, and the requests it was sent."""

    def __init__(self, schemas: dict[int, dict]) -> None:
        self.schemas = schemas
        self.requests: list[httpx.Request] = []
        self.status: int | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.status is not None:
            return httpx.Response(self.status, text="no")
        if request.url.path == "/subjects":
            return httpx.Response(200, json=[])
        schema_id = int(request.url.path.rsplit("/", 1)[1])
        if schema_id not in self.schemas:
            return httpx.Response(404, json={"error_code": 40403, "message": "Schema not found"})
        return httpx.Response(200, json={"schema": json.dumps(self.schemas[schema_id])})


def _registry(answers: _Registry, **settings) -> SchemaRegistry:
    registry = SchemaRegistry(RegistrySettings(url="http://registry:8081/", **settings))
    registry._client = httpx.AsyncClient(  # noqa: SLF001 - the transport stands in for the network
        transport=httpx.MockTransport(answers), **registry.client_options()
    )
    return registry


# --- reading ---------------------------------------------------------------------------------


async def test_a_message_is_decoded_with_the_schema_its_id_names():
    answers = _Registry({7: _V1})
    registry = _registry(answers)
    assert await registry.decode(_framed(7, _V1, {"id": 42})) == {"id": 42}
    assert [r.url.path for r in answers.requests] == ["/schemas/ids/7"]


async def test_a_schema_is_fetched_once():
    answers = _Registry({7: _V1})
    registry = _registry(answers)
    for n in range(5):
        assert await registry.decode(_framed(7, _V1, {"id": n})) == {"id": n}
    assert len(answers.requests) == 1


async def test_an_evolved_schema_is_read_by_its_own_id():
    """Evolution is by id: messages written before and after a change sit on one topic, each
    naming the schema that wrote it."""
    answers = _Registry({7: _V1, 8: _V2})
    registry = _registry(answers)
    assert await registry.decode(_framed(7, _V1, {"id": 1})) == {"id": 1}
    assert await registry.decode(_framed(8, _V2, {"id": 2, "region": "eu"})) == {
        "id": 2,
        "region": "eu",
    }
    assert await registry.decode(_framed(7, _V1, {"id": 3})) == {"id": 3}
    assert [r.url.path for r in answers.requests] == ["/schemas/ids/7", "/schemas/ids/8"]


async def test_an_id_the_registry_does_not_know_is_refused_with_the_id():
    registry = _registry(_Registry({7: _V1}))
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await registry.decode(_framed(99, _V1, {"id": 1}))
    assert refused.value.code == "subscribe.schema_id_unknown"
    assert refused.value.params["schema_id"] == 99
    assert "99" in str(refused.value)


async def test_a_schema_that_is_not_avro_is_refused_by_kind():
    def answer(_request):
        return httpx.Response(200, json={"schemaType": "PROTOBUF", "schema": "syntax..."})

    registry = SchemaRegistry(RegistrySettings(url="http://registry:8081"))
    registry._client = httpx.AsyncClient(transport=httpx.MockTransport(answer))  # noqa: SLF001
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await registry.schema(3)
    assert (refused.value.code, refused.value.params["kind"]) == (
        "subscribe.schema_not_avro",
        "PROTOBUF",
    )


def test_the_first_byte_tells_avro_from_json():
    assert sr.is_registry_framed(_framed(7, _V1, {"id": 1}))
    assert not sr.is_registry_framed(b'{"op": "c"}')
    assert not sr.is_registry_framed(b"\x00\x00")  # shorter than the header: not a message


# --- a registry that does not answer ------------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403])
async def test_refused_credentials_are_named_with_the_method(status):
    answers = _Registry({})
    answers.status = status
    registry = _registry(answers, auth="basic", username="u", password="p")
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await registry.reach()
    assert refused.value.code == "subscribe.schema_registry_refused_credentials"
    assert refused.value.params["method"] == "basic"
    assert refused.value.params["http_status"] == status


async def test_a_registry_that_cannot_be_reached_is_named():
    def answer(_request):
        raise httpx.ConnectError("connection refused")

    registry = SchemaRegistry(RegistrySettings(url="http://registry:8081"))
    registry._client = httpx.AsyncClient(transport=httpx.MockTransport(answer))  # noqa: SLF001
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await registry.reach()
    assert refused.value.status == 503
    assert refused.value.code == "subscribe.schema_registry_unreachable"
    assert refused.value.params["registry"] == "http://registry:8081"


async def test_a_registry_that_hangs_is_bounded_and_named():
    def answer(_request):
        raise httpx.ReadTimeout("timed out")

    registry = SchemaRegistry(RegistrySettings(url="http://registry:8081"))
    registry._client = httpx.AsyncClient(transport=httpx.MockTransport(answer))  # noqa: SLF001
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await registry.schema(1)
    assert refused.value.code == "subscribe.schema_registry_unreachable"
    assert "ReadTimeout" in refused.value.params["error"]


async def test_the_registrys_bounds_are_its_own_and_a_requests_deadline_shortens_them(monkeypatch):
    from provisa.core import request_deadline

    registry = SchemaRegistry(RegistrySettings(url="http://registry:8081"))
    own = registry._timeout()  # noqa: SLF001
    assert (own.connect, own.read) == (sr.CONNECT_SECONDS, sr.READ_SECONDS)

    monkeypatch.setattr(request_deadline, "remaining", lambda: 1.5)
    monkeypatch.setattr(request_deadline, "check", lambda: None)
    bounded = registry._timeout()  # noqa: SLF001
    assert (bounded.connect, bounded.read) == (1.5, 1.5)


# --- how each method reaches the registry --------------------------------------------------------


async def test_none_sends_no_credentials():
    answers = _Registry({})
    registry = _registry(answers)
    await registry.reach()
    assert "authorization" not in answers.requests[0].headers


async def test_basic_sends_the_username_and_password(monkeypatch):
    monkeypatch.setenv("IT_REGISTRY_PASSWORD", "s3cret")
    answers = _Registry({})
    registry = _registry(
        answers, auth="basic", username="reader", password="${env:IT_REGISTRY_PASSWORD}"
    )
    await registry.reach()
    sent = httpx.BasicAuth("reader", "s3cret")._auth_header  # noqa: SLF001
    assert answers.requests[0].headers["authorization"] == sent


async def test_bearer_sends_the_token(monkeypatch):
    monkeypatch.setenv("IT_REGISTRY_TOKEN", "tok-123")
    answers = _Registry({})
    registry = _registry(answers, auth="bearer", token="${env:IT_REGISTRY_TOKEN}")
    await registry.reach()
    assert answers.requests[0].headers["authorization"] == "Bearer tok-123"


def test_mtls_presents_the_certificate_and_trusts_the_ca():
    registry = SchemaRegistry(
        RegistrySettings(
            url="https://registry:8081",
            auth="mtls",
            client_cert="/etc/provisa/registry.crt",
            client_key="/etc/provisa/registry.key",
            ca="/etc/provisa/ca.pem",
        )
    )
    options = registry.client_options()
    assert options["cert"] == ("/etc/provisa/registry.crt", "/etc/provisa/registry.key")
    assert options["verify"] == "/etc/provisa/ca.pem"
    assert "Authorization" not in options["headers"]


def test_a_ca_bundle_is_trusted_with_any_method():
    registry = SchemaRegistry(
        RegistrySettings(url="https://registry:8081", auth="bearer", token="t", ca="/etc/ca.pem")
    )
    assert registry.client_options()["verify"] == "/etc/ca.pem"


def test_settings_are_read_from_a_sources_cdc_block():
    from provisa.core.models import SourceCdcConfig

    cdc = SourceCdcConfig(
        bootstrap_servers="b:9092",
        topic_prefix="p",
        schema_registry_url="http://registry:8081",
        schema_registry_auth="basic",
        schema_registry_username="u",
        schema_registry_password="${secret:x}",
    )
    assert RegistrySettings.of(cdc) == RegistrySettings(
        url="http://registry:8081", auth="basic", username="u", password="${secret:x}"
    )
    assert RegistrySettings.of(cdc.model_dump()) == RegistrySettings.of(cdc)
    assert RegistrySettings.of(SourceCdcConfig(bootstrap_servers="b", topic_prefix="p")) is None


# --- settings refused when the source is saved ---------------------------------------------------

_URL = {"schema_registry_url": "http://registry:8081"}


def _refusal(fields: dict) -> RegistrySettingRefused:
    with pytest.raises(RegistrySettingRefused) as refused:
        validate_settings("orders-db", fields)
    return refused.value


@pytest.mark.parametrize(
    "fields",
    [
        {},
        _URL,
        {**_URL, "schema_registry_auth": "none"},
        {
            **_URL,
            "schema_registry_auth": "basic",
            "schema_registry_username": "u",
            "schema_registry_password": "${secret:p}",
        },
        {**_URL, "schema_registry_auth": "bearer", "schema_registry_token": "${secret:t}"},
        {
            **_URL,
            "schema_registry_auth": "mtls",
            "schema_registry_client_cert": "/c.crt",
            "schema_registry_client_key": "/c.key",
            "schema_registry_ca": "/ca.pem",
        },
    ],
)
def test_settings_that_say_how_to_reach_the_registry_are_accepted(fields):
    validate_settings("orders-db", fields)


def test_an_unknown_method_is_refused_with_the_methods_there_are():
    refused = _refusal({**_URL, "schema_registry_auth": "kerberos"})
    assert refused.code == "schema.registry_auth_unknown"
    assert refused.params == {
        "source": "orders-db",
        "method": "kerberos",
        "methods": "none, basic, bearer, mtls",
    }


@pytest.mark.parametrize(
    ("method", "given", "missing"),
    [
        ("basic", {"schema_registry_password": "p"}, "schema_registry_username"),
        ("basic", {"schema_registry_username": "u"}, "schema_registry_password"),
        ("bearer", {}, "schema_registry_token"),
        ("mtls", {"schema_registry_client_key": "/c.key"}, "schema_registry_client_cert"),
        ("mtls", {"schema_registry_client_cert": "/c.crt"}, "schema_registry_client_key"),
    ],
)
def test_each_field_a_method_needs_is_refused_by_name_when_missing(method, given, missing):
    refused = _refusal({**_URL, "schema_registry_auth": method, **given})
    assert refused.code == "schema.registry_field_required"
    assert refused.params == {"source": "orders-db", "method": method, "field": missing}


@pytest.mark.parametrize(
    "field", ["schema_registry_client_cert", "schema_registry_client_key", "schema_registry_ca"]
)
def test_a_file_is_named_by_absolute_path(field):
    fields = {
        **_URL,
        "schema_registry_auth": "mtls",
        "schema_registry_client_cert": "/c.crt",
        "schema_registry_client_key": "/c.key",
        field: "certs/file.pem",
    }
    refused = _refusal(fields)
    assert refused.code == "schema.registry_path_not_absolute"
    assert refused.params == {"source": "orders-db", "field": field, "path": "certs/file.pem"}
    assert "must be an absolute path, got 'certs/file.pem'" in str(refused)


@pytest.mark.parametrize(
    "fields",
    [
        {"schema_registry_auth": "basic"},
        {"schema_registry_token": "t"},
        {"schema_registry_username": "u"},
        {"schema_registry_ca": "/ca.pem"},
    ],
)
def test_registry_settings_without_a_registry_are_refused(fields):
    assert _refusal(fields).code == "schema.registry_url_required"


# --- saving a source: refused by name, credentials vaulted ---------------------------------------


def _source_input(**cdc):
    from provisa.api.admin.types import SourceCdcConfigInput

    return type(
        "Input",
        (),
        {
            "id": "orders-db",
            "cdc": SourceCdcConfigInput(bootstrap_servers="b:9092", topic_prefix="p", **cdc),
        },
    )()


def test_a_save_with_settings_that_do_not_reach_the_registry_is_refused_with_its_code():
    from provisa.api.admin._cdc_registry import refuse_registry_settings

    refused = refuse_registry_settings(
        _source_input(schema_registry_url="http://r:8081", schema_registry_auth="bearer")
    )
    assert refused is not None and refused.success is False
    assert refused.code == "schema.registry_field_required"
    assert refused.params == {
        "source": "orders-db",
        "method": "bearer",
        "field": "schema_registry_token",
    }
    assert refuse_registry_settings(_source_input(schema_registry_url="http://r:8081")) is None
    assert refuse_registry_settings(type("Input", (), {"id": "s", "cdc": None})()) is None


async def test_a_typed_registry_credential_goes_to_the_vault_and_the_block_names_it(monkeypatch):
    """REQ-1695: the row never holds a credential. A literal is stored under a name of the
    source's own; a reference is kept as written."""
    from provisa.api.admin import _cdc_registry, capabilities, schema_common
    from provisa.core import request_context

    stored: list[tuple[str, str]] = []

    async def _store(_actor, name, value, _description):
        if "${" in value:
            return value
        stored.append((name, value))
        return f"${{secret:{name}}}"

    monkeypatch.setattr(schema_common, "_store_source_secret", _store)
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda _info: None)
    monkeypatch.setattr(request_context, "active_env", lambda: "prod")

    cdc = await _cdc_registry.stored_cdc(
        None,
        _source_input(
            schema_registry_url="http://r:8081",
            schema_registry_auth="basic",
            schema_registry_username="reader",
            schema_registry_password="typed-in",
            schema_registry_token="${secret:already_there}",
        ),
    )
    name = schema_common.source_mapping_secret_name("orders-db", "schema_registry_password", "prod")
    assert stored == [(name, "typed-in")]
    assert cdc.schema_registry_password == f"${{secret:{name}}}"
    assert cdc.schema_registry_token == "${secret:already_there}"
    assert cdc.schema_registry_username == "reader"  # not a credential: kept as typed
