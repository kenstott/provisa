# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8ab5ca-8e86-409d-bcbd-20c97a7ea162
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2095: a demo declares its secrets in secrets.yaml beside its config.yaml; they are written
to the vault of each org built from it before its configuration loads, and a parse of the
configuration where that vault is not bound resolves them from the same file."""

# Requirements: REQ-2095

from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest
import yaml

from provisa.core import demo_secrets

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_NAMED = _REPO / "demo" / "named"
_TESTDATA = _NAMED / "testdata"
_REFERENCE = re.compile(r"\$\{secret:([A-Za-z_][A-Za-z0-9_]*)\}")
_VALUE = "s3cret-value"


def _secrets_file(directory: Path, body: object) -> None:
    (directory / demo_secrets.FILE).write_text(yaml.safe_dump(body))


def _config(directory: Path) -> dict:
    config = {
        "federation_engine": "duckdb",
        "auth": {"provider": "none", "assignments_source": "provisa"},
        "naming": {"domain_prefix": False, "rules": []},
        "domains": [{"id": "shop", "description": "the shop"}],
        "roles": [],
        "sources": [
            {
                "id": "shop-db",
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "shop",
                "username": "reader",
                "password": "${secret:SHOP_DB_PASSWORD}",
            }
        ],
        "tables": [],
        "relationships": [],
    }
    (directory / "config.yaml").write_text(yaml.safe_dump(config))
    return config


def test_a_demo_without_a_secrets_file_declares_none(tmp_path):
    assert demo_secrets.declared(tmp_path) == {}


def test_the_secrets_file_maps_each_name_to_its_value(tmp_path):
    _secrets_file(tmp_path, {"secrets": {"SHOP_DB_PASSWORD": _VALUE, "API_TOKEN": "t"}})
    assert demo_secrets.declared(tmp_path) == {"SHOP_DB_PASSWORD": _VALUE, "API_TOKEN": "t"}


@pytest.mark.parametrize(
    "body",
    [
        [_VALUE],
        {"secrets": [_VALUE]},
        {"secrets": {"SHOP_DB_PASSWORD": _VALUE}, "other": 1},
        {"secrets": {"SHOP_DB_PASSWORD": ""}},
        {"secrets": {"SHOP_DB_PASSWORD": 7}},
        {"secrets": {"bad name": _VALUE}},
    ],
    ids=["a-list", "secrets-a-list", "another-key", "empty-value", "not-text", "bad-name"],
)
def test_a_file_of_another_shape_is_refused_without_its_value(tmp_path, body):
    _secrets_file(tmp_path, body)
    with pytest.raises(ValueError) as refused:
        demo_secrets.declared(tmp_path)
    assert _VALUE not in str(refused.value)


def test_each_declared_secret_is_written_to_the_org_vault(tmp_path, monkeypatch):
    from provisa.core import secrets_store

    written = []

    async def put(admin_db, org_id, name, value, *, owner_id, description=None, actor=None):
        written.append((admin_db, org_id, name, value, owner_id))

    monkeypatch.setattr(secrets_store, "put", put)
    _secrets_file(tmp_path, {"secrets": {"B": "2", "A": "1"}})
    names = asyncio.run(demo_secrets.seed("db", "sbxtplshop", tmp_path))
    assert names == ["A", "B"]
    assert sorted(written) == [
        ("db", "sbxtplshop", "A", "1", secrets_store.ORG_OWNER),
        ("db", "sbxtplshop", "B", "2", secrets_store.ORG_OWNER),
    ]


def test_a_demo_declaring_none_writes_nothing(tmp_path, monkeypatch):
    from provisa.core import secrets_store

    async def put(*_a, **_k):
        raise AssertionError("nothing is declared, so nothing is written")

    monkeypatch.setattr(secrets_store, "put", put)
    assert asyncio.run(demo_secrets.seed("db", "org", tmp_path)) == []


def test_a_parse_resolves_the_declared_secret_and_stores_the_reference(tmp_path):
    from provisa.core.config_loader import parse_config_dict

    raw = _config(tmp_path)
    _secrets_file(tmp_path, {"secrets": {"SHOP_DB_PASSWORD": _VALUE}})
    with demo_secrets.resolving(tmp_path, "org"):
        config = parse_config_dict(raw)
    assert config.sources[0].password == _VALUE
    assert config.written.sources[0].password == "${secret:SHOP_DB_PASSWORD}"


def test_outside_the_block_the_reference_is_not_resolved(tmp_path):
    from provisa.core.config_loader import parse_config_dict

    raw = _config(tmp_path)
    _secrets_file(tmp_path, {"secrets": {"SHOP_DB_PASSWORD": _VALUE}})
    with pytest.raises(KeyError, match="no organization is bound"):
        parse_config_dict(raw)


def test_a_template_seed_resolves_its_demo_s_secrets(tmp_path, monkeypatch):
    from provisa.api import demo_templates

    _config(tmp_path)
    _secrets_file(tmp_path, {"secrets": {"SHOP_DB_PASSWORD": _VALUE}})
    demo = demo_templates.PublishedDemo(name="shop", title="Shop", directory=tmp_path)
    monkeypatch.setattr(demo_templates, "demo_of_template", lambda _org: demo)
    monkeypatch.setattr(demo_templates, "bundled_demos", lambda: ())
    seed = demo_templates.template_seed(demo.template_org)
    assert seed.written.sources[0].password == "${secret:SHOP_DB_PASSWORD}"
    assert seed.sources[0].password == _VALUE


def test_the_deployment_org_s_demo_is_the_started_one_else_the_file_s_own(tmp_path, monkeypatch):
    from provisa.core.demo import NAMED_DEMO_DIR_ENV

    launched = tmp_path / "wrapper" / "provisa-with-shop.yaml"
    monkeypatch.delenv(NAMED_DEMO_DIR_ENV, raising=False)
    assert demo_secrets.launched_directory(launched) == launched.parent
    monkeypatch.setenv(NAMED_DEMO_DIR_ENV, str(tmp_path / "shop"))
    assert demo_secrets.launched_directory(launched) == tmp_path / "shop"


@pytest.mark.parametrize("demo", sorted(p.parent.name for p in _NAMED.glob("*/config.yaml")))
def test_every_secret_a_demo_names_it_declares(demo):
    directory = _NAMED / demo
    named = set(_REFERENCE.findall((directory / "config.yaml").read_text()))
    assert named <= set(demo_secrets.declared(directory)), (
        f"{demo}'s config.yaml names {sorted(named)}; its secrets.yaml declares "
        f"{sorted(demo_secrets.declared(directory))}"
    )


def test_the_testdata_demo_reads_prod_as_provisa_s_own_secret():
    from provisa.judgments.posture import is_reference

    raw = yaml.safe_load((_TESTDATA / "config.yaml").read_text())
    (source,) = [s for s in raw["sources"] if s["id"] == "shop-db"]
    assert is_reference(source["password"])
    reference = _REFERENCE.fullmatch(source["password"])
    assert reference is not None and reference.group(1) in demo_secrets.declared(_TESTDATA)
