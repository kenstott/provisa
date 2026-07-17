# Copyright (c) 2026 Kenneth Stott
# Canary: 0f172b3e-762c-4149-adb0-b41e83745340
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the tri-state `naming.use_domains` feature.

Three modes:
  * use_domains absent (None) — legacy/inert: byte-for-byte pre-feature behavior.
  * use_domains=False — single-domain: all registrations stored under default_domain.
  * use_domains=True — namespaced: domain_id required.
"""

import pytest
from pydantic import ValidationError

from provisa.core import domain_policy
from provisa.core.models import NamingConfig, ProvisaConfig


@pytest.fixture(autouse=True)
def _reset_policy():
    """Every test starts and ends with the inert legacy policy."""
    domain_policy.reset()
    yield
    domain_policy.reset()


# ---------------------------------------------------------------------------
# domain_policy.resolve_domain_id
# ---------------------------------------------------------------------------


class TestResolveDomainId:
    def test_legacy_passes_through_empty(self):
        # Inert: falsy stays "", truthy stays as-is — identical to pre-feature code.
        assert domain_policy.resolve_domain_id("") == ""
        assert domain_policy.resolve_domain_id(None) == ""

    def test_legacy_passes_through_explicit(self):
        assert domain_policy.resolve_domain_id("sales") == "sales"

    def test_legacy_never_raises(self):
        # No hard errors while inert, regardless of value.
        assert domain_policy.resolve_domain_id("anything") == "anything"

    def test_single_domain_coerces_falsy_to_default(self):
        domain_policy.configure(False, "global")
        assert domain_policy.resolve_domain_id("") == "global"
        assert domain_policy.resolve_domain_id(None) == "global"

    def test_single_domain_allows_matching_default(self):
        domain_policy.configure(False, "global")
        assert domain_policy.resolve_domain_id("global") == "global"

    def test_single_domain_rejects_foreign_domain(self):
        domain_policy.configure(False, "global")
        with pytest.raises(ValueError, match="cannot register domain"):
            domain_policy.resolve_domain_id("sales")

    def test_namespaced_requires_domain(self):
        domain_policy.configure(True, "default")
        with pytest.raises(ValueError, match="required"):
            domain_policy.resolve_domain_id("")
        with pytest.raises(ValueError, match="required"):
            domain_policy.resolve_domain_id(None)

    def test_namespaced_passes_explicit(self):
        domain_policy.configure(True, "default")
        assert domain_policy.resolve_domain_id("sales") == "sales"


# ---------------------------------------------------------------------------
# domain_policy.system_domain_ids / import_default / flags
# ---------------------------------------------------------------------------


class TestSystemDomainIds:
    def test_legacy_system_ids(self):
        assert domain_policy.system_domain_ids() == ["", "meta", "ops"]

    def test_single_domain_appends_default(self):
        domain_policy.configure(False, "global")
        assert domain_policy.system_domain_ids() == ["", "meta", "ops", "global"]

    def test_namespaced_keeps_legacy_ids(self):
        domain_policy.configure(True, "default")
        assert domain_policy.system_domain_ids() == ["", "meta", "ops"]


class TestImportDefault:
    def test_legacy_preserves_default_literal(self):
        # Hasura/FK importers historically used "default"; legacy keeps that.
        assert domain_policy.import_default() == "default"

    def test_single_domain_uses_configured(self):
        domain_policy.configure(False, "global")
        assert domain_policy.import_default() == "global"

    def test_namespaced_uses_default_domain(self):
        domain_policy.configure(True, "default")
        assert domain_policy.import_default() == "default"


class TestFlags:
    def test_active(self):
        assert domain_policy.active() is False
        domain_policy.configure(False, "global")
        assert domain_policy.active() is True
        domain_policy.configure(True, "default")
        assert domain_policy.active() is True

    def test_single_domain_flag(self):
        assert domain_policy.single_domain() is False
        domain_policy.configure(False, "global")
        assert domain_policy.single_domain() is True
        domain_policy.configure(True, "default")
        assert domain_policy.single_domain() is False


# ---------------------------------------------------------------------------
# NamingConfig validation
# ---------------------------------------------------------------------------


class TestNamingConfig:
    def test_defaults_legacy(self):
        nc = NamingConfig()
        assert nc.use_domains is None
        assert nc.default_domain == "default"

    def test_single_domain_requires_nonempty_default(self):
        with pytest.raises(ValidationError, match="non-empty"):
            NamingConfig(use_domains=False, default_domain="")

    def test_single_domain_rejects_invalid_identifier(self):
        with pytest.raises(ValidationError, match="not a valid identifier"):
            NamingConfig(use_domains=False, default_domain="has spaces")

    def test_single_domain_accepts_valid_identifier(self):
        nc = NamingConfig(use_domains=False, default_domain="global")
        assert nc.default_domain == "global"

    def test_legacy_allows_empty_default(self):
        # When inert, the non-empty rule does not apply.
        nc = NamingConfig(use_domains=None, default_domain="")
        assert nc.default_domain == ""


# ---------------------------------------------------------------------------
# ProvisaConfig cross-field validation
# ---------------------------------------------------------------------------


def _config(naming: dict, domains: list, tables: list) -> dict:
    return {
        "sources": [
            {
                "id": "pg1",
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "d",
                "username": "u",
                "password": "p",
            }
        ],
        "domains": domains,
        "naming": naming,
        "tables": tables,
        "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
    }


def _table(domain_id: str) -> dict:
    return {
        "source_id": "pg1",
        "domain_id": domain_id,
        "schema": "public",
        "table": "orders",
        "columns": [{"name": "id", "visible_to": ["admin"]}],
    }


class TestProvisaConfigDomainPolicy:
    def test_legacy_unaffected(self):
        # use_domains absent: domains list + arbitrary domain_id allowed, no errors.
        cfg = ProvisaConfig.model_validate(
            _config({}, [{"id": "sales"}], [_table("sales")])
        )
        assert cfg.naming.use_domains is None

    def test_single_domain_rejects_domains_list(self):
        with pytest.raises(ValidationError, match="mutually exclusive"):
            ProvisaConfig.model_validate(
                _config(
                    {"use_domains": False, "default_domain": "global"},
                    [{"id": "sales"}],
                    [_table("global")],
                )
            )

    def test_single_domain_rejects_foreign_table_domain(self):
        with pytest.raises(ValidationError, match="permits only domain"):
            ProvisaConfig.model_validate(
                _config(
                    {"use_domains": False, "default_domain": "global"},
                    [],
                    [_table("sales")],
                )
            )

    def test_single_domain_allows_matching_and_empty(self):
        cfg = ProvisaConfig.model_validate(
            _config(
                {"use_domains": False, "default_domain": "global"},
                [],
                [_table("global"), _table_named("public2", "")],
            )
        )
        assert cfg.naming.default_domain == "global"

    def test_namespaced_allows_domains(self):
        cfg = ProvisaConfig.model_validate(
            _config(
                {"use_domains": True},
                [{"id": "sales"}],
                [_table("sales")],
            )
        )
        assert cfg.naming.use_domains is True


def _table_named(table: str, domain_id: str) -> dict:
    t = _table(domain_id)
    t["table"] = table
    return t
