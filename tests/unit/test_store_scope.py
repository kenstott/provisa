# Copyright (c) 2026 Kenneth Stott
# Canary: 1681c66e-c6cc-447e-ba2a-96f58e08aa13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where an environment's landed replicas go, and what retiring it may remove (REQ-1622).

Pure logic — no store is opened. The one test that would open one asserts the refusal that
happens BEFORE any connection, which is the whole guarantee: ``drop_env_store`` declines a
schema this module did not name rather than connecting and deciding afterwards.
"""

from __future__ import annotations

import pytest

from provisa.core.environments import PROD
from provisa.federation.store_scope import (
    ENV_SCHEMA_PREFIX,
    StoreNotIsolable,
    dsn_names_env,
    drop_env_store,
    store_schema,
)

PG = "postgresql://u:p@h:5432/store"
SQLITE = "sqlite:////var/provisa/store.db"


# --- prod keeps the store's default namespace ----------------------------------


def test_prod_on_a_schema_capable_store_is_mat():
    assert store_schema(PG, PROD) == "mat"


def test_prod_on_a_schemaless_store_is_main():
    assert store_schema(SQLITE, PROD) == "main"


# --- every other environment gets a namespace of its own -----------------------


def test_non_prod_gets_its_own_schema():
    assert store_schema(PG, "feature_x") == f"{ENV_SCHEMA_PREFIX}feature_x"


def test_two_environments_do_not_share_a_schema():
    assert store_schema(PG, "feature_x") != store_schema(PG, "feature_y")


def test_an_expiring_environment_is_not_a_special_case():
    # The rule is per-environment, not per-expiry: two long-lived environments clobbering each
    # other's replicas is the same defect with a slower clock.
    assert store_schema(PG, "ephemeral_ab12").startswith(ENV_SCHEMA_PREFIX)


def test_a_name_that_is_not_an_environment_is_refused():
    with pytest.raises(ValueError):
        store_schema(PG, "not a name")


# --- a store whose own address already names the environment -------------------


def test_a_templated_dsn_already_addresses_the_environment():
    assert dsn_names_env("sqlite:////var/store_feature_x.db", "feature_x") is True


def test_prod_is_never_read_as_a_templated_dsn():
    assert dsn_names_env("postgresql://h/prod_store", PROD) is False


def test_a_templated_schemaless_store_keeps_its_default_schema():
    assert store_schema("sqlite:////var/store_feature_x.db", "feature_x") == "main"


def test_a_templated_schema_capable_store_keeps_mat():
    assert store_schema("postgresql://h/store_feature_x", "feature_x") == "mat"


# --- the refusal, and what it tells the author ---------------------------------


def test_a_schemaless_untemplated_store_is_refused_for_a_non_prod_environment():
    with pytest.raises(StoreNotIsolable) as excinfo:
        store_schema(SQLITE, "feature_x")
    assert "${scope:ENV}" in str(excinfo.value)
    assert excinfo.value.env == "feature_x"


# --- what retire may drop ------------------------------------------------------


@pytest.mark.asyncio
async def test_retire_declines_a_store_it_did_not_namespace():
    # A DSN that already addressed the environment is the AUTHOR's store; its lifetime is the
    # author's to state, so nothing is dropped and nothing is even connected to.
    assert await drop_env_store("postgresql://h/store_feature_x", "feature_x") is None


@pytest.mark.asyncio
async def test_retire_never_reaches_prods_schema():
    assert await drop_env_store(PG, PROD) is None
