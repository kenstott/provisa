# Copyright (c) 2026 Kenneth Stott
# Canary: 1db8387e-d1ff-4cbc-a731-fe8e25a6dfd5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1488, REQ-1523: an environment's schema name and what an environment may be called."""

import pytest

from provisa.core.environments import (
    MAX_IDENTIFIER_BYTES,
    PROD,
    SCHEMA_SUFFIXES,
    EnvironmentNameError,
    env_schemas,
    is_env_name,
    max_env_name_length,
    org_schema,
    validate_env_name,
)


class TestOrgSchema:
    def test_prod_keeps_the_pre_environment_schema_name(self):
        # REQ-1488: an org that never creates an environment is unchanged.
        assert org_schema("acme") == "org_acme"
        assert org_schema("acme", PROD) == "org_acme"
        assert org_schema("acme", None, "_mv_cache") == "org_acme_mv_cache"
        assert org_schema("acme", PROD, "_mv_cache") == "org_acme_mv_cache"

    def test_an_environment_extends_the_org_schema_name(self):
        assert org_schema("acme", "dev_alice") == "org_acme_env_dev_alice"
        assert org_schema("acme", "dev_alice", "_api_cache") == "org_acme_env_dev_alice_api_cache"

    def test_the_separator_splits_one_org_from_one_environment(self):
        # REQ-1309 forbids an underscore in an org id, so the first `_env_` after the prefix is
        # always the boundary however either name is spelled.
        name = org_schema("acme", "env_of_env")
        assert name.split("_env_", 1) == ["org_acme", "env_of_env"]

    def test_an_unknown_store_suffix_is_refused(self):
        with pytest.raises(ValueError, match="unknown org store suffix"):
            org_schema("acme", "dev", "_not_a_store")

    def test_every_store_schema_of_one_environment_is_enumerated(self):
        assert env_schemas("acme", "dev") == [
            "org_acme_env_dev",
            "org_acme_env_dev_mv_cache",
            "org_acme_env_dev_api_cache",
            "org_acme_env_dev_gql_cache",
        ]
        assert env_schemas("acme") == [f"org_acme{s}" for s in SCHEMA_SUFFIXES]


class TestEnvName:
    @pytest.mark.parametrize("name", ["dev", "dev_alice", "pre_prod", "dev_1482_refund_fix", "t1"])
    def test_accepts_the_names_an_organization_actually_creates(self, name):
        assert is_env_name(name)
        validate_env_name("acme", name)

    @pytest.mark.parametrize(
        "name", ["", "d", "Dev", "dev-alice", "1dev", "_dev", "dev.alice", "d" * 33, "dev alice"]
    )
    def test_refuses_anything_that_is_not_an_unquoted_identifier(self, name):
        assert not is_env_name(name)
        with pytest.raises(EnvironmentNameError):
            validate_env_name("acme", name)

    def test_prod_is_never_created(self):
        # REQ-1487: prod exists from the org's creation.
        with pytest.raises(EnvironmentNameError, match="cannot be created again"):
            validate_env_name("acme", PROD)

    @pytest.mark.parametrize("name", ["org", "pg_temp", "pg_catalog"])
    def test_refuses_reserved_names(self, name):
        with pytest.raises(EnvironmentNameError, match="reserved"):
            validate_env_name("acme", name)

    def test_the_length_a_name_may_reach_depends_on_the_org_id(self):
        assert max_env_name_length("acme") == 32
        assert max_env_name_length("a" * 40) == 4

    def test_a_name_that_would_truncate_a_derived_schema_is_refused(self):
        long_org = "a" * 40
        allowed = max_env_name_length(long_org)
        validate_env_name(long_org, "d" * allowed)
        with pytest.raises(EnvironmentNameError, match="identifier limit"):
            validate_env_name(long_org, "d" * (allowed + 1))

    def test_no_accepted_name_derives_an_identifier_postgres_would_truncate(self):
        for org_id in ("a", "acme", "a" * 39, "a" * 40):
            name = "d" * max_env_name_length(org_id)
            validate_env_name(org_id, name)
            for schema in env_schemas(org_id, name):
                assert len(schema.encode()) <= MAX_IDENTIFIER_BYTES
