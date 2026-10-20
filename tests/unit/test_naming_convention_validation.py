# Copyright (c) 2026 Kenneth Stott
# Canary: c14efd89-774d-4dd2-9b6e-a31e07b06be1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-416 — the admin naming-convention update path validates the convention.

`update_gql_naming_convention` calls `validation_error_for_convention` and rejects
free-form strings before reconfiguring naming and rebuilding schemas; only the presets
(and their literal aliases) are valid.
"""

from provisa.compiler.naming import (
    VALID_CONVENTIONS,
    validation_error_for_convention,
)


def test_invalid_convention_returns_error():
    err = validation_error_for_convention("free-form-garbage")
    assert err is not None
    assert "Invalid naming convention" in err


def test_invalid_convention_lists_valid_options():
    err = validation_error_for_convention("nope")
    assert err is not None
    for preset in ("snake", "hasura_graphql", "apollo_graphql"):
        assert preset in err


def test_valid_presets_pass():
    for preset in ("snake", "hasura_graphql", "apollo_graphql"):
        assert preset in VALID_CONVENTIONS
        assert validation_error_for_convention(preset) is None


def test_literal_aliases_pass():
    # Aliases like "hasura-default"/"graphql" are in VALID_CONVENTIONS and must be accepted.
    for alias in VALID_CONVENTIONS:
        assert validation_error_for_convention(alias) is None


def test_the_admin_lists_the_presets_the_server_takes():
    """REQ-416: the admin offers the conventions read from the server, each one it accepts."""
    from provisa.api.admin.schema_query import Query
    from provisa.compiler.naming import validation_error_for_convention

    listed = Query().naming_conventions()
    assert listed == ["snake", "hasura_graphql", "apollo_graphql"]
    assert all(validation_error_for_convention(c) is None for c in listed)


def test_a_config_names_a_convention_by_a_hasura_spelling_and_holds_its_preset():
    """REQ-195, REQ-416: a config may spell a convention as Hasura or DDN does; the source, the
    table and the deployment's naming hold the preset it names, and any other name is refused."""
    import pytest
    from pydantic import ValidationError

    from provisa.core.models import NamingConfig, Source, Table

    source = Source.model_validate(
        {
            "id": "pg",
            "type": "postgresql",
            "host": "h",
            "database": "d",
            "username": "u",
            "gql_naming_convention": "hasura-default",
        }
    )
    assert source.gql_naming_convention == "hasura_graphql"
    table = Table.model_validate(
        {
            "source_id": "pg",
            "domain_id": "d",
            "table_name": "t",
            "columns": [],
            "gql_naming_convention": "graphql-default",
        }
    )
    assert table.gql_naming_convention == "apollo_graphql"
    assert NamingConfig(convention="graphql").convention == "apollo_graphql"
    for bad in ("camelCase", "PascalCase", "none"):
        with pytest.raises(ValidationError):
            NamingConfig(convention=bad)
        with pytest.raises(ValidationError):
            Table.model_validate(
                {
                    "source_id": "pg",
                    "domain_id": "d",
                    "table_name": "t",
                    "columns": [],
                    "gql_naming_convention": bad,
                }
            )
