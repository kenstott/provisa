# Copyright (c) 2026 Kenneth Stott
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
