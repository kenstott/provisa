# Copyright (c) 2026 Kenneth Stott
# Canary: d1023ce1-9d83-4d0a-bef3-8c001a31e11d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986: a read missing a required parameter is refused naming each parameter once."""

from __future__ import annotations

from provisa.core.parameters import missing_parameters


def _param(name: str) -> dict:
    return {
        "column_name": name,
        "native_filter_type": "graphql_arg",
        "native_filter_required": True,
    }


def test_a_field_that_is_also_an_argument_is_one_parameter():
    """A remote field that is also an argument is held as its column and as its ``_nf_`` column;
    the refusal read "requires the parameter(s) ['id', 'id']"."""
    columns = [_param("id"), _param("_nf_id"), {"column_name": "name"}]

    assert missing_parameters(columns, {}, lambda _c: False) == ["id"]
    assert missing_parameters(columns, {"id": 3}, lambda _c: False) == []


def test_an_argument_spelled_unlike_its_field_is_one_parameter_named_by_its_column():
    """#297: the field ``breed_name`` is also the argument ``breedName`` (its ``_nf_`` column); the
    refusal read "requires the parameter(s) ['breed_name', 'breedName']"."""
    field, argument = _param("breed_name"), _param("_nf_breedName")

    assert missing_parameters([field, argument], {}, lambda _c: False) == ["breed_name"]
    assert missing_parameters([argument, field], {}, lambda _c: False) == ["breed_name"]
    assert missing_parameters([argument], {}, lambda _c: False) == ["breedName"]
    assert missing_parameters([field, argument], {"breedName": "pug"}, lambda _c: False) == []
