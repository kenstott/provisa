# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7d1e62-9a35-4c08-8f1d-2e6a0c93b5d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy_view_to_db writes a real view back only to the RDBs that take its DDL; every other
source type is refused by name before any DDL runs."""

import pytest

from provisa.api.admin.schema_mutation_ops import (
    VIEW_DEPLOY_SOURCE_TYPES,
    view_deploy_type_refusal,
)
from provisa.core.source_registry import SOURCE_TO_DIALECT


@pytest.mark.parametrize("source_type", sorted(VIEW_DEPLOY_SOURCE_TYPES))
def test_a_supported_type_is_not_refused(source_type):
    assert view_deploy_type_refusal("src1", source_type) is None


@pytest.mark.parametrize("source_type", sorted(set(SOURCE_TO_DIALECT) - VIEW_DEPLOY_SOURCE_TYPES))
def test_every_other_registered_type_is_refused_naming_source_and_type(source_type):
    refusal = view_deploy_type_refusal("src1", source_type)
    assert refusal is not None
    assert refusal.success is False
    assert refusal.code == "schema.view_deploy_unsupported_type"
    assert "'src1'" in refusal.message and repr(source_type) in refusal.message


def test_the_supported_types_are_all_registered_source_types():
    assert VIEW_DEPLOY_SOURCE_TYPES <= set(SOURCE_TO_DIALECT)
