# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7d5e19-8c04-4b63-9f1e-d6b3a0c8e527
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A synthetic dataset's name and store schema, and the refusal to read its tables beside tables
reading real data (REQ-1939, REQ-1487)."""

from __future__ import annotations

import pytest

from provisa.federation.registry_view import operator_floor
from provisa.federation.replica_address import (
    ReplicaRoutes,
    is_replicas_schema,
    is_write_surface,
    synthetic_schema,
)
from provisa.synthetic.datasets import SyntheticJoinRefused, check_name, check_scale


def test_a_dataset_schema_is_its_environments_and_a_write_surface():
    assert synthetic_schema("acme", "dev", "load_test") == "org_acme_env_dev_syn__load_test"
    assert is_replicas_schema("org_acme_env_dev_syn__load_test")
    assert is_write_surface("org_acme_env_dev_syn__load_test")
    # A live attach's folded schema, and an environment named with "syn", are neither.
    assert not is_write_surface("org_acme__syn__public")
    assert not is_write_surface("org_acme_env_x_syn")


def test_a_dataset_schema_over_the_identifier_limit_is_refused():
    with pytest.raises(ValueError, match="longer than 63 bytes"):
        synthetic_schema("acme", "a" * 31, "b" * 30)


@pytest.mark.parametrize("name", ["Load", "1x", "a", "a__b", "a-b", "x" * 33])
def test_a_dataset_name_must_be_a_plain_identifier(name):
    with pytest.raises(ValueError, match="synthetic dataset name"):
        check_name(name)


def test_a_scale_must_be_above_zero():
    check_scale(0.5, "d")
    with pytest.raises(ValueError, match="above 0"):
        check_scale(0, "d")


def _state(**routes):
    return type("S", (), {"replica_routes": ReplicaRoutes(**routes)})()


def test_a_synthetic_table_is_floored_and_never_read_beside_real_data():
    state = _state(
        floored={1: ("pg", "synthetic dataset 'd'")},
        unfloored={2: "pg", 3: "provisa-admin"},
        synthetic={1: "d"},
    )
    assert operator_floor(state, [1]) == {"pg": "synthetic dataset 'd'"}
    # The admin's own tables read neither and do not count as real data.
    assert operator_floor(state, [1, 3]) == {"pg": "synthetic dataset 'd'"}
    with pytest.raises(SyntheticJoinRefused, match="synthetic dataset 'd' and table 2"):
        operator_floor(state, [1, 2])
    assert operator_floor(state, [2]) == {}
