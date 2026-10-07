# Copyright (c) 2026 Kenneth Stott
# Canary: bcf80947-16ea-4554-acfd-1707943c6b89
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role with no domains: a configuration declaring one fails to load; the model the store holds
loads with it, and the role reaches nothing (REQ-1530, REQ-039, REQ-1919).

The save paths refuse an empty ``domain_access``; a stored row that nevertheless has none must not
refuse the whole org's model, which the store alone owns once seeded.
"""

from __future__ import annotations

import pytest

from provisa.core.config_loader import parse_config_dict, parse_store_raw


def _model(domain_access: list[str]) -> dict:
    return {
        "sources": [],
        "domains": [{"id": "sales", "description": "Sales"}],
        "tables": [],
        "roles": [
            {
                "id": "nodomains",
                "capabilities": ["query_development"],
                "domain_access": domain_access,
            }
        ],
    }


def test_a_configuration_declaring_a_role_with_no_domains_fails_to_load():
    with pytest.raises(ValueError, match="Role 'nodomains' must list at least one domain"):
        parse_config_dict(_model([]))


def test_the_stored_model_loads_with_a_role_that_has_no_domains():
    config = parse_store_raw(_model([]))
    (role,) = [r for r in config.roles if r.id == "nodomains"]
    assert role.domain_access == []


def test_a_stored_role_with_no_domains_reaches_no_domain():
    from provisa.security.rights import reaches_all_domains

    config = parse_store_raw(_model([]))
    (role,) = [r for r in config.roles if r.id == "nodomains"]
    assert not reaches_all_domains(role.domain_access)
    assert "sales" not in role.domain_access
