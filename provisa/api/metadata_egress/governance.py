# Copyright (c) 2026 Kenneth Stott
# Canary: 6b41d8ce-90a2-4f37-b5c1-2e7d0a6f8934
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Project enforcement facts outward as catalog tags (REQ-1071).

Provisa already computes which columns are masked, which tables an RLS rule filters, and which
columns a role cannot see (REQ-039, REQ-040, REQ-041). Those facts are published so a consumer
reading the external catalog knows an asset is restricted without opening Provisa.

What is published is THAT an asset is governed, which rule governs it, and who is or is not
subject to it. What is never published is the rule body — a mask pattern or an RLS predicate is
the policy itself, and putting it in an external catalog next to the restricted asset hands a
reader the shape of the data the policy exists to withhold. ``tests/unit/
test_metadata_egress_governance.py`` asserts the bodies never appear in a snapshot.
"""

# Requirements: REQ-039, REQ-040, REQ-041, REQ-1071

from __future__ import annotations

from provisa.api.metadata_egress.model import (
    GovernanceSignal,
    GovernanceTag,
)
from provisa.api.metadata_egress.refs import TableIndex, column_ref, table_ref
from provisa.core.models import Column, ProvisaConfig, Table


def _mask_tags(table: Table, column: Column, all_roles: tuple[str, ...]) -> list[GovernanceTag]:
    if column.mask_type is None:
        return []
    exempt = tuple(sorted(column.unmasked_to))
    return [
        GovernanceTag(
            asset=column_ref(table, column.name),
            signal=GovernanceSignal.MASKED,
            # Identifies the rule without carrying it: the masked asset and the mask KIND.
            # mask_pattern / mask_replace / mask_value are the policy body and stay in Provisa.
            rule_id=f"mask:{'.'.join(column_ref(table, column.name).parts)}:{column.mask_type}",
            restricted_roles=tuple(r for r in all_roles if r not in column.unmasked_to),
            exempt_roles=exempt,
        )
    ]


def _visibility_tags(
    table: Table, column: Column, all_roles: tuple[str, ...]
) -> list[GovernanceTag]:
    # Membership is literal — provisa/security/visibility.py enforces ``role_id in visible_to``
    # with no wildcard for columns. The tag says exactly what the engine does.
    visible = set(column.visible_to)
    restricted = tuple(r for r in all_roles if r not in visible)
    if not restricted:
        return []
    return [
        GovernanceTag(
            asset=column_ref(table, column.name),
            signal=GovernanceSignal.VISIBILITY_RESTRICTED,
            rule_id=f"visibility:{'.'.join(column_ref(table, column.name).parts)}",
            restricted_roles=restricted,
            exempt_roles=tuple(sorted(visible)),
        )
    ]


def _rls_tags(
    config: ProvisaConfig, index: TableIndex, all_roles: tuple[str, ...]
) -> list[GovernanceTag]:
    """One tag per (rule, restricted table). A domain-scoped rule tags every table in the domain.

    The rule's ``filter`` predicate is never read here. Its identity is the scope it applies to
    plus the role it filters, which is enough for a catalog consumer to trace it back to Provisa.
    """
    tags: list[GovernanceTag] = []
    for rule in config.rls_rules:
        if rule.table_id is not None:
            targets = [index.resolve(rule.table_id, f"RLS rule for role {rule.role_id!r}")]
            scope = rule.table_id
        elif rule.domain_id is not None:
            targets = [t for t in config.tables if t.domain_id == rule.domain_id]
            scope = rule.domain_id
        else:
            raise ValueError(
                f"RLS rule for role {rule.role_id!r} names neither a table nor a domain; "
                "its scope cannot be published"
            )
        for target in targets:
            tags.append(
                GovernanceTag(
                    asset=table_ref(target),
                    signal=GovernanceSignal.RLS_RESTRICTED,
                    rule_id=f"rls:{scope}:{rule.role_id}",
                    restricted_roles=(rule.role_id,),
                    # An RLS rule filters exactly the role it names; every other configured role
                    # reads the table unfiltered by THIS rule.
                    exempt_roles=tuple(r for r in all_roles if r != rule.role_id),
                )
            )
    return tags


def build_governance_tags(config: ProvisaConfig) -> list[GovernanceTag]:  # REQ-1071
    """Every enforcement fact in the config, as catalog tags."""
    all_roles = tuple(sorted(role.id for role in config.roles))
    index = TableIndex(config.tables)
    tags: list[GovernanceTag] = []
    for table in config.tables:
        for column in table.columns:
            tags.extend(_mask_tags(table, column, all_roles))
            tags.extend(_visibility_tags(table, column, all_roles))
    tags.extend(_rls_tags(config, index, all_roles))
    return tags
