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
test_metadata_export_governance.py`` asserts the bodies never appear in a snapshot.
"""

# Requirements: REQ-039, REQ-040, REQ-041, REQ-1071

from __future__ import annotations

from provisa.api.metadata_export.model import (
    GovernanceSignal,
    GovernanceTag,
)
from provisa.api.metadata_export.refs import (
    TableIndex,
    column_ref,
    column_uri,
    domain_uri,
    table_ref,
    table_uri,
    tag_uri,
)
from provisa.core.models import Column, ProvisaConfig, Table


def _mask_tags(
    table: Table, column: Column, all_roles: tuple[str, ...], org_id: str | None
) -> list[GovernanceTag]:
    """One tag per entry of the column's mask list that masks (REQ-2059): the roles that read the
    column through it are restricted by it, the roles that read it unmasked are exempt."""
    from provisa.security.masking import entry_for

    masks = [e.model_dump() for e in column.masks]
    reads = {r: entry_for(masks, [r]) for r in all_roles}
    exempt = tuple(sorted(r for r, i in reads.items() if i is None or masks[i]["mask"] is None))
    uri = column_uri(org_id, table, column.name, column.alias)
    return [
        GovernanceTag(
            asset=column_ref(table, column.name),
            signal=GovernanceSignal.MASKED,
            # Identifies the rule without carrying it: the masked asset, the entry and the mask
            # KIND. The pattern, replacement and value are the policy body and stay in Provisa.
            # REQ-1385: the governed element is addressed by its URN, not physical coordinates.
            rule_id=f"mask:{uri}:{i}:{entry['mask']['type']}",
            restricted_roles=tuple(r for r in all_roles if reads[r] == i),
            exempt_roles=exempt,
        )
        for i, entry in enumerate(masks)
        if entry["mask"] is not None
    ]


def _visibility_tags(
    table: Table, column: Column, all_roles: tuple[str, ...], org_id: str | None
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
            rule_id=f"visibility:{column_uri(org_id, table, column.name, column.alias)}",  # REQ-1385
            restricted_roles=restricted,
            exempt_roles=tuple(sorted(visible)),
        )
    ]


def _rls_tags(
    config: ProvisaConfig, index: TableIndex, all_roles: tuple[str, ...], org_id: str | None
) -> list[GovernanceTag]:
    """One tag per (rule, restricted table). A domain-scoped rule tags every table in the domain,
    and a tag-scoped rule (REQ-1384) every table carrying the tag.

    The rule's ``filter`` predicate is never read here. Its identity is the scope it applies to
    plus the role it filters, which is enough for a catalog consumer to trace it back to Provisa.
    """
    tags: list[GovernanceTag] = []
    for rule in config.rls_rules:
        if rule.table_id is not None:
            targets = [index.resolve(rule.table_id, f"RLS rule for role {rule.role_id!r}")]
            # REQ-1385: scope is the governed element's URN, not its config-vocabulary name.
            scope = table_uri(org_id, targets[0])
        elif rule.domain_id is not None:
            targets = [t for t in config.tables if t.domain_id == rule.domain_id]
            scope = domain_uri(org_id, rule.domain_id)
        elif rule.tag_id is not None:  # REQ-1384
            targets = [
                index.resolve(
                    a.table_ref, f"RLS rule on tag {rule.tag_id!r} for role {rule.role_id!r}"
                )
                for a in config.tag_assignments
                if a.object_type == "table"
                and a.base_tag_id() == rule.tag_id
                and a.table_ref is not None
            ]
            scope = tag_uri(org_id, rule.tag_id)
        else:
            raise ValueError(
                f"RLS rule for role {rule.role_id!r} names neither a table, a domain nor a "
                "tag; its scope cannot be published"
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


def build_governance_tags(
    config: ProvisaConfig, org_id: str | None
) -> list[GovernanceTag]:  # REQ-1071
    """Every enforcement fact in the config, as catalog tags."""
    all_roles = tuple(sorted(role.id for role in config.roles))
    index = TableIndex(config.tables)
    tags: list[GovernanceTag] = []
    for table in config.tables:
        for column in table.columns:
            tags.extend(_mask_tags(table, column, all_roles, org_id))
            tags.extend(_visibility_tags(table, column, all_roles, org_id))
    tags.extend(_rls_tags(config, index, all_roles, org_id))
    return tags
