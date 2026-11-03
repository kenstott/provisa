# Copyright (c) 2026 Kenneth Stott
# Canary: 45ccf2e5-f2b3-4c25-883a-c2a70cd719bf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's model as an OData V4 CSDL document ($metadata, REQ-2193) and its service document."""

# Requirements: REQ-2193

from __future__ import annotations

from typing import Any
from xml.sax.saxutils import quoteattr

from provisa.api.odata.model import EntitySet, Navigation, Property

NAMESPACE = "Provisa"
# Each entity set names the domain of the table it reads, so a client narrows the model to the
# domains it works in. A term the schema declares, so a client that validates the document finds it.
DOMAIN_TERM = f"{NAMESPACE}.Domain"
_DOMAIN_TERM_DECLARATION = '<Term Name="Domain" Type="Edm.String" AppliesTo="EntitySet"/>'
_EDMX = "http://docs.oasis-open.org/odata/ns/edmx"
_EDM = "http://docs.oasis-open.org/odata/ns/edm"


def _attributes(**attributes: Any) -> str:  # noqa: ANN401 — attribute values
    return "".join(
        f" {name}={quoteattr(str(value))}"
        for name, value in attributes.items()
        if value is not None
    )


def _property(prop: Property, key: tuple[str, ...]) -> str:
    # Only a key is declared non-null: a mask or a source may give any other column a NULL its
    # registration does not, and a client refuses a NULL in a non-nullable property.
    return "<Property{}/>".format(
        _attributes(
            Name=prop.name,
            Type=prop.type.name,
            Nullable="false" if prop.name in key else None,
            Precision=prop.type.precision,
            Scale=prop.type.scale,
        )
    )


def _navigation(navigation: Navigation) -> str:
    target = f"{NAMESPACE}.{navigation.target_type}"
    declared = _attributes(
        Name=navigation.name,
        Type=f"Collection({target})" if navigation.collection else target,
    )
    constraints = "".join(
        f"<ReferentialConstraint{_attributes(Property=name, ReferencedProperty=referenced)}/>"
        for name, referenced in navigation.constraints
    )
    return f"<NavigationProperty{declared}>{constraints}</NavigationProperty>"


def _entity_type(entity: EntitySet) -> str:
    key = "".join(f"<PropertyRef{_attributes(Name=name)}/>" for name in entity.key)
    members = "".join(_property(p, entity.key) for p in entity.properties)
    navigations = "".join(_navigation(n) for n in entity.navigations)
    return (
        f"<EntityType{_attributes(Name=entity.type_name)}>"
        + (f"<Key>{key}</Key>" if key else "")
        + f"{members}{navigations}</EntityType>"
    )


def _entity_set(entity: EntitySet) -> str:
    bindings = "".join(
        f"<NavigationPropertyBinding{_attributes(Path=n.name, Target=n.target_set)}/>"
        for n in entity.navigations
    )
    declared = _attributes(Name=entity.name, EntityType=f"{NAMESPACE}.{entity.type_name}")
    domain = f"<Annotation{_attributes(Term=DOMAIN_TERM, String=entity.domain)}/>"
    return f"<EntitySet{declared}>{bindings}{domain}</EntitySet>"


def metadata_document(entities: list[EntitySet]) -> str:
    """The CSDL of ``entities``, one role's whole model."""
    types = "".join(_entity_type(e) for e in entities)
    sets = "".join(_entity_set(e) for e in entities)
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        f'<edmx:Edmx xmlns:edmx="{_EDMX}" Version="4.0"><edmx:DataServices>'
        f'<Schema xmlns="{_EDM}" Namespace="{NAMESPACE}">{_DOMAIN_TERM_DECLARATION}{types}'
        f'<EntityContainer Name="Default">{sets}</EntityContainer>'
        "</Schema></edmx:DataServices></edmx:Edmx>"
    )


def service_document(root: str, entities: list[EntitySet]) -> dict[str, Any]:
    """The service document of ``entities``, under the service root ``root``."""
    return {
        "@odata.context": f"{root}/$metadata",
        "value": [{"name": e.name, "kind": "EntitySet", "url": e.name} for e in entities],
    }
