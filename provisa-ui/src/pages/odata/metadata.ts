// Copyright (c) 2026 Kenneth Stott
// Canary: 8b86ff7d-158f-4fda-94d7-b02582ceac11
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2193

// The OData explorer's model of the service, read from the role's $metadata (CSDL, as
// provisa/api/odata/csdl.py writes it): each entity set with its entity type's key, properties and
// navigation properties, a navigation property led to the set its binding names.

const EDM = "http://docs.oasis-open.org/odata/ns/edm";

export interface ODataProperty {
  name: string;
  /** The Edm type, `Edm.String` and so on. */
  type: string;
  nullable: boolean;
  precision: number | null;
  scale: string | null;
}

export interface ODataNavigation {
  name: string;
  /** The target's entity type, unqualified. */
  targetType: string;
  /** The entity set the container binds it to; null when it binds none. */
  targetSet: string | null;
  collection: boolean;
  /** Each referential constraint: this type's property and the target's it refers to. */
  constraints: { property: string; referenced: string }[];
}

export interface ODataEntitySet {
  name: string;
  /** The entity type, unqualified. */
  typeName: string;
  key: string[];
  properties: ODataProperty[];
  navigations: ODataNavigation[];
}

function children(parent: Element, localName: string): Element[] {
  return Array.from(parent.children).filter(
    (c) => c.namespaceURI === EDM && c.localName === localName,
  );
}

function required(el: Element, attribute: string): string {
  const value = el.getAttribute(attribute);
  if (value === null) {
    throw new Error(`$metadata: <${el.localName}> has no ${attribute}`);
  }
  return value;
}

/** `Namespace.Name` → `Name`; a name with no namespace is itself. */
function unqualified(name: string): string {
  return name.slice(name.lastIndexOf(".") + 1);
}

function property(el: Element): ODataProperty {
  const precision = el.getAttribute("Precision");
  return {
    name: required(el, "Name"),
    type: required(el, "Type"),
    nullable: el.getAttribute("Nullable") !== "false",
    precision: precision === null ? null : Number(precision),
    scale: el.getAttribute("Scale"),
  };
}

function navigation(el: Element, bindings: Map<string, string>): ODataNavigation {
  const name = required(el, "Name");
  const type = required(el, "Type");
  const collection = /^Collection\((.*)\)$/.exec(type);
  return {
    name,
    targetType: unqualified(collection ? collection[1] : type),
    targetSet: bindings.get(name) ?? null,
    collection: collection !== null,
    constraints: children(el, "ReferentialConstraint").map((c) => ({
      property: required(c, "Property"),
      referenced: required(c, "ReferencedProperty"),
    })),
  };
}

/** The entity sets of a CSDL document, in the order its container declares them. */
export function parseMetadata(xml: string): ODataEntitySet[] {
  const doc = new DOMParser().parseFromString(xml, "application/xml");
  if (doc.getElementsByTagName("parsererror").length > 0) {
    throw new Error("$metadata is not well-formed XML");
  }
  const types = new Map<string, Element>();
  for (const el of Array.from(doc.getElementsByTagNameNS(EDM, "EntityType"))) {
    const schema = el.parentElement;
    const namespace = schema?.getAttribute("Namespace");
    const name = required(el, "Name");
    types.set(namespace ? `${namespace}.${name}` : name, el);
  }
  return Array.from(doc.getElementsByTagNameNS(EDM, "EntitySet")).map((set) => {
    const typeName = required(set, "EntityType");
    const type = types.get(typeName);
    if (!type) throw new Error(`$metadata: entity type ${typeName} is not declared`);
    const bindings = new Map(
      children(set, "NavigationPropertyBinding").map((b) => [
        required(b, "Path"),
        required(b, "Target"),
      ]),
    );
    const keys = children(type, "Key");
    return {
      name: required(set, "Name"),
      typeName: unqualified(typeName),
      key: keys.length ? children(keys[0], "PropertyRef").map((r) => required(r, "Name")) : [],
      properties: children(type, "Property").map(property),
      navigations: children(type, "NavigationProperty").map((n) => navigation(n, bindings)),
    };
  });
}
