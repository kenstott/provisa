// Copyright (c) 2026 Kenneth Stott
// Canary: 7d1f3b9e-4c2a-4e8f-9b61-2a5c8e0d4f17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2114 (amended 2026-11-24): each source type's settings are defined once, on the server
// (provisa/api/admin/source_types.py), and written for the UI to src/generated/sourceTypes.json
// (scripts/gen_source_types.py). The form builders read their keys, required and secret flags,
// choices and defaults from here, so the form and Polly's source_types tool cannot disagree.
// A key a builder asks for that the descriptor does not list is refused by name at load.

import generated from "../../generated/sourceTypes.json";

/** Where a field is given (FieldAt in source_types.py). */
export type FieldAt = "input" | "mappingJson" | "federationHintsJson" | "body" | "setup";

/** One field of a type, as the descriptor states it. */
export interface DescriptorField {
  name: string;
  at: FieldAt;
  required?: boolean;
  secret?: boolean;
  /** The choice it belongs to: `key=a|b`, `key!=a`, or a bare condition. */
  when?: string;
  note?: string;
  value?: unknown;
  default?: unknown;
  choices?: string[];
}

interface TypeDescriptor {
  name: string;
  fields: DescriptorField[];
  mapping_json: string[];
  federation_hints: string[];
}

const TYPES = new Map<string, TypeDescriptor>(
  (generated.types as unknown as TypeDescriptor[]).map((t) => [t.name, t]),
);

/** Whether a field is required. The descriptor's JSON leaves a false flag out (as_json). */
export const isRequired = (f: DescriptorField): boolean => f.required === true;

/** Whether a field is a secret. The descriptor's JSON leaves a false flag out (as_json). */
export const isSecret = (f: DescriptorField): boolean => f.secret === true;

/** A type's descriptor; refused by name when the descriptor offers no such type. */
function typeDescriptor(type: string): TypeDescriptor {
  const found = TYPES.get(type);
  if (!found) throw new Error(`source type ${type} is not in the descriptor`);
  return found;
}

/** A type's fields given at `at`, in the descriptor's order. */
export function descriptorFields(type: string, at: FieldAt): DescriptorField[] {
  return typeDescriptor(type).fields.filter((f) => f.at === at);
}

/** One field; refused by name when the type has no such field there. A key stated for more than
 * one choice (an Iceberg catalog's URI) is its first entry. */
export function descriptorField(type: string, at: FieldAt, name: string): DescriptorField {
  const found = descriptorFields(type, at).find((f) => f.name === name);
  if (!found) throw new Error(`source type ${type} has no ${at} field ${name}`);
  return found;
}

/** The mapping keys a type is saved with. */
export const mappingKeys = (type: string): string[] => typeDescriptor(type).mapping_json;

/** The federation hint keys a type is saved with. */
export const hintKeys = (type: string): string[] => typeDescriptor(type).federation_hints;

/** A mapping key of `type`, refused by name when the descriptor does not list it. */
export const mappingKey = (type: string, name: string): string =>
  descriptorField(type, "mappingJson", name).name;

/** A mapping key of `type` the descriptor lists by its parts (`settings.cluster` lists `settings`),
 * refused by name when it lists neither the key nor a part of it. */
export function mappingParentKey(type: string, name: string): string {
  const fields = descriptorFields(type, "mappingJson");
  if (!fields.some((f) => f.name === name || f.name.startsWith(`${name}.`))) {
    throw new Error(`source type ${type} has no mappingJson field ${name}`);
  }
  return name;
}

/** Several mapping keys of `type` by name, each refused by name when the descriptor does not list
 * it. */
export function mappingKeysOf<N extends string>(
  type: string,
  names: readonly N[],
): Record<N, string> {
  return Object.fromEntries(names.map((n) => [n, mappingKey(type, n)])) as Record<N, string>;
}

/** A federation hint key of `type`, refused by name when the descriptor does not list it. */
export const hintKey = (type: string, name: string): string =>
  descriptorField(type, "federationHintsJson", name).name;

/** The values a field takes; refused by name when the descriptor states none. */
export function fieldChoices(type: string, at: FieldAt, name: string): string[] {
  const choices = descriptorField(type, at, name).choices;
  if (!choices?.length) throw new Error(`${type} ${at} field ${name} states no choices`);
  return choices;
}

/** The default a field takes; refused by name when the descriptor states none. */
export function fieldDefault(type: string, at: FieldAt, name: string): unknown {
  const value = descriptorField(type, at, name).default;
  if (value === undefined) throw new Error(`${type} ${at} field ${name} states no default`);
  return value;
}

/** The fixed value a field always takes; refused by name when the descriptor states none. */
export function fieldValue(type: string, at: FieldAt, name: string): unknown {
  const value = descriptorField(type, at, name).value;
  if (value === undefined) throw new Error(`${type} ${at} field ${name} states no value`);
  return value;
}

/** The values a `when` of the form `key=a|b` names, or null for any other condition. */
function whenValues(when: string | undefined, key: string): string[] | null {
  const prefix = `${key}=`;
  return when?.startsWith(prefix) ? when.slice(prefix.length).split("|") : null;
}

/** The fields given at `at` that belong to the choice `key=value`, in the descriptor's order. */
export function fieldsWhen(
  type: string,
  at: FieldAt,
  key: string,
  value: string,
): DescriptorField[] {
  return descriptorFields(type, at).filter(
    (f) => whenValues(f.when, key)?.includes(value) ?? false,
  );
}

/** The values of `key` the type's fields are stated for, in the descriptor's order. */
export function whenChoices(type: string, at: FieldAt, key: string): string[] {
  const out: string[] = [];
  for (const f of descriptorFields(type, at)) {
    for (const v of whenValues(f.when, key) ?? []) if (!out.includes(v)) out.push(v);
  }
  return out;
}

/** The fields given at `at` that belong to no choice. */
export const fieldsWithoutChoice = (type: string, at: FieldAt): DescriptorField[] =>
  descriptorFields(type, at).filter((f) => !f.when);
