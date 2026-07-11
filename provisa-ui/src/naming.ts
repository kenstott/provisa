// Copyright (c) 2026 Kenneth Stott
// Canary: a1f2052b-6d0c-4f11-ac0a-a427d9968419
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * TypeScript mirror of provisa/cypher/label_map.py naming utilities.
 *
 * These functions derive Cypher labels from raw DB table names using the same
 * rules as the Python backend so that the UI and backend agree on label format.
 */

/** Mirror of Python _pascal(): uppercase first letter of each word segment. */
export function toPascal(s: string): string {
  const parts = s.split(/[_-]+/).filter(Boolean);
  if (parts.length === 1) return s.length > 0 ? s[0].toUpperCase() + s.slice(1) : s;
  return parts.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join("");
}

/**
 * Mirror of Python _split_cypher_labels():
 * "shelter__animalBreeds" → ["Shelter", "AnimalBreeds"]
 * "orders"               → [null,      "Orders"]
 */
export function splitCypherLabels(fieldName: string): [string | null, string] {
  const idx = fieldName.indexOf("__");
  if (idx >= 0) {
    return [toPascal(fieldName.slice(0, idx)), toPascal(fieldName.slice(idx + 2))];
  }
  return [null, toPascal(fieldName)];
}

/**
 * Extract the table-label part from a raw DB table name.
 * "shelter__animalBreeds" → "AnimalBreeds"
 * "pets"                 → "Pets"
 */
export function tableLabel(dbTableName: string): string {
  return splitCypherLabels(dbTableName)[1];
}

/** Mirror of Python apply_sql_name (snake convention): camelCase/PascalCase → snake_case. */
export function toSnakeCase(name: string): string {
  // camelCase / PascalCase → snake_case (intermediate normalization only — not for UI display)
  let s = name.replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2");
  s = s.replace(/([a-z0-9])([A-Z])/g, "$1_$2");
  return s.toLowerCase();
}

/** Mirror of Python apply_gql_name (camelCase convention). */
export function toCamelCase(name: string): string {
  return toSnakeCase(name).replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
}

/** Mirror of Python apply_cql_label (PascalCase convention). */
export function toPascalCase(name: string): string {
  const cc = toCamelCase(name);
  // Mirror of Python _to_pascal_case: capitalize first letter
  return cc.charAt(0).toUpperCase() + cc.slice(1);
}

/** Mirror of Python apply_convention: apply a named convention to a name. */
export function applyConvention(name: string, convention: string | null | undefined): string {
  if (convention === "snake_case") return toSnakeCase(name);
  if (convention === "camelCase") return toCamelCase(name);
  if (convention === "PascalCase") return toPascalCase(name);
  return toSnakeCase(name);
}
