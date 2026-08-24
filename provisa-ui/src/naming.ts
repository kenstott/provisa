// Copyright (c) 2026 Kenneth Stott
// Canary: bc4d562d-6c76-4105-ba11-da583219d683
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

/** Mirror of Python _upper_snake (provisa/cypher/label_map.py): any name → UPPER_SNAKE. */
export function upperSnake(text: string): string {
  const spaced = text.replace(/([a-z0-9])([A-Z])/g, "$1_$2");
  return spaced
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "")
    .toUpperCase();
}

/**
 * REQ-1586: the Cypher relationship type a relationship row is exposed as.
 *
 * Mirror of Python junction_rel_type plus the ordinary alias path. A junction-backed row carries no
 * usable alias of its own — its type comes from whichever source the steward nominated — so any
 * client that names an edge in a generated pattern has to derive it the way the compiler does.
 * Returns null when the row names no type at all, which is a row that cannot be matched by type.
 */
export function cypherRelType(r: {
  alias: string | null;
  computedCypherAlias?: string | null;
  viaTableName?: string | null;
  viaTypeValue?: string | null;
  viaLabelSource?: string | null;
}): string | null {
  if (r.viaTableName) {
    // No fallback chain: the nomination is required to be present for the source it names, so a
    // missing value is a defect in the stored row, not a case to paper over.
    if (r.viaLabelSource === "column") return r.viaTypeValue ? upperSnake(r.viaTypeValue) : null;
    if (r.viaLabelSource === "table") return upperSnake(r.viaTableName);
    if (r.viaLabelSource === "fixed") return r.alias ? upperSnake(r.alias) : null;
    return null;
  }
  const name = r.alias ?? r.computedCypherAlias ?? "";
  return name ? name.toUpperCase() : null;
}

/**
 * REQ-1586: split a stored key declaration into its ordered columns. Mirror of Python key_list
 * (provisa/compiler/sql_types.py). A junction end and the relationship end it pairs with are each
 * a comma-separated ordered list, so a composite key is mapped by listing its columns in order.
 */
export function keyList(stored: string | null | undefined): string[] {
  if (!stored) return [];
  return stored
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
}
