// Copyright (c) 2026 Kenneth Stott
// Canary: 31410c38-b388-4957-8ee4-b5d49b2777ef
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-990: the catalog an Iceberg source's table is read through when a SingleStore store lands
// it by pipeline. Mirrors provisa/federation/singlestore_pipeline.py (ICEBERG_CATALOG_HINTS): the
// four catalog types (maintainer, 2026-10-05) and the federation_hints each one needs.

import { descriptorField, fieldChoices, fieldsWhen, isRequired } from "./descriptor";

// REQ-2114: the catalog types, their fields and which are required are the descriptor's
// (provisa/api/admin/source_types.py ICEBERG_CATALOG).
const ICEBERG = "iceberg";
const HINTS = "federationHintsJson";
const TYPE_KEY = descriptorField(ICEBERG, HINTS, "iceberg_catalog_type").name;
const TABLE_ID = descriptorField(ICEBERG, HINTS, "iceberg_table_id");

export const ICEBERG_CATALOG_TYPES: readonly string[] = fieldChoices(ICEBERG, HINTS, TYPE_KEY);
export type IcebergCatalogType = string;

/** The table id, shown once a catalog type is chosen, and whether it is required. */
export const ICEBERG_TABLE_ID = { key: TABLE_ID.name, required: isRequired(TABLE_ID) };

/** The catalog fields each type shows (besides the type and the table id), and which are required. */
export const ICEBERG_CATALOG_FIELDS: Record<
  IcebergCatalogType,
  readonly { key: string; required: boolean }[]
> = Object.fromEntries(
  ICEBERG_CATALOG_TYPES.map((type) => [
    type,
    fieldsWhen(ICEBERG, HINTS, TYPE_KEY, type).map((f) => ({
      key: f.name,
      required: isRequired(f),
    })),
  ]),
);

/** Catalog types whose password SingleStore keeps in its unredacted pipeline CONFIG. */
export const CATALOG_PASSWORD_IN_CONFIG = new Set<IcebergCatalogType>(["JDBC", "SNOWFLAKE"]);

/** The catalog hints to save: every filled catalog field of the chosen type, and nothing of the
 * others (switching type does not carry a REST URI into a JDBC catalog). */
export function icebergCatalogHints(fields: Record<string, string>): Record<string, string> {
  const type = fields[TYPE_KEY];
  if (!type || !ICEBERG_CATALOG_TYPES.includes(type)) return {};
  const keys = [TYPE_KEY, TABLE_ID.name, ...ICEBERG_CATALOG_FIELDS[type].map((f) => f.key)];
  return Object.fromEntries(keys.filter((k) => fields[k]).map((k) => [k, fields[k]]));
}
