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

export const ICEBERG_CATALOG_TYPES = ["GLUE", "REST", "JDBC", "SNOWFLAKE"] as const;
export type IcebergCatalogType = (typeof ICEBERG_CATALOG_TYPES)[number];

/** Every catalog hint the form can show, in the order it shows them. */
export const ICEBERG_HINT_KEYS = [
  "iceberg_catalog_type",
  "iceberg_table_id",
  "iceberg_catalog_uri",
  "iceberg_catalog_name",
  "iceberg_catalog_warehouse",
  "iceberg_catalog_user",
  "iceberg_catalog_password",
  "iceberg_catalog_role",
] as const;

/** The catalog fields each type shows (besides the type and the table id), and which are required. */
export const ICEBERG_CATALOG_FIELDS: Record<
  IcebergCatalogType,
  readonly { key: (typeof ICEBERG_HINT_KEYS)[number]; required: boolean }[]
> = {
  GLUE: [],
  REST: [{ key: "iceberg_catalog_uri", required: true }],
  JDBC: [
    { key: "iceberg_catalog_name", required: true },
    { key: "iceberg_catalog_warehouse", required: true },
    { key: "iceberg_catalog_uri", required: true },
    { key: "iceberg_catalog_user", required: false },
    { key: "iceberg_catalog_password", required: false },
  ],
  SNOWFLAKE: [
    { key: "iceberg_catalog_uri", required: true },
    { key: "iceberg_catalog_user", required: true },
    { key: "iceberg_catalog_password", required: true },
    { key: "iceberg_catalog_role", required: true },
  ],
};

/** Catalog types whose password SingleStore keeps in its unredacted pipeline CONFIG. */
export const CATALOG_PASSWORD_IN_CONFIG = new Set<IcebergCatalogType>(["JDBC", "SNOWFLAKE"]);

/** The catalog hints to save: every filled catalog field of the chosen type, and nothing of the
 * others (switching type does not carry a REST URI into a JDBC catalog). */
export function icebergCatalogHints(fields: Record<string, string>): Record<string, string> {
  const type = fields.iceberg_catalog_type as IcebergCatalogType | undefined;
  if (!type || !ICEBERG_CATALOG_TYPES.includes(type)) return {};
  const keys = [
    "iceberg_catalog_type",
    "iceberg_table_id",
    ...ICEBERG_CATALOG_FIELDS[type].map((f) => f.key),
  ];
  return Object.fromEntries(keys.filter((k) => fields[k]).map((k) => [k, fields[k]]));
}
