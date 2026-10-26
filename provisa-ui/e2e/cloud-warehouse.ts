// Copyright (c) 2026 Kenneth Stott
// Canary: e99c8621-bed5-4286-8296-c3ce24191543
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The live cloud warehouses (Snowflake, Databricks, BigQuery, Fabric) are standing accounts every
// CI run shares. A run seeds a schema of its own there: with one fixed schema, a concurrent run's
// teardown dropped the table another run was reading, and a concurrent seed's DDL failed another's
// snapshot read (Fabric, SQL Server error 3961).

import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const SEED = path.join(ROOT, "provisa-ui", "e2e", "cloud_warehouse_seed.py");

/** This worker's own schema in each warehouse (Snowflake: its database, upper-cased there). */
export const WAREHOUSE_SCHEMA = `provisa_ui_e2e_${process.env.GITHUB_RUN_ID ?? "local"}_${process.pid}`;

export type Warehouse = "snowflake" | "databricks" | "bigquery" | "fabric";

/** Creates (up) or drops (down) this worker's schema and its widgets table in ``engine``. */
export function seedWarehouse(engine: Warehouse, action: "up" | "down"): void {
  execFileSync(PYTHON, [SEED, engine, action, WAREHOUSE_SCHEMA], { stdio: "pipe" });
}
