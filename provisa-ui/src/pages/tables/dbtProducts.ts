// Copyright (c) 2026 Kenneth Stott
// Canary: 5a3f8c20-6e1b-47d9-b2c4-e91d07f6a3b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2228: a dbt source's public models with enforced contracts, registered all at once from
// Register Table as draft data products, after a preview: one per dbt group, or one for a project
// with no groups.

import { requestFailed, serverMessage } from "../../i18n/serverMessage";

export interface DbtProductModel {
  unique_id: string;
  name: string;
  schema: string;
  relation: string;
  version: string | null;
  description: string | null;
  table_id: number | null;
  columns: { name: string; data_type: string | null; required: boolean }[];
}

export interface DbtPlannedProduct {
  id: string;
  group: string | null;
  owner: { name: string | null; email: string | null } | null;
  owner_role: string | null;
  exists: boolean;
  tests: string[];
  models: DbtProductModel[];
}

export interface DbtProductsPreview {
  products: DbtPlannedProduct[];
  ungrouped: string[];
}

async function readJson<T>(resp: Response, op: string): Promise<T> {
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed(op, resp.status)));
  }
  return (await resp.json()) as T;
}

const productsUrl = (sourceId: string) => `/admin/dbt/products/${encodeURIComponent(sourceId)}`;

export const messageOf = (err: unknown) => (err instanceof Error ? err.message : String(err));

export async function fetchDbtProductsPreview(sourceId: string): Promise<DbtProductsPreview> {
  return readJson<DbtProductsPreview>(await fetch(productsUrl(sourceId)), "dbt products");
}

/** Registers every public model and creates the draft products; the ids of those created. */
export async function registerDbtProducts(sourceId: string, domainId: string): Promise<string[]> {
  const said = await readJson<{ data_products: string[] }>(
    await fetch(productsUrl(sourceId), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ domain_id: domainId }),
    }),
    "dbt products",
  );
  return said.data_products;
}

/** Whether the preview can be confirmed: something is offered and no product id is in use. */
export const confirmable = (preview: DbtProductsPreview | null) =>
  preview !== null && preview.products.length > 0 && preview.products.every((p) => !p.exists);
