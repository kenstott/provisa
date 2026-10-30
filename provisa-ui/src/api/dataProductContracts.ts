// Copyright (c) 2026 Kenneth Stott
// Canary: 4c2e9a71-8d35-4b60-a1f7-3e9b0c5d2a84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158, REQ-2118: a data product's output-port contracts as ODCS 3.1 documents, each element
// declared or derived -- the one document a catalog pulls, read by the product's contracts list.

import { serverMessage, requestFailed } from "../i18n/serverMessage";
import type { ContractVersion } from "../types/admin";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** An ODCS custom property, SLA property or other keyed element: its fields as ODCS names them. */
export type OdcsRecord = Record<string, unknown>;

export interface OdcsProperty extends OdcsRecord {
  name: string;
  logicalType?: string;
  physicalType?: string;
  logicalTypeOptions?: OdcsRecord;
  required?: boolean;
  unique?: boolean;
  primaryKey?: boolean;
  classification?: string;
  description?: string;
  examples?: unknown[];
  quality?: OdcsRecord[];
}

export interface OdcsSchemaObject extends OdcsRecord {
  name: string;
  physicalName?: string;
  physicalType?: string;
  description?: string;
  properties?: OdcsProperty[];
  relationships?: OdcsRecord[];
  quality?: OdcsRecord[];
}

export interface OdcsContract extends OdcsRecord {
  id: string;
  name?: string;
  version: string;
  status: string;
  domain?: string;
  dataProduct?: string;
  tenant?: string;
  description?: { purpose?: string; limitations?: string; usage?: string };
  contractCreatedTs?: string;
  schema?: OdcsSchemaObject[];
  slaProperties?: OdcsRecord[];
  slaDefaultElement?: string;
  support?: OdcsRecord[];
  team?: OdcsRecord & { name?: string; members?: OdcsRecord[] };
  roles?: OdcsRecord[];
  servers?: OdcsRecord[];
  price?: OdcsRecord;
  customProperties?: { property: string; value: unknown }[];
  tags?: string[];
}

/** One output port's contract, the port named by its member's address (source.schema.table): read
 * when its row in the product's contracts list opens. */
export async function fetchProductContract(
  productId: string,
  member: string,
): Promise<OdcsContract> {
  const resp = await fetch(
    `${API_BASE}/data-products/${encodeURIComponent(productId)}/contracts/${encodeURIComponent(member)}.json`,
  );
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Product contract", resp.status)));
  }
  return resp.json();
}

/** One output port's saved versions, oldest first: read with its contract when its row opens. */
export async function fetchContractVersions(
  productId: string,
  member: string,
): Promise<ContractVersion[]> {
  const resp = await fetch(
    `${API_BASE}/data-products/${encodeURIComponent(productId)}/contracts/${encodeURIComponent(member)}/versions.json`,
  );
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Contract versions", resp.status)));
  }
  return resp.json();
}
