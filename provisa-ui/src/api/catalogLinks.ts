// Copyright (c) 2026 Kenneth Stott
// Canary: 0e5b8c27-9d41-4f36-a7e2-3b6d9f1a5c84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// --- A table's catalogs, as its editor uses them (REQ-2122) ---

/** One catalog of the table's domain: the asset the table is linked to there, when its
 *  descriptions were last imported from it, and whether the table is locked to it. */
export interface TableCatalog {
  id: string;
  provider: string;
  link: { ref: string; name: string; url: string | null } | null;
  imported_at: string | null;
  locked: boolean;
}

export interface CatalogAsset {
  ref: string;
  name: string;
  physical: string[];
  description: string;
  url: string | null;
}

/** One catalog's pick list for the table, or the reason it could not be read. */
export interface CatalogCandidates {
  catalog: string;
  provider: string;
  linked?: string | null;
  preselected?: string | null;
  assets?: CatalogAsset[];
  error?: string;
}

export interface OfferedName {
  name: string;
  /** The registry tag or live glossary term of the same name, or null when there is none. */
  tag?: string | null;
  term?: string | null;
}

/** A catalog asset's descriptive metadata beside the table's own. */
export interface CatalogOffer {
  catalog: string;
  provider: string;
  asset: CatalogAsset;
  description: { current: string; catalog: string };
  columns: { column: string; current: string; catalog_column: string | null }[];
  catalog_columns: {
    name: string;
    description: string;
    tags: OfferedName[];
    terms: OfferedName[];
  }[];
  owners: string[];
  tags: OfferedName[];
  terms: OfferedName[];
  current_tags: [string, string][];
  current_terms: Record<string, string>;
  imported_at: string | null;
  locked: boolean;
}

/** What the steward accepted of an offer. */
export interface AcceptedOffer {
  ref: string;
  description: boolean;
  /** Each Provisa column that takes a catalog column's description, by the Provisa column. */
  columns: Record<string, string>;
  tags: { column: string | null; tag: string }[];
  /** Each Provisa column and the live glossary term to link it to. */
  terms: Record<string, string>;
}

/** What was imported: the values written to the table. */
export interface Imported {
  description?: string;
  columns?: Record<string, string>;
  tags?: [string | null, string][];
  terms?: Record<string, string>;
}

/** The server's refusal, worded by its code where this build has the wording. */
async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

const base = (tableId: number) => `${API_BASE}/admin/tables/${tableId}/catalog`;

export async function fetchTableCatalogs(tableId: number): Promise<TableCatalog[]> {
  const body = await ok<{ catalogs: TableCatalog[] }>(
    await fetch(base(tableId)),
    "Table catalogs fetch",
  );
  return body.catalogs;
}

export async function fetchCatalogCandidates(
  tableId: number,
  search?: string,
): Promise<CatalogCandidates[]> {
  const query = search ? `?search=${encodeURIComponent(search)}` : "";
  const body = await ok<{ catalogs: CatalogCandidates[] }>(
    await fetch(`${base(tableId)}/candidates${query}`),
    "Catalog search",
  );
  return body.catalogs;
}

/** The asset's metadata beside the table's own; without `ref`, the linked asset's. */
export async function fetchCatalogOffer(
  tableId: number,
  catalog: string,
  ref?: string,
): Promise<CatalogOffer> {
  const query = ref ? `?ref=${encodeURIComponent(ref)}` : "";
  return ok(
    await fetch(`${base(tableId)}/${encodeURIComponent(catalog)}/offer${query}`),
    "Catalog read",
  );
}

export async function applyCatalogOffer(
  tableId: number,
  catalog: string,
  accepted: AcceptedOffer,
): Promise<Imported> {
  const body = await ok<{ imported: Imported }>(
    await fetch(`${base(tableId)}/${encodeURIComponent(catalog)}/apply`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(accepted),
    }),
    "Catalog import",
  );
  return body.imported;
}

/** Lock the table to `catalog`, or release its lock (null). */
export async function lockTableToCatalog(tableId: number, catalog: string | null): Promise<void> {
  await ok(
    await fetch(`${base(tableId)}/lock`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ catalog }),
    }),
    "Catalog lock",
  );
}
