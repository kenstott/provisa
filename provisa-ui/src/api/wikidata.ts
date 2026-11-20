// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1b8d24-a93c-4e57-b0d6-3c9e7a2f5b81
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: what a Wikidata source's table form asks the server (provisa/api/admin/
// wikidata_router.py): its starter tables, items by name, a class to build a table of, and the
// SPARQL table of that class. Nothing is saved by these calls.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const BASE = `${API_BASE}/admin/sources/sparql/wikidata`;

/** A column a table is registered with: its name and its registered data type. */
export interface WikidataColumn {
  name: string;
  data_type: string;
}

/** A table the form fills itself from: its SPARQL query and typed columns. */
export interface WikidataTable {
  query: string;
  columns: WikidataColumn[];
}

export interface WikidataStarter extends WikidataTable {
  name: string;
}

/** An item found by name; its label and description may be absent in the language asked. */
export interface WikidataEntity {
  id: string;
  label: string | null;
  description: string | null;
}

export interface WikidataProperty {
  id: string;
  label: string | null;
  data_type: string;
  /** Its values are other items, read as their id and label. */
  item: boolean;
  uses: number;
}

export interface WikidataClass {
  id: string;
  instance_count: number;
  limit: number;
  filter_required: boolean;
  broader: { id: string; label: string | null }[];
  narrower: { id: string; label: string | null }[];
  properties: WikidataProperty[];
}

export interface WikidataFilter {
  property: string;
  value: string;
}

async function answer<T>(res: Response, what: string): Promise<T> {
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(body, requestFailed(what, res.status)));
  }
  return res.json() as Promise<T>;
}

export async function fetchWikidataStarters(): Promise<WikidataStarter[]> {
  return answer(await fetch(`${BASE}/starters`), "Wikidata starter tables");
}

export async function searchWikidata(q: string, language: string): Promise<WikidataEntity[]> {
  const params = new URLSearchParams({ q, language });
  return answer(await fetch(`${BASE}/search?${params}`), "Wikidata search");
}

export async function fetchWikidataClass(id: string, language: string): Promise<WikidataClass> {
  const params = new URLSearchParams({ language });
  return answer(
    await fetch(`${BASE}/classes/${encodeURIComponent(id)}?${params}`),
    "Wikidata class",
  );
}

export async function buildWikidataClassTable(body: {
  class_id: string;
  properties: Pick<WikidataProperty, "id" | "label" | "data_type" | "item">[];
  filter: WikidataFilter | null;
  language: string;
}): Promise<WikidataTable> {
  return answer(
    await fetch(`${BASE}/class-table`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "Wikidata class table",
  );
}
