// Copyright (c) 2026 Kenneth Stott
// Canary: 1b9a4102-8560-44b2-989d-abad05c5affa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: business-glossary curation client. Terms are lifecycle-managed by
// the semantic layer; this surface carries the human curation — rename,
// definitions, ref moves, experts, abstract terms and their typed edges.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// Closed enums — the server rejects anything else.
export const GLOSSARY_REL_TYPES = [
  "KIND_OF",
  "RELATED_TO",
  "PART_OF",
  "SYNONYM_OF",
  "VALID_VALUE_OF",
  "DERIVED_FROM",
  "REPLACES",
  "PREFERRED_TERM_FOR",
  "TRANSLATION_OF",
  "ANTONYM_OF",
] as const;
export type GlossaryRelType = (typeof GLOSSARY_REL_TYPES)[number];

export const GLOSSARY_EXPERT_KINDS = ["expert", "author"] as const;
export type GlossaryExpertKind = (typeof GLOSSARY_EXPERT_KINDS)[number];

export interface GlossaryTermSummary {
  id: number;
  name: string;
  definition: string | null;
  is_abstract: boolean;
  deprecated: boolean;
  ref_count: number;
  export_excluded: boolean;
}

export interface GlossaryRef {
  table_id: number;
  column_name: string;
  source_id: string;
  schema_name: string;
  table_name: string;
  alias: string | null;
  domain_id: string;
}

export interface GlossaryEdge {
  term_id: number;
  rel_type: GlossaryRelType;
  name: string;
}

export interface GlossaryExpert {
  user_id: string;
  kind: GlossaryExpertKind;
}

export interface GlossaryTermDetail extends GlossaryTermSummary {
  refs: GlossaryRef[];
  edges_out: GlossaryEdge[];
  edges_in: GlossaryEdge[];
  experts: GlossaryExpert[];
}

async function mutationError(res: Response, op: string): Promise<Error> {
  const data = await res.json().catch(() => ({ detail: res.statusText }));
  return new Error(serverMessage(data, requestFailed(op, res.status)));
}

export async function listGlossaryTerms(
  q: string,
  includeDeprecated: boolean,
): Promise<GlossaryTermSummary[]> {
  const params = new URLSearchParams({ include_deprecated: String(includeDeprecated) });
  if (q) params.set("q", q);
  const res = await fetch(`${API_BASE}/admin/glossary/terms?${params.toString()}`);
  if (!res.ok) throw new Error(requestFailed("listGlossaryTerms", res.status));
  return res.json();
}

export async function fetchGlossaryTerm(termId: number): Promise<GlossaryTermDetail> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}`);
  if (!res.ok) throw new Error(requestFailed("fetchGlossaryTerm", res.status));
  return res.json();
}

// REQ-1387: column-name hover card lookup. A 404 ({code:"glossary.ref_not_found"})
// is the server's designed "this column has no glossary term" signal — the null
// mapping is that contract, not a fallback. Any other failure is a real error.
export async function fetchGlossaryTermByRef(
  tableId: number,
  columnName: string,
): Promise<GlossaryTermDetail | null> {
  const params = new URLSearchParams({
    table_id: String(tableId),
    column_name: columnName,
  });
  const res = await fetch(`${API_BASE}/admin/glossary/ref?${params.toString()}`);
  if (res.status === 404) return null;
  if (!res.ok) throw await mutationError(res, "fetchGlossaryTermByRef");
  return res.json();
}

export async function createGlossaryTerm(body: {
  name: string;
  definition?: string;
}): Promise<{ id: number }> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await mutationError(res, "createGlossaryTerm");
  return res.json();
}

export async function updateGlossaryTerm(
  termId: number,
  body: { name?: string; definition?: string | null; export_excluded?: boolean },
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await mutationError(res, "updateGlossaryTerm");
}

export async function generateAllGlossaryDefinitions(): Promise<{ generated: number }> {
  const res = await fetch(`${API_BASE}/admin/glossary/definitions/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw await mutationError(res, "generateAllGlossaryDefinitions");
  return res.json();
}

export async function generateGlossaryRelationships(): Promise<{ added: number }> {
  const res = await fetch(`${API_BASE}/admin/glossary/relationships/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw await mutationError(res, "generateGlossaryRelationships");
  return res.json();
}

export async function generateGlossaryDefinition(termId: number): Promise<string> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/definition/generate`, {
    method: "POST",
  });
  if (!res.ok) throw await mutationError(res, "generateGlossaryDefinition");
  const data = (await res.json()) as { definition: string };
  return data.definition;
}

export async function deleteGlossaryTerm(termId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}`, { method: "DELETE" });
  if (!res.ok) throw await mutationError(res, "deleteGlossaryTerm");
}

export async function moveGlossaryRef(body: {
  table_id: number;
  column_name: string;
  to_term_id: number;
}): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/refs/move`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await mutationError(res, "moveGlossaryRef");
}

export async function addGlossaryEdge(
  termId: number,
  toTermId: number,
  relType: GlossaryRelType,
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/edges`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ to_term_id: toTermId, rel_type: relType }),
  });
  if (!res.ok) throw await mutationError(res, "addGlossaryEdge");
}

export async function removeGlossaryEdge(
  termId: number,
  toTermId: number,
  relType: GlossaryRelType,
): Promise<void> {
  const params = new URLSearchParams({ to_term_id: String(toTermId), rel_type: relType });
  const res = await fetch(
    `${API_BASE}/admin/glossary/terms/${termId}/edges?${params.toString()}`,
    { method: "DELETE" },
  );
  if (!res.ok) throw await mutationError(res, "removeGlossaryEdge");
}

export async function addGlossaryExpert(
  termId: number,
  userId: string,
  kind: GlossaryExpertKind,
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/experts`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId, kind }),
  });
  if (!res.ok) throw await mutationError(res, "addGlossaryExpert");
}

export async function removeGlossaryExpert(termId: number, userId: string): Promise<void> {
  const res = await fetch(
    `${API_BASE}/admin/glossary/terms/${termId}/experts/${encodeURIComponent(userId)}`,
    { method: "DELETE" },
  );
  if (!res.ok) throw await mutationError(res, "removeGlossaryExpert");
}
