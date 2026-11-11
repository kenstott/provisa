// Copyright (c) 2026 Kenneth Stott
// Canary: 4c1e8f20-7a3d-4b96-9e52-d0b8a61f3c27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: the ontology import client. A steward uploads an ontology (an Apache Ossie model or an
// RDF/OWL file); it is read into items, each with the alignments proposed between it and the
// glossary's terms. The steward edits or excludes items, approves or rejects proposals, adds
// alignments of their own, and applies the import. Endpoints: provisa/api/admin/
// ontology_import_router.py.

import { requestFailed, serverMessage } from "../i18n/serverMessage";
import { base64Of } from "./referenceLibrary";
import type { TermGrounding, TermRole, TermStatus } from "./glossary";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const BASE = `${API_BASE}/admin/glossary/ontology-imports`;

/** The formats the server reads; a file whose name says none is given one. */
export const ONTOLOGY_FORMATS = ["ossie", "turtle", "xml", "nt", "json-ld"] as const;
export type OntologyFormat = (typeof ONTOLOGY_FORMATS)[number];

/** The files the import picker offers: the ontology formats, and a whole release as a zip. */
export const ONTOLOGY_ACCEPT = ".yaml,.yml,.json,.ttl,.owl,.rdf,.xml,.nt,.jsonld,.zip";

export const ITEM_KINDS = ["class", "property", "value", "concept"] as const;
export type ItemKind = (typeof ITEM_KINDS)[number];

export type ItemStatus = "pending" | "excluded" | "applied";
export type AlignmentStatus = "proposed" | "approved" | "rejected" | "written";

export const ALIGNMENT_TYPES = [
  "SAME_AS",
  "SYNONYM_OF",
  "KIND_OF",
  "PART_OF",
  "VALID_VALUE_OF",
  "RELATED_TO",
] as const;
export type AlignmentType = (typeof ALIGNMENT_TYPES)[number];

/** Why an alignment was proposed (provisa/ontology_import/align.py). */
export type AlignmentBasis =
  | "iri"
  | "name"
  | "qualified name"
  | "normalized name"
  | "synonym"
  | "head noun"
  | "qualifier"
  | "ontology"
  | "steward";

export interface CountsByStatus {
  items: Partial<Record<ItemStatus, number>>;
  alignments: Partial<Record<AlignmentStatus, number>>;
}

export interface OntologyImport {
  id: number;
  title: string;
  file_name: string;
  format: string;
  domains: string[];
  /** What the file held: its items, its statements, and what was left out of it, by why. */
  summary: { items: number; statements: number; left_out: Record<string, number> };
  imported_by: string | null;
  imported_at: string | null;
  applied_by: string | null;
  applied_at: string | null;
  counts: CountsByStatus;
}

/** A glossary term as the review names it; one outside the caller's domains is never named. */
export type ReviewTerm =
  | {
      term_id: number;
      name: string;
      definition: string | null;
      role: TermRole;
      status: TermStatus;
      grounding: TermGrounding;
      iri: string | null;
    }
  | { outside_domains: true };

export interface Alignment {
  id: number;
  item_id: number;
  rel_type: AlignmentType;
  /** The edge reads from the item to its target; false reads it the other way. */
  item_is_source: boolean;
  basis: AlignmentBasis;
  status: AlignmentStatus;
  use_definition: boolean;
  decided_by: string | null;
  decided_at: string | null;
  term: ReviewTerm | null;
  other_item: { item_id: number; name: string | null } | null;
}

export interface ImportItem {
  id: number;
  name: string;
  kind: ItemKind;
  definition: string | null;
  iri: string | null;
  keys: string[];
  labels: string[];
  status: ItemStatus;
  term: ReviewTerm | null;
  alignments: Alignment[];
}

export interface ImportReview {
  import: OntologyImport;
  items: ImportItem[];
}

export interface ApplyResult {
  id: number;
  created: { item_id: number; term_id: number; name: string }[];
  aligned: { item_id: number; term_id: number; name: string }[];
  edges: { alignment_id: number; from: number; rel_type: string; to: number }[];
  waiting: { alignment_id: number; reason: string }[];
}

/** An apply the server refused as a whole: every reason, each naming what to do. */
export class ApplyRefused extends Error {
  readonly problems: string[];
  constructor(message: string, problems: string[]) {
    super(message);
    this.problems = problems;
  }
}

async function refusal(res: Response, op: string): Promise<Error> {
  const body = await res.json().catch(() => ({ detail: res.statusText }));
  return new Error(serverMessage(body, requestFailed(op, res.status)));
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) throw await refusal(res, op);
  return res.json() as Promise<T>;
}

function post(url: string, body: unknown, method = "POST"): Promise<Response> {
  return fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function listOntologyImports(): Promise<OntologyImport[]> {
  return (await ok<{ imports: OntologyImport[] }>(await fetch(BASE), "listOntologyImports"))
    .imports;
}

export async function fetchOntologyImport(id: number): Promise<ImportReview> {
  return ok(await fetch(`${BASE}/${id}`), "fetchOntologyImport");
}

/** Read the file in the steward's domains; ``format`` only where its name says none. */
export async function importOntology(
  file: File,
  format: OntologyFormat | null,
  domains: string[] | null,
): Promise<ImportReview> {
  return ok(
    await post(BASE, {
      file_name: file.name,
      content_b64: await base64Of(file),
      ...(format ? { format } : {}),
      ...(domains ? { domains } : {}),
    }),
    "importOntology",
  );
}

export async function discardOntologyImport(id: number): Promise<void> {
  await ok(await fetch(`${BASE}/${id}`, { method: "DELETE" }), "discardOntologyImport");
}

export interface ItemEdit {
  name?: string;
  definition?: string | null;
  excluded?: boolean;
}

export async function editImportItem(itemId: number, edit: ItemEdit): Promise<ImportReview> {
  return ok(await post(`${BASE}/items/${itemId}`, edit, "PATCH"), "editImportItem");
}

export interface NewAlignment {
  item_id: number;
  rel_type: AlignmentType;
  term_id?: number;
  other_item_id?: number;
  item_is_source: boolean;
  use_definition?: boolean;
}

export async function addAlignment(importId: number, body: NewAlignment): Promise<Alignment> {
  return ok(await post(`${BASE}/${importId}/alignments`, body), "addAlignment");
}

export async function approveAlignment(id: number, useDefinition: boolean): Promise<void> {
  await ok(
    await post(`${BASE}/alignments/${id}/approve`, { use_definition: useDefinition }),
    "approveAlignment",
  );
}

export async function rejectAlignment(id: number): Promise<void> {
  await ok(await post(`${BASE}/alignments/${id}/reject`, {}), "rejectAlignment");
}

export async function bulkApprove(
  importId: number,
  alignmentIds: number[],
): Promise<{ approved: number[]; refused: { id: number; message: string }[] }> {
  return ok(
    await post(`${BASE}/${importId}/approve`, { alignment_ids: alignmentIds }),
    "bulkApprove",
  );
}

/** Apply the import; a 409 is refused with every problem that stops it. */
export async function applyOntologyImport(importId: number): Promise<ApplyResult> {
  const res = await post(`${BASE}/${importId}/apply`, {});
  if (res.status === 409) {
    const body = (await res.json().catch(() => ({}))) as {
      detail?: string;
      params?: { problems?: unknown };
    };
    const problems = Array.isArray(body.params?.problems)
      ? body.params.problems.filter((p): p is string => typeof p === "string")
      : [];
    throw new ApplyRefused(serverMessage(body, requestFailed("applyOntologyImport", 409)), problems);
  }
  return ok(res, "applyOntologyImport");
}

/** What applying would do, read from the review: the server applies exactly this. */
export interface ApplyPreview {
  /** Pending items that become draft terms. */
  create: number;
  /** Pending items approved as the same entity as a term: the term takes the item's IRI. */
  sameAs: number;
  /** Approved alignments written as glossary edges. */
  edges: number;
  /** Approved alignments left waiting on an excluded item. */
  waiting: number;
  excluded: number;
  /** Proposals nobody has decided: applying leaves them as they are. */
  undecided: number;
}

export function applyPreview(items: ImportItem[]): ApplyPreview {
  const byId = new Map(items.map((i) => [i.id, i]));
  const preview: ApplyPreview = {
    create: 0,
    sameAs: 0,
    edges: 0,
    waiting: 0,
    excluded: 0,
    undecided: 0,
  };
  for (const item of items) {
    if (item.status === "excluded") preview.excluded += 1;
    const approved = item.alignments.filter((a) => a.status === "approved");
    if (item.status === "pending") {
      if (approved.some((a) => a.rel_type === "SAME_AS")) preview.sameAs += 1;
      else preview.create += 1;
    }
    for (const a of item.alignments) {
      if (a.status === "proposed") preview.undecided += 1;
      if (a.status !== "approved" || a.rel_type === "SAME_AS") continue;
      const other = a.other_item ? byId.get(a.other_item.item_id) : undefined;
      if (item.status === "excluded" || other?.status === "excluded") preview.waiting += 1;
      else preview.edges += 1;
    }
  }
  return preview;
}
