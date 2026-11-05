// Copyright (c) 2026 Kenneth Stott
// Canary: 8c4e1f27-5a93-4d06-b7e2-0f6d3a9c51b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2179, REQ-2202: find terms in a document or a registered table. A scan of a reference library document (picked there, or
// given as a file or URL and added to the library as proposed), and the review queue of the
// candidate terms it proposes, each a draft the steward edits, accepts, merges or rejects.

import { requestFailed, serverMessage } from "../i18n/serverMessage";
import { base64Of } from "./referenceLibrary";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const BASE = `${API_BASE}/admin/glossary/term-finding`;

export type ScanStatus = "running" | "done" | "failed";
export type DraftStatus = "draft" | "accepted" | "merged" | "rejected";
export type DefinitionKind = "explicit" | "synthesized" | "missing";
export type PassageRole = "definition" | "synthesis" | "supporting";
export type VariantKind = "abbreviation" | "alternative name" | "form" | "reference";

/** A library document the steward may scan. */
export interface ScannableDocument {
  id: number;
  title: string;
  source_kind: "file" | "url";
  source: string;
  status: "proposed" | "kept";
  domains: string[];
  passages: number;
  added_at: string;
  refreshed_at: string | null;
  replaced_at: string | null;
}

export interface ScanSummary {
  passages: number;
  mentions: number;
  candidates: number;
  calls: number;
  left_out: {
    not_a_concept: number;
    quote_not_in_passage: number;
    unknown_passage: number;
    malformed: number;
    unresolved: number;
  };
}

/** A registered table a scan reads (REQ-2202): a table of terms and their definitions, each row a
 * term (no model), or a table of text, each row one passage cited by its id. */
export type TableScanSource =
  | { domain: string; table: string; term_column: string; definition_column: string }
  | { domain: string; table: string; id_column: string; text_column: string };

export interface Scan {
  id: number;
  document_id: number | null;
  document_title: string;
  document_source: string;
  /** The document was removed from the library; its citations stay. */
  document_removed: boolean;
  /** A table's scan: the table it read; null for a document's. */
  source_table: TableScanSource | null;
  domains: string[];
  status: ScanStatus;
  error: string | null;
  summary: ScanSummary | null;
  started_by: string | null;
  started_at: string;
  finished_at: string | null;
  /** The model the scan asks, as vendor:model. */
  model: string | null;
  /** The step a scan is on, or stopped on; null until its first. */
  step: "reading" | "merging" | null;
  /** While reading: the batch of the batches of passages. */
  batch: number | null;
  batches: number | null;
  /** What the role's AI budget held a call's output to, in tokens, when it held one short. */
  budget_limit: number | null;
  counts: Partial<Record<DraftStatus, number>>;
}

/** A source passage a draft cites. */
export interface Citation {
  mention: number;
  ordinal: number;
  section: string | null;
  page: number | null;
  /** The id of the table row the passage is, in a table's scan. */
  row?: string | null;
  quote: string;
  explicit: boolean;
  sense: string;
  role: PassageRole;
}

export interface Variant {
  text: string;
  kind: VariantKind;
  /** Proposed as a synonym. */
  keep: boolean;
}

/** A glossary term a draft names: by id and name, or outside the caller's domains. */
export type TermRef = { term_id: number; name: string } | { outside_domains: true };

export interface Draft {
  id: number;
  scan_id: number;
  name: string;
  category: string;
  sense: string;
  definition: string | null;
  definition_kind: DefinitionKind;
  proposal: "new" | "merge";
  match: TermRef | null;
  match_kind: "duplicate" | "synonym" | null;
  status: DraftStatus;
  term: TermRef | null;
  decided_by: string | null;
  decided_at: string | null;
  passages: Citation[];
  variants: Variant[];
  conflicts: [Citation, Citation][];
  homonyms: string[];
  matches: (TermRef & { via: string })[];
}

/** What a scan reads: a library document, a new one by file or URL, or a registered table. */
export type ScanSource =
  | { document_id: number }
  | { url: string }
  | { file: File }
  | { table: TableScanSource };

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

function send(method: string, body?: unknown): RequestInit {
  return body === undefined
    ? { method }
    : { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) };
}

export async function fetchScannableDocuments(): Promise<ScannableDocument[]> {
  const body = await ok<{ documents: ScannableDocument[] }>(
    await fetch(`${BASE}/documents`),
    "Reference library",
  );
  return body.documents;
}

export async function startScan(
  source: ScanSource,
  domains: string[],
): Promise<{ scan: Scan; document?: ScannableDocument }> {
  const given =
    "file" in source
      ? { file_name: source.file.name, content_b64: await base64Of(source.file) }
      : source;
  return ok(await fetch(`${BASE}/scans`, send("POST", { ...given, domains })), "Find terms");
}

export async function fetchScans(documentId?: number): Promise<Scan[]> {
  const query = documentId === undefined ? "" : `?document_id=${documentId}`;
  const body = await ok<{ scans: Scan[] }>(await fetch(`${BASE}/scans${query}`), "Scans");
  return body.scans;
}

export async function fetchScan(id: number): Promise<{ scan: Scan; drafts: Draft[] }> {
  return ok(await fetch(`${BASE}/scans/${id}`), "Scan");
}

export async function cancelScan(id: number): Promise<{ scan: Scan }> {
  return ok(await fetch(`${BASE}/scans/${id}/cancel`, send("POST")), "Cancel scan");
}

export async function editDraft(
  id: number,
  edits: { name?: string; definition?: string | null; synonyms?: string[] },
): Promise<Draft> {
  return ok(await fetch(`${BASE}/drafts/${id}`, send("PATCH", edits)), "Edit draft");
}

export async function acceptDraft(id: number): Promise<{ term_id: number }> {
  return ok(await fetch(`${BASE}/drafts/${id}/accept`, send("POST")), "Accept draft");
}

export async function mergeDraft(
  id: number,
  into: { term_id?: number; use_definition?: boolean },
): Promise<{ term_id: number }> {
  return ok(await fetch(`${BASE}/drafts/${id}/merge`, send("POST", into)), "Merge draft");
}

export async function rejectDraft(id: number): Promise<void> {
  await ok(await fetch(`${BASE}/drafts/${id}/reject`, send("POST")), "Reject draft");
}

export async function bulkAccept(
  scanId: number,
  draftIds: number[],
): Promise<{ decided: { id: number }[]; refused: { id: number; message: string }[] }> {
  return ok(
    await fetch(`${BASE}/scans/${scanId}/accept`, send("POST", { draft_ids: draftIds })),
    "Accept drafts",
  );
}
