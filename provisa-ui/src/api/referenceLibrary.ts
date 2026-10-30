// Copyright (c) 2026 Kenneth Stott
// Canary: 3b6f0e52-8c1d-4a97-b2e4-5d9a71c0f826
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: the org's reference library, as the Admin Reference library page reads and changes it.
// A file travels as base64 in the JSON body, as every admin upload does.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const BASE = `${API_BASE}/admin/reference-library/documents`;

export type DocumentStatus = "proposed" | "kept";
export type SourceKind = "file" | "url";

/** A library document as the server reports it. `domains` is `["*"]` for the whole org. */
export interface LibraryDocument {
  id: number;
  title: string;
  source_kind: SourceKind;
  source: string;
  media_type: string;
  status: DocumentStatus;
  domains: string[];
  embedding_model: string;
  /** True when the document was embedded by a model other than the org's current one. */
  needs_reembedding: boolean;
  passages: number;
  added_by: string | null;
  added_at: string;
  refreshed_at: string | null;
  replaced_at: string | null;
}

export interface LibraryPassage {
  ordinal: number;
  section: string | null;
  page: number | null;
  text: string;
}

export interface LibraryDocumentDetail extends LibraryDocument {
  passage_list: LibraryPassage[];
}

/** A document to add: by its URL, or by a file. */
export type NewDocument =
  | { url: string; domains: string[]; title?: string }
  | { file: File; domains: string[]; title?: string };

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

/** The file's bytes as base64. */
export async function base64Of(file: File): Promise<string> {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

async function upload(file: File): Promise<{ file_name: string; content_b64: string }> {
  return { file_name: file.name, content_b64: await base64Of(file) };
}

export async function fetchLibrary(): Promise<LibraryDocument[]> {
  const body = await ok<{ documents: LibraryDocument[] }>(await fetch(BASE), "Reference library");
  return body.documents;
}

export async function fetchLibraryDocument(id: number): Promise<LibraryDocumentDetail> {
  return ok(await fetch(`${BASE}/${id}`), "Reference library document");
}

export async function addLibraryDocument(doc: NewDocument): Promise<LibraryDocument> {
  const title = doc.title?.trim() ? { title: doc.title.trim() } : {};
  const source = "url" in doc ? { url: doc.url } : await upload(doc.file);
  return ok(
    await fetch(BASE, send("POST", { ...source, ...title, domains: doc.domains })),
    "Add document",
  );
}

export async function setLibraryDomains(id: number, domains: string[]): Promise<LibraryDocument> {
  return ok(await fetch(`${BASE}/${id}`, send("PATCH", { domains })), "Change domains");
}

export async function keepLibraryDocument(id: number): Promise<LibraryDocument> {
  return ok(await fetch(`${BASE}/${id}`, send("PATCH", { status: "kept" })), "Keep document");
}

export async function refreshLibraryDocument(id: number): Promise<LibraryDocument> {
  return ok(await fetch(`${BASE}/${id}/refresh`, send("POST")), "Refresh document");
}

export async function uploadLibraryVersion(id: number, file: File): Promise<LibraryDocument> {
  return ok(
    await fetch(`${BASE}/${id}/version`, send("POST", await upload(file))),
    "Upload new version",
  );
}

/** A document the re-embedding of all could not do, and why; it is unchanged. */
export interface ReembedFailure {
  document_id: number;
  title: string;
  reason: string;
}

export interface ReembedOutcome {
  embedding_model: string;
  reembedded: number[];
  failed: ReembedFailure[];
}

export async function reembedLibraryDocument(id: number): Promise<LibraryDocument> {
  return ok(await fetch(`${BASE}/${id}/reembed`, send("POST")), "Re-embed document");
}

export async function reembedLibrary(): Promise<ReembedOutcome> {
  return ok(await fetch(`${API_BASE}/admin/reference-library/reembed`, send("POST")), "Re-embed");
}

export async function removeLibraryDocument(id: number): Promise<void> {
  await ok(await fetch(`${BASE}/${id}`, send("DELETE")), "Remove document");
}
