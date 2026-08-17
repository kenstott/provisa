// Copyright (c) 2026 Kenneth Stott
// Canary: 1c8b47d0-3e5a-4f92-b6c1-7a0d95e2f318
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// --- Interactive Hasura v2 / DDN import (REQ-1483) ---
//
// Two calls, because the import is an approval flow: preview converts and returns what the
// conversion produced, apply loads the YAML the administrator approved. The server keeps nothing
// between them — what apply receives is exactly what was reviewed, including any hand edits.

export type ImportFlavor = "auto" | "hasura_v2" | "ddn";

export interface ImportSummary {
  sources: number;
  domains: number;
  tables: number;
  columns: number;
  roles: number;
  relationships: number;
  rls_rules: number;
  source_ids: string[];
  domain_ids: string[];
  role_ids: string[];
}

export interface ImportWarning {
  category: string;
  message: string;
  source_path: string;
}

export interface ImportPreview {
  flavor: string;
  config_yaml: string;
  warnings: ImportWarning[];
  summary: ImportSummary;
}

export interface ImportPreviewRequest {
  filename: string;
  content_b64: string;
  flavor: ImportFlavor;
  domain_map: Record<string, string>;
  source_overrides: Record<string, unknown>;
}

/** Base64 a picked file without blowing the stack on a large archive. */
export async function fileToBase64(file: File): Promise<string> {
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = "";
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

export async function previewImport(body: ImportPreviewRequest): Promise<ImportPreview> {
  const resp = await fetch(`${API_BASE}/admin/import/hasura/preview`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Import preview", resp.status));
  return resp.json();
}

export async function applyImport(
  configYaml: string,
  replace: boolean,
): Promise<{ summary: ImportSummary; replace: boolean }> {
  const resp = await fetch(`${API_BASE}/admin/import/hasura/apply`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config_yaml: configYaml, replace }),
  });
  if (!resp.ok) throw new Error(requestFailed("Import apply", resp.status));
  return resp.json();
}
