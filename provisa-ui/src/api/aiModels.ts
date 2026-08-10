// Copyright (c) 2026 Kenneth Stott
// Canary: 30a28c4f-0e34-48da-aaaf-affdaffbe903
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// --- AI models / vector models / NL rate limit (REQ-464, REQ-419, REQ-500, REQ-370) ---

/** The five model-role assignments. Each value is a model-name string (full dict form is
 *  returned verbatim by the API but edited as a string in the UI). */
export interface AiModelAssignments {
  table_description: string | Record<string, unknown>;
  column_description: string | Record<string, unknown>;
  relationship_inference: string | Record<string, unknown>;
  sql_generation: string | Record<string, unknown>;
  table_selection: string | Record<string, unknown>;
}

export interface VectorModel {
  id: string;
  provider: string;
  dimensions: number;
  api_key_env: string | null;
  base_url: string | null;
  enabled: boolean;
}

// REQ-1398: aisuite vendors that take a plain api_key — mirrors
// provisa.core.org_secrets.LLM_VENDORS. Any operation's ai_models.<op>.vendor may name one of
// these (or a keyless local vendor like ollama, which needs no entry here).
export const LLM_VENDORS = [
  "anthropic",
  "openai",
  "cohere",
  "groq",
  "mistral",
  "xai",
  "deepseek",
  "together",
  "fireworks",
  "nebius",
  "sambanova",
  "inception",
] as const;
export type LlmVendor = (typeof LLM_VENDORS)[number];

export interface AiModelsState {
  ai_models: AiModelAssignments;
  vector_models: VectorModel[];
  nl: { rate_limit: number | null };
  // REQ-1395, REQ-1398: which vendors the org has set its own key for. Keys themselves are
  // never returned.
  api_keys_set: Record<string, boolean>;
  restart_required_note: string;
}

export interface AiModelsUpdate {
  // REQ-1398: a role may be a bare model-name string (vendor implicitly anthropic) or a full
  // {vendor, model, fallback?} object — the same two forms AiModelsState.ai_models accepts.
  ai_models?: Partial<Record<keyof AiModelAssignments, string | Record<string, unknown>>>;
  vector_models?: VectorModel[];
  nl?: { rate_limit: number | null };
  // REQ-1395, REQ-1398: per-vendor keys to set/replace/clear. A non-empty string sets/replaces;
  // an empty string clears; a vendor omitted from the map is left unchanged.
  api_keys?: Record<string, string>;
}

export async function fetchAiModels(): Promise<AiModelsState> {
  const resp = await fetch(`${API_BASE}/admin/ai-models`);
  if (!resp.ok) throw new Error(requestFailed("AI models fetch", resp.status));
  return resp.json();
}

/** The model names a vendor currently serves, read live from the vendor's list-models API
 *  (REQ-1409). Throws when the vendor publishes no such API, has no key set, or rejects the call —
 *  the picker then stays a plain typed field rather than showing an empty list. */
export async function fetchVendorModels(vendor: string): Promise<string[]> {
  const resp = await fetch(
    `${API_BASE}/admin/ai-models/vendors/${encodeURIComponent(vendor)}/models`,
  );
  if (!resp.ok) throw new Error(requestFailed("vendor model listing", resp.status));
  const body: { vendor: string; models: string[] } = await resp.json();
  return body.models;
}

export async function setAiModels(
  body: AiModelsUpdate,
): Promise<{ success: boolean; updated: string[]; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE}/admin/ai-models`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("AI models update", resp.status));
  return resp.json();
}
