// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

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

export interface AiModelsState {
  ai_models: AiModelAssignments;
  vector_models: VectorModel[];
  nl: { rate_limit: number | null };
  restart_required_note: string;
}

export interface AiModelsUpdate {
  ai_models?: Partial<Record<keyof AiModelAssignments, string>>;
  vector_models?: VectorModel[];
  nl?: { rate_limit: number | null };
}

export async function fetchAiModels(): Promise<AiModelsState> {
  const resp = await fetch(`${API_BASE}/admin/ai-models`);
  if (!resp.ok) throw new Error(`AI models fetch failed: ${resp.status}`);
  return resp.json();
}

export async function setAiModels(
  body: AiModelsUpdate,
): Promise<{ success: boolean; updated: string[]; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE}/admin/ai-models`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`AI models update failed: ${resp.status}`);
  return resp.json();
}
