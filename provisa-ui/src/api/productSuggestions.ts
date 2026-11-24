// Copyright (c) 2026 Kenneth Stott
// Canary: 4e2a8c71-9b13-4d6f-a057-1c3e9f8b2d46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-10-28, SUGGEST A PRODUCT FROM A LISTED TABLE): suggesting a data product
// from listed tables of one domain -- the client of provisa/api/admin/product_suggestions_router.py.
// Shapes are the server's (snake_case), never re-derived here.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface SuggestionInput {
  domain_id: string;
  tables: string[];
  reason: string;
}

export interface Suggestion extends SuggestionInput {
  id: number;
  suggested_by: string;
  recipients: string[];
  created_at: string;
}

export async function suggestProduct(input: SuggestionInput): Promise<Suggestion> {
  const resp = await fetch(`${API_BASE}/admin/product-suggestions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("suggestProduct", resp.status)));
  }
  return resp.json() as Promise<Suggestion>;
}
