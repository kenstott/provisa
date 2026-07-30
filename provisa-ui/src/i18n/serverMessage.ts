// Copyright (c) 2026 Kenneth Stott
// Canary: e928e902-ff51-4824-bd45-230ca872fa1f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The bare i18next singleton, not ./index: importing ./index would pull the
// full locale catalog graph into every src/api module (main.tsx boots ./index;
// REQ-1343's English-fallback design covers the uninitialized case).
import i18n from "i18next";

// Hybrid server i18n (REQ-1343). API errors and MutationResult envelopes
// carry a stable `code` + `params` alongside the English message. When the
// serverErrors catalog has the code, render the localized template; otherwise
// fall back to the English server message (unmigrated sites, engine
// passthrough errors), then to the caller-supplied fallback.
export interface ServerMessageShape {
  code?: string | null;
  params?: Record<string, unknown> | null;
  message?: string | null;
  detail?: string | null;
}

export function serverMessage(body: ServerMessageShape | null | undefined, fallback: string): string {
  if (body?.code) {
    const key = `serverErrors.${body.code}`;
    if (i18n.isInitialized && i18n.exists(key)) {
      return i18n.t(key, { ...(body.params ?? {}) });
    }
  }
  const raw = body?.message ?? body?.detail;
  return typeof raw === "string" && raw.length > 0 ? raw : fallback;
}

// Standard localized fallback for failed HTTP requests where the response
// body had no usable message: "<op> failed (<status>)".
export function requestFailed(op: string, status: number | string): string {
  if (!i18n.isInitialized) return `${op} failed (${status})`;
  return i18n.t("serverErrors._requestFailed", { op, status });
}
