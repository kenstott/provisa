// Copyright (c) 2026 Kenneth Stott
// Canary: 8a4c1e63-2f7b-4d90-b5a8-3e6d9c0f1b72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/**
 * REQ-1910: the operator's debug-trace controls.
 *
 * Tracing is lean by default; debug detail is switched on for an org, a role or a source for a
 * stated window that ends on its own, and a single request may ask for it with a hint the
 * operator permits per role. Every call here needs the deployment-wide `platform_settings` right.
 */
export type DebugTraceScope = "org" | "role" | "source";

export interface DebugTraceWindow {
  id: string;
  scope: DebugTraceScope;
  org_id: string;
  // The role id or source id the window covers; null for an org-wide window.
  target: string | null;
  started_at: string;
  expires_at: string;
  // By the server's clock at the moment of the read.
  remaining_seconds: number;
  created_by: string | null;
}

export interface DebugTraceHintRole {
  org_id: string;
  role_id: string;
}

export interface DebugTraceState {
  windows: DebugTraceWindow[];
  hint_roles: DebugTraceHintRole[];
  max_minutes: number;
  hint_setting: string;
}

async function send(path: string, method: string, body?: unknown): Promise<DebugTraceState> {
  const res = await fetch(`${API_BASE}/admin/platform/debug-trace${path}`, {
    method,
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(
      serverMessage(await res.json().catch(() => null), requestFailed("Debug trace", res.status)),
    );
  }
  return res.json();
}

export function fetchDebugTrace(): Promise<DebugTraceState> {
  return send("", "GET");
}

export function startDebugWindow(body: {
  scope: DebugTraceScope;
  org_id: string;
  target: string | null;
  minutes: number;
}): Promise<DebugTraceState> {
  return send("/windows", "POST", body);
}

export function stopDebugWindow(id: string): Promise<DebugTraceState> {
  return send(`/windows/${encodeURIComponent(id)}`, "DELETE");
}

export function setDebugHintRole(body: {
  org_id: string;
  role_id: string;
  permitted: boolean;
}): Promise<DebugTraceState> {
  return send("/hint-roles", "PUT", body);
}
