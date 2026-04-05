// Copyright (c) 2025 Kenneth Stott
// Canary: c543c04b-a6b6-4082-beef-b38df177f30a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MutationResult } from "../types/admin";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

export interface ActionArg {
  name: string;
  type: string;
}

export interface InlineField {
  name: string;
  type: string;
}

export interface TrackedFunction {
  name: string;
  sourceId: string;
  schemaName: string;
  functionName: string;
  returns: string;
  arguments: ActionArg[];
  visibleTo: string[];
  writableBy: string[];
  domainId: string;
  description: string | null;
}

export interface TrackedWebhook {
  name: string;
  url: string;
  method: string;
  timeoutMs: number;
  returns: string | null;
  inlineReturnType: InlineField[];
  arguments: ActionArg[];
  visibleTo: string[];
  domainId: string;
  description: string | null;
}

export async function fetchActions(): Promise<{ functions: TrackedFunction[]; webhooks: TrackedWebhook[] }> {
  const resp = await fetch(`${API_BASE}/admin/actions`);
  if (!resp.ok) throw new Error(`Fetch actions failed: ${resp.status}`);
  return resp.json();
}

export async function saveFunction(input: {
  name: string;
  sourceId: string;
  schemaName: string;
  functionName: string;
  returns: string;
  arguments: ActionArg[];
  visibleTo: string[];
  writableBy: string[];
  domainId: string;
  description?: string;
}): Promise<MutationResult> {
  const resp = await fetch(`${API_BASE}/admin/actions/functions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!resp.ok) throw new Error(`Save function failed: ${resp.status}`);
  return resp.json();
}

export async function saveWebhook(input: {
  name: string;
  url: string;
  method: string;
  timeoutMs: number;
  returns?: string;
  inlineReturnType: InlineField[];
  arguments: ActionArg[];
  visibleTo: string[];
  domainId: string;
  description?: string;
}): Promise<MutationResult> {
  const resp = await fetch(`${API_BASE}/admin/actions/webhooks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  if (!resp.ok) throw new Error(`Save webhook failed: ${resp.status}`);
  return resp.json();
}

export async function deleteFunction(name: string): Promise<MutationResult> {
  const resp = await fetch(`${API_BASE}/admin/actions/functions/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
  if (!resp.ok) throw new Error(`Delete function failed: ${resp.status}`);
  return resp.json();
}

export async function deleteWebhook(name: string): Promise<MutationResult> {
  const resp = await fetch(`${API_BASE}/admin/actions/webhooks/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
  if (!resp.ok) throw new Error(`Delete webhook failed: ${resp.status}`);
  return resp.json();
}

export async function testAction(
  actionType: "function" | "webhook",
  name: string,
): Promise<unknown> {
  const resp = await fetch(`${API_BASE}/admin/actions/test`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ actionType, name }),
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}
