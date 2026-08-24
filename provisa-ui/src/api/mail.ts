// Copyright (c) 2026 Kenneth Stott
// Canary: 8f5a1d02-3c47-4b6e-9a10-2ed7c4b8f931
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1576: the deployment's mail transport. Two questions, two shapes: what is it wired to
// (`MailState`), and is mail actually going out (`MailStats`, read from real attempts rather than
// from the configuration).
//
// REQ-1575: no call here can be handed a stored credential. `config` carries every field EXCEPT
// the secret ones, and `secret_set` says only whether a value is on file.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

export interface MailConfigField {
  config_key: string;
  label: string;
  type: string;
  required: boolean;
  secret?: boolean;
  placeholder?: string;
}

/** A transport the build knows about, whether or not this deployment can use it. */
export interface MailProviderChoice {
  key: string;
  label: string;
  description: string;
  /** Whether the client library is installed. An unavailable transport is shown, not hidden. */
  available: boolean;
  /** The distribution to install to make it available — named in the greyed-out row. */
  requires: string | null;
  config_fields: MailConfigField[];
}

export interface MailState {
  provider: string;
  from_address: string;
  base_url: string;
  timeout_seconds: number;
  providers: MailProviderChoice[];
  /** Per-provider config, minus every field the registry marks secret (REQ-1575). */
  config: Record<string, Record<string, string>>;
  /** REQ-1575: per provider, per secret field, whether a value is stored. Never the value. */
  secret_set: Record<string, Record<string, boolean>>;
}

export interface MailEvent {
  sent_at: string | null;
  provider: string;
  kind: string;
  recipient: string;
  org_id: string | null;
  succeeded: boolean;
  error: string | null;
  requested_by: string | null;
}

export interface MailCounts {
  attempted: number;
  delivered: number;
  failed: number;
}

export interface MailStats {
  total: MailCounts;
  windows: { day: MailCounts; week: MailCounts };
  last_success: MailEvent | null;
  last_failure: MailEvent | null;
  recent: MailEvent[];
}

export async function fetchMail(): Promise<MailState> {
  const res = await fetch(`${API_BASE}/admin/mail`);
  return ok<MailState>(res, "load mail settings");
}

export async function setMail(body: {
  provider: string;
  from_address: string;
  base_url: string;
  timeout_seconds: number;
  config: Record<string, string>;
}): Promise<{ success: boolean; provider: string }> {
  const res = await fetch(`${API_BASE}/admin/mail`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return ok<{ success: boolean; provider: string }>(res, "save mail settings");
}

export async function fetchMailStats(): Promise<MailStats> {
  const res = await fetch(`${API_BASE}/admin/mail/stats`);
  return ok<MailStats>(res, "load mail statistics");
}

/**
 * Send a test message through the configured transport.
 *
 * A refusal by the mail server is a 200 carrying the transport's own words, not an HTTP error:
 * the request asked a question and got an answer, and the answer is what the page renders.
 */
export async function sendTestMail(to: string): Promise<{ success: boolean; error?: string }> {
  const res = await fetch(`${API_BASE}/admin/mail/test`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ to }),
  });
  return ok<{ success: boolean; error?: string }>(res, "send test message");
}
