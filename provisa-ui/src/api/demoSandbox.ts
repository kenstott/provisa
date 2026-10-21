// Copyright (c) 2026 Kenneth Stott
// Canary: a3d1ed48-6bfa-4639-9e46-89b1b78c6e3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2006: opening a demo sandbox, and the limits every demo sandbox runs under.

import { serverMessage, requestFailed } from "../i18n/serverMessage";
import { anonymousFetch } from "../lib/authFetch";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface OpenedSandbox {
  org_id: string;
  demo: string;
  token: string;
}

/** A refusal the page shows as it is (unknown demo, a full house, a demo still being prepared). */
export class SandboxRefused extends Error {
  readonly code: string | null;

  constructor(message: string, code: string | null) {
    super(message);
    this.code = code;
  }
}

/** Open a visitor's sandbox of the published demo ``name``: no sign-in, and a login held in this
 * browser is not sent. */
export async function openDemoSandbox(name: string): Promise<OpenedSandbox> {
  const res = await anonymousFetch(`${API_BASE}/public/demos/${encodeURIComponent(name)}`, {
    method: "POST",
  });
  if (res.ok) return res.json();
  const body = await res.json().catch(() => null);
  throw new SandboxRefused(
    serverMessage(body, requestFailed("openDemoSandbox", res.status)),
    body?.code ?? null,
  );
}

export interface DemoStatus {
  demo: string;
  ready: boolean;
  state: string;
  places_free: number;
}

/** Whether the published demo ``name`` can be opened now: a probe that opens nothing. */
export async function fetchDemoStatus(name: string): Promise<DemoStatus> {
  const res = await anonymousFetch(`${API_BASE}/public/demos/${encodeURIComponent(name)}/status`);
  if (res.ok) return res.json();
  const body = await res.json().catch(() => null);
  throw new SandboxRefused(
    serverMessage(body, requestFailed("fetchDemoStatus", res.status)),
    body?.code ?? null,
  );
}

export interface SandboxLimits {
  engine: string;
  memory_limit: number;
  disk_limit: number;
  idle_ttl: number;
  max_concurrent: number;
}

/** The limits read from the settings that enforce them. */
export async function fetchSandboxLimits(): Promise<SandboxLimits> {
  const res = await fetch(`${API_BASE}/sandboxes/limits`);
  if (!res.ok) throw new Error(requestFailed("fetchSandboxLimits", res.status));
  return res.json();
}
