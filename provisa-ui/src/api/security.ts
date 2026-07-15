// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Security posture config (REQ-693).

const API_BASE_RAW = import.meta.env.VITE_API_BASE || "";

export interface SecurityMode {
  key: string;
  label: string;
  description: string;
}

export interface SecurityState {
  mode: string;
  modes: SecurityMode[];
  restart_required_note: string;
}

export async function fetchSecurity(): Promise<SecurityState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/security`);
  if (!resp.ok) throw new Error(`Security fetch failed: ${resp.status}`);
  return resp.json();
}

export async function setSecurity(
  body: { mode: string },
): Promise<{ success: boolean; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/security`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`Security update failed: ${resp.status}`);
  return resp.json();
}
