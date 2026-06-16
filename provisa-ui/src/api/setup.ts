// Copyright (c) 2026 Kenneth Stott
// Canary: 43fa7478-105c-4619-ac48-41a6e9a714b0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface SetupStatus {
  needs_setup: boolean;
  demo_mode: boolean;
}

export async function fetchSetupStatus(): Promise<SetupStatus> {
  const res = await fetch('/setup/status');
  if (!res.ok) return { needs_setup: false, demo_mode: false };
  return res.json();
}

export async function runSetup(body: {
  provider: 'basic' | 'firebase' | 'none';
  mode: 'single' | 'multi';
  admin_username?: string;
  admin_password?: string;
  firebase_project_id?: string;
  use_domains?: boolean | null;
  default_domain?: string;
}): Promise<{ success: boolean; provider: string }> {
  const res = await fetch('/setup', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || `Setup failed: ${res.status}`);
  }
  return res.json();
}
