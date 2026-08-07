// Copyright (c) 2026 Kenneth Stott
// Canary: 43fa7478-105c-4619-ac48-41a6e9a714b0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

export interface SetupStatus {
  needs_setup: boolean;
  demo_mode: boolean;
  // Runtime auth-enforcement flag (REQ-1267): true when the server has a real auth
  // provider configured. The SPA's login gate keys off this, not build-time VITE_AUTH_ENABLED,
  // so one image serves unsecured and firebase/basic deploys alike.
  auth_enabled: boolean;
  // Tenancy mode chosen at setup (REQ-1387-adjacent): drives whether the SPA shows
  // org-lifecycle affordances (e.g. Delete Organization) that make no sense single-tenant.
  multitenancy: boolean;
}

export async function fetchSetupStatus(): Promise<SetupStatus> {
  const res = await fetch('/setup/status');
  if (!res.ok) {
    throw new Error(requestFailed("Setup status check", res.status));
  }
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
    throw new Error(serverMessage(data, requestFailed("Setup", res.status)));
  }
  return res.json();
}
