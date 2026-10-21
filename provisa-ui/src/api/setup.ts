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
import type { DemoLanding, DemoTourStepDef } from "../tour/demoTour";

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
  // REQ-2009: the example GraphQL queries the named demo ships (demo/named/<name>/queries), in
  // order; none when no named demo was started. The GraphQL explorer opens them as its first tabs.
  example_queries: ExampleQuery[];
  // REQ-2010: the started named demo's name, and its own tour (demo/named/<name>/tour.yaml); null
  // and none when no named demo was started.
  named_demo: string | null;
  demo_tour: DemoTourStepDef[];
  // Where the tour's last "Look around on my own" leaves the visitor (tour.yaml's landing).
  demo_landing: DemoLanding | null;
  // REQ-2084: the code cells of the notebook the named demo ships (notebook.ipynb), in order;
  // none when it ships none. The Notebook tab offers them, and a tour's notebook step runs them.
  demo_notebook: string[];
  // REQ-2101 (amended): the org files the named demo ships, each in the org's folder as
  // {files}/<path>; none when it ships none.
  demo_org_files: string[];
}

export interface ExampleQuery {
  name: string;
  query: string;
}

export async function fetchSetupStatus(): Promise<SetupStatus> {
  const res = await fetch("/setup/status");
  if (!res.ok) {
    throw new Error(requestFailed("Setup status check", res.status));
  }
  return res.json();
}

export async function runSetup(body: {
  provider: "basic" | "firebase" | "none";
  mode: "single" | "multi";
  admin_username?: string;
  admin_password?: string;
  firebase_project_id?: string;
  use_domains?: boolean | null;
  default_domain?: string;
}): Promise<{ success: boolean; provider: string }> {
  const res = await fetch("/setup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("Setup", res.status)));
  }
  return res.json();
}
