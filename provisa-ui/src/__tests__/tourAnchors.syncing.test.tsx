// Copyright (c) 2026 Kenneth Stott
// Canary: 1e576e9d-8844-4b95-8c84-52683763cbad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the Publish step on the Syncing page (REQ-2247), in every
// state the page renders once loaded. Its anchor was once on the not-entitled alert alone; an
// entitled org's tour waited on its form forever.

import { describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { walkTourStep } from "../test-utils/tourAnchor";
import { SyncingTab } from "../components/admin/syncing/SyncingTab";
import type { SyncEntry, SyncingState } from "../api/syncing";

vi.mock("../api/syncing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/syncing")>()),
  fetchSyncing: vi.fn(),
  setSyncing: vi.fn(),
  checkEntry: vi.fn(),
  publishEntry: vi.fn(),
}));

import { fetchSyncing } from "../api/syncing";

const MARQUEZ: SyncEntry = {
  id: "marquez",
  type: "data_catalog",
  direction: "export",
  provider: "openlineage",
  endpoint: "http://marquez:5000",
  auth_mode: "api_key",
  username: "",
  entra_tenant_id: "",
  entra_client_id: "",
  gcp_project: "",
  gcp_location: "",
  aws_region: "",
  datazone_domain_id: "",
  timeout_seconds: 30,
  domains: ["sales"],
  role: "steward",
  datazone_project_id: "",
  delta_share: false,
  import_cron: "",
  api_key_set: false,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
  last_run: null,
  last_import: null,
  products: [],
};

const BASE: SyncingState = {
  entitled: true,
  required_tier: "premium",
  providers: ["openlineage"],
  beta_providers: [],
  publishable_roles: ["steward"],
  reconcile_cron: "0 * * * *",
  entries: [MARQUEZ],
};

const STATES: Record<string, SyncingState> = {
  "entitled, configured": BASE,
  "entitled, nothing configured": { ...BASE, entries: [] },
  "not entitled": { ...BASE, entitled: false },
};

describe("the Publish tour step on the Syncing page", () => {
  for (const [name, state] of Object.entries(STATES)) {
    it(`finds its anchor: ${name}`, async () => {
      vi.mocked(fetchSyncing).mockResolvedValue(state);
      render(<SyncingTab />);
      expect(await walkTourStep("stepPublish")).toBeInTheDocument();
    });
  }
});
