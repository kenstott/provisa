// Copyright (c) 2026 Kenneth Stott
// Canary: 1e576e9d-8844-4b95-8c84-52683763cbad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the Metadata Export page's step, in every state the page
// renders once loaded. Its anchor was on the not-entitled alert alone; an entitled org's tour
// waited on its form forever.

import { describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { walkTourStep } from "../test-utils/tourAnchor";
import { MetadataExportTab } from "../components/admin/MetadataExportTab";
import type { MetadataExportState } from "../api/metadataExport";

vi.mock("../api/metadataExport", () => ({
  fetchMetadataExport: vi.fn(),
  setMetadataExport: vi.fn(),
  checkMetadataExport: vi.fn(),
  publishMetadataExport: vi.fn(),
}));

vi.mock("../api/catalogs", () => ({
  fetchCatalogs: vi.fn().mockResolvedValue({ providers: [], catalogs: [] }),
}));

import { fetchMetadataExport } from "../api/metadataExport";

const config: MetadataExportState["config"] = {
  enabled: true,
  reconcile_cron: "0 * * * *",
  targets: [{ catalog: "marquez", datazone_project_id: "", delta_share: false }],
};

const STATES: Record<string, MetadataExportState> = {
  "entitled, configured": {
    entitled: true,
    required_tier: "premium",
    providers: ["openlineage"],
    config,
    last_publish: null,
  },
  "entitled, nothing configured": {
    entitled: true,
    required_tier: "premium",
    providers: ["openlineage"],
    config: { ...config, enabled: false, targets: [] },
    last_publish: null,
  },
  "not entitled": {
    entitled: false,
    required_tier: "premium",
    providers: [],
    config,
    last_publish: null,
  },
};

describe("the Publish tour step on the Metadata Export page", () => {
  for (const [name, state] of Object.entries(STATES)) {
    it(`finds its anchor: ${name}`, async () => {
      vi.mocked(fetchMetadataExport).mockResolvedValue(state);
      render(<MetadataExportTab />);
      expect(await walkTourStep("stepPublish")).toBeInTheDocument();
    });
  }
});
