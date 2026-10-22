// Copyright (c) 2026 Kenneth Stott
// Canary: c9bfe3d1-fd93-4aaa-90d5-e4e4fdd13a7f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the core tour's steps on the Sources page, with no source
// registered and with the pet store's (tourPrerequisites.json). step0 and step1 point at the
// navigation (tourAnchors.nav) and wait on this page's "+ Source"; step2 is "+ Source"; step3
// opens the type picker through it.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { tourStep, walkTourStep } from "../test-utils/tourAnchor";
import { waitFor } from "@testing-library/react";
import type { Source } from "../types/admin";

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchSourceReach: vi.fn().mockResolvedValue(null),
}));

vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => AUTH,
}));

// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const AUTH = {
  role: "admin",
  selectedRoles: ["admin"],
  capabilities: ["admin", "source_registration"],
  domainAccess: ["*"],
};
const REFETCH = vi.fn();
const NONE: never[] = [];
const PET_STORE = [
  {
    id: "pet-store-sqlite",
    type: "sqlite",
    host: "",
    port: null,
    database: "",
    username: "",
    path: "pets.db",
    cacheEnabled: true,
    cacheTtl: null,
    changeSignal: "ttl",
    mappingJson: null,
    passwordRef: null,
    allowedDomains: [],
  } as unknown as Source,
];
let SOURCES = { sources: NONE as Source[], loading: false, refetch: REFETCH };
const DOMAINS = { domains: NONE, loading: false, refetch: REFETCH };

vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useSources: () => SOURCES,
  useDomains: () => DOMAINS,
}));

vi.mock("../components/SchemaDiscovery", () => ({ SchemaDiscovery: () => null }));

import { SourcesPage } from "../pages/SourcesPage";

beforeEach(() => {
  vi.spyOn(globalThis, "fetch").mockImplementation(
    async () => new Response(JSON.stringify({ configured: true })),
  );
});

const SUPPLIES = { ...PET_STORE[0], id: "supplies", type: "parquet" } as unknown as Source;
const STATES: Record<string, Source[]> = {
  "no source registered": NONE,
  "the pet store registered": PET_STORE,
};

describe("the core tour's live-built source on the Sources page", () => {
  // Built live by the step's actions (tourActing.test); here, entered again, it explains the row.
  it("finds stepBuildSource's anchor once the supplies source is there", async () => {
    SOURCES = { sources: [...PET_STORE, SUPPLIES], loading: false, refetch: REFETCH };
    render(<SourcesPage />, { initialEntries: ["/sources"] });
    expect(await walkTourStep("stepBuildSource")).toBeInTheDocument();
  });
});

describe("the core tour's steps on the Sources page", () => {
  for (const [name, sources] of Object.entries(STATES)) {
    describe(name, () => {
      beforeEach(() => {
        SOURCES = { sources, loading: false, refetch: REFETCH };
      });
      for (const key of ["step0", "step1"]) {
        it(`is ready for ${key}`, async () => {
          render(<SourcesPage />, { initialEntries: [tourStep(key).route!] });
          await waitFor(() =>
            expect(document.querySelector(tourStep(key).readySelector!)).toBeInTheDocument(),
          );
        });
      }
      for (const key of ["step2", "step3"]) {
        it(`finds ${key}'s anchor`, async () => {
          render(<SourcesPage />, { initialEntries: ["/sources"] });
          expect(await walkTourStep(key)).toBeInTheDocument();
        });
      }
    });
  }
});
