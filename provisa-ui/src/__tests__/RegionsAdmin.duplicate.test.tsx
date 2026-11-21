// Copyright (c) 2026 Kenneth Stott
// Canary: ee35ed58-2003-47cd-9c4c-52b38c65c8c1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a platform region, a granted region and a store: "+" opens an empty form whatever item
// was selected or edited before it, and Duplicate (the panel head, beside Edit and Delete) opens
// the same create form filled from the selected item (rule:
// components/list/__tests__/panelDuplicateGuard.test.ts). A region's id takes no separator
// ([a-z][a-z0-9]{1,39}), so its copy is offered as "<id>copy". A store URL that holds a secret of
// its own is not copied.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import { expectHoldsNoneOf } from "../test-utils/createForm";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    loading: false,
    capabilities: ["org_settings"],
    demonstrated: [],
    billing: false,
    activeOrgId: "acme",
  }),
}));
vi.mock("../hooks/useRegionQueries", () => ({
  useRegionChoices: () => ({ regions: ["eu"], connected: "eu" }),
}));
vi.mock("../components/admin/CacheStorageTab", () => ({
  HotCacheStoreSettingsPanel: () => <div />,
  MaterializedSettingsPanel: () => <div />,
}));
vi.mock("../api/regions", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/regions")>()),
  fetchPlatformRegions: vi.fn(),
  savePlatformRegions: vi.fn(),
  fetchOrgGrants: vi.fn(async () => []),
  fetchOrgGrant: vi.fn(),
  saveOrgGrant: vi.fn(),
}));

import {
  fetchPlatformRegions,
  savePlatformRegions,
  type GrantView,
  type OrgRegion,
  type PlatformRegion,
  type PlatformRegions,
} from "../api/regions";
import { GrantEditor } from "../components/admin/GrantEditor";
import { PlatformRegionsTab } from "../components/admin/PlatformRegionsTab";

const EU: OrgRegion = {
  id: "eu",
  engine: "eu-trino",
  replicas: "eu-pg",
  views: "eu-pg",
  cache: "redis",
  state: "eu-pg",
  record: "eu-pg",
};
const STORES = [
  { id: "eu-pg", url: "postgresql://app:hunter2@eu.db/provisa", kind: null },
  { id: "eu-trino", url: "trino://eu:8080", kind: "trino-byo" },
  { id: "redis", url: "${env:REDIS_URL}", kind: null },
];
const EU_REGION: PlatformRegion = {
  id: "eu",
  description: "Customer data that stays in the EU",
  purposes: ["residency"],
  data_addresses: ["https://data.eu.example.com"],
};
const US_REGION: PlatformRegion = {
  id: "us",
  description: "Analytics",
  purposes: ["latency"],
  data_addresses: [],
};
const GRANT: GrantView = {
  platform_regions: [EU_REGION, US_REGION],
  stores: STORES,
  regions: [EU],
  engine_kinds: ["duckdb", "trino-byo"],
};
const PLATFORM: PlatformRegions = {
  regions: [EU_REGION],
  default_grant: { stores: [], regions: [] },
  engine_kinds: ["duckdb", "trino-byo"],
  read_only: false,
};

beforeEach(() => {
  vi.mocked(fetchPlatformRegions).mockReset().mockResolvedValue(PLATFORM);
  vi.mocked(savePlatformRegions)
    .mockReset()
    .mockImplementation(async (regions, defaultGrant) => ({
      ...PLATFORM,
      regions,
      default_grant: defaultGrant,
    }));
});

describe("Platform › Regions", () => {
  const VALUES = [
    "eu",
    "Customer data that stays in the EU",
    "https://data.eu.example.com",
    "eucopy",
  ];
  const panel = () => screen.getByTestId("platform-regions-split-panel");

  it("+ Add region opens empty after a region was selected", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-region-row-eu"));
    fireEvent.click(await screen.findByTestId("platform-regions-add"));
    await screen.findByTestId("platform-region-form");
    expectHoldsNoneOf(panel(), VALUES);
  });

  it("+ Add region opens empty after the region was edited and the edit cancelled", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-region-row-eu"));
    fireEvent.click(await screen.findByTestId("platform-region-edit"));
    fireEvent.click(await screen.findByText("Cancel"));
    fireEvent.click(await screen.findByTestId("platform-regions-add"));
    await screen.findByTestId("platform-region-form");
    expectHoldsNoneOf(panel(), VALUES);
  });

  it("offers Duplicate beside Edit and Delete, named Duplicate", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-region-row-eu"));
    expect(await screen.findByTestId("platform-region-duplicate")).toHaveAccessibleName(
      "Duplicate",
    );
  });

  it("opens the add form filled from the region, under an id that takes no separator", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-region-row-eu"));
    fireEvent.click(await screen.findByTestId("platform-region-duplicate"));
    expect(await screen.findByTestId("platform-region-id")).toHaveValue("eucopy");
    expect(screen.getByTestId("platform-region-id")).toBeEnabled();
    expect(screen.getByTestId("platform-region-description")).toHaveValue(
      "Customer data that stays in the EU",
    );
    fireEvent.click(screen.getByTestId("platform-region-save"));
    await waitFor(() => expect(savePlatformRegions).toHaveBeenCalledTimes(1));
    const saved = vi.mocked(savePlatformRegions).mock.calls[0][0];
    expect(saved.map((r) => r.id)).toEqual(["eu", "eucopy"]);
    expect(saved[1]).toMatchObject({
      description: "Customer data that stays in the EU",
      purposes: ["residency"],
      data_addresses: ["https://data.eu.example.com"],
    });
  });
});

describe("a grant", () => {
  const onSave = vi.fn();
  beforeEach(() => onSave.mockReset().mockImplementation(async (grant) => grant));
  const editor = () => render(<GrantEditor data={GRANT} onSave={onSave} storageKey="test.grant" />);
  const panel = () => screen.getByTestId("org-regions-split-panel");

  it("+ Add region opens empty after a region was selected", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-region-row-eu"));
    fireEvent.click(screen.getByTestId("org-regions-add-region"));
    await screen.findByTestId("org-region-form");
    expectHoldsNoneOf(panel(), ["eu", "eu-trino", "eu-pg", "redis"]);
  });

  it("+ Add store opens empty after a store was selected", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-store-row-eu-trino"));
    fireEvent.click(screen.getByTestId("org-regions-add-store"));
    await screen.findByTestId("org-store-form");
    expectHoldsNoneOf(panel(), ["eu-trino", "trino://eu:8080", "trino-byo", "eu-trino-copy"]);
  });

  it("offers Duplicate on a region and on a store", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-region-row-eu"));
    expect(await screen.findByTestId("org-region-duplicate")).toHaveAccessibleName("Duplicate");
    fireEvent.click(screen.getByTestId("org-store-row-redis"));
    expect(await screen.findByTestId("org-store-duplicate")).toHaveAccessibleName("Duplicate");
  });

  it("a region's copy holds its stores and asks for the platform region it is granted for", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-region-row-eu"));
    fireEvent.click(await screen.findByTestId("org-region-duplicate"));
    const form = await screen.findByTestId("org-region-form");
    expect(within(form).getByTestId("org-region-id")).toHaveValue("");
    expect(within(form).getByTestId("org-region-engine")).toHaveValue("eu-trino");
    expect(within(form).getByTestId("org-region-cache")).toHaveValue("redis");
    expect(screen.getByTestId("org-region-save")).toBeDisabled();
  });

  it("a store's copy is the same create form, under <id>-copy, with a whole reference kept", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-store-row-redis"));
    fireEvent.click(await screen.findByTestId("org-store-duplicate"));
    expect(await screen.findByTestId("org-store-id")).toHaveValue("redis-copy");
    expect(screen.getByTestId("org-store-id")).toBeEnabled();
    expect(screen.getByTestId("org-store-url")).toHaveValue("${env:REDIS_URL}");
    expect(screen.queryByTestId("org-store-duplicate-note")).toBeNull();
  });

  it("a store whose URL holds a password has the URL left empty, saying it must be entered", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-store-row-eu-pg"));
    fireEvent.click(await screen.findByTestId("org-store-duplicate"));
    expect(await screen.findByTestId("org-store-id")).toHaveValue("eu-pg-copy");
    expect(screen.getByTestId("org-store-url")).toHaveValue("");
    expect(screen.getByTestId("org-store-duplicate-note")).toHaveTextContent("eu-pg");
    expect(screen.getByTestId("org-store-save")).toBeDisabled();
  });
});
