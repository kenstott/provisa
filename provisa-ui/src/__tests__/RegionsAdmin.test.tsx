// Copyright (c) 2026 Kenneth Stott
// Canary: 1b7d4e92-c35a-4f86-9d20-6a8e3f5c7b14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: regions edited in the UI. Admin › Regions lists the org's regions and stores, adds,
// edits and removes them as one saved set, and shows a refusal naming the field; a store URL is
// sent and shown as entered. The Platform page edits the platform's regions and says a change
// takes effect on restart. Admin › Regions is offered only where the platform declares regions.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const auth = vi.hoisted(() => ({
  loading: false,
  capabilities: ["org_settings"] as string[],
  demonstrated: [] as string[],
  billing: false,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
const choices = vi.hoisted(() => ({
  value: { regions: [] as string[], connected: null as string | null },
}));
vi.mock("../hooks/useRegionQueries", () => ({ useRegionChoices: () => choices.value }));
vi.mock("../api/regions", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api/regions")>();
  return {
    ...actual,
    fetchOrgRegions: vi.fn(),
    saveOrgRegions: vi.fn(),
    fetchPlatformRegions: vi.fn(),
    savePlatformRegions: vi.fn(),
  };
});

import {
  fetchOrgRegions,
  fetchPlatformRegions,
  RegionsRefused,
  saveOrgRegions,
  savePlatformRegions,
  type OrgRegion,
  type OrgRegions,
} from "../api/regions";
import { OrgRegionsTab } from "../components/admin/OrgRegionsTab";
import { PlatformRegionsTab } from "../components/admin/PlatformRegionsTab";
import { AdminRail } from "../components/AdminRail";
import { NAV_GROUPS } from "../components/navGroups";

const fetchOrg = vi.mocked(fetchOrgRegions);
const saveOrg = vi.mocked(saveOrgRegions);
const fetchPlatform = vi.mocked(fetchPlatformRegions);
const savePlatform = vi.mocked(savePlatformRegions);

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
  { id: "eu-pg", url: "postgresql://app@eu.db/provisa", kind: null },
  { id: "eu-trino", url: "trino://eu:8080", kind: "trino-byo" },
  { id: "redis", url: "${env:REDIS_URL}", kind: null },
];
const ORG: OrgRegions = {
  platform_regions: [
    { id: "eu", address: "https://eu.example.com" },
    { id: "us", address: "https://us.example.com" },
  ],
  stores: STORES,
  regions: [EU],
  engine_kinds: ["duckdb", "trino-byo"],
};

beforeEach(() => {
  auth.capabilities = ["org_settings"];
  choices.value = { regions: ["eu"], connected: "eu" };
  fetchOrg.mockReset().mockResolvedValue(ORG);
  saveOrg.mockReset().mockImplementation(async (stores, regions) => ({ stores, regions }));
  fetchPlatform.mockReset().mockResolvedValue([{ id: "eu", address: "${env:EU_ADDRESS}" }]);
  savePlatform.mockReset().mockImplementation(async (regions) => regions);
});

/** Choose ``option`` in the Select ``testId``. */
async function choose(testId: string, option: string) {
  const input = screen.getByTestId(testId);
  fireEvent.click(input);
  await waitFor(() => expect(input).toHaveAttribute("aria-controls"));
  const list = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(within(list as HTMLElement).getByRole("option", { name: option, hidden: true }));
}

describe("Admin › Regions", () => {
  it("is an org setting offered only where the platform declares regions", () => {
    const item = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === "/admin/regions");
    expect(item?.capability).toBe("org_settings");
    render(<AdminRail />, { initialEntries: ["/admin/tags"] });
    expect(screen.getByRole("link", { name: "Regions" })).toHaveAttribute("href", "/admin/regions");
    cleanup();
    choices.value = { regions: [], connected: null };
    render(<AdminRail />, { initialEntries: ["/admin/tags"] });
    expect(screen.queryByRole("link", { name: "Regions" })).toBeNull();
  });

  it("lists the org's regions and stores, each store's URL as entered", async () => {
    render(<OrgRegionsTab />);
    expect(await screen.findByTestId("org-region-row-eu")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("org-store-row-redis")).getByText("${env:REDIS_URL}"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("org-store-row-redis"));
    expect(await screen.findByTestId("org-store-read-redis")).toHaveTextContent("${env:REDIS_URL}");
  });

  it("adds a region, saving the org's whole set", async () => {
    render(<OrgRegionsTab />);
    fireEvent.click(await screen.findByTestId("org-regions-add-region"));
    await choose("org-region-id", "us");
    for (const role of ["engine", "replicas", "views", "cache", "state", "record"]) {
      await choose(
        `org-region-${role}`,
        role === "engine" ? "eu-trino" : role === "cache" ? "redis" : "eu-pg",
      );
    }
    fireEvent.click(screen.getByTestId("org-region-save"));
    await waitFor(() => expect(saveOrg).toHaveBeenCalledTimes(1));
    const [stores, regions] = saveOrg.mock.calls[0];
    expect(stores).toEqual(STORES);
    expect(regions.map((r) => r.id)).toEqual(["eu", "us"]);
    expect(await screen.findByTestId("org-region-read-us")).toBeInTheDocument();
  });

  it("shows a refused save's reason and marks the field it names", async () => {
    saveOrg.mockRejectedValue(
      new RegionsRefused(
        "region 'eu' engine store 'eu-pg' names no engine kind; give it one of duckdb, trino-byo",
        "regions.eu.engine",
      ),
    );
    render(<OrgRegionsTab />);
    fireEvent.click(await screen.findByTestId("org-region-row-eu"));
    fireEvent.click(await screen.findByTestId("org-region-edit"));
    await choose("org-region-engine", "eu-pg");
    fireEvent.click(screen.getByTestId("org-region-save"));
    expect(await screen.findByText(/names no engine kind/)).toBeInTheDocument();
    expect(screen.getByTestId("org-region-engine")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByTestId("org-region-replicas")).not.toHaveAttribute("aria-invalid", "true");
  });

  it("edits a store, sending its URL exactly as typed", async () => {
    render(<OrgRegionsTab />);
    fireEvent.click(await screen.findByTestId("org-store-row-eu-pg"));
    fireEvent.click(await screen.findByTestId("org-store-edit"));
    fireEvent.change(screen.getByTestId("org-store-url"), {
      target: { value: "${env:EU_PG_URL}" },
    });
    fireEvent.click(screen.getByTestId("org-store-save"));
    await waitFor(() => expect(saveOrg).toHaveBeenCalledTimes(1));
    const [stores] = saveOrg.mock.calls[0];
    expect(stores.find((s) => s.id === "eu-pg")?.url).toBe("${env:EU_PG_URL}");
  });

  it("removes a store, and says why when the server refuses", async () => {
    saveOrg.mockRejectedValueOnce(
      new RegionsRefused(
        "region 'eu' cache store 'redis' is not declared in stores",
        "regions.eu.cache",
      ),
    );
    render(<OrgRegionsTab />);
    fireEvent.click(await screen.findByTestId("org-store-row-redis"));
    fireEvent.click(await screen.findByTestId("org-store-delete"));
    fireEvent.click(await screen.findByTestId("org-store-delete-dialog-confirm"));
    expect(await screen.findByText(/is not declared in stores/)).toBeInTheDocument();
    const [stores] = saveOrg.mock.calls[0];
    expect(stores.map((s) => s.id)).toEqual(["eu-pg", "eu-trino"]);
  });

  it("removes a region", async () => {
    fetchOrg.mockResolvedValue({ ...ORG, regions: [EU, { ...EU, id: "us" }] });
    render(<OrgRegionsTab />);
    fireEvent.click(await screen.findByTestId("org-region-row-us"));
    fireEvent.click(await screen.findByTestId("org-region-delete"));
    fireEvent.click(await screen.findByTestId("org-region-delete-dialog-confirm"));
    await waitFor(() => expect(saveOrg).toHaveBeenCalledTimes(1));
    expect(saveOrg.mock.calls[0][1].map((r) => r.id)).toEqual(["eu"]);
    await waitFor(() => expect(screen.queryByTestId("org-region-row-us")).toBeNull());
  });

  it("holds a store added before the org selects a region, and saves it with the first", async () => {
    fetchOrg.mockResolvedValue({ ...ORG, stores: [], regions: [] });
    render(<OrgRegionsTab />);
    fireEvent.click(await screen.findByTestId("org-regions-add-store"));
    fireEvent.change(screen.getByTestId("org-store-id"), { target: { value: "eu-duck" } });
    fireEvent.change(screen.getByTestId("org-store-url"), {
      target: { value: "duckdb:///eu.duckdb" },
    });
    await choose("org-store-kind", "duckdb");
    fireEvent.click(screen.getByTestId("org-store-save"));
    expect(await screen.findByTestId("org-regions-held")).toBeInTheDocument();
    expect(saveOrg).not.toHaveBeenCalled();

    fireEvent.click(screen.getByTestId("org-regions-add-region"));
    await choose("org-region-id", "eu");
    for (const role of ["engine", "replicas", "views", "cache", "state", "record"]) {
      await choose(`org-region-${role}`, "eu-duck");
    }
    fireEvent.click(screen.getByTestId("org-region-save"));
    await waitFor(() => expect(saveOrg).toHaveBeenCalledTimes(1));
    expect(saveOrg.mock.calls[0][0]).toEqual([
      { id: "eu-duck", url: "duckdb:///eu.duckdb", kind: "duckdb" },
    ]);
    await waitFor(() => expect(screen.queryByTestId("org-regions-held")).toBeNull());
  });

  it("shows nothing to edit when the platform declares no regions", async () => {
    fetchOrg.mockResolvedValue({ ...ORG, platform_regions: [], stores: [], regions: [] });
    render(<OrgRegionsTab />);
    expect(await screen.findByTestId("org-regions-none-declared")).toBeInTheDocument();
    expect(screen.queryByTestId("org-regions-add-region")).toBeNull();
  });
});

describe("Platform › Regions", () => {
  it("is a platform setting", () => {
    const item = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === "/admin/platform");
    expect(item?.capability).toBe("platform_settings");
  });

  it("lists the platform's regions as written and says a change applies on restart", async () => {
    render(<PlatformRegionsTab />);
    expect(await screen.findByTestId("platform-region-row-eu")).toHaveTextContent(
      "${env:EU_ADDRESS}",
    );
    expect(screen.getByTestId("platform-regions-restart")).toHaveTextContent(
      "Changes to the platform's regions take effect when the nodes restart.",
    );
  });

  it("adds a region and then says the nodes run the old list until they restart", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-regions-add"));
    fireEvent.change(screen.getByTestId("platform-region-id"), { target: { value: "us" } });
    fireEvent.change(screen.getByTestId("platform-region-address"), {
      target: { value: "${env:US_ADDRESS}" },
    });
    fireEvent.click(screen.getByTestId("platform-region-save"));
    await waitFor(() =>
      expect(savePlatform).toHaveBeenCalledWith([
        { id: "eu", address: "${env:EU_ADDRESS}" },
        { id: "us", address: "${env:US_ADDRESS}" },
      ]),
    );
    expect(await screen.findByTestId("platform-regions-restart")).toHaveTextContent(
      "until they restart",
    );
  });

  it("edits and removes a region", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-region-row-eu"));
    fireEvent.click(await screen.findByTestId("platform-region-edit"));
    fireEvent.change(screen.getByTestId("platform-region-address"), {
      target: { value: "https://eu.example.com" },
    });
    fireEvent.click(screen.getByTestId("platform-region-save"));
    await waitFor(() =>
      expect(savePlatform).toHaveBeenCalledWith([{ id: "eu", address: "https://eu.example.com" }]),
    );
    fireEvent.click(await screen.findByTestId("platform-region-delete"));
    fireEvent.click(await screen.findByTestId("platform-region-delete-dialog-confirm"));
    await waitFor(() => expect(savePlatform).toHaveBeenLastCalledWith([]));
  });

  it("shows a refusal naming the field", async () => {
    savePlatform.mockRejectedValue(
      new RegionsRefused("platform region 'us' names no address", "regions.1.address"),
    );
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-regions-add"));
    fireEvent.change(screen.getByTestId("platform-region-id"), { target: { value: "us" } });
    fireEvent.change(screen.getByTestId("platform-region-address"), { target: { value: "x" } });
    fireEvent.click(screen.getByTestId("platform-region-save"));
    expect(await screen.findByText("platform region 'us' names no address")).toBeInTheDocument();
    expect(screen.getByTestId("platform-region-address")).toHaveAttribute("aria-invalid", "true");
  });
});
