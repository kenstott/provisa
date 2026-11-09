// Copyright (c) 2026 Kenneth Stott
// Canary: 1b7d4e92-c35a-4f86-9d20-6a8e3f5c7b14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: regions edited in the UI. The Platform page edits the platform's regions (id,
// description, purposes, data addresses), the default grant and each org's grant, says a change
// applies when the nodes restart, and lists the orgs a region's removal affects before making it.
// Admin › Regions shows the org's grant read-only. Each purpose's one-line explanation is shown
// wherever regions are listed. A demo sandbox opens the Platform page's regions read-only, with a
// note saying why. Admin › Regions is offered only where the platform declares regions.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const auth = vi.hoisted(() => ({
  loading: false,
  capabilities: ["org_settings"] as string[],
  demonstrated: [] as string[],
  billing: false,
  activeOrgId: "acme" as string | null,
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
    fetchPlatformRegions: vi.fn(),
    savePlatformRegions: vi.fn(),
    fetchOrgGrants: vi.fn(),
    fetchOrgGrant: vi.fn(),
    saveOrgGrant: vi.fn(),
  };
});

import {
  fetchOrgGrant,
  fetchOrgGrants,
  fetchOrgRegions,
  fetchPlatformRegions,
  RegionsRefused,
  saveOrgGrant,
  savePlatformRegions,
  type GrantView,
  type OrgRegion,
  type PlatformRegion,
  type PlatformRegions,
} from "../api/regions";
import { GrantEditor } from "../components/admin/GrantEditor";
import { OrgRegionsTab } from "../components/admin/OrgRegionsTab";
import { PlatformRegionsTab } from "../components/admin/PlatformRegionsTab";
import { AdminRail } from "../components/AdminRail";
import { NAV_GROUPS } from "../components/navGroups";

const fetchOrg = vi.mocked(fetchOrgRegions);
const fetchPlatform = vi.mocked(fetchPlatformRegions);
const savePlatform = vi.mocked(savePlatformRegions);
const fetchGrants = vi.mocked(fetchOrgGrants);
const fetchGrant = vi.mocked(fetchOrgGrant);
const saveGrant = vi.mocked(saveOrgGrant);

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
const EU_REGION: PlatformRegion = {
  id: "eu",
  description: "Customer data that stays in the EU",
  purposes: ["residency"],
  data_addresses: ["https://data.eu.example.com"],
};
const US_REGION: PlatformRegion = {
  id: "us",
  description: "Analytics",
  purposes: ["workload", "latency"],
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
  auth.capabilities = ["org_settings"];
  auth.activeOrgId = "acme";
  choices.value = { regions: ["eu"], connected: "eu" };
  fetchOrg.mockReset().mockResolvedValue(GRANT);
  fetchPlatform.mockReset().mockResolvedValue(PLATFORM);
  savePlatform.mockReset().mockImplementation(async (regions, defaultGrant) => ({
    ...PLATFORM,
    regions,
    default_grant: defaultGrant,
  }));
  fetchGrants.mockReset().mockResolvedValue([{ id: "acme", regions: ["eu"] }]);
  fetchGrant.mockReset().mockResolvedValue(GRANT);
  saveGrant.mockReset().mockImplementation(async (_org, grant) => ({ ...GRANT, ...grant }));
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

  it("shows the org's granted regions and stores read-only", async () => {
    render(<OrgRegionsTab />);
    expect(await screen.findByTestId("org-region-row-eu")).toBeInTheDocument();
    expect(screen.getByTestId("org-regions-granted-note")).toHaveTextContent("Platform page");
    expect(
      within(screen.getByTestId("org-store-row-redis")).getByText("${env:REDIS_URL}"),
    ).toBeInTheDocument();
    // Nothing to add, edit or remove.
    expect(screen.queryByTestId("org-regions-add-region")).toBeNull();
    expect(screen.queryByTestId("org-regions-add-store")).toBeNull();
    fireEvent.click(screen.getByTestId("org-region-row-eu"));
    expect(await screen.findByTestId("org-region-read-eu")).toHaveTextContent(
      "Customer data that stays in the EU",
    );
    expect(screen.queryByTestId("org-region-edit")).toBeNull();
    expect(screen.queryByTestId("org-region-delete")).toBeNull();
    fireEvent.click(screen.getByTestId("org-store-row-redis"));
    expect(await screen.findByTestId("org-store-read-redis")).toHaveTextContent("${env:REDIS_URL}");
    expect(screen.queryByTestId("org-store-edit")).toBeNull();
  });

  it("shows each purpose with its one-line explanation", async () => {
    render(<OrgRegionsTab />);
    expect(await screen.findByTestId("org-region-row-eu")).toHaveTextContent("Residency");
    const legend = screen.getByTestId("region-purposes-legend");
    expect(within(legend).getByTestId("region-purposes-legend-residency")).toHaveTextContent(
      "Data must stay in a jurisdiction.",
    );
    expect(within(legend).getByTestId("region-purposes-legend-dedicated")).toHaveTextContent(
      "reserved for one organization",
    );
    expect(legend).toHaveTextContent("Purposes explain a region");
  });

  it("shows nothing when the platform declares no regions", async () => {
    fetchOrg.mockResolvedValue({ ...GRANT, platform_regions: [], stores: [], regions: [] });
    render(<OrgRegionsTab />);
    expect(await screen.findByTestId("org-regions-none-declared")).toBeInTheDocument();
    expect(screen.queryByTestId("org-region-row-eu")).toBeNull();
  });
});

describe("a grant, as the platform administrator edits it", () => {
  const onSave = vi.fn();
  beforeEach(() => {
    onSave.mockReset().mockImplementation(async (grant) => grant);
  });
  const editor = (data: GrantView = GRANT) =>
    render(<GrantEditor data={data} onSave={onSave} storageKey="test.grant" />);

  it("adds a region, saving the whole set", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-regions-add-region"));
    await choose("org-region-id", "us");
    for (const role of ["engine", "replicas", "views", "cache", "state", "record"]) {
      await choose(
        `org-region-${role}`,
        role === "engine" ? "eu-trino" : role === "cache" ? "redis" : "eu-pg",
      );
    }
    fireEvent.click(screen.getByTestId("org-region-save"));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    const [grant, confirm] = onSave.mock.calls[0];
    expect(grant.stores).toEqual(STORES);
    expect(grant.regions.map((r: OrgRegion) => r.id)).toEqual(["eu", "us"]);
    expect(confirm).toBe(false);
    expect(await screen.findByTestId("org-region-read-us")).toBeInTheDocument();
  });

  it("shows a refused save's reason and marks the field it names", async () => {
    onSave.mockRejectedValue(
      new RegionsRefused(
        "region 'eu' engine store 'eu-pg' names no engine kind; give it one of duckdb, trino-byo",
        "regions.eu.engine",
      ),
    );
    editor();
    fireEvent.click(await screen.findByTestId("org-region-row-eu"));
    fireEvent.click(await screen.findByTestId("org-region-edit"));
    await choose("org-region-engine", "eu-pg");
    fireEvent.click(screen.getByTestId("org-region-save"));
    expect(await screen.findByText(/names no engine kind/)).toBeInTheDocument();
    expect(screen.getByTestId("org-region-engine")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByTestId("org-region-replicas")).not.toHaveAttribute("aria-invalid", "true");
  });

  it("edits a store, sending its URL exactly as typed", async () => {
    editor();
    fireEvent.click(await screen.findByTestId("org-store-row-eu-pg"));
    fireEvent.click(await screen.findByTestId("org-store-edit"));
    fireEvent.change(screen.getByTestId("org-store-url"), {
      target: { value: "${env:EU_PG_URL}" },
    });
    fireEvent.click(screen.getByTestId("org-store-save"));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    const [grant] = onSave.mock.calls[0];
    expect(grant.stores.find((s: { id: string }) => s.id === "eu-pg")?.url).toBe(
      "${env:EU_PG_URL}",
    );
  });

  it("removes a region once the dialog confirms it", async () => {
    editor({ ...GRANT, regions: [EU, { ...EU, id: "us" }] });
    fireEvent.click(await screen.findByTestId("org-region-row-us"));
    fireEvent.click(await screen.findByTestId("org-region-delete"));
    fireEvent.click(await screen.findByTestId("org-region-delete-dialog-confirm"));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    const [grant, confirm] = onSave.mock.calls[0];
    expect(grant.regions.map((r: OrgRegion) => r.id)).toEqual(["eu"]);
    expect(confirm).toBe(true);
    await waitFor(() => expect(screen.queryByTestId("org-region-row-us")).toBeNull());
  });

  it("holds a store added before any region, and saves it with the first", async () => {
    editor({ ...GRANT, stores: [], regions: [] });
    fireEvent.click(await screen.findByTestId("org-regions-add-store"));
    fireEvent.change(screen.getByTestId("org-store-id"), { target: { value: "eu-duck" } });
    fireEvent.change(screen.getByTestId("org-store-url"), {
      target: { value: "duckdb:///eu.duckdb" },
    });
    await choose("org-store-kind", "duckdb");
    fireEvent.click(screen.getByTestId("org-store-save"));
    expect(await screen.findByTestId("org-regions-held")).toBeInTheDocument();
    expect(onSave).not.toHaveBeenCalled();

    fireEvent.click(screen.getByTestId("org-regions-add-region"));
    await choose("org-region-id", "eu");
    for (const role of ["engine", "replicas", "views", "cache", "state", "record"]) {
      await choose(`org-region-${role}`, "eu-duck");
    }
    fireEvent.click(screen.getByTestId("org-region-save"));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(onSave.mock.calls[0][0].stores).toEqual([
      { id: "eu-duck", url: "duckdb:///eu.duckdb", kind: "duckdb" },
    ]);
    await waitFor(() => expect(screen.queryByTestId("org-regions-held")).toBeNull());
  });
});

describe("the Platform page", () => {
  it("is a platform setting", () => {
    const item = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === "/admin/platform");
    expect(item?.capability).toBe("platform_settings");
  });

  it("lists the regions with their purposes explained and says changes apply on restart", async () => {
    render(<PlatformRegionsTab />);
    const row = await screen.findByTestId("platform-region-row-eu");
    expect(row).toHaveTextContent("Customer data that stays in the EU");
    expect(row).toHaveTextContent("Residency");
    expect(screen.getByTestId("region-purposes-legend-residency")).toHaveTextContent(
      "Data must stay in a jurisdiction.",
    );
    expect(screen.getByTestId("platform-regions-restart")).toHaveTextContent(
      "take effect when the nodes restart",
    );
    fireEvent.click(row);
    expect(await screen.findByTestId("platform-region-read-data-addresses")).toHaveTextContent(
      "https://data.eu.example.com",
    );
  });

  // REQ-1412: the engine lane's route is the commercial plugin's, and a lane applies at once.
  it("offers each org's engine only with the plugin, without the restart note", async () => {
    auth.billing = false;
    const { unmount } = render(<PlatformRegionsTab />);
    await screen.findByTestId("platform-region-row-eu");
    expect(screen.queryByTestId("platform-section-engines")).not.toBeInTheDocument();
    unmount();

    auth.billing = true;
    try {
      fetchGrants.mockResolvedValue([{ id: "acme", regions: [] }]);
      render(<PlatformRegionsTab />);
      fireEvent.click(await screen.findByTestId("platform-section-engines"));
      expect(await screen.findByTestId("platform-engine-org-pick")).toBeInTheDocument();
      expect(screen.queryByTestId("platform-regions-restart")).not.toBeInTheDocument();
    } finally {
      auth.billing = false;
    }
  });

  it("adds a region with its description, purposes and data addresses", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-regions-add"));
    fireEvent.change(screen.getByTestId("platform-region-id"), { target: { value: "us" } });
    fireEvent.change(screen.getByTestId("platform-region-description"), {
      target: { value: "Analytics" },
    });
    expect(screen.getByTestId("platform-region-save")).toBeDisabled(); // no purpose yet
    fireEvent.click(screen.getByTestId("platform-region-purpose-workload"));
    fireEvent.click(screen.getByTestId("platform-region-purpose-latency"));
    fireEvent.click(screen.getByTestId("platform-region-save"));
    await waitFor(() =>
      expect(savePlatform).toHaveBeenCalledWith(
        [
          EU_REGION,
          {
            id: "us",
            description: "Analytics",
            purposes: ["workload", "latency"],
            data_addresses: [],
          },
        ],
        PLATFORM.default_grant,
        false,
      ),
    );
    expect(await screen.findByTestId("platform-regions-restart")).toHaveTextContent(
      "until they restart",
    );
  });

  it("lists the orgs a region's removal affects, and removes it only once confirmed", async () => {
    savePlatform.mockRejectedValueOnce(
      new RegionsRefused("removing region eu affects the orgs granted it: acme, globex", null, [
        "acme",
        "globex",
      ]),
    );
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-region-row-eu"));
    fireEvent.click(await screen.findByTestId("platform-region-delete"));
    fireEvent.click(await screen.findByTestId("platform-region-delete-dialog-confirm"));
    const affected = await screen.findByTestId("platform-region-affected");
    expect(affected).toHaveTextContent("acme, globex");
    expect(savePlatform).toHaveBeenLastCalledWith([], PLATFORM.default_grant, false);
    fireEvent.click(screen.getByTestId("platform-region-remove-anyway"));
    await waitFor(() =>
      expect(savePlatform).toHaveBeenLastCalledWith([], PLATFORM.default_grant, true),
    );
  });

  it("marks the field a refusal names", async () => {
    savePlatform.mockRejectedValue(
      new RegionsRefused("platform region 'us' purposes: too short", "regions.1.purposes"),
    );
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-regions-add"));
    fireEvent.change(screen.getByTestId("platform-region-id"), { target: { value: "us" } });
    fireEvent.change(screen.getByTestId("platform-region-description"), {
      target: { value: "x" },
    });
    fireEvent.click(screen.getByTestId("platform-region-purpose-workload"));
    fireEvent.click(screen.getByTestId("platform-region-save"));
    expect(await screen.findByText("platform region 'us' purposes: too short")).toBeInTheDocument();
    expect(screen.getByTestId("platform-region-description")).not.toHaveAttribute(
      "aria-invalid",
      "true",
    );
  });

  it("edits an org's grant in the Organizations section", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-section-orgs"));
    await screen.findByTestId("platform-org-pick");
    await choose("platform-org-pick", "acme (granted eu)");
    expect(await screen.findByTestId("org-region-row-eu")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("org-store-row-eu-pg"));
    fireEvent.click(await screen.findByTestId("org-store-edit"));
    fireEvent.change(screen.getByTestId("org-store-url"), {
      target: { value: "${env:EU_PG_URL}" },
    });
    fireEvent.click(screen.getByTestId("org-store-save"));
    await waitFor(() => expect(saveGrant).toHaveBeenCalledTimes(1));
    const [org, grant, confirm] = saveGrant.mock.calls[0];
    expect(org).toBe("acme");
    expect(grant.stores.find((s) => s.id === "eu-pg")?.url).toBe("${env:EU_PG_URL}");
    expect(confirm).toBe(false);
  });

  it("edits the default grant every new org receives", async () => {
    render(<PlatformRegionsTab />);
    fireEvent.click(await screen.findByTestId("platform-section-default"));
    fireEvent.click(await screen.findByTestId("org-regions-add-store"));
    fireEvent.change(screen.getByTestId("org-store-id"), { target: { value: "eu-duck" } });
    fireEvent.change(screen.getByTestId("org-store-url"), {
      target: { value: "duckdb:///eu.duckdb" },
    });
    await choose("org-store-kind", "duckdb");
    fireEvent.click(screen.getByTestId("org-store-save"));
    fireEvent.click(screen.getByTestId("org-regions-add-region"));
    await choose("org-region-id", "eu");
    for (const role of ["engine", "replicas", "views", "cache", "state", "record"]) {
      await choose(`org-region-${role}`, "eu-duck");
    }
    fireEvent.click(screen.getByTestId("org-region-save"));
    await waitFor(() => expect(savePlatform).toHaveBeenCalledTimes(1));
    const [regions, grant] = savePlatform.mock.calls[0];
    expect(regions).toEqual([EU_REGION]);
    expect(grant.regions.map((r) => r.id)).toEqual(["eu"]);
  });
});

describe("the Platform page in a demo sandbox", () => {
  beforeEach(() => {
    auth.activeOrgId = "sbx0123456789ab";
    fetchPlatform.mockResolvedValue({ ...PLATFORM, read_only: true });
  });

  it("is offered to the sandbox's visitor, who holds no platform right", () => {
    render(<AdminRail />, { initialEntries: ["/admin/tags"] });
    expect(screen.getByRole("link", { name: "Platform" })).toHaveAttribute(
      "href",
      "/admin/platform",
    );
    cleanup();
    auth.activeOrgId = "acme";
    render(<AdminRail />, { initialEntries: ["/admin/tags"] });
    expect(screen.queryByRole("link", { name: "Platform" })).toBeNull();
  });

  it("shows the regions read-only with a note saying why", async () => {
    render(<PlatformRegionsTab />);
    expect(await screen.findByTestId("platform-regions-sandbox-note")).toHaveTextContent(
      "shown in the demo to explain regions",
    );
    expect(screen.getByTestId("region-purposes-legend")).toBeInTheDocument();
    expect(screen.queryByTestId("platform-regions-add")).toBeNull();
    expect(screen.queryByTestId("platform-section-orgs")).toBeNull();
    expect(screen.queryByTestId("platform-section-default")).toBeNull();
    fireEvent.click(screen.getByTestId("platform-region-row-eu"));
    expect(await screen.findByTestId("platform-region-read-eu")).toBeInTheDocument();
    expect(screen.queryByTestId("platform-region-edit")).toBeNull();
    expect(screen.queryByTestId("platform-region-delete")).toBeNull();
  });
});
