// Copyright (c) 2026 Kenneth Stott
// Canary: 2ab10e3b-73ce-4f57-9b66-ad21aed2b52a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: Admin > Federation has a tab per region when the org uses regions, each that region's
// engine store as the platform assigned it -- its engine kind and its URL exactly as entered --
// saved by the platform administrator through the org's grant, read by anyone else. With no
// regions the page is the one engine, as before.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import type { MockedResponse } from "@apollo/client/testing";
import { render, screen, waitFor, within } from "../../../test-utils/render";
import { RegionChoices } from "../../../hooks/admin.graphql";
import * as admin from "../../../api/admin";
import {
  fetchOrgGrant,
  fetchOrgRegions,
  RegionsRefused,
  saveOrgGrant,
  type GrantView,
} from "../../../api/regions";
import { FederationEngineTab } from "../FederationEngineTab";
import { OrgEngineTab } from "../OrgEngineTab";

const auth = { capabilities: [] as string[], activeOrgId: "acme" as string | null };
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../settingsCards", () => ({ FederationSettingsCards: () => null }));
vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchFederationEngine: vi.fn(),
  fetchOrgEngine: vi.fn(),
}));
vi.mock("../../../api/regions", async (orig) => ({
  ...(await orig<typeof import("../../../api/regions")>()),
  fetchOrgGrant: vi.fn(),
  fetchOrgRegions: vi.fn(),
  saveOrgGrant: vi.fn(),
}));

const fetchGrant = vi.mocked(fetchOrgGrant);
const fetchRegions = vi.mocked(fetchOrgRegions);
const saveGrant = vi.mocked(saveOrgGrant);

const PLATFORM_ADMIN = ["platform_settings", "cross_org", "org_settings"];
const ORG_ADMIN = ["platform_settings", "org_settings"];

const choices = (regions: string[]): MockedResponse[] => [
  {
    request: { query: RegionChoices },
    result: {
      data: {
        regionChoices: {
          __typename: "RegionChoices",
          regions,
          connected: regions[0] ?? null,
        },
      },
    },
  },
];

const GRANT: GrantView = {
  platform_regions: [
    { id: "eu", description: "EU residency", purposes: ["residency"] },
    { id: "us", description: "US analytics", purposes: ["workload"] },
  ],
  engine_kinds: ["duckdb", "trino", "trino-byo"],
  stores: [
    { id: "eu-pg", url: "postgresql://provisa@eu-db.internal:5432/provisa", kind: null },
    // As entered: the reference stays one, and is never resolved for the page.
    { id: "eu-trino", url: "${env:EU_TRINO_URL}", kind: "trino-byo" },
    { id: "us-pg", url: "postgresql://provisa@us-db.internal:5432/provisa", kind: null },
    { id: "us-trino", url: "trino://us-trino.internal:8080/hive", kind: "trino" },
  ],
  regions: [
    {
      id: "eu",
      engine: "eu-trino",
      replicas: "eu-pg",
      views: "eu-pg",
      cache: "eu-pg",
      state: "eu-pg",
      record: "eu-pg",
    },
    {
      id: "us",
      engine: "us-trino",
      replicas: "us-pg",
      views: "us-pg",
      cache: "us-pg",
      state: "us-pg",
      record: "us-pg",
    },
  ],
};

const ENGINE: admin.FederationEngineState = {
  current: "duckdb",
  persisted: "duckdb",
  env_pinned_engine: null,
  config: {},
  secret_set: {},
  engines: [
    { key: "duckdb", label: "DuckDB", description: "In-process.", config_fields: [], unavailable: null },
  ],
  restart_required_note: "restart",
};

beforeEach(() => {
  auth.capabilities = PLATFORM_ADMIN;
  auth.activeOrgId = "acme";
  vi.mocked(admin.fetchFederationEngine).mockReset().mockResolvedValue(ENGINE);
  fetchGrant.mockReset().mockResolvedValue(GRANT);
  fetchRegions.mockReset().mockResolvedValue(GRANT);
  saveGrant.mockReset();
});

describe("Admin > Federation with no regions", () => {
  it("is the one engine form, as before, and asks for no grant", async () => {
    render(<FederationEngineTab />, { mocks: choices([]) });
    expect(await screen.findByTestId("federation-engine-select")).toBeInTheDocument();
    expect(screen.queryByTestId("federation-regions")).not.toBeInTheDocument();
    expect(fetchGrant).not.toHaveBeenCalled();
    expect(fetchRegions).not.toHaveBeenCalled();
  });

  it("is the one engine form when the platform declares regions the org is not granted", async () => {
    fetchGrant.mockResolvedValue({ ...GRANT, stores: [], regions: [] });
    render(<FederationEngineTab />, { mocks: choices(["eu", "us"]) });
    expect(await screen.findByTestId("federation-engine-select")).toBeInTheDocument();
    expect(screen.queryByTestId("federation-regions")).not.toBeInTheDocument();
  });
});

describe("Admin > Federation with regions", () => {
  it("shows one tab per region, each its engine kind and URL as entered", async () => {
    const user = userEvent.setup();
    render(<FederationEngineTab />, { mocks: choices(["eu", "us"]) });

    expect(await screen.findByTestId("federation-region-tab-eu")).toBeInTheDocument();
    expect(screen.getByTestId("federation-region-tab-us")).toBeInTheDocument();
    expect(screen.queryByTestId("federation-engine-select")).not.toBeInTheDocument();
    expect(fetchGrant).toHaveBeenCalledWith("acme");

    expect(screen.getByTestId("federation-region-eu-kind")).toHaveTextContent("trino-byo");
    expect(screen.getByTestId("federation-region-eu-url")).toHaveTextContent(
      "${env:EU_TRINO_URL}",
    );

    await user.click(screen.getByTestId("federation-region-tab-us"));
    expect(await screen.findByTestId("federation-region-us-kind")).toHaveTextContent("trino");
    expect(screen.getByTestId("federation-region-us-url")).toHaveTextContent(
      "trino://us-trino.internal:8080/hive",
    );
  });

  it("saves the platform administrator's change through the org's grant, URL as entered", async () => {
    const user = userEvent.setup();
    const url = "trino://eu-trino-2.internal:8443/hive?SSL=true&x=%2F";
    saveGrant.mockImplementation(async (_org, grant) => ({ ...GRANT, ...grant }));
    render(<FederationEngineTab />, { mocks: choices(["eu", "us"]) });

    await user.click(await screen.findByTestId("federation-region-eu-edit"));
    const form = await screen.findByTestId("federation-region-eu-form");
    const input = within(form).getByTestId("federation-region-eu-url-input");
    await user.clear(input);
    // Pasted, so the URL arrives as one value, exactly as an operator would enter it.
    await user.click(input);
    await user.paste(url);
    await user.click(within(form).getByTestId("federation-region-eu-save"));

    await waitFor(() => expect(saveGrant).toHaveBeenCalledTimes(1));
    const [org, sent, confirm] = saveGrant.mock.calls[0];
    expect(org).toBe("acme");
    expect(confirm).toBe(false);
    expect(sent.regions).toEqual(GRANT.regions);
    expect(sent.stores).toEqual(
      GRANT.stores.map((s) => (s.id === "eu-trino" ? { ...s, url, kind: "trino-byo" } : s)),
    );
    expect(await screen.findByTestId("federation-region-eu-url")).toHaveTextContent(url);
    expect(screen.getByTestId("federation-regions-restart")).toBeInTheDocument();
  });

  it("names the server's refusal in the form and keeps the form open", async () => {
    const user = userEvent.setup();
    saveGrant.mockRejectedValue(
      new RegionsRefused("store eu-trino: url is not a trino URL", "stores.eu-trino.url"),
    );
    render(<FederationEngineTab />, { mocks: choices(["eu", "us"]) });

    await user.click(await screen.findByTestId("federation-region-eu-edit"));
    const form = await screen.findByTestId("federation-region-eu-form");
    await user.click(within(form).getByTestId("federation-region-eu-save"));
    expect(await screen.findByText("store eu-trino: url is not a trino URL")).toBeInTheDocument();
    expect(screen.getByTestId("federation-region-eu-form")).toBeInTheDocument();
  });

  it("is read-only for an org administrator, read from the org's own regions", async () => {
    auth.capabilities = ORG_ADMIN;
    render(<FederationEngineTab />, { mocks: choices(["eu", "us"]) });

    expect(await screen.findByTestId("federation-region-eu-url")).toHaveTextContent(
      "${env:EU_TRINO_URL}",
    );
    expect(screen.getByTestId("federation-regions-read-only")).toBeInTheDocument();
    expect(screen.queryByTestId("federation-region-eu-edit")).not.toBeInTheDocument();
    expect(fetchRegions).toHaveBeenCalledTimes(1);
    expect(fetchGrant).not.toHaveBeenCalled();
  });

  it("is read-only in a demo sandbox, even for a visitor holding the platform's rights", async () => {
    auth.activeOrgId = "sbx-visitor-1";
    render(<FederationEngineTab />, { mocks: choices(["eu", "us"]) });

    const note = await screen.findByTestId("federation-regions-read-only");
    expect(note).toHaveTextContent("It cannot be changed in a demo.");
    expect(screen.queryByTestId("federation-region-eu-edit")).not.toBeInTheDocument();
    expect(fetchGrant).not.toHaveBeenCalled();
  });

  it("is the same tabs on the org's engine page", async () => {
    auth.capabilities = ORG_ADMIN;
    render(<OrgEngineTab />, { mocks: choices(["eu", "us"]) });
    expect(await screen.findByTestId("federation-region-tab-eu")).toBeInTheDocument();
    expect(screen.getByTestId("federation-region-tab-us")).toBeInTheDocument();
    expect(admin.fetchOrgEngine).not.toHaveBeenCalled();
  });
});
