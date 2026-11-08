// Copyright (c) 2026 Kenneth Stott
// Canary: d30fa8f9-cec2-406c-94e3-8a5b89c6a162
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327: naming and the processing settings an org owns are editable by its administrator;
// the store connections and the rest of the platform's settings stay on the platform's surfaces.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";
import * as api from "../../../api/admin";
import type { PlatformSettings } from "../../../api/admin";
import {
  FederationSettingsCards,
  NamingConventionsCard,
  OrgProcessingCards,
} from "../settingsCards";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettings: vi.fn(),
  updateSettings: vi.fn(),
}));
vi.mock("../../../hooks/useNamingConventions", () => ({
  useNamingConventions: () => ["apollo_graphql", "hasura_graphql"],
  namingConventionLabel: (_t: unknown, value: string) => value,
}));

const ORG_ADMIN = {
  features: { deployment_settings: false, platform_settings: false },
  naming: {
    convention: "apollo_graphql",
    sql_convention: "snake",
    domain_prefix: false,
    use_domains: false,
    default_domain: "default",
  },
  sampling: { default_sample_size: 500 },
  graphql_remote: { max_rows: 2000 },
} as unknown as PlatformSettings;

const PLATFORM_ADMIN = {
  ...ORG_ADMIN,
  features: { deployment_settings: true, platform_settings: true },
  cdc: { consumer_group_id: "receiver" },
  graphql_remote: { max_object_depth: 3, max_list_depth: 2, max_list_items: 100, max_rows: 2000 },
} as unknown as PlatformSettings;

beforeEach(() => {
  vi.mocked(api.fetchSettings).mockReset().mockResolvedValue(ORG_ADMIN);
  vi.mocked(api.updateSettings)
    .mockReset()
    .mockResolvedValue({ success: true, updated: [], restart_required: false });
});

const sent = () => vi.mocked(api.updateSettings).mock.calls[0][0] as Record<string, unknown>;

describe("org-owned settings cards", () => {
  it("shows an org administrator the naming conventions and saves them", async () => {
    render(<NamingConventionsCard />);
    expect(await screen.findByTestId("naming-settings")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(api.updateSettings).toHaveBeenCalledTimes(1));
    expect(sent().naming).toEqual({
      convention: "apollo_graphql",
      sql_convention: "snake",
      domain_prefix: false,
    });
  });

  it("saves the sample size and only the row limit of remote GraphQL", async () => {
    vi.mocked(api.fetchSettings).mockResolvedValue(PLATFORM_ADMIN);
    render(<OrgProcessingCards />);
    const sample = await screen.findByTestId("org-sample-size");
    fireEvent.change(sample, { target: { value: "750" } });
    fireEvent.change(screen.getByTestId("org-remote-max-rows"), { target: { value: "90" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(api.updateSettings).toHaveBeenCalledTimes(1));
    expect(sent()).toEqual({
      sampling: { default_sample_size: 750 },
      graphql_remote: { max_rows: 90 },
    });
  });

  it("leaves the org's fields out of the deployment's federation card", async () => {
    vi.mocked(api.fetchSettings).mockResolvedValue(PLATFORM_ADMIN);
    render(<FederationSettingsCards />);
    const panel = await screen.findByTestId("federation-settings");
    fireEvent.click(panel.querySelector("button") as HTMLButtonElement);
    fireEvent.click(await screen.findByRole("button", { name: /save/i }));
    await waitFor(() => expect(api.updateSettings).toHaveBeenCalledTimes(1));
    expect(sent().sampling).toBeUndefined();
    expect(sent().graphql_remote).toEqual({
      max_object_depth: 3,
      max_list_depth: 2,
      max_list_items: 100,
    });
    expect(sent().cdc).toEqual({ consumer_group_id: "receiver" });
  });
});
