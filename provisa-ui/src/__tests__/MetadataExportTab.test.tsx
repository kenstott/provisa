// Copyright (c) 2026 Kenneth Stott
// Canary: 8c31d94f-07a5-4e62-b1d8-2f960ae7c53b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1074: the metadata-export admin tab.
//
// What is asserted is what a wrong tab gets wrong silently: showing the form to an org the plan
// does not cover, sending back a credential the user never typed (which would overwrite the
// stored one with an empty string), and swallowing the per-asset reasons a partial publish
// returned — the one thing the tab exists to show.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { MetadataExportTab } from "../components/admin/MetadataExportTab";
import type { MetadataExportState } from "../api/metadataExport";

vi.mock("../api/metadataExport", () => ({
  fetchMetadataExport: vi.fn(),
  setMetadataExport: vi.fn(),
  checkMetadataExport: vi.fn(),
  publishMetadataExport: vi.fn(),
}));

import {
  fetchMetadataExport,
  setMetadataExport,
  checkMetadataExport,
  publishMetadataExport,
} from "../api/metadataExport";

const mockFetch = vi.mocked(fetchMetadataExport);
const mockSet = vi.mocked(setMetadataExport);
const mockHealth = vi.mocked(checkMetadataExport);
const mockPublish = vi.mocked(publishMetadataExport);

function state(overrides: Partial<MetadataExportState> = {}): MetadataExportState {
  return {
    entitled: true,
    required_tier: "premium",
    providers: ["openlineage", "openmetadata"],
    config: {
      enabled: true,
      provider: "openlineage",
      endpoint: "http://marquez:5000",
      auth_mode: "api_key",
      username: "",
      entra_tenant_id: "",
      entra_client_id: "",
      reconcile_cron: "0 * * * *",
      timeout_seconds: 30,
      api_key_set: true,
      token_set: false,
      entra_client_secret_set: false,
    },
    last_publish: null,
    ...overrides,
  };
}

describe("MetadataExportTab", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
    mockHealth.mockReset();
    mockPublish.mockReset();
  });

  it("renders the configured target", async () => {
    mockFetch.mockResolvedValue(state());
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-endpoint")).toBeInTheDocument());
    expect(screen.getByTestId("metadata-export-endpoint")).toHaveValue("http://marquez:5000");
    expect(screen.getByTestId("metadata-export-reconcile-cron")).toHaveValue("0 * * * *");
  });

  it("shows the plan gate instead of the form for an unentitled org", async () => {
    // REQ-1073: an org below the tier gets told which plan opens the feature, not an empty form
    // whose save would be refused by the server anyway.
    mockFetch.mockResolvedValue(state({ entitled: false, required_tier: "premium" }));
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-not-entitled")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("metadata-export-not-entitled")).toHaveTextContent("premium");
    expect(screen.queryByTestId("metadata-export-endpoint")).toBeNull();
  });

  it("never renders a stored credential and does not send one the user did not type", async () => {
    // The stored key is reported only as set. Sending the blank field back would clear it, and
    // the failure would surface at the next publish rather than at this save.
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue({ success: true, provider: "openlineage", enabled: true });
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-api-key")).toBeInTheDocument());
    expect(screen.getByTestId("metadata-export-api-key")).toHaveValue("");

    fireEvent.change(screen.getByTestId("metadata-export-endpoint"), {
      target: { value: "http://marquez:5001" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const body = mockSet.mock.calls[0][0];
    expect(body.endpoint).toBe("http://marquez:5001");
    expect("api_key" in body).toBe(false);
  });

  it("sends a credential the user did type", async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue({ success: true, provider: "openlineage", enabled: true });
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-api-key")).toBeInTheDocument());
    fireEvent.change(screen.getByTestId("metadata-export-api-key"), {
      target: { value: "new-key" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].api_key).toBe("new-key");
  });

  it("asks for a username and a password when the target authenticates with HTTP basic", async () => {
    // Stock Apache Atlas answers a bearer token with 401, so its deployments run `basic` — the
    // one mode that needs an account name beside the secret.
    mockFetch.mockResolvedValue(
      state({
        providers: ["atlas", "openlineage"],
        config: { ...state().config, provider: "atlas", auth_mode: "basic" },
      }),
    );
    mockSet.mockResolvedValue({ success: true, provider: "atlas", enabled: true });
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-username")).toBeInTheDocument());
    expect(screen.queryByTestId("metadata-export-api-key")).toBeNull();
    fireEvent.change(screen.getByTestId("metadata-export-username"), {
      target: { value: "admin" },
    });
    fireEvent.change(screen.getByTestId("metadata-export-token"), {
      target: { value: "secret" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].username).toBe("admin");
    expect(mockSet.mock.calls[0][0].token).toBe("secret");
  });

  it("reports the reason the target refused the connection", async () => {
    mockFetch.mockResolvedValue(state());
    mockHealth.mockResolvedValue({
      ok: false,
      provider: "openlineage",
      error: "ConnectError: Name or service not known",
    });
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-health")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("metadata-export-health"));

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-health-result")).toHaveTextContent(
        "Name or service not known",
      ),
    );
  });

  it("lists the assets a partial publish had rejected", async () => {
    mockFetch.mockResolvedValue(state());
    mockPublish.mockResolvedValue({
      provider: "openlineage",
      ok: false,
      published: { dataset: 2 },
      total_published: 2,
      errors: [{ asset: "wh.public.orders", message: "422 unknown field type" }],
    });
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-publish")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("metadata-export-publish"));

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-publish-errors")).toBeInTheDocument(),
    );
    const errors = screen.getByTestId("metadata-export-publish-errors");
    expect(errors).toHaveTextContent("wh.public.orders");
    expect(errors).toHaveTextContent("422 unknown field type");
  });

  it("shows the outcome of a publish made before this page load", async () => {
    // The server remembers the last publish, so an admin who navigates back sees what happened
    // without republishing to find out.
    mockFetch.mockResolvedValue(
      state({
        last_publish: {
          provider: "openlineage",
          ok: true,
          published: { dataset: 3, lineage: 1 },
          total_published: 4,
          errors: [],
        },
      }),
    );
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-last-publish")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("metadata-export-last-publish")).toHaveTextContent("4");
  });
});
