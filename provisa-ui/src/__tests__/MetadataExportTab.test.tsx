// Copyright (c) 2026 Kenneth Stott
// Canary: 8c31d94f-07a5-4e62-b1d8-2f960ae7c53b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1074, REQ-2118: the metadata-export admin tab, with every catalog the org publishes to.
//
// What is asserted is what a wrong tab gets wrong silently: showing the form to an org the plan
// does not cover, sending back a credential the user never typed (which would overwrite the
// stored one with an empty string), and swallowing the per-asset reasons a partial publish
// returned — the one thing the tab exists to show.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { MetadataExportTab } from "../components/admin/MetadataExportTab";
import type { MetadataExportState, MetadataExportTarget } from "../api/metadataExport";

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

const MARQUEZ: MetadataExportTarget = {
  id: "marquez",
  provider: "openlineage",
  endpoint: "http://marquez:5000",
  auth_mode: "api_key",
  username: "",
  entra_tenant_id: "",
  entra_client_id: "",
  gcp_project: "",
  gcp_location: "",
  timeout_seconds: 30,
  api_key_set: true,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
};

function state(
  overrides: Partial<MetadataExportState> = {},
  targets: MetadataExportTarget[] = [MARQUEZ],
): MetadataExportState {
  return {
    entitled: true,
    required_tier: "premium",
    providers: ["atlas", "openlineage", "openmetadata"],
    config: { enabled: true, reconcile_cron: "0 * * * *", targets },
    last_publish: null,
    ...overrides,
  };
}

const SAVED = { success: true, enabled: true, targets: ["marquez"] };

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

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-endpoint-0")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("metadata-export-id-0")).toHaveValue("marquez");
    expect(screen.getByTestId("metadata-export-endpoint-0")).toHaveValue("http://marquez:5000");
    expect(screen.getByTestId("metadata-export-reconcile-cron")).toHaveValue("0 * * * *");
  });

  it("renders two targets of one provider, each with its own settings", async () => {
    // REQ-2118: an org may publish to two catalogs of one type.
    mockFetch.mockResolvedValue(
      state({}, [MARQUEZ, { ...MARQUEZ, id: "marquez-lab", endpoint: "http://lab:5000" }]),
    );
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-id-1")).toBeInTheDocument());
    expect(screen.getByTestId("metadata-export-endpoint-0")).toHaveValue("http://marquez:5000");
    expect(screen.getByTestId("metadata-export-endpoint-1")).toHaveValue("http://lab:5000");
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
    expect(screen.queryByTestId("metadata-export-endpoint-0")).toBeNull();
  });

  it("never renders a stored credential and does not send one the user did not type", async () => {
    // The stored key is reported only as set. Sending the blank field back would clear it, and
    // the failure would surface at the next publish rather than at this save.
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-api-key-0")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("metadata-export-api-key-0")).toHaveValue("");

    fireEvent.change(screen.getByTestId("metadata-export-endpoint-0"), {
      target: { value: "http://marquez:5001" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0].targets!;
    expect(sent.endpoint).toBe("http://marquez:5001");
    expect("api_key" in sent).toBe(false);
  });

  it("sends a credential the user did type, for that target only", async () => {
    mockFetch.mockResolvedValue(state({}, [MARQUEZ, { ...MARQUEZ, id: "marquez-lab" }]));
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-api-key-1")).toBeInTheDocument(),
    );
    fireEvent.change(screen.getByTestId("metadata-export-api-key-1"), {
      target: { value: "new-key" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [first, second] = mockSet.mock.calls[0][0].targets!;
    expect("api_key" in first).toBe(false);
    expect(second.api_key).toBe("new-key");
  });

  it("adds a target and removes one, and saves the list as it stands", async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-id-0")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("metadata-export-add-target"));
    fireEvent.change(screen.getByTestId("metadata-export-id-1"), {
      target: { value: "om" },
    });
    fireEvent.change(screen.getByTestId("metadata-export-endpoint-1"), {
      target: { value: "http://om:8585" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-remove-0"));
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const targets = mockSet.mock.calls[0][0].targets!;
    expect(targets.map((t) => [t.id, t.endpoint])).toEqual([["om", "http://om:8585"]]);
  });

  it("asks for a username and a password when the target authenticates with HTTP basic", async () => {
    // Stock Apache Atlas answers a bearer token with 401, so its deployments run `basic` — the
    // one mode that needs an account name beside the secret.
    mockFetch.mockResolvedValue(
      state({}, [{ ...MARQUEZ, id: "atlas", provider: "atlas", auth_mode: "basic" }]),
    );
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-username-0")).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("metadata-export-api-key-0")).toBeNull();
    fireEvent.change(screen.getByTestId("metadata-export-username-0"), {
      target: { value: "admin" },
    });
    fireEvent.change(screen.getByTestId("metadata-export-token-0"), {
      target: { value: "secret" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0].targets!;
    expect(sent.username).toBe("admin");
    expect(sent.token).toBe("secret");
  });

  it("asks a Dataplex target for its project, location and service account key", async () => {
    // REQ-2118: a Dataplex data product lives in a project and a location.
    mockFetch.mockResolvedValue(
      state({}, [
        {
          ...MARQUEZ,
          id: "dataplex",
          provider: "dataplex",
          endpoint: "https://dataplex.googleapis.com",
          auth_mode: "google_service_account",
        },
      ]),
    );
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-gcp-project-0")).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("metadata-export-api-key-0")).toBeNull();
    fireEvent.change(screen.getByTestId("metadata-export-gcp-project-0"), {
      target: { value: "acme-data" },
    });
    fireEvent.change(screen.getByTestId("metadata-export-gcp-location-0"), {
      target: { value: "us" },
    });
    fireEvent.change(screen.getByTestId("metadata-export-google-service-account-json-0"), {
      target: { value: "{}" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0].targets!;
    expect([sent.gcp_project, sent.gcp_location]).toEqual(["acme-data", "us"]);
    expect(sent.google_service_account_json).toBe("{}");
  });

  it("reports the reason each target refused the connection", async () => {
    mockFetch.mockResolvedValue(state());
    mockHealth.mockResolvedValue({
      ok: false,
      targets: [
        {
          id: "marquez",
          provider: "openlineage",
          ok: false,
          error: "ConnectError: Name or service not known",
        },
      ],
    });
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-health")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("metadata-export-health"));

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-health-marquez")).toHaveTextContent(
        "Name or service not known",
      ),
    );
  });

  it("lists the assets a partial publish had rejected, under the target that rejected them", async () => {
    mockFetch.mockResolvedValue(state());
    mockPublish.mockResolvedValue({
      ok: false,
      targets: [
        {
          id: "marquez",
          provider: "openlineage",
          ok: false,
          published: { dataset: 2 },
          total_published: 2,
          errors: [{ asset: "wh.public.orders", message: "422 unknown field type" }],
        },
      ],
    });
    render(<MetadataExportTab />);

    await waitFor(() => expect(screen.getByTestId("metadata-export-publish")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("metadata-export-publish"));

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-publish-marquez")).toBeInTheDocument(),
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
          ok: true,
          targets: [
            {
              id: "marquez",
              provider: "openlineage",
              ok: true,
              published: { dataset: 3, lineage: 1 },
              total_published: 4,
              errors: [],
            },
          ],
        },
      }),
    );
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-last-publish")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("metadata-export-publish-marquez")).toHaveTextContent("4");
  });
});
