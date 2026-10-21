// Copyright (c) 2026 Kenneth Stott
// Canary: 8c31d94f-07a5-4e62-b1d8-2f960ae7c53b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1074, REQ-2118, REQ-2122: the metadata-export admin tab, with every catalog the org
// publishes to. Each target names a catalog of the Catalogs panel and keeps only the export's own
// settings for it.
//
// What is asserted is what a wrong tab gets wrong silently: showing the form to an org the plan
// does not cover, losing the export settings a catalog's provider needs, and swallowing the
// per-asset reasons a partial publish returned — the one thing the tab exists to show.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { MetadataExportTab } from "../components/admin/MetadataExportTab";
import type { MetadataExportState, MetadataExportTarget } from "../api/metadataExport";
import type { CatalogConnection } from "../api/catalogs";

vi.mock("../api/metadataExport", () => ({
  fetchMetadataExport: vi.fn(),
  setMetadataExport: vi.fn(),
  checkMetadataExport: vi.fn(),
  publishMetadataExport: vi.fn(),
}));

vi.mock("../api/catalogs", () => ({ fetchCatalogs: vi.fn() }));

import {
  fetchMetadataExport,
  setMetadataExport,
  checkMetadataExport,
  publishMetadataExport,
} from "../api/metadataExport";
import { fetchCatalogs } from "../api/catalogs";

const mockFetch = vi.mocked(fetchMetadataExport);
const mockSet = vi.mocked(setMetadataExport);
const mockHealth = vi.mocked(checkMetadataExport);
const mockPublish = vi.mocked(publishMetadataExport);
const mockCatalogs = vi.mocked(fetchCatalogs);

const catalog = (id: string, provider: string): CatalogConnection => ({
  id,
  provider,
  endpoint: "",
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
  api_key_set: false,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
});

const CATALOGS = [
  catalog("marquez", "openlineage"),
  catalog("marquez-lab", "openlineage"),
  catalog("datazone", "datazone"),
  catalog("uc", "unity_catalog"),
];

const MARQUEZ: MetadataExportTarget = {
  catalog: "marquez",
  datazone_project_id: "",
  delta_share: false,
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
    mockCatalogs.mockReset();
    mockCatalogs.mockResolvedValue({ providers: [], catalogs: CATALOGS });
  });

  it("renders each target by the catalog it names", async () => {
    mockFetch.mockResolvedValue(state({}, [MARQUEZ, { ...MARQUEZ, catalog: "marquez-lab" }]));
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-catalog-1")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("metadata-export-catalog-0")).toHaveValue("marquez · openlineage");
    expect(screen.getByTestId("metadata-export-catalog-1")).toHaveValue(
      "marquez-lab · openlineage",
    );
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
    expect(screen.queryByTestId("metadata-export-catalog-0")).toBeNull();
  });

  it("adds a target and removes one, and saves the list as it stands", async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-catalog-0")).toBeInTheDocument(),
    );
    fireEvent.click(screen.getByTestId("metadata-export-add-target"));
    fireEvent.click(screen.getByTestId("metadata-export-remove-0"));
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].targets).toEqual([
      { catalog: "", datazone_project_id: "", delta_share: false },
    ]);
  });

  it("asks the export to a DataZone catalog for the project that owns its products", async () => {
    // REQ-2118: a DataZone data product is owned by a project, which the export names.
    mockFetch.mockResolvedValue(state({}, [{ ...MARQUEZ, catalog: "datazone" }]));
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-datazone-project-id-0")).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("metadata-export-delta-share-0")).toBeNull();
    fireEvent.change(screen.getByTestId("metadata-export-datazone-project-id-0"), {
      target: { value: "prj_1" },
    });
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].targets).toEqual([
      { catalog: "datazone", datazone_project_id: "prj_1", delta_share: false },
    ]);
  });

  it("offers the export to a Unity Catalog catalog its Delta Share", async () => {
    // REQ-2117: Unity Catalog may also get each product as a Delta Share.
    mockFetch.mockResolvedValue(state({}, [{ ...MARQUEZ, catalog: "uc" }]));
    mockSet.mockResolvedValue(SAVED);
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-delta-share-0")).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("metadata-export-datazone-project-id-0")).toBeNull();
    fireEvent.click(screen.getByTestId("metadata-export-delta-share-0"));
    fireEvent.click(screen.getByTestId("metadata-export-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].targets?.[0].delta_share).toBe(true);
  });

  it("asks an OpenLineage catalog's export for no further setting", async () => {
    mockFetch.mockResolvedValue(state());
    render(<MetadataExportTab />);

    await waitFor(() =>
      expect(screen.getByTestId("metadata-export-catalog-0")).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("metadata-export-datazone-project-id-0")).toBeNull();
    expect(screen.queryByTestId("metadata-export-delta-share-0")).toBeNull();
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
