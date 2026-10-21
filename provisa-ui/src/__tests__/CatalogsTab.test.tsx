// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6d8a24-b7c3-4e95-8a02-5c39e71d4b68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: the Catalogs panel, where each catalog an org works with is connected once.
//
// What is asserted is what a wrong panel gets wrong silently: sending back a credential the user
// never typed (which would overwrite the stored one with an empty string), losing the domains a
// catalog serves, and hiding the reason a connection test failed.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { CatalogsTab } from "../components/admin/CatalogsTab";
import type { CatalogConnection, CatalogsState } from "../api/catalogs";

vi.mock("../api/catalogs", () => ({
  fetchCatalogs: vi.fn(),
  setCatalogs: vi.fn(),
  checkCatalog: vi.fn(),
}));

vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useDomains: () => ({
    domains: [{ id: "sales" }, { id: "finance" }],
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));

import { checkCatalog, fetchCatalogs, setCatalogs } from "../api/catalogs";

const mockFetch = vi.mocked(fetchCatalogs);
const mockSet = vi.mocked(setCatalogs);
const mockCheck = vi.mocked(checkCatalog);

const MARQUEZ: CatalogConnection = {
  id: "marquez",
  provider: "openlineage",
  endpoint: "http://marquez:5000",
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
  api_key_set: true,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
};

function state(catalogs: CatalogConnection[] = [MARQUEZ]): CatalogsState {
  return { providers: ["atlas", "datazone", "openlineage", "unity_catalog"], catalogs };
}

const SAVED = { success: true, catalogs: ["marquez"] };

describe("CatalogsTab", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
    mockCheck.mockReset();
  });

  it("renders each catalog with its own settings, two of one provider among them", async () => {
    mockFetch.mockResolvedValue(
      state([MARQUEZ, { ...MARQUEZ, id: "marquez-lab", endpoint: "http://lab:5000" }]),
    );
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-id-1")).toBeInTheDocument());
    expect(screen.getByTestId("catalog-id-0")).toHaveValue("marquez");
    expect(screen.getByTestId("catalog-endpoint-0")).toHaveValue("http://marquez:5000");
    expect(screen.getByTestId("catalog-endpoint-1")).toHaveValue("http://lab:5000");
  });

  it("never renders a stored credential and does not send one the user did not type", async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue(SAVED);
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-api-key-0")).toBeInTheDocument());
    expect(screen.getByTestId("catalog-api-key-0")).toHaveValue("");
    fireEvent.change(screen.getByTestId("catalog-endpoint-0"), {
      target: { value: "http://marquez:5001" },
    });
    fireEvent.click(screen.getByTestId("catalogs-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0];
    expect(sent.endpoint).toBe("http://marquez:5001");
    expect(sent.domains).toEqual(["sales"]);
    expect("api_key" in sent).toBe(false);
  });

  it("sends a credential the user did type, for that catalog only", async () => {
    mockFetch.mockResolvedValue(state([MARQUEZ, { ...MARQUEZ, id: "marquez-lab" }]));
    mockSet.mockResolvedValue(SAVED);
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-api-key-1")).toBeInTheDocument());
    fireEvent.change(screen.getByTestId("catalog-api-key-1"), { target: { value: "new-key" } });
    fireEvent.click(screen.getByTestId("catalogs-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [first, second] = mockSet.mock.calls[0][0];
    expect("api_key" in first).toBe(false);
    expect(second.api_key).toBe("new-key");
  });

  it("adds a catalog and removes one, and saves the list as it stands", async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue(SAVED);
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-id-0")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("catalogs-add"));
    fireEvent.change(screen.getByTestId("catalog-id-1"), { target: { value: "om" } });
    fireEvent.change(screen.getByTestId("catalog-endpoint-1"), {
      target: { value: "http://om:8585" },
    });
    fireEvent.click(screen.getByTestId("catalog-remove-0"));
    fireEvent.click(screen.getByTestId("catalogs-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].map((c) => [c.id, c.endpoint])).toEqual([
      ["om", "http://om:8585"],
    ]);
  });

  it("asks for a username and a password when the catalog authenticates with HTTP basic", async () => {
    // Stock Apache Atlas answers a bearer token with 401, so its deployments run `basic`.
    mockFetch.mockResolvedValue(
      state([{ ...MARQUEZ, id: "atlas", provider: "atlas", auth_mode: "basic" }]),
    );
    mockSet.mockResolvedValue(SAVED);
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-username-0")).toBeInTheDocument());
    expect(screen.queryByTestId("catalog-api-key-0")).toBeNull();
    fireEvent.change(screen.getByTestId("catalog-username-0"), { target: { value: "admin" } });
    fireEvent.change(screen.getByTestId("catalog-token-0"), { target: { value: "secret" } });
    fireEvent.click(screen.getByTestId("catalogs-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0];
    expect([sent.username, sent.token]).toEqual(["admin", "secret"]);
  });

  it("asks a Dataplex catalog for its project, location and service account key", async () => {
    mockFetch.mockResolvedValue(
      state([
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
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-gcp-project-0")).toBeInTheDocument());
    expect(screen.queryByTestId("catalog-api-key-0")).toBeNull();
    fireEvent.change(screen.getByTestId("catalog-gcp-project-0"), {
      target: { value: "acme-data" },
    });
    fireEvent.change(screen.getByTestId("catalog-gcp-location-0"), { target: { value: "us" } });
    fireEvent.change(screen.getByTestId("catalog-google-service-account-json-0"), {
      target: { value: "{}" },
    });
    fireEvent.click(screen.getByTestId("catalogs-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0];
    expect([sent.gcp_project, sent.gcp_location]).toEqual(["acme-data", "us"]);
    expect(sent.google_service_account_json).toBe("{}");
  });

  it("asks a DataZone catalog for its region, domain and keys, not an endpoint or a project", async () => {
    // REQ-2122: the owning project is the export's setting, not the catalog's.
    mockFetch.mockResolvedValue(
      state([{ ...MARQUEZ, id: "datazone", provider: "datazone", endpoint: "" }]),
    );
    mockSet.mockResolvedValue(SAVED);
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-aws-region-0")).toBeInTheDocument());
    expect(screen.queryByTestId("catalog-endpoint-0")).toBeNull();
    expect(screen.queryByTestId("catalog-auth-mode-0")).toBeNull();
    expect(screen.queryByTestId("catalog-datazone-project-id-0")).toBeNull();
    for (const [field, value] of [
      ["aws-region", "us-east-1"],
      ["datazone-domain-id", "dzd_1"],
      ["aws-access-key-id", "AKIA-TYPED"],
      ["aws-secret-access-key", "typed-secret"],
    ]) {
      fireEvent.change(screen.getByTestId(`catalog-${field}-0`), { target: { value } });
    }
    fireEvent.click(screen.getByTestId("catalogs-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0];
    expect([sent.aws_region, sent.datazone_domain_id]).toEqual(["us-east-1", "dzd_1"]);
    expect([sent.aws_access_key_id, sent.aws_secret_access_key]).toEqual([
      "AKIA-TYPED",
      "typed-secret",
    ]);
  });

  it("asks a Unity Catalog catalog for no endpoint: it works through the engine", async () => {
    mockFetch.mockResolvedValue(
      state([{ ...MARQUEZ, id: "uc", provider: "unity_catalog", endpoint: "" }]),
    );
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-id-0")).toBeInTheDocument());
    expect(screen.queryByTestId("catalog-endpoint-0")).toBeNull();
    expect(screen.queryByTestId("catalog-api-key-0")).toBeNull();
  });

  it("reports the reason a catalog refused the connection test", async () => {
    mockFetch.mockResolvedValue(state());
    mockCheck.mockResolvedValue({
      id: "marquez",
      provider: "openlineage",
      ok: false,
      error: "ConnectError: Name or service not known",
    });
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalog-test-0")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("catalog-test-0"));

    await waitFor(() =>
      expect(screen.getByTestId("catalog-health-0")).toHaveTextContent("Name or service not known"),
    );
    expect(mockCheck).toHaveBeenCalledWith("marquez");
  });

  it("offers the test only for a catalog that is saved", async () => {
    mockFetch.mockResolvedValue(state([]));
    render(<CatalogsTab />);

    await waitFor(() => expect(screen.getByTestId("catalogs-none")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("catalogs-add"));
    expect(screen.getByTestId("catalog-test-0")).toBeDisabled();
  });
});
