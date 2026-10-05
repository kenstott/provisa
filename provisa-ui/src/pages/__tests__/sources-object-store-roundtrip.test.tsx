// Copyright (c) 2026 Kenneth Stott
// Canary: 0b597abc-a7c8-4d43-9d4a-d23360e14321
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-990: an object-store credential the source form shows is saved, and comes back when the
// source is edited. The S3 region of a delta_lake/iceberg source was shown and then dropped on
// save; a CSV on GCS saves and reopens its HMAC keys.

import { describe, it, expect, vi } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { Source } from "../../types/admin";

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchFederationEngine: vi.fn().mockResolvedValue(null),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

function source(overrides: Partial<Source>): Source {
  return {
    id: "lake",
    type: "iceberg",
    host: "",
    port: 0,
    database: "",
    username: "",
    path: "s3://bucket/warehouse/orders",
    cacheEnabled: true,
    cacheTtl: 300,
    ...overrides,
  } as unknown as Source;
}

let SOURCES: Source[] = [];
const ok = { success: true, message: "" };
const updateSource = vi.fn().mockResolvedValue(ok);

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: vi.fn().mockResolvedValue({}) }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useUpdateSource: () => ({ updateSource }),
  useRenameSource: () => ({ renameSource: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceCache: () => ({ updateSourceCache: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceReplicate: () => ({ updateSourceReplicate: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceLoadProtection: () => ({
    updateSourceLoadProtection: vi.fn().mockResolvedValue(ok),
  }),
  useUpdateSourceNaming: () => ({ updateSourceNaming: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceAllowedDomains: () => ({
    updateSourceAllowedDomains: vi.fn().mockResolvedValue(ok),
  }),
}));

import { SourcesPage } from "../SourcesPage";

async function openForEdit(id: string): Promise<void> {
  render(<SourcesPage />);
  await userEvent.click(await screen.findByText(id));
  await userEvent.click(await screen.findByTestId("source-detail-edit"));
}

async function saved(): Promise<Record<string, string>> {
  await userEvent.click(screen.getByRole("button", { name: /save/i }));
  await waitFor(() => expect(updateSource).toHaveBeenCalled());
  const payload = updateSource.mock.calls.at(-1)![0] as { federationHintsJson?: string };
  return JSON.parse(payload.federationHintsJson ?? "{}");
}

describe("object-store credentials round-trip through the source form", () => {
  it("keeps an iceberg source's S3 region when it is edited and saved", async () => {
    SOURCES = [
      source({
        federationHintsJson: JSON.stringify({ access_key_id: "AKIA1", region: "eu-west-1" }),
      } as Partial<Source>),
    ];
    updateSource.mockClear();
    await openForEdit("lake");
    expect(screen.getByDisplayValue("eu-west-1")).toBeInTheDocument();
    // The lake form's secret is required and never read back: it is entered again, as by a user.
    await userEvent.type(screen.getByLabelText(/Secret Access Key/), "${{secret:aws_secret}");
    const hints = await saved();
    expect(hints).toEqual({
      access_key_id: "AKIA1",
      secret_access_key: "${secret:aws_secret}",
      region: "eu-west-1",
    });
  });

  it("reopens and saves an iceberg source's catalog with its S3 credentials", async () => {
    const catalog = {
      iceberg_catalog_type: "REST",
      iceberg_table_id: "sales.orders",
      iceberg_catalog_uri: "http://catalog:8181",
    };
    SOURCES = [
      source({
        federationHintsJson: JSON.stringify({
          access_key_id: "AKIA1",
          region: "eu-west-1",
          ...catalog,
        }),
      } as Partial<Source>),
    ];
    updateSource.mockClear();
    await openForEdit("lake");
    expect(screen.getByTestId("iceberg-iceberg_catalog_uri")).toHaveValue("http://catalog:8181");
    await userEvent.type(screen.getByLabelText(/Secret Access Key/), "${{secret:aws_secret}");
    const hints = await saved();
    expect(hints).toEqual({
      access_key_id: "AKIA1",
      secret_access_key: "${secret:aws_secret}",
      region: "eu-west-1",
      ...catalog,
    });
  });

  it("reopens and saves a CSV on GCS with its HMAC keys", async () => {
    SOURCES = [
      source({
        id: "gcs-csv",
        type: "csv",
        path: "gs://bucket/customers.csv",
        federationHintsJson: JSON.stringify({ gcs_access_id: "GOOG1", gcs_secret_key: "${secret:h}" }),
      } as Partial<Source>),
    ];
    updateSource.mockClear();
    await openForEdit("gcs-csv");
    expect(screen.getByTestId("object-store-gcs_access_id")).toHaveValue("GOOG1");
    const hints = await saved();
    expect(hints).toEqual({ gcs_access_id: "GOOG1", gcs_secret_key: "${secret:h}" });
  });
});
