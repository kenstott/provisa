// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8a1f5c-9b2d-4e7a-8c0f-6d4b2a9e1c53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1316: Ossie interchange surface — endpoint URL + download, and the import
// review screen: proposals land default-checked, trimming = unchecking, and
// Apply registers ONLY checked items through the existing mutations (imported
// definitions never bypass registration review).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { OssieImportProposals } from "../../api/admin";

const importOssie = vi.fn();
const fetchOssieYaml = vi.fn();
const fetchDbtSourcesYaml = vi.fn();
const auth = vi.hoisted(() => ({ role: { id: "analyst" } as { id: string } | null }));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => auth }));
// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files and drops exports they need.
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  importOssie: (...a: unknown[]) => importOssie(...a),
  fetchOssieYaml: (...a: unknown[]) => fetchOssieYaml(...a),
  fetchDbtSourcesYaml: (...a: unknown[]) => fetchDbtSourcesYaml(...a),
}));

const registerTable = vi.fn();
const upsertRelationship = vi.fn();
const upsertMetric = vi.fn();
// Spread the real module (same registry-sharing reason as above).
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRegisterTable: () => ({ registerTable, loading: false }),
  useUpsertRelationship: () => ({ upsertRelationship, loading: false }),
  useUpsertMetric: () => ({ upsertMetric, loading: false }),
}));

import { OssieInterchangePanel } from "../metrics/OssieInterchangePanel";

// Proposal shapes mirror provisa/ossie/convert.py OssieImport (asdict).
const PROPOSALS: OssieImportProposals = {
  model_name: "sales",
  tables: [
    {
      name: "orders",
      table_name: "orders",
      schema_name: "public",
      source_id: "pg1",
      description: "Order lines",
      columns: [
        {
          name: "id",
          alias: "order_key",
          datatype: "bigint",
          description: null,
          is_primary_key: true,
        },
        {
          name: "amount",
          alias: null,
          datatype: "decimal",
          description: "Line amount",
          is_primary_key: false,
        },
      ],
      not_importable: [{ name: "amount_with_tax", expression: "amount * 1.2" }],
      primary_key: ["id"],
      unique_keys: [["id"]],
      modeling_role: "fact",
      modeling_history: null,
    },
  ],
  relationships: [
    {
      name: "orders_customers",
      from: "orders",
      to: "customers",
      from_columns: ["customer_id"],
      to_columns: ["id"],
    },
  ],
  metrics: [
    {
      name: "revenue",
      expression: "SUM(orders.amount)",
      datatype: "numeric",
      description: null,
      ai_context: "Total booked revenue",
    },
  ],
};

async function openReview() {
  render(<OssieInterchangePanel />);
  const file = new File(["version: 0.2.0.dev0\n"], "provisa.ossie.yaml", { type: "text/yaml" });
  fireEvent.change(screen.getByTestId("ossie-file-input"), { target: { files: [file] } });
  // The modal root mounts before its transition-mounted content — wait for the content.
  await waitFor(() => expect(screen.getByTestId("ossie-apply")).toBeInTheDocument());
}

beforeEach(() => {
  vi.clearAllMocks();
  importOssie.mockResolvedValue(PROPOSALS);
  registerTable.mockResolvedValue({ success: true, message: "registered (id=7)" });
  upsertRelationship.mockResolvedValue({ success: true, message: "ok" });
  upsertMetric.mockResolvedValue({ success: true, message: "ok" });
});

describe("Ossie interchange panel (REQ-1316)", () => {
  it("shows the copyable endpoint URL and downloads the live document", async () => {
    fetchOssieYaml.mockResolvedValue("version: 0.2.0.dev0\n");
    const createObjectURL = vi.fn().mockReturnValue("blob:x");
    const revokeObjectURL = vi.fn();
    vi.stubGlobal("URL", Object.assign(URL, { createObjectURL, revokeObjectURL }));

    const user = userEvent.setup();
    render(<OssieInterchangePanel />);
    expect(screen.getByTestId("ossie-endpoint-url")).toHaveTextContent("/admin/ossie");

    await user.click(screen.getByTestId("ossie-download"));
    await waitFor(() => expect(fetchOssieYaml).toHaveBeenCalled());
    await waitFor(() => expect(createObjectURL).toHaveBeenCalled());
    vi.unstubAllGlobals();
  });

  it("downloads a dbt sources file for the role in use (REQ-1967)", async () => {
    auth.role = { id: "analyst" };
    fetchDbtSourcesYaml.mockResolvedValue("version: 2\nsources: []\n");
    const createObjectURL = vi.fn().mockReturnValue("blob:x");
    vi.stubGlobal("URL", Object.assign(URL, { createObjectURL, revokeObjectURL: vi.fn() }));

    const user = userEvent.setup();
    render(<OssieInterchangePanel />);
    await user.click(screen.getByTestId("dbt-sources-download"));
    await waitFor(() => expect(fetchDbtSourcesYaml).toHaveBeenCalledWith("analyst"));
    await waitFor(() => expect(createObjectURL).toHaveBeenCalled());
    vi.unstubAllGlobals();
  });

  it("says in one line what a download does not carry", () => {
    render(<OssieInterchangePanel />);
    const line = screen.getByTestId("export-governance-line");
    expect(line).toHaveTextContent("Row security, masking, roles and lineage stay in Provisa");
    expect(line).toHaveTextContent("only when it reads through Provisa's own SQL endpoint");
  });

  it("offers no sources file without a role to write it for, and shows a refusal", async () => {
    auth.role = null;
    render(<OssieInterchangePanel />);
    expect(screen.getByTestId("dbt-sources-download")).toBeDisabled();

    auth.role = { id: "analyst" };
    fetchDbtSourcesYaml.mockRejectedValue(new Error("No role named analyst exists."));
    const user = userEvent.setup();
    render(<OssieInterchangePanel />);
    await user.click(screen.getAllByTestId("dbt-sources-download")[1]);
    expect(await screen.findByTestId("ossie-error")).toHaveTextContent("No role named analyst");
  });

  it("names a table's computed fields as not imported, and registers a renamed column by its alias", async () => {
    await openReview();
    const named = screen.getByTestId("ossie-not-importable-orders");
    expect(named).toHaveTextContent("amount_with_tax");
    expect(named).toHaveTextContent("computed fields");

    const user = userEvent.setup();
    await user.click(screen.getByTestId("ossie-apply"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    const columns = registerTable.mock.calls[0][0].columns as { name: string; alias?: string }[];
    expect(columns.map((c) => [c.name, c.alias])).toEqual([
      ["id", "order_key"],
      ["amount", undefined],
    ]);
  });

  it("upload lands as a review screen with everything checked and nothing registered", async () => {
    await openReview();
    expect(importOssie).toHaveBeenCalledWith("version: 0.2.0.dev0\n");

    expect(screen.getByTestId("ossie-check-table:orders")).toBeChecked();
    expect(screen.getByTestId("ossie-check-rel:orders_customers")).toBeChecked();
    expect(screen.getByTestId("ossie-check-metric:revenue")).toBeChecked();

    // Review only — nothing registers until Apply.
    expect(registerTable).not.toHaveBeenCalled();
    expect(upsertRelationship).not.toHaveBeenCalled();
    expect(upsertMetric).not.toHaveBeenCalled();
  });

  it("Apply registers checked items through the existing mutations; trimmed items are skipped", async () => {
    const user = userEvent.setup();
    await openReview();

    // Trim the relationship (uncheck), keep table + metric.
    await user.click(screen.getByTestId("ossie-check-rel:orders_customers"));
    await user.click(screen.getByTestId("ossie-apply"));

    await waitFor(() => expect(registerTable).toHaveBeenCalledTimes(1));
    expect(registerTable).toHaveBeenCalledWith({
      sourceId: "pg1",
      domainId: "sales",
      schemaName: "public",
      tableName: "orders",
      alias: undefined,
      description: "Order lines",
      columns: [
        {
          name: "id",
          alias: "order_key",
          visibleTo: ["*"],
          dataType: "bigint",
          description: undefined,
          isPrimaryKey: true,
        },
        {
          name: "amount",
          alias: undefined,
          visibleTo: ["*"],
          dataType: "decimal",
          description: "Line amount",
          isPrimaryKey: false,
        },
      ],
      uniqueConstraints: [{ name: "orders_uq_1", columns: ["id"] }],
      modelingRole: "fact",
      modelingHistory: undefined,
    });
    expect(upsertMetric).toHaveBeenCalledWith({
      name: "revenue",
      expression: "SUM(orders.amount)",
      datatype: "numeric",
      description: null,
      aiContext: "Total booked revenue",
    });
    expect(upsertRelationship).not.toHaveBeenCalled();

    // Per-item MutationResult surfaces on the review rows.
    expect(screen.getByTestId("ossie-result-table:orders")).toHaveTextContent("Applied");
    expect(screen.getByTestId("ossie-result-metric:revenue")).toHaveTextContent("Applied");
    expect(screen.queryByTestId("ossie-result-rel:orders_customers")).not.toBeInTheDocument();
  });

  it("shows the per-item failure message when a mutation rejects the item", async () => {
    registerTable.mockResolvedValue({ success: false, message: "already registered" });
    const user = userEvent.setup();
    await openReview();

    await user.click(screen.getByTestId("ossie-apply"));
    await waitFor(() =>
      expect(screen.getByTestId("ossie-result-table:orders")).toHaveTextContent(
        "already registered",
      ),
    );
  });

  it("applies the relationship through upsertRelationship with the proposal columns", async () => {
    const user = userEvent.setup();
    await openReview();
    await user.click(screen.getByTestId("ossie-apply"));
    await waitFor(() => expect(upsertRelationship).toHaveBeenCalledTimes(1));
    expect(upsertRelationship).toHaveBeenCalledWith({
      id: "orders_customers",
      sourceTableId: "orders",
      targetTableId: "customers",
      sourceColumn: "customer_id",
      targetColumn: "id",
      cardinality: "many-to-one",
    });
  });
});
