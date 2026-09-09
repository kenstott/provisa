// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2e5c91-3f48-4b0d-9e67-1c8d4a2f6b53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1663: registering a table on a data-quality checker source.
 *
 * A checker has no remote schema — nothing exists upstream until a scan runs — so the form must not
 * ask for one. It asks for the governed table to scan (the contract panel's dataset picker) and
 * derives the results table's name, alias and description from it, leaving the operator with the
 * rules. The payload carries the contract and the one placeholder column the server replaces.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const useAvailableSchemas = vi.fn((_sourceId: string | null) => ({
  schemas: [] as string[],
  loading: false,
}));
const useAvailableTables = vi.fn((_sourceId: string | null, _schema: string | null) => ({
  tables: [] as { name: string }[],
  loading: false,
}));
const parseContract = vi.fn();
const buildContract = vi.fn();
const checkCatalog = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: (sourceId: string | null) => useAvailableSchemas(sourceId),
  useAvailableTables: (sourceId: string | null, schema: string | null) =>
    useAvailableTables(sourceId, schema),
  useDqContract: () => ({
    parseContract,
    buildContract,
    checkCatalog,
    buildCheck: vi.fn(),
    dryRunContract: vi.fn(),
    runCheckNow: vi.fn(),
  }),
  useTables: () => ({
    tables: [{ schemaName: "pet_store", tableName: "pets", dqDataset: "provisa/pet_store/pets" }],
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const GX = "name: x\nmeta:\n  dataset: provisa/pet_store/pets\nexpectations: []\n";

function renderForm() {
  const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
  const setError = vi.fn();
  const suggestTableAlias = vi.fn().mockResolvedValue("suggested");
  render(
    <RegisterTableForm
      sources={[{ id: "dq-checker", type: "great_expectations", allowedDomains: [] } as never]}
      domainHints={["pet-store"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "org_admin" } as never, { id: "analyst" } as never]}
      getAvailableColumnsMetadata={vi.fn().mockResolvedValue([])}
      suggestTableAlias={suggestTableAlias}
      registerTable={registerTable}
      onSuccess={vi.fn()}
      setError={setError}
    />,
  );
  return { registerTable, setError, suggestTableAlias };
}

beforeEach(() => {
  vi.clearAllMocks();
  parseContract.mockImplementation(async ({ contractText }: { contractText: string }) =>
    contractText === GX
      ? { dataset: "provisa/pet_store/pets", checks: [], error: null }
      : { dataset: null, checks: [], error: "names no dataset" },
  );
  buildContract.mockResolvedValue({ text: GX, error: null });
  checkCatalog.mockResolvedValue({ error: null, datasetChecks: [], columns: [] });
});

/** Pick the checker source, then the governed table its contract will scan. */
async function pickScanTarget() {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "dq-checker");
  await user.selectOptions(screen.getByTestId("register-table-domain-select"), "pet-store");
  const select = await screen.findByTestId("dq-dataset-table-select");
  fireEvent.change(select, { target: { value: "pet_store.pets" } });
  fireEvent.click(await screen.findByText("pet_store.pets"));
  await waitFor(() =>
    expect(screen.getByTestId("register-table-dq-results-table")).toHaveValue("pets_scan"),
  );
  return user;
}

describe("RegisterTableForm on a checker source (REQ-1663)", () => {
  it("replaces the schema/table pickers with the table-to-scan picker and never introspects the checker", async () => {
    renderForm();
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "dq-checker");

    expect(await screen.findByTestId("register-table-dq")).toBeInTheDocument();
    expect(screen.queryByTestId("register-table-schema-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("register-table-table-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("discover-columns-checkbox")).not.toBeInTheDocument();
    expect(screen.queryByTestId("register-table-watermark-select")).not.toBeInTheDocument();
    // "Run now" fires a registered poll job; there is none yet.
    expect(screen.queryByTestId("dq-run-now")).not.toBeInTheDocument();
    // The lookups are never made for a checker source.
    for (const call of useAvailableSchemas.mock.calls) expect(call[0]).toBeNull();
    for (const call of useAvailableTables.mock.calls) expect(call[0]).toBeNull();
  });

  it("derives the results table, alias and description from the scanned table", async () => {
    const { suggestTableAlias } = renderForm();
    await pickScanTarget();

    expect(buildContract).toHaveBeenCalledWith({
      checker: "great_expectations",
      dataset: "provisa/pet_store/pets",
      checks: [],
    });
    expect(screen.getByPlaceholderText("Semantic name override")).toHaveValue("pets_quality");
    expect(
      (screen.getByPlaceholderText("Appears in SDL docs") as HTMLInputElement).value,
    ).toContain("pet_store.pets");
    expect(suggestTableAlias).not.toHaveBeenCalled();
  });

  it("refuses to register until a table to scan is picked", async () => {
    const { registerTable, setError } = renderForm();
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "dq-checker");
    await screen.findByTestId("register-table-dq");

    await user.click(screen.getByTestId("register-table-submit"));
    expect(registerTable).not.toHaveBeenCalled();
    expect(setError).toHaveBeenLastCalledWith(expect.stringContaining("required"));
  });

  it("registers the contract with the placeholder column carrying visible_to", async () => {
    const { registerTable } = renderForm();
    const user = await pickScanTarget();

    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalledTimes(1));
    expect(registerTable).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceId: "dq-checker",
        domainId: "pet-store",
        schemaName: "pet_store",
        tableName: "pets_scan",
        alias: "pets_quality",
        dqContract: GX,
        watermarkColumn: null,
        discover: false,
        columns: [
          expect.objectContaining({
            name: "scan_id",
            dataType: "varchar",
            visibleTo: ["org_admin", "analyst"],
          }),
        ],
      }),
    );
  });
});
