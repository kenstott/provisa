// Copyright (c) 2026 Kenneth Stott
// Canary: 32812277-2fc7-47cc-91e4-03b13c086f3b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-788: combining files into one table is its own action of Register Table. On a files source,
// two or more files picked in the Tables field, or a glob typed beside it, offer "Combine into one
// table"; chosen, the form shows the files matched, the columns they share and the files whose
// columns differ, asks for the new table's name, and registers ONE table over a glob derived from
// the picked files (or as typed).

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";
import type { ColumnMetadata } from "../../../api/admin";
import type { FileGlobPreview } from "../useFileGlobPreview";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

// The source's one schema; a test may hold it back, as a files source does while its server starts.
const listedSchemas = { schemas: ["default"] };
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: (source: string | null) => ({
    schemas: source ? listedSchemas.schemas : [],
    loading: false,
  }),
  useAvailableTables: (_source: string | null, schema: string | null) => ({
    tables: schema
      ? ["complaints_2026_01", "complaints_2026_02", "complaints_2026_03", "other"].map((name) => ({
          name,
          comment: null,
        }))
      : [],
    loading: false,
  }),
}));

const col = (name: string, dataType = "varchar"): ColumnMetadata => ({
  name,
  dataType,
  comment: null,
  nativeFilterType: null,
  isPrimaryKey: false,
});
const readColumns = vi.fn();
vi.mock("../../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));

const registerTables = vi.fn();
vi.mock("../../../hooks/useTableQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useTableQueries")>()),
  useRegisterTables: () => ({ registerTables, loading: false }),
}));

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  fetchTableUniqueConstraints: vi.fn().mockResolvedValue([]),
}));

// What the server answers of a glob: typed, or derived from the tables picked.
const previewOf = vi.fn();
vi.mock("../useFileGlobPreview", () => ({
  useFileGlobPreview: (sourceId: string | null, glob: string | null, tables: string[]) => ({
    preview: sourceId && (glob || tables.length > 0) ? previewOf(glob, tables) : null,
    checking: false,
    error: null,
  }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const PICKED: FileGlobPreview = {
  glob: "complaints_2026_0[12].csv",
  files: [
    { file: "complaints_2026_01.csv", table: "complaints_2026_01" },
    { file: "complaints_2026_02.csv", table: "complaints_2026_02" },
  ],
  sharedColumns: ["id", "amount"],
  differing: [{ file: "complaints_2026_02.csv", missing: [], extra: ["region"], error: null }],
  extraFiles: [],
  unmatchedTables: [],
  referenceTable: "complaints_2026_01",
};

beforeEach(() => {
  listedSchemas.schemas = ["default"];
  readColumns.mockReset().mockImplementation(async () => [col("id", "integer"), col("amount")]);
  registerTables.mockReset().mockResolvedValue([
    { tableName: "complaints", result: { success: true, message: "", params: { id: 7 } } },
  ]);
  previewOf.mockReset().mockImplementation((glob: string | null) =>
    glob
      ? {
          ...PICKED,
          glob,
          files: [],
          sharedColumns: [],
          differing: [],
          referenceTable: null,
        }
      : { ...PICKED, differing: [] },
  );
});

function renderForm(sourceType = "files") {
  const onRegisteredMany = vi.fn();
  const setError = vi.fn();
  const view = render(
    <RegisterTableForm
      sources={[{ id: "complaints", type: sourceType, allowedDomains: [] } as never]}
      domainHints={["sales"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={true}
      tables={[]}
      roles={[{ id: "admin" } as never]}
      suggestTableAlias={vi.fn(async (name: string) => `${name}_v`)}
      onContinue={vi.fn()}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={onRegisteredMany}
      setError={setError}
      initialSourceId="complaints"
    />,
  );
  return { onRegisteredMany, setError, ...view };
}

const tablesField = () => screen.getByRole("textbox", { name: "Tables" });
const option = (name: string) =>
  screen.getByRole("option", { name: new RegExp(`^${name}`), hidden: true });
const submit = () => screen.getByTestId("register-many-tables-submit");
const combineChoice = () => screen.queryByTestId("register-tables-combine-mode");

async function pick(...names: string[]) {
  const user = userEvent.setup();
  await waitFor(() => expect(tablesField()).not.toBeDisabled());
  await user.click(tablesField());
  for (const name of names) {
    await waitFor(() => expect(option(name)).not.toHaveAttribute("data-combobox-disabled"));
    await user.click(option(name));
  }
  return user;
}

describe("Combine into one table", () => {
  it("is offered once two files of a files source are picked, not before", async () => {
    renderForm();
    const user = await pick("complaints_2026_01");
    expect(combineChoice()).toBeNull();
    await user.click(option("complaints_2026_02"));
    expect(combineChoice()).toBeInTheDocument();
    // One table per file until the person chooses otherwise.
    expect(submit()).toHaveTextContent("Register 2 tables");
  });

  it("shows the files matched, the columns they share and the files that differ", async () => {
    previewOf.mockImplementation(() => PICKED);
    renderForm();
    const user = await pick("complaints_2026_01", "complaints_2026_02");
    await user.click(screen.getByText("Combine into one table"));
    const section = await screen.findByTestId("register-combined-files");
    expect(section).toHaveTextContent("2 files matched");
    expect(section).toHaveTextContent("complaints_2026_01.csv");
    expect(section).toHaveTextContent("complaints_2026_02.csv");
    expect(section).toHaveTextContent("Columns they share: id, amount");
    expect(screen.getByTestId("register-combined-differing")).toHaveTextContent(
      "complaints_2026_02.csv adds region",
    );
    // The derived glob is asked of the picked tables, not typed.
    expect(previewOf).toHaveBeenCalledWith(null, ["complaints_2026_01", "complaints_2026_02"]);
    // Files whose columns differ are not registered as one table.
    expect(submit()).toBeDisabled();
  });

  it("requires the new table's name and registers ONE table over the derived glob", async () => {
    const { onRegisteredMany, setError } = renderForm();
    const user = await pick("complaints_2026_01", "complaints_2026_02");
    await user.click(screen.getByText("Combine into one table"));
    // No existing table stands for it: the name is its own, asked for.
    expect(submit()).toHaveTextContent("Register 1 table");
    await user.click(submit());
    expect(setError).toHaveBeenLastCalledWith("Name the new table.");
    expect(registerTables).not.toHaveBeenCalled();
    await user.type(screen.getByTestId("register-combined-name"), "complaints");
    await user.type(screen.getByTestId("register-combined-source-file-column"), "source_file");
    await user.click(submit());
    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));
    const inputs = registerTables.mock.calls[0][0];
    expect(inputs).toHaveLength(1);
    expect(inputs[0]).toMatchObject({
      sourceId: "complaints",
      domainId: "sales",
      schemaName: "default",
      tableName: "complaints",
      fileGlob: "complaints_2026_0[12].csv",
      sourceFileColumn: "source_file",
    });
    expect(inputs[0].columns.map((c: { name: string }) => c.name)).toEqual(["id", "amount"]);
    expect(onRegisteredMany).toHaveBeenCalled();
  });

  it("names the files a derived glob would add and registers only once they are accepted", async () => {
    previewOf.mockImplementation(() => ({
      ...PICKED,
      glob: "complaints_2026_*.csv",
      differing: [],
      extraFiles: ["complaints_2026_03.csv"],
    }));
    const { setError } = renderForm();
    const user = await pick("complaints_2026_01", "complaints_2026_02");
    await user.click(screen.getByText("Combine into one table"));
    expect(await screen.findByTestId("register-combined-extra")).toHaveTextContent(
      "complaints_2026_*.csv also matches complaints_2026_03.csv",
    );
    await user.type(screen.getByTestId("register-combined-name"), "complaints");
    await user.click(submit());
    expect(registerTables).not.toHaveBeenCalled();
    expect(setError).toHaveBeenLastCalledWith(
      "Include the files the glob also matches, or type a glob that matches only the files you want.",
    );
    await user.click(screen.getByLabelText("Include them too"));
    await user.click(submit());
    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));
    expect(registerTables.mock.calls[0][0][0].fileGlob).toBe("complaints_2026_*.csv");
  });

  it("says at once that a typed glob matches nothing", async () => {
    const user = userEvent.setup();
    renderForm();
    const glob = await screen.findByTestId("register-tables-file-glob");
    await user.type(glob, "complaints_2025_*.csv");
    await waitFor(() =>
      expect(screen.getByText("No file matches this glob.")).toBeInTheDocument(),
    );
    // A typed glob offers the choice; combined, nothing can be registered.
    await user.click(screen.getByText("Combine into one table"));
    expect(submit()).toBeDisabled();
    expect(registerTables).not.toHaveBeenCalled();
  });

  it("keeps a typed glob as typed", async () => {
    previewOf.mockImplementation((glob: string | null) => ({
      ...PICKED,
      glob: glob ?? "",
      differing: [],
    }));
    const user = userEvent.setup();
    renderForm();
    await user.type(await screen.findByTestId("register-tables-file-glob"), "complaints_*.csv");
    await user.click(screen.getByText("Combine into one table"));
    await user.type(screen.getByTestId("register-combined-name"), "complaints");
    await user.click(submit());
    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));
    expect(registerTables.mock.calls[0][0][0].fileGlob).toBe("complaints_*.csv");
  });

  it("is never offered on a source that is not a files source", async () => {
    renderForm("mysql");
    await pick("complaints_2026_01", "complaints_2026_02");
    expect(combineChoice()).toBeNull();
    expect(screen.queryByTestId("register-tables-file-glob")).toBeNull();
    expect(submit()).toHaveTextContent("Register 2 tables");
  });
});

describe("the one schema of a files source", () => {
  it("is the schema registered, also when it is listed after the source is chosen", async () => {
    listedSchemas.schemas = [];
    const { rerender } = renderForm();
    listedSchemas.schemas = ["default"];
    rerender(
      <RegisterTableForm
        sources={[{ id: "complaints", type: "files", allowedDomains: [] } as never]}
        domainHints={["sales"]}
        domainAccess={["*"]}
        checkedDomains={new Set<string>()}
        domainsEnabled={true}
        tables={[]}
        roles={[{ id: "admin" } as never]}
        suggestTableAlias={vi.fn(async (name: string) => `${name}_v`)}
        onContinue={vi.fn()}
        onCancel={() => {}}
        onProductsRegistered={vi.fn()}
        onRegisteredMany={vi.fn()}
        setError={vi.fn()}
        initialSourceId="complaints"
      />,
    );
    // The Schema field is absent: the state holds the one schema it stands for.
    expect(screen.queryByTestId("register-table-schema-select")).toBeNull();
    const user = await pick("complaints_2026_01", "complaints_2026_02");
    await user.click(screen.getByText("Combine into one table"));
    await user.type(screen.getByTestId("register-combined-name"), "complaints");
    await user.click(submit());
    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));
    expect(registerTables.mock.calls[0][0][0].schemaName).toBe("default");
  });
});
