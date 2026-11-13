// Copyright (c) 2026 Kenneth Stott
// Canary: 5e1a9c34-7b02-4d6f-9c81-2a4f0e7b63d5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1426: onboarding must complete every data type.
 *
 * Data types are design-time metadata — nothing infers one after registration, so a column whose
 * type discovery could not supply has to be assigned before the table is registered. REQ-2234:
 * Register Table offers such a table but it cannot be picked, naming the untyped column; its
 * Register opens it unsaved in the table's panel, whose Columns section assigns the type, and the
 * registration carries it.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../../../test-utils/render";
import { expectReason, held, openTables, registerAction } from "./registerFormHelpers";

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({ tables: [{ name: "events", comment: null }], loading: false }),
}));

// A schemaless source: discovery types what it can and leaves `payload` untyped.
const COLUMNS = [
  { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: false },
  { name: "payload", dataType: "", comment: null, nativeFilterType: null, isPrimaryKey: false },
];
const readColumns = async () => COLUMNS;
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
  fetchIrTypes: vi.fn().mockResolvedValue(["integer", "text", "timestamp"]),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");
const { TableColumnsSection } = await import("../TableColumnsSection");
const { draftFromRegistration } = await import("../draftTable");
const { registrationInput } = await import("../useCreateTable");

function renderForm() {
  const onContinue = vi.fn();
  render(
    <RegisterTableForm
      sources={[{ id: "src", type: "mongodb", allowedDomains: [] } as never]}
      domainHints={["sales"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={true}
      tables={[]}
      roles={[{ id: "admin" } as never]}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={onContinue}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={vi.fn()}
      setError={vi.fn()}
      initialSourceId="src"
    />,
  );
  return { onContinue };
}

/** The table opened on its own from its option, as it will be registered. */
async function registeredOnItsOwn() {
  const { onContinue } = renderForm();
  const user = await openTables();
  await waitFor(() => expect(registerAction("events")).not.toBeNull());
  await user.click(registerAction("events") as HTMLElement);
  await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
  return onContinue.mock.calls[0][0];
}

describe("RegisterTableForm data types (REQ-1426)", () => {
  beforeEach(() => vi.clearAllMocks());

  it("offers a table with a column discovery could not type, which cannot be picked and names it", async () => {
    renderForm();
    const user = await openTables();
    await waitFor(() => expect(registerAction("events")).not.toBeNull());
    expect(held("events")).toBe(true);
    await expectReason(
      user,
      "events",
      "Needs input: no type was found for payload. Register it on its own.",
    );
  });

  it("hands the table on with the untyped column untyped, nothing registered", async () => {
    const registration = await registeredOnItsOwn();
    expect(registerTables).not.toHaveBeenCalled();
    const cols = registration.input.columns as { name: string; dataType: string }[];
    expect(cols.map((c) => [c.name, c.dataType])).toEqual([
      ["id", "integer"],
      ["payload", ""],
    ]);
  });

  it("is typed in the panel's Columns section, and the registration sends the type", async () => {
    const registration = await registeredOnItsOwn();
    const draft = draftFromRegistration(registration);
    document.body.innerHTML = "";
    const updateEditCol = vi.fn();
    render(
      <TableColumnsSection
        editingTable={draft}
        updateEditCol={updateEditCol}
        editSource={{ id: "src", type: "mongodb" } as never}
        roles={[]}
        hidingOnly={false}
        writeGrants={null}
      />,
    );
    const user = userEvent.setup();
    await user.click(screen.getByRole("tab", { name: /Type and keys/ }));
    const row = screen.getByTestId("column-row-payload");
    const type = within(row).getByRole("textbox", { name: "Data Type" });
    expect(type).toHaveValue("");
    await user.click(type);
    // Mantine keeps the drop-down out of the accessibility tree in jsdom: reach it through the
    // input's aria-controls listbox.
    const listbox = document.getElementById(type.getAttribute("aria-controls") as string);
    await user.click(
      within(listbox as HTMLElement).getByRole("option", { name: "timestamp", hidden: true }),
    );
    expect(updateEditCol).toHaveBeenLastCalledWith(1, "dataType", "timestamp");

    const typed = {
      ...draft,
      columns: draft.columns.map((c) =>
        c.columnName === "payload" ? { ...c, dataType: "timestamp" } : c,
      ),
    };
    const sent = registrationInput(typed, registration, null).columns as {
      name: string;
      dataType: string;
    }[];
    expect(sent.map((c) => [c.name, c.dataType])).toEqual([
      ["id", "integer"],
      ["payload", "timestamp"],
    ]);
  });
});
