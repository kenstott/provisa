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
 * type discovery could not supply has to be assigned here. The form shows the discovered type,
 * refuses to submit while any selected column has none, and carries the type in the payload.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";

// The schema/table pickers are the only network the form does on its own; the column metadata it
// needs arrives through a prop.
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({ tables: [{ name: "events" }], loading: false }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

// A schemaless source: discovery types what it can and leaves `payload` untyped.
const COLUMNS = [
  { name: "id", dataType: "integer" },
  { name: "payload", dataType: "" },
];

function renderForm() {
  const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
  const setError = vi.fn();
  render(
    <RegisterTableForm
      sources={[{ id: "src", type: "mongodb", allowedDomains: [] } as never]}
      domainHints={[]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={false}
      tables={[]}
      roles={[{ id: "admin" } as never]}
      getAvailableColumnsMetadata={vi.fn().mockResolvedValue(COLUMNS)}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      registerTable={registerTable}
      onSuccess={vi.fn()}
      setError={setError}
    />,
  );
  return { registerTable, setError };
}

/** Drive the source → schema → table cascade so the column rows render. */
async function pickTable() {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "src");
  await user.selectOptions(await screen.findByTestId("register-table-table-select"), "events");
  await waitFor(() => expect(screen.getByTestId("register-table-col-datatype-id")).toBeTruthy());
  return user;
}

describe("RegisterTableForm data types (REQ-1426)", () => {
  beforeEach(() => vi.clearAllMocks());

  it("offers a type picker for every column and shows the discovered type", async () => {
    renderForm();
    await pickTable();
    expect(screen.getByTestId("register-table-col-datatype-id")).toHaveValue("integer");
    expect(screen.getByTestId("register-table-col-datatype-payload")).toHaveValue("");
  });

  it("refuses to register while a selected column has no type", async () => {
    const { registerTable, setError } = renderForm();
    const user = await pickTable();
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(setError).toHaveBeenCalled());
    expect(setError.mock.calls.at(-1)?.[0]).toContain("payload");
    expect(registerTable).not.toHaveBeenCalled();
  });

  it("registers once the steward assigns the missing type, and sends it", async () => {
    const { registerTable, setError } = renderForm();
    const user = await pickTable();
    await user.click(screen.getByTestId("register-table-col-datatype-payload"));
    await user.keyboard("timestamp");
    await user.keyboard("{ArrowDown}{Enter}");
    await waitFor(() =>
      expect(screen.getByTestId("register-table-col-datatype-payload")).toHaveValue("timestamp"),
    );
    await user.keyboard("{Escape}");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    expect(setError).toHaveBeenLastCalledWith(null);
    const cols = registerTable.mock.calls[0][0].columns as { name: string; dataType: string }[];
    expect(cols.map((c) => [c.name, c.dataType])).toEqual([
      ["id", "integer"],
      ["payload", "timestamp"],
    ]);
  });
});
