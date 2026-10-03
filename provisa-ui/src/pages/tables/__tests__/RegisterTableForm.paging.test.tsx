// Copyright (c) 2026 Kenneth Stott
// Canary: 9b5b15b5-f722-4862-8ff7-b66893ec5b4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-318: registering an OpenAPI table shows the paging its source suggests; the steward
// accepts or edits it, and the table is registered with what is declared.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";
import { NO_PAGING } from "../paging";

const OFFERED = [
  {
    name: "listPets",
    comment: null,
    pagingKind: "endpoint",
    pagination: { ...NO_PAGING, type: "offset", pageParam: "offset", pageSizeParam: "limit" },
    pagingCeilingRows: null,
  },
  { name: "getStore", comment: null, pagingKind: "endpoint", pagination: null },
];

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({ tables: OFFERED, loading: false }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

function renderForm() {
  const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
  const setError = vi.fn();
  render(
    <RegisterTableForm
      sources={[{ id: "petstore", type: "openapi", allowedDomains: [] } as never]}
      domainHints={[]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={false}
      tables={[]}
      roles={[{ id: "admin" } as never]}
      getAvailableColumnsMetadata={vi.fn().mockResolvedValue([{ name: "id", dataType: "integer" }])}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      registerTable={registerTable}
      onSuccess={vi.fn()}
      setError={setError}
    />,
  );
  return { registerTable, setError };
}

async function pickTable(name: string) {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "petstore");
  await user.selectOptions(await screen.findByTestId("register-table-table-select"), name);
  await waitFor(() => expect(screen.getByTestId("register-table-col-datatype-id")).toBeTruthy());
  return user;
}

describe("RegisterTableForm paging (REQ-318)", () => {
  beforeEach(() => vi.clearAllMocks());

  it("shows the paging the source suggests and registers the table with it", async () => {
    const { registerTable } = renderForm();
    const user = await pickTable("listPets");
    expect(await screen.findByRole("textbox", { name: "Paging type" })).toHaveValue(
      "Offset and limit",
    );
    expect(screen.getByRole("textbox", { name: "Page parameter" })).toHaveValue("offset");
    await user.type(screen.getByRole("textbox", { name: "Max pages" }), "5");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    expect(registerTable.mock.calls[0][0].pagination).toEqual({
      type: "offset",
      pageParam: "offset",
      pageSizeParam: "limit",
      maxPages: 5,
    });
  });

  it("registers a table whose source suggests none with no paging", async () => {
    const { registerTable } = renderForm();
    const user = await pickTable("getStore");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    expect(registerTable.mock.calls[0][0].pagination).toBeUndefined();
  });
});
