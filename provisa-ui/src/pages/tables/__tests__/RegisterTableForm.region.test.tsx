// Copyright (c) 2026 Kenneth Stott
// Canary: 6db7cc68-3ef2-4442-be87-3a252ea1b60c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a new table starts in its source's region, else the region the operator is
// connected to; the operator may change it or choose No region. With no regions declared there
// is no field, and the table is registered with none.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";

const choices = vi.hoisted(() => ({
  value: { regions: ["eu", "us"], connected: "us" as string | null },
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({
    tables: [{ name: "orders", comment: null, pagingKind: null, pagination: null }],
    loading: false,
  }),
}));
vi.mock("../../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useRegionQueries")>()),
  useRegionChoices: () => choices.value,
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const SOURCES = [
  { id: "crm", type: "postgresql", allowedDomains: [], region: "eu" },
  { id: "erp", type: "postgresql", allowedDomains: [], region: null },
];

function renderForm() {
  const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
  render(
    <RegisterTableForm
      sources={SOURCES as never}
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
      setError={vi.fn()}
    />,
  );
  return { registerTable };
}

async function pick(sourceId: string) {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), sourceId);
  await user.selectOptions(await screen.findByTestId("register-table-table-select"), "orders");
  await waitFor(() => expect(screen.getByTestId("register-table-col-datatype-id")).toBeTruthy());
  return user;
}

describe("RegisterTableForm region (REQ-1921)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    choices.value = { regions: ["eu", "us"], connected: "us" };
  });

  it("starts a table in its source's region", async () => {
    const { registerTable } = renderForm();
    const user = await pick("crm");
    expect(screen.getByTestId("register-table-region-select")).toHaveValue("eu");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    expect(registerTable.mock.calls[0][0].region).toBe("eu");
  });

  it("starts a table of a source naming none in the connected region", async () => {
    renderForm();
    await pick("erp");
    expect(screen.getByTestId("register-table-region-select")).toHaveValue("us");
  });

  it("registers with no region only when the operator removes it", async () => {
    const { registerTable } = renderForm();
    const user = await pick("crm");
    await user.click(screen.getByTestId("register-table-region-select"));
    await user.click(await screen.findByRole("option", { name: "No region", hidden: true }));
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    expect(registerTable.mock.calls[0][0].region).toBeNull();
  });

  it("shows no region and sends none when the platform declares no regions", async () => {
    choices.value = { regions: [], connected: null };
    const { registerTable } = renderForm();
    const user = await pick("crm");
    expect(screen.queryByTestId("register-table-region-select")).toBeNull();
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    expect("region" in registerTable.mock.calls[0][0]).toBe(false);
  });
});
