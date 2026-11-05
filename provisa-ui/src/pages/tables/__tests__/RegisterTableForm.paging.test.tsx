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
  const onContinue = vi.fn();
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
      onContinue={onContinue}
      onCancel={() => {}}
      setError={setError}
    />,
  );
  return { onContinue, setError };
}

async function pickTable(name: string) {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "petstore");
  await user.selectOptions(await screen.findByTestId("register-table-table-select"), name);
  // Discovery is done when the short step counts the columns it registers.
  await screen.findByTestId("register-table-columns-summary");
  return user;
}

describe("RegisterTableForm paging (REQ-318)", () => {
  beforeEach(() => vi.clearAllMocks());

  // Paging is not asked in the short step: the paging the source proposes is carried to the
  // Load and timeliness section that follows it, where it is accepted or edited (REQ-2109, REQ-318).
  it("carries the paging a source proposes out of the short step without asking it", async () => {
    const { onContinue } = renderForm();
    const user = await pickTable("listPets");
    expect(screen.queryByRole("textbox", { name: "Paging type" })).toBeNull();
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(onContinue.mock.calls[0][0].pagination).toEqual(OFFERED[0].pagination);
  });

  it("registers a table whose source suggests none with no paging", async () => {
    const { onContinue } = renderForm();
    const user = await pickTable("getStore");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(onContinue.mock.calls[0][0].pagination).toBeNull();
  });
});
