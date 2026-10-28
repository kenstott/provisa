// Copyright (c) 2026 Kenneth Stott
// Canary: 8c1d2e4f-7a35-4b9e-a6d0-2f5b1c9e3a77
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.


// A source whose connector server is still starting answers the columns read with null. The form
// shows the starting state (not an empty table), then the columns once the server answers.

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({
    tables: [{ name: "getStore", comment: "The store", pagingKind: null, pagination: null }],
    loading: false,
  }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

describe("RegisterTableForm — columns of a starting source", () => {
  it("shows the starting state, then the columns the poll returns", async () => {
    const props = {
      sources: [{ id: "petstore", type: "openapi", allowedDomains: [] } as never],
      domainHints: ["sales"],
      domainAccess: ["*"],
      checkedDomains: new Set<string>(),
      domainsEnabled: true,
      tables: [],
      roles: [{ id: "admin" } as never],
      getAvailableColumnsMetadata: vi.fn().mockResolvedValue(null),
      suggestTableAlias: vi.fn().mockResolvedValue(""),
      onContinue: vi.fn(),
      onCancel: () => {},
      setError: vi.fn(),
    };
    const { rerender } = render(<RegisterTableForm {...props} columnsStarting />);
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "petstore");
    await user.selectOptions(await screen.findByTestId("register-table-table-select"), "getStore");
    expect(await screen.findByTestId("register-table-columns-starting")).toBeInTheDocument();
    expect(screen.queryByTestId("register-table-columns-summary")).toBeNull();

    rerender(
      <RegisterTableForm
        {...props}
        columnsStarting={false}
        lateColumns={{
          sourceId: "petstore",
          schemaName: "openapi",
          tableName: "getStore",
          columns: [
            { name: "id", dataType: "integer", isPrimaryKey: true, comment: null, nativeFilterType: null },
          ],
        }}
      />,
    );
    expect(await screen.findByTestId("register-table-columns-summary")).toBeInTheDocument();
    expect(screen.queryByTestId("register-table-columns-starting")).toBeNull();
  });
});
