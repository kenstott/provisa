// Copyright (c) 2026 Kenneth Stott
// Canary: 7d12911b-a237-4e8a-adc4-e9b89aaeac12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table registered into a domain the domain filter has not seen yet — a domain with no tables
// until this one — is revealed in the filter, so the tables list shows it without a reload.

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";

const ensureDomainChecked = vi.fn();

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked }),
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({
    tables: [{ name: "getStore", comment: null, pagingKind: "endpoint", pagination: null }],
    loading: false,
  }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

describe("RegisterTableForm reveals the domain it registered into", () => {
  it("checks the new table's domain in the filter once registration succeeds", async () => {
    const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
    render(
      <RegisterTableForm
        sources={[{ id: "petstore", type: "openapi", allowedDomains: [] } as never]}
        domainHints={["inventory-copies"]}
        domainAccess={["*"]}
        checkedDomains={new Set<string>()}
        domainsEnabled={true}
        tables={[]}
        roles={[{ id: "admin" } as never]}
        getAvailableColumnsMetadata={vi
          .fn()
          .mockResolvedValue([{ name: "id", dataType: "integer" }])}
        suggestTableAlias={vi.fn().mockResolvedValue("")}
        registerTable={registerTable}
        onSuccess={vi.fn()}
        onCancel={() => {}}
        setError={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "petstore");
    await user.selectOptions(
      screen.getByTestId("register-table-domain-select"),
      "inventory-copies",
    );
    await user.selectOptions(await screen.findByTestId("register-table-table-select"), "getStore");
    await waitFor(() => expect(screen.getByTestId("register-table-col-datatype-id")).toBeTruthy());
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalled());
    await waitFor(() => expect(ensureDomainChecked).toHaveBeenCalledWith("inventory-copies"));
  });
});
