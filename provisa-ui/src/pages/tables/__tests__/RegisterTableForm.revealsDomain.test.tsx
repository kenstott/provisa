// Copyright (c) 2026 Kenneth Stott
// Canary: 7d12911b-a237-4e8a-adc4-e9b89aaeac12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The create step names the domain a table is registered into; the domain is revealed in the
// filter only once the table is registered, by the Save that follows its settings (REQ-2109,
// pages/__tests__/tables-create.test.tsx).

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

describe("RegisterTableForm names the domain it registers into", () => {
  it("hands on the one domain, revealing nothing before the table is registered", async () => {
    const onContinue = vi.fn();
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
        onContinue={onContinue}
        onCancel={() => {}}
        setError={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "petstore");
    // The one domain the caller may register in is the domain: it is not asked.
    expect(screen.queryByTestId("register-table-domain-select")).toBeNull();
    await user.selectOptions(await screen.findByTestId("register-table-table-select"), "getStore");
    await screen.findByTestId("register-table-columns-summary");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(onContinue.mock.calls[0][0].input.domainId).toBe("inventory-copies");
    expect(ensureDomainChecked).not.toHaveBeenCalled();
  });
});
