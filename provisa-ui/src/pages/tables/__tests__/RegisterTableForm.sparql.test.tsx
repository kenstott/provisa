// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** REQ-1683: registering a table on a sparql source takes the Neo4j-shaped path with a SPARQL editor. */

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const preview = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: [] as string[], loading: false }),
  useAvailableTables: () => ({ tables: [] as { name: string }[], loading: false }),
}));
vi.mock("../../../hooks/useQueryPreview", () => ({
  useQueryPreview: () => ({ preview }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const QUERY = "SELECT ?volunteer_id ?name WHERE { ?v <urn:id> ?volunteer_id ; <urn:name> ?name }";

beforeEach(() => {
  vi.clearAllMocks();
  preview.mockResolvedValue({
    rows: [{ volunteer_id: "V-01", name: "Grace" }],
    columns: [
      { name: "volunteer_id", dataType: "text" },
      { name: "name", dataType: "text" },
    ],
    error: null,
  });
});

describe("RegisterTableForm on a sparql source (REQ-1683)", () => {
  it("previews the SELECT and registers its variables as text columns with the query", async () => {
    const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
    render(
      <RegisterTableForm
        sources={[{ id: "sparql-demo", type: "sparql", allowedDomains: [] } as never]}
        domainHints={["shelter"]}
        domainAccess={["*"]}
        checkedDomains={new Set<string>()}
        domainsEnabled
        tables={[]}
        roles={[{ id: "org_admin" } as never]}
        getAvailableColumnsMetadata={vi.fn().mockResolvedValue([])}
        suggestTableAlias={vi.fn().mockResolvedValue("")}
        registerTable={registerTable}
        onSuccess={vi.fn()}
        setError={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "sparql-demo");
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shelter");
    expect(screen.queryByTestId("register-table-schema-select")).not.toBeInTheDocument();
    fireEvent.change(await screen.findByTestId("register-table-sparql-table-name"), {
      target: { value: "volunteer" },
    });
    fireEvent.change(screen.getByTestId("register-table-sparql-query"), {
      target: { value: QUERY },
    });
    await user.click(screen.getByTestId("register-table-sparql-preview"));
    await waitFor(() =>
      expect(screen.getByTestId("register-table-sparql-preview-rows")).toBeInTheDocument(),
    );
    expect(preview).toHaveBeenCalledWith({
      sourceType: "sparql",
      sourceId: "sparql-demo",
      query: QUERY,
    });
    fireEvent.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(registerTable).toHaveBeenCalledTimes(1));
    expect(registerTable.mock.calls[0][0]).toMatchObject({
      sourceId: "sparql-demo",
      schemaName: "sparql",
      tableName: "volunteer",
      queryTemplate: QUERY,
    });
  });
});
