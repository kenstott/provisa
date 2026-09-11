// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e4a52-8d3b-42f7-9c60-1b8ef4a2d905
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1722: the group-by nodes projection resolves a dot-path at any depth
// (provisa/api/jsonapi/generator.py::_insert_jsonapi_include_path), but the Include picker
// offered only a table's direct relationships — a caller who wanted a joined relationship's own
// joined column (assignment.employee.id) had to type the URL by hand, and got a plain 400 the
// one time it tried a dot-path the picker had never validated. The picker now offers one further
// level: a relationship's own relationships, each with their own columns.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "org_admin" } }),
}));

vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));

vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "pet-store", description: "Pet store" }] }),
  useTables: () => ({
    tables: [
      {
        id: "t1",
        domainId: "pet-store",
        tableName: "pets",
        columns: [{ columnName: "id" }],
      },
      {
        id: "t2",
        domainId: "pet-store",
        tableName: "assignments",
        columns: [{ columnName: "id" }, { columnName: "role" }],
      },
      {
        id: "t3",
        domainId: "pet-store",
        tableName: "employees",
        columns: [{ columnName: "id" }, { columnName: "last_name" }],
      },
    ],
  }),
  useAllRelationships: () => ({
    relationships: [
      {
        id: "r1",
        sourceTableId: "t1",
        targetTableId: "t2",
        graphqlAlias: "assignment",
        physicalName: "assignment",
      },
      {
        id: "r2",
        sourceTableId: "t2",
        targetTableId: "t3",
        graphqlAlias: "employee",
        physicalName: "employee",
      },
    ],
  }),
}));

import { JsonApiPage } from "../pages/JsonApiPage";

const NAV_URL =
  "/data/jsonapi/pet-store/pets?groupBy=id&aggregate=count&includeNodes=true" +
  "&include=assignment.employee.id";

function renderWithNav() {
  return render(<JsonApiPage />, {
    initialEntries: [{ pathname: "/jsonapi", state: { jsonapiUrl: NAV_URL } }],
  });
}

describe("JsonApiPage — two-level relationship dot-paths in the Include picker", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response("[]", { status: 200, headers: { "content-type": "application/json" } }),
      ),
    );
  });

  it("rebuilds the URL with a two-level dot-path it was handed", async () => {
    const { container } = renderWithNav();
    await waitFor(() => {
      const shown = container.querySelector(".jsonapi-url")?.textContent ?? "";
      expect(shown).toContain("include=assignment.employee.id");
    });
  });

  it("shows the second-level relationship's own columns, and reflects the handed-off selection", async () => {
    const user = userEvent.setup();
    renderWithNav();
    await waitFor(() => expect(screen.getByTestId("jsonapi-include-nodes-checkbox")).toBeChecked());

    await user.click(screen.getByTestId("jsonapi-include-trigger"));

    const secondLevelColumn = await screen.findByTestId("jsonapi-include-assignment.employee.id");
    expect(secondLevelColumn).toBeChecked();
    expect(screen.getByTestId("jsonapi-include-assignment.employee.last_name")).not.toBeChecked();
    // The relationship one hop past that (assignment.employee's own relationships, were there
    // any) is not offered — REQ-1722 caps the picker at two levels.
    expect(
      screen.queryByTestId("jsonapi-include-assignment.employee.employee"),
    ).not.toBeInTheDocument();
  });

  it("picking a second-level column produces the matching dot-path", async () => {
    const user = userEvent.setup();
    const { container } = renderWithNav();
    await waitFor(() => expect(screen.getByTestId("jsonapi-include-nodes-checkbox")).toBeChecked());
    await user.click(screen.getByTestId("jsonapi-include-trigger"));

    await user.click(await screen.findByTestId("jsonapi-include-assignment.employee.last_name"));

    await waitFor(() => {
      const shown = container.querySelector(".jsonapi-url")?.textContent ?? "";
      expect(shown).toContain("assignment.employee.last_name");
    });
  });
});
