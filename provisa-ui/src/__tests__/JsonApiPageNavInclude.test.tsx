// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e4a52-8d3b-42f7-9c60-1b8ef4a2d905
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1408: NL's "Open in JSON:API" hands over the whole group-by URL, whose ?include= carries the
// nodes projection as "rel.col" dot-paths. The explorer rebuilds the URL from its own pickers, so
// the projection has to be seeded from the hand-off and survive the table-change reset — otherwise
// the page runs a narrower query than the one the visitor clicked through from.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, waitFor } from "../test-utils/render";

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
        tableName: "inquiries",
        columns: [{ columnName: "id" }, { columnName: "user_id" }],
      },
      {
        id: "t2",
        domainId: "pet-store",
        tableName: "users",
        columns: [{ columnName: "id" }, { columnName: "name" }],
      },
    ],
  }),
  useAllRelationships: () => ({
    relationships: [{ id: "r1", sourceTableId: "t1", targetTableId: "t2", graphqlAlias: "user" }],
  }),
}));

import { JsonApiPage } from "../pages/JsonApiPage";

const NAV_URL =
  "/data/jsonapi/pet-store/inquiries?groupBy=user_id&aggregate=count&includeNodes=true" +
  "&include=user.id,user.name";

function renderWithNav() {
  return render(
    <MemoryRouter initialEntries={[{ pathname: "/jsonapi", state: { jsonapiUrl: NAV_URL } }]}>
      <JsonApiPage />
    </MemoryRouter>,
  );
}

describe("JsonApiPage — navigation hand-off", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response("[]", { status: 200, headers: { "content-type": "application/json" } }),
      ),
    );
  });

  it("rebuilds the URL with the include dot-paths it was handed", async () => {
    const { container } = renderWithNav();
    await waitFor(() => {
      const shown = container.querySelector(".jsonapi-url")?.textContent ?? "";
      expect(shown).toContain("includeNodes=true");
      expect(shown).toContain("include=user.id%2Cuser.name");
    });
  });
});
