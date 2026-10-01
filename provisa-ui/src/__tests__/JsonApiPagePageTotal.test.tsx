// Copyright (c) 2026 Kenneth Stott
// Canary: 4b8e2d6a-1c93-4f57-a0e4-7d5b9c3f2a18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1197/REQ-257: JSON:API counts a list's total only when the request carries
// page[total]=true. The explorer shows "N of TOTAL" and a last-page button, so every row request
// it builds asks for the total; an aggregate request returns no page of rows and carries no page
// parameters.

import { describe, it, expect, vi, beforeEach } from "vitest";
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
    ],
  }),
  useAllRelationships: () => ({ relationships: [] }),
}));

import { JsonApiPage } from "../pages/JsonApiPage";

function renderWithNav(jsonapiUrl: string) {
  return render(<JsonApiPage />, {
    initialEntries: [{ pathname: "/jsonapi", state: { jsonapiUrl } }],
  });
}

function shownUrl(container: HTMLElement): URLSearchParams {
  const shown = container.querySelector(".jsonapi-url")?.textContent ?? "";
  return new URLSearchParams(shown.includes("?") ? shown.split("?")[1] : "");
}

describe("JsonApiPage — page total", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response("[]", { status: 200, headers: { "content-type": "application/json" } }),
      ),
    );
  });

  it("asks for the total on a row request", async () => {
    const { container } = renderWithNav("/data/jsonapi/pet-store/inquiries?page[size]=20");
    await waitFor(() => {
      const params = shownUrl(container);
      expect(params.get("page[size]")).toBe("20");
      expect(params.get("page[total]")).toBe("true");
    });
  });

  it("sends no page parameters on an aggregate request", async () => {
    const { container } = renderWithNav("/data/jsonapi/pet-store/inquiries?aggregate=count");
    await waitFor(() => {
      const params = shownUrl(container);
      expect(params.get("aggregate")).toBe("count");
      expect(params.has("page[total]")).toBe(false);
      expect(params.has("page[size]")).toBe(false);
    });
  });
});
