// Copyright (c) 2026 Kenneth Stott
// Canary: 81132377-e700-4c29-9d0c-dd65dd49b204
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A JSON:API URL handed to the explorer with autoRun (NL "Open in JSON:API", Polly) is applied and
// run, whether the page was just opened or was already open.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { render } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "org_admin" }, selectedRoles: [{ id: "org_admin" }] }) }));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "pet-store", description: "Pet store" }] }),
  useTables: () => ({
    tables: [
      { id: "t1", domainId: "pet-store", tableName: "inquiries", columns: [{ columnName: "id" }] },
      { id: "t2", domainId: "pet-store", tableName: "users", columns: [{ columnName: "id" }] },
    ],
  }),
  useAllRelationships: () => ({ relationships: [] }),
}));

import { JsonApiPage } from "../pages/JsonApiPage";

const runs: string[] = [];

function Polly({ url }: { url: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/jsonapi", { state: { jsonapiUrl: url, autoRun: true } })}>
      {url}
    </button>
  );
}

describe("JsonApiPage — a hand-off while the page is open", () => {
  beforeEach(() => {
    localStorage.clear();
    runs.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        if (String(url).startsWith("/data/jsonapi/")) runs.push(String(url));
        return new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
      }),
    );
  });

  it("applies and runs each handed URL", async () => {
    render(
      <>
        <Polly url="/data/jsonapi/pet-store/users" />
        <Polly url="/data/jsonapi/pet-store/inquiries" />
        <JsonApiPage />
      </>,
      { initialEntries: ["/jsonapi"] },
    );
    act(() => screen.getByText("/data/jsonapi/pet-store/users").click());
    await waitFor(() =>
      expect(runs.some((u) => u.startsWith("/data/jsonapi/pet-store/users"))).toBe(true),
    );

    act(() => screen.getByText("/data/jsonapi/pet-store/inquiries").click());
    await waitFor(() =>
      expect(runs.some((u) => u.startsWith("/data/jsonapi/pet-store/inquiries"))).toBe(true),
    );
  });
});
