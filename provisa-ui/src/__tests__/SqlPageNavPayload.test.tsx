// Copyright (c) 2026 Kenneth Stott
// Canary: de68cdf1-cd9e-44a3-9f8a-d4f4c9691f3e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Polly opens SQL in the SQL explorer by navigating to /sql with the query as router state. The
// user is often already there; the query must open then too, not only after a refresh.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { renderWithProviders } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "org_admin" }, selectedRoles: [{ id: "org_admin" }] }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>(), ensureDomainChecked: vi.fn() }),
}));
vi.mock("../hooks/useCapability", () => ({ useCapability: () => true }));
vi.mock("../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "org_admin" }] }),
  useDomains: () => ({ domains: [] }),
  useTables: () => ({ tables: [], refetch: vi.fn() }),
  useRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useMetrics: () => ({ metrics: [] }),
  useRegisterTable: () => ({ registerTable: vi.fn() }),
  useUpdateTable: () => ({ updateTable: vi.fn() }),
}));

import { SqlPage } from "../pages/SqlPage";
import { tabSqlKey } from "../pages/sql/types";

// CodeMirror measures text with Range.getClientRects, which jsdom does not implement.
Range.prototype.getClientRects = () => [] as unknown as DOMRectList;
Range.prototype.getBoundingClientRect = () => new DOMRect();

/** The SQL each open tab holds, as the page persists it. */
function tabSql(): string[] {
  return Object.keys(localStorage)
    .filter((k) => k.startsWith(tabSqlKey("")))
    .map((k) => localStorage.getItem(k) ?? "");
}

function Polly() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/sql", { state: { sql: "SELECT 42 AS answer" } })}>
      polly
    </button>
  );
}

describe("SqlPage — a hand-off while the page is open", () => {
  beforeEach(() => localStorage.clear());

  it("opens the query in a new tab without a remount", async () => {
    renderWithProviders(
      <>
        <Polly />
        <SqlPage />
      </>,
      { initialEntries: ["/sql"] },
    );
    await screen.findAllByText(/Query 1/);
    expect(screen.queryAllByText(/Query 2/)).toHaveLength(0);

    act(() => screen.getByText("polly").click());

    await waitFor(() => expect(screen.getAllByText(/Query 2/).length).toBeGreaterThan(0));
    await waitFor(() => expect(tabSql()).toContain("SELECT 42 AS answer"));
  });
});
