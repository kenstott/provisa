// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7e1c52-9a4d-4f08-b6e3-71d0c5a2e894
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The query editors' hover actions (prettify, copy) are the shared canvas controls: a toolbar of
// icon buttons styled from theme tokens, shown on hover by CSS. No inline colour sits on them, so
// they follow the light and the dark theme.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, screen, waitFor, fireEvent } from "@testing-library/react";
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
import { SqlModelingModal } from "../components/SqlModelingModal";
import { persistTabsMeta, emptyTab } from "../pages/sql/tabHelpers";

Range.prototype.getClientRects = () => [] as unknown as DOMRectList;
Range.prototype.getBoundingClientRect = () => new DOMRect();

const UGLY = "select a,b from t where a=1";

function editorText(): string {
  return (document.querySelector(".cm-content")?.textContent ?? "").replace(/\s+/g, " ");
}

/** The toolbar a button sits in, and every element of it that carries an inline colour. */
function barOf(button: HTMLElement) {
  const bar = button.closest('[role="toolbar"]') as HTMLElement | null;
  const coloured = bar
    ? [bar, ...Array.from(bar.querySelectorAll<HTMLElement>("*"))].filter(
        (el) => el.style.background !== "" || el.style.backgroundColor !== "",
      )
    : [];
  return { bar, coloured };
}

describe("SQL editor hover controls", () => {
  const writeText = vi.fn(async () => {});
  beforeEach(() => {
    localStorage.clear();
    writeText.mockClear();
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify({ columns: [], rows: [] }), { status: 200 })),
    );
    persistTabsMeta([emptyTab("t1", "Query 1", UGLY)], "t1");
  });

  it("puts prettify and copy in the shared toolbar, with no inline colour", async () => {
    renderWithProviders(<SqlPage />, { initialEntries: ["/sql"] });
    const prettify = await screen.findByTestId("sql-prettify");
    const copy = screen.getByTestId("sql-copy");
    const { bar, coloured } = barOf(prettify);
    expect(bar).not.toBeNull();
    expect(bar!.contains(copy)).toBe(true);
    expect(bar!.getAttribute("data-testid")).toMatch(/-canvas-bar$/);
    expect(prettify).toHaveAccessibleName("Prettify SQL");
    expect(copy).toHaveAccessibleName("Copy SQL");
    expect(coloured).toEqual([]);
    expect(document.querySelector(".hover-sql-btn")).toBeNull();
  });

  it("still prettifies and copies", async () => {
    renderWithProviders(<SqlPage />, { initialEntries: ["/sql"] });
    await waitFor(() => expect(editorText()).toBe(UGLY));
    act(() => screen.getByTestId("sql-prettify").click());
    await waitFor(() => expect(editorText()).not.toBe(UGLY));
    expect(editorText().toLowerCase()).toContain("select");
    fireEvent.click(screen.getByTestId("sql-copy"));
    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
  });
});

describe("SQL modeling dialog hover controls", () => {
  it("puts copy in the shared toolbar, with no inline colour", () => {
    const writeText = vi.fn(async () => {});
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    renderWithProviders(
      <SqlModelingModal tables={[]} existingRels={[]} onClose={() => {}} onPromote={async () => {}} />,
    );
    const copy = screen.getByTestId("sql-modeling-copy-sql");
    const { bar, coloured } = barOf(copy);
    expect(bar).not.toBeNull();
    expect(copy).toHaveAccessibleName(/copy/i);
    expect(coloured).toEqual([]);
    expect(document.querySelector(".copy-sql-btn")).toBeNull();
    fireEvent.click(copy);
    expect(writeText).toHaveBeenCalledTimes(1);
  });
});
