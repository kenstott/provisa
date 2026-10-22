// Copyright (c) 2026 Kenneth Stott
// Canary: 641ec6b2-e201-46a8-ab03-1eb48f3b8409
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The GraphQL explorer's introspected schema is kept in memory for the page load, never in browser
// storage: one org's schema for many roles and domains is larger than the storage quota, and a
// write past it threw into the fetch and showed a schema that had loaded as a schema error.

import React from "react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { buildSchema, introspectionFromSchema } from "graphql";

vi.mock("graphiql", () => {
  const GraphiQL = ({ children }: { children?: React.ReactNode }) =>
    React.createElement("div", null, children);
  const Part = ({ children }: { children?: React.ReactNode }) =>
    React.createElement("div", null, children);
  GraphiQL.Footer = Part;
  GraphiQL.Toolbar = Part;
  GraphiQL.Logo = Part;
  return { GraphiQL };
});
vi.mock("@graphiql/react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@graphiql/react")>()),
  useGraphiQL: (selector: (s: Record<string, unknown>) => unknown) =>
    selector({
      queryEditor: null,
      schema: null,
      tabs: [{ title: "Query", query: "" }],
      activeTabIndex: 0,
    }),
  useGraphiQLActions: () => ({ addTab: vi.fn(), updateActiveTabValues: vi.fn(), run: vi.fn() }),
}));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "org_admin" }, selectedRoles: [{ id: "org_admin" }] }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set(["shop"]) }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));
vi.mock("../lib/engineWake", () => ({ prewarmEngine: () => undefined }));
vi.mock("../plugins/table-view", () => ({ ResponseTableOverlay: () => null }));
vi.mock("../plugins/headers-quick-insert", () => ({ HeadersQuickInsert: () => null }));

import { MantineProvider } from "@mantine/core";
import { QueryPage } from "../pages/QueryPage";

/** What a browser's per-origin storage holds. */
const QUOTA_CHARS = 5_000_000;

/** A schema whose introspection is past the quota, as one for many roles and domains is. */
const BIG = introspectionFromSchema(
  buildSchema(`"""${"x".repeat(QUOTA_CHARS + 1000)}"""\ntype Query { orders: Int }`),
);

let introspections = 0;

function renderPage() {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/query"]}>
        <QueryPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

describe("QueryPage — the introspected schema", () => {
  beforeEach(() => {
    introspections = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        if (String(url).startsWith("/data/schema-version")) {
          return new Response(JSON.stringify({ version: 7 }), { status: 200 });
        }
        if (String(url).startsWith("/data/introspection")) {
          introspections += 1;
          return new Response(JSON.stringify({ data: BIG }), { status: 200 });
        }
        return new Response("{}", { status: 200 });
      }),
    );
    // The browser refuses a write past its quota.
    const setItem = Storage.prototype.setItem;
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (
      this: Storage,
      key: string,
      value: string,
    ) {
      if (value.length > QUOTA_CHARS) {
        throw new DOMException("exceeded the quota", "QuotaExceededError");
      }
      setItem.call(this, key, value);
    });
  });
  afterEach(() => vi.restoreAllMocks());

  it("loads a schema larger than the storage quota, keeps it out of storage, and reads it once", async () => {
    const first = renderPage();
    await waitFor(() => expect(introspections).toBe(1));
    // Settled, and not shown as a schema error.
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByText(/schema error/i)).toBeNull();
    const stored = [...Array(sessionStorage.length).keys()].map((i) => sessionStorage.key(i));
    expect(stored.some((k) => k?.includes("introspection:"))).toBe(false);
    first.unmount();

    // Opened again in this page load: the schema kept in memory, not read again.
    renderPage();
    await new Promise((r) => setTimeout(r, 50));
    expect(introspections).toBe(1);
    expect(screen.queryByText(/schema error/i)).toBeNull();
  });
});
