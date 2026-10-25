// Copyright (c) 2026 Kenneth Stott
// Canary: adce12d7-c9f2-40d0-9375-74218c8a7d83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2009: a named demo's example GraphQL queries are the GraphQL explorer's first tabs, the first
// time that demo is opened. GraphiQL takes `defaultTabs` only while its storage holds no tab state
// (its documented contract), and its storage is the demo's scope (REQ-2008).

import React from "react";
import { afterEach, describe, it, expect, vi, beforeEach } from "vitest";
import { render } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { StorageAPI } from "@graphiql/toolkit";

const graphiql = vi.hoisted(() => ({ props: null as Record<string, unknown> | null }));

vi.mock("graphiql", () => {
  const GraphiQL = (props: Record<string, unknown> & { children?: React.ReactNode }) => {
    graphiql.props = props;
    return React.createElement("div", null, props.children);
  };
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
  useAuth: () => ({ role: { id: "analyst" }, selectedRoles: [{ id: "analyst" }] }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
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
import { NamedDemoContext, NO_NAMED_DEMO } from "../context/NamedDemoContext";
import { bindStorageScope } from "../lib/browserStorage";
import type { ExampleQuery } from "../api/setup";

const EXAMPLES: ExampleQuery[] = [
  { name: "1-slowest-pages", query: "query SlowestPages { a }" },
  { name: "2-errors", query: "query Errors { b }" },
];

function open(examples: ExampleQuery[]) {
  graphiql.props = null;
  return render(
    <MantineProvider>
      <NamedDemoContext.Provider value={{ ...NO_NAMED_DEMO, exampleQueries: examples }}>
        <MemoryRouter initialEntries={["/query"]}>
          <QueryPage />
        </MemoryRouter>
      </NamedDemoContext.Provider>
    </MantineProvider>,
  );
}

describe("the GraphQL explorer's example tabs", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("{}", { status: 200 })),
    );
  });
  afterEach(() => localStorage.clear());

  it("offers the demo's examples as the default tabs, in order", () => {
    open(EXAMPLES);
    expect(graphiql.props?.defaultTabs).toEqual([
      { query: "query SlowestPages { a }" },
      { query: "query Errors { b }" },
    ]);
  });

  it("offers none where no demo ships examples", () => {
    open([]);
    expect(graphiql.props?.defaultTabs).toBeUndefined();
  });

  it("keeps GraphiQL's tabs per demo, so the examples open the first time each is opened", () => {
    bindStorageScope({ controlPlaneId: "cp-weblogs", orgId: "default" });
    const first = open(EXAMPLES);
    const weblogs = new StorageAPI(graphiql.props?.storage as never);
    // Nothing saved in this demo yet: GraphiQL takes the default tabs.
    expect(weblogs.get("tabState")).toBeNull();
    weblogs.set("tabState", JSON.stringify({ tabs: [], activeTabIndex: 0 }));
    first.unmount();

    bindStorageScope({ controlPlaneId: "cp-pets", orgId: "default" });
    const second = open(EXAMPLES);
    // Another demo has saved nothing either: its own first visit.
    expect(new StorageAPI(graphiql.props?.storage as never).get("tabState")).toBeNull();
    second.unmount();

    bindStorageScope({ controlPlaneId: "cp-weblogs", orgId: "default" });
    open(EXAMPLES);
    // Back in weblogs, its saved tabs are what GraphiQL opens; the examples are not offered again.
    expect(new StorageAPI(graphiql.props?.storage as never).get("tabState")).not.toBeNull();
  });

  it("starts nowhere in particular the first time, so GraphiQL opens the first example", () => {
    bindStorageScope({ controlPlaneId: "cp-first", orgId: "default" });
    open(EXAMPLES);
    expect(graphiql.props?.initialQuery).toBeUndefined();
    expect(graphiql.props?.initialVariables).toBeUndefined();
    expect(graphiql.props?.initialHeaders).toBeUndefined();
  });

  it("opened again, starts on the tab it last had open, never a new empty one", () => {
    // GraphiQL reopens its saved tabs on the one equal to its starting query, variables and
    // headers, and adds an empty "<untitled>" tab when none is: the seeded examples never wrote
    // the starting query, so the explorer reopened on an empty tab.
    bindStorageScope({ controlPlaneId: "cp-again", orgId: "default" });
    const first = open(EXAMPLES);
    const saved = new StorageAPI(graphiql.props?.storage as never);
    saved.set(
      "tabState",
      JSON.stringify({
        activeTabIndex: 1,
        tabs: [
          {
            id: "a",
            title: "SlowestPages",
            query: "query SlowestPages { a }",
            variables: null,
            headers: null,
            operationName: "SlowestPages",
            response: null,
          },
          {
            id: "b",
            title: "Errors",
            query: "query Errors { b }",
            variables: '{"n":1}',
            headers: null,
            operationName: "Errors",
            response: null,
          },
        ],
      }),
    );
    first.unmount();

    open(EXAMPLES);
    expect(graphiql.props?.initialQuery).toBe("query Errors { b }");
    expect(graphiql.props?.initialVariables).toBe('{"n":1}');
    expect(graphiql.props?.initialHeaders).toBe("");
  });
});
