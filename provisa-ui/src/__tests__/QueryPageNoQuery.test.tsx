// Copyright (c) 2026 Kenneth Stott
// Canary: 5d83c1f7-2a49-4b6e-8e05-c7a19f3b2d68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A GraphQL editor run with no statement in it (blank, or only comments such as GraphiQL's welcome
// text) says so in the response pane, before anything is sent. A real query still runs.
//
// GraphiQL itself does not run in jsdom (Monaco): its run is stood in for as in
// QueryPageStopQuery.test.tsx, here with the editor's text a variable.

import React from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import "../i18n";

interface StoreState {
  isFetching: boolean;
  subscription: { unsubscribe: () => void } | null;
  queryId: number;
  response: string;
  [k: string]: unknown;
}

const typed = vi.hoisted(() => ({ text: "" }));

const graphiql = vi.hoisted(() => {
  const listeners = new Set<() => void>();
  const responseEditor = { setValue: (v: string) => graphiql.set({ response: v }) };
  const queryEditor = { setValue: () => undefined, getValue: () => typed.text };
  const g = {
    state: {} as StoreState,
    fetcher: null as null | ((p: unknown, o: unknown) => unknown),
    reset() {
      g.state = {
        isFetching: false,
        subscription: null,
        queryId: 0,
        response: "",
        queryEditor,
        responseEditor,
        schema: null,
        tabs: [{ title: "Query", query: "{ orders { id } }" }],
        activeTabIndex: 0,
      };
    },
    set(part: Partial<StoreState>) {
      g.state = { ...g.state, ...part };
      listeners.forEach((l) => l());
    },
    subscribe(l: () => void) {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    actions: {
      addTab: () => undefined,
      updateActiveTabValues: () => undefined,
      stop() {
        g.state.subscription?.unsubscribe();
        g.set({ isFetching: false, subscription: null });
      },
      async run() {
        if (g.state.subscription) {
          g.actions.stop();
          return;
        }
        const id = g.state.queryId + 1;
        g.set({ queryId: id, response: "", isFetching: true });
        const show = (result: unknown) => {
          if (id !== g.state.queryId) return;
          g.set({ isFetching: false, response: JSON.stringify(result) });
        };
        try {
          const value = (await g.fetcher!({ query: typed.text }, { headers: {} })) as
            | AsyncIterable<unknown>
            | unknown;
          if (value && typeof value === "object" && Symbol.asyncIterator in value) {
            const iterable = value as AsyncIterable<unknown>;
            g.set({
              subscription: { unsubscribe: () => void iterable[Symbol.asyncIterator]().return?.() },
            });
            for await (const result of iterable) show(result);
            g.set({ isFetching: false, subscription: null });
          } else show(value);
        } catch (error) {
          g.set({ isFetching: false, response: `error: ${String(error)}`, subscription: null });
        }
      },
    },
  };
  g.reset();
  return g;
});

vi.mock("graphiql", () => {
  const GraphiQL = ({
    children,
    fetcher,
  }: {
    children?: React.ReactNode;
    fetcher: (p: unknown, o: unknown) => unknown;
  }) => {
    React.useEffect(() => {
      graphiql.fetcher = fetcher;
    }, [fetcher]);
    const state = React.useSyncExternalStore(graphiql.subscribe, () => graphiql.state);
    return (
      <div>
        {!(state.isFetching || state.subscription) && (
          <button className="graphiql-execute-button" onClick={() => void graphiql.actions.run()}>
            execute
          </button>
        )}
        {state.isFetching && <span data-testid="graphiql-fetching" />}
        <pre data-testid="graphiql-response">{state.response}</pre>
        {children}
      </div>
    );
  };
  const Part = ({ children }: { children?: React.ReactNode }) => <div>{children}</div>;
  const Toolbar = ({
    children,
  }: {
    children?: React.ReactNode | ((p: Record<string, React.ReactNode>) => React.ReactNode);
  }) => (
    <div>
      {typeof children === "function"
        ? children({ prettify: null, merge: null, copy: null })
        : children}
    </div>
  );
  GraphiQL.Footer = Part;
  GraphiQL.Toolbar = Toolbar;
  GraphiQL.Logo = Part;
  return { GraphiQL };
});
vi.mock("@graphiql/react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@graphiql/react")>()),
  useGraphiQL: (selector: (s: StoreState) => unknown) =>
    React.useSyncExternalStore(graphiql.subscribe, () => selector(graphiql.state)),
  useGraphiQLActions: () => graphiql.actions,
}));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "org_admin" }, selectedRoles: [{ id: "org_admin" }] }),
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

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

function serve() {
  const f = vi.fn(async (url: string, _init?: RequestInit) =>
    String(url).endsWith("/data/graphql") ? json({ data: { orders: [{ id: "real" }] } }) : json({}),
  );
  vi.stubGlobal("fetch", f);
  return f;
}
const posted = (f: ReturnType<typeof serve>) =>
  f.mock.calls.filter(([u, i]) => String(u).endsWith("/data/graphql") && i?.method === "POST");

async function runWith(text: string) {
  typed.text = text;
  const f = serve();
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/query"]}>
        <QueryPage />
      </MemoryRouter>
    </MantineProvider>,
  );
  fireEvent.click(await screen.findByText("execute"));
  return f;
}

describe("GraphQL editor with no statement", () => {
  beforeEach(() => {
    localStorage.clear();
    graphiql.reset();
  });

  it.each([
    ["blank", "  \n"],
    ["comment-only", "# Welcome to GraphiQL\n# type a query here\n"],
  ])("%s: the response says there is no query and nothing is sent", async (_n, text) => {
    const f = await runWith(text);
    await waitFor(() =>
      expect(screen.getByTestId("graphiql-response")).toHaveTextContent(
        "There is no query to run.",
      ),
    );
    expect(screen.getByTestId("graphiql-response")).not.toHaveTextContent("Syntax Error");
    expect(posted(f)).toHaveLength(0);
  });

  it("a real query still runs", async () => {
    const f = await runWith("# c\n{ orders { id } }");
    await waitFor(() => expect(screen.getByTestId("graphiql-response")).toHaveTextContent("real"));
    expect(posted(f)).toHaveLength(1);
  });
});
