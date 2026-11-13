// Copyright (c) 2026 Kenneth Stott
// Canary: 7f4b9e21-08c6-4a3d-95e7-d2c8b1a60f34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A query running in the GraphQL explorer can be stopped. While it runs, Stop stands where
// GraphiQL's run button is; Stop aborts the request and the response pane says "Stopped": not an
// error, no longer running. An answer that comes after Stop is dropped.
//
// GraphiQL itself does not run in jsdom (Monaco). It is stood in for by its run and stop as
// @graphiql/react 0.37's execution store writes them (stores/execution.js): run() calls the
// fetcher, and an async iterable it returns is "subscribed", its answers written to the response
// unless another run has started since; stop() unsubscribes, which calls the iterator's return().

import React from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import "../i18n";

interface StoreState {
  isFetching: boolean;
  subscription: { unsubscribe: () => void } | null;
  queryId: number;
  response: string;
  [k: string]: unknown;
}

const graphiql = vi.hoisted(() => {
  const listeners = new Set<() => void>();
  const responseEditor = { setValue: (v: string) => graphiql.set({ response: v }) };
  const queryEditor = { setValue: () => undefined, getValue: () => "{ orders { id } }" };
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
          const value = (await g.fetcher!({ query: "{ orders { id } }" }, { headers: {} })) as
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

interface Held {
  signal?: AbortSignal;
  answer: (r: Response) => void;
}

function serve(honorAbort = true): Held[] {
  const held: Held[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (String(url).endsWith("/data/graphql") && init?.method === "POST")
        return new Promise<Response>((resolve, reject) => {
          held.push({ signal: init.signal ?? undefined, answer: resolve });
          // As fetch does, unless the answer is to come regardless (one already on its way).
          if (honorAbort) init.signal?.addEventListener("abort", () => reject(init.signal?.reason));
        });
      return json({});
    }),
  );
  return held;
}

async function startRun(honorAbort = true): Promise<Held[]> {
  const held = serve(honorAbort);
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/query"]}>
        <QueryPage />
      </MemoryRouter>
    </MantineProvider>,
  );
  expect(screen.queryByTestId("graphql-stop")).toBeNull();
  fireEvent.click(await screen.findByText("execute"));
  await waitFor(() => expect(held).toHaveLength(1));
  return held;
}

const LATE = () => json({ data: { orders: [{ id: "late" }] } });

describe("stopping a running GraphQL query", () => {
  beforeEach(() => {
    localStorage.clear();
    graphiql.reset();
  });

  it("shows Stop in place of the run button while the query runs", async () => {
    const held = await startRun();
    expect(await screen.findByTestId("graphql-stop")).toBeInTheDocument();
    await act(async () => held[0].answer(LATE()));
    await waitFor(() => expect(screen.getByTestId("graphiql-response")).toHaveTextContent("late"));
    expect(screen.queryByTestId("graphql-stop")).toBeNull();
  });

  it("aborts the request, and the response says Stopped, not an error and not running", async () => {
    const held = await startRun();
    fireEvent.click(await screen.findByTestId("graphql-stop"));
    expect(held[0].signal?.aborted).toBe(true);
    await waitFor(() =>
      expect(screen.getByTestId("graphiql-response")).toHaveTextContent(/^Stopped$/),
    );
    expect(screen.queryByTestId("graphiql-fetching")).toBeNull();
    expect(screen.queryByTestId("graphql-stop")).toBeNull();
    await act(async () => new Promise((r) => setTimeout(r, 20)));
    expect(screen.getByTestId("graphiql-response")).toHaveTextContent(/^Stopped$/);
  });

  it("drops an answer that comes after Stop", async () => {
    const held = await startRun(false);
    fireEvent.click(await screen.findByTestId("graphql-stop"));
    await act(async () => held[0].answer(LATE()));
    await act(async () => new Promise((r) => setTimeout(r, 20)));
    expect(screen.getByTestId("graphiql-response")).toHaveTextContent(/^Stopped$/);
    expect(screen.getByTestId("graphiql-response")).not.toHaveTextContent("late");
  });
});
