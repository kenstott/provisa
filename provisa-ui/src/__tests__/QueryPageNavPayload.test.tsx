// Copyright (c) 2026 Kenneth Stott
// Canary: 2d50fad0-11ce-457c-be98-c2695d7003b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A GraphQL query handed to the GraphQL explorer (NL "Open in GraphQL", Polly) opens in a new tab,
// and runs when the hand-off asks, whether the page was just opened or was already open.

import React from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useNavigate } from "react-router-dom";

const actions = { addTab: vi.fn(), updateActiveTabValues: vi.fn(), run: vi.fn() };
const editor = { setValue: vi.fn() };
// GraphiQL's own default formatter (DEFAULT_PRETTIFY_QUERY in @graphiql/react), which the
// toolbar's Prettify button calls through the store's onPrettifyQuery.
async function graphiqlPrettify(query: string): Promise<string> {
  const [prettier, { printers }, { parsers }] = await Promise.all([
    import("prettier/standalone"),
    import("prettier/plugins/graphql"),
    import("prettier/parser-graphql"),
  ]);
  return prettier.format(query, { parser: "graphql", plugins: [{ printers }, { parsers }] });
}

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
      queryEditor: editor,
      onPrettifyQuery: graphiqlPrettify,
      schema: null,
      tabs: [{ title: "Query", query: "" }],
      activeTabIndex: 0,
    }),
  useGraphiQLActions: () => actions,
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
// The footer widgets read GraphiQL's own store; they are not what this test is about.
vi.mock("../plugins/table-view", () => ({ ResponseTableOverlay: () => null }));
vi.mock("../plugins/headers-quick-insert", () => ({ HeadersQuickInsert: () => null }));

import { MantineProvider } from "@mantine/core";
import { QueryPage } from "../pages/QueryPage";

const PRETTY = "{\n  orders {\n    id\n  }\n}\n";

function Polly({
  query = "{ orders { id } }",
  autoRun = true,
}: {
  query?: string;
  autoRun?: boolean;
}) {
  const navigate = useNavigate();
  return <button onClick={() => navigate("/query", { state: { query, autoRun } })}>polly</button>;
}

function renderPage(polly: React.ReactElement = <Polly />) {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/query"]}>
        {polly}
        <QueryPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

describe("QueryPage — a hand-off while the page is open", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("{}", { status: 200 })),
    );
  });

  it("opens and runs the query each time one is handed over", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/query"]}>
          <Polly />
          <QueryPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    expect(editor.setValue).not.toHaveBeenCalled();

    act(() => screen.getByText("polly").click());
    await waitFor(() => expect(editor.setValue).toHaveBeenCalledWith(PRETTY));
    await waitFor(() => expect(actions.run).toHaveBeenCalledTimes(1));

    act(() => screen.getByText("polly").click());
    await waitFor(() => expect(actions.addTab).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(actions.run).toHaveBeenCalledTimes(2));
  });

  it("titles the new tab by the handed query's own operation", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/query"]}>
          <Polly />
          <QueryPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    act(() => screen.getByText("polly").click());
    // An anonymous query has no name: the title of the tab it was opened from is not kept.
    await waitFor(() =>
      expect(actions.updateActiveTabValues).toHaveBeenCalledWith({
        query: PRETTY,
        operationName: null,
      }),
    );
  });

  it("opens a one-line query as the Prettify button formats it, comments kept", async () => {
    const handed = "# complaints\n{ ps__complaints_2026_08(limit: 25) { complaint_id product } }";
    renderPage(<Polly query={handed} autoRun={false} />);
    act(() => screen.getByText("polly").click());
    const expected = await graphiqlPrettify(handed);
    expect(expected).toBe(
      "# complaints\n{\n  ps__complaints_2026_08(limit: 25) {\n    complaint_id\n    product\n  }\n}\n",
    );
    await waitFor(() => expect(editor.setValue).toHaveBeenCalledWith(expected));
    expect(actions.run).not.toHaveBeenCalled();
  });

  it("opens a query that does not parse exactly as written, and still runs it", async () => {
    const handed = "{ orders { id ";
    renderPage(<Polly query={handed} />);
    act(() => screen.getByText("polly").click());
    await waitFor(() => expect(editor.setValue).toHaveBeenCalledWith(handed));
    expect(actions.updateActiveTabValues).toHaveBeenCalledWith({
      query: handed,
      operationName: null,
    });
    await waitFor(() => expect(actions.run).toHaveBeenCalledTimes(1));
  });
});
