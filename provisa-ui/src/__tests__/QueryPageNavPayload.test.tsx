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
    selector({ queryEditor: editor, schema: null, tabs: [], activeTabIndex: 0 }),
  useGraphiQLActions: () => actions,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "org_admin" } }) }));
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

function Polly() {
  const navigate = useNavigate();
  return (
    <button
      onClick={() => navigate("/query", { state: { query: "{ orders { id } }", autoRun: true } })}
    >
      polly
    </button>
  );
}

describe("QueryPage — a hand-off while the page is open", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal("fetch", vi.fn(async () => new Response("{}", { status: 200 })));
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
    await waitFor(() => expect(editor.setValue).toHaveBeenCalledWith("{ orders { id } }"));
    await waitFor(() => expect(actions.run).toHaveBeenCalledTimes(1));

    act(() => screen.getByText("polly").click());
    await waitFor(() => expect(actions.addTab).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(actions.run).toHaveBeenCalledTimes(2));
  });
});
