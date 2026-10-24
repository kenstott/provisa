// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2a9d14-7b38-4c50-9f06-1d8e3b5c2a79
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A navigation to the Graph page (Polly's navigate) carries the page's view controls beside its
// query: group by, collapse every group, layout, max nodes. With a query, the view is the new
// frame's; with none, it is the newest frame's, whose query is kept and not run again.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, renderHook, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import type { FrameData, FrameView } from "../components/graph/graph-model";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "org_admin" },
    selectedRoles: [{ id: "org_admin" }],
    loading: false,
  }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useAllRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useTables: () => ({ tables: [], loading: false }),
}));
// A frame says what it was asked to show: the canvas itself needs a browser.
vi.mock("../components/graph/GraphFrame", () => ({
  GraphFrame: ({ frame, requestedView }: { frame: FrameData; requestedView?: FrameView }) => (
    <div data-testid="frame" data-query={frame.query}>
      {JSON.stringify(requestedView ?? null)}
    </div>
  ),
}));
vi.mock("../components/graph/GraphSidebar", () => ({ Sidebar: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";
import { useRequestedView } from "../components/graph/use-requested-view";
import { graphStateOf } from "../components/graph/graph-persistence";

const graphState = graphStateOf("cypher");
import { MAX_NODES_KEY } from "../components/graph/graph-drawing";
import { getStored } from "../lib/browserStorage";

const cypherCalls: string[] = [];

/** Polly's navigates, one button each, to the page already open. */
function Polly({ states }: { states: Record<string, unknown>[] }) {
  const navigate = useNavigate();
  return (
    <>
      {states.map((state, i) => (
        <button key={i} onClick={() => navigate("/graph", { state })}>
          {`polly ${i}`}
        </button>
      ))}
    </>
  );
}

function renderPage(...states: Record<string, unknown>[]) {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/graph"]}>
        <Polly states={states} />
        <GraphPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

describe("GraphPage: the view a navigation asks for", () => {
  beforeEach(() => {
    localStorage.clear();
    graphState.frames = [];
    graphState.history = [];
    cypherCalls.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        if (String(url).includes("/data/cypher")) cypherCalls.push(String(init?.body));
        return new Response(
          JSON.stringify({ columns: [], data: [], nodes: [], relationships: [] }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }),
    );
  });

  it("gives the query's frame its group by, collapse and layout, and sets max nodes", async () => {
    renderPage({
      query: "MATCH (c:Shop:Customers) RETURN c LIMIT 25",
      autoRun: true,
      view: { groupBy: "region", collapseGroups: true, layout: "hierarchy", maxNodes: 120 },
    });
    act(() => screen.getByText("polly 0").click());
    const frame = await screen.findByTestId("frame");
    expect(frame).toHaveAttribute("data-query", "MATCH (c:Shop:Customers) RETURN c LIMIT 25");
    expect(JSON.parse(frame.textContent ?? "null")).toMatchObject({
      groupBy: "region",
      collapseGroups: true,
      layout: "hierarchy",
    });
    expect(getStored(MAX_NODES_KEY)).toBe("120");
  });

  it("sets the view of the graph on screen without changing or rerunning its query", async () => {
    renderPage(
      { query: "MATCH (c:Shop:Customers) RETURN c LIMIT 25", autoRun: true },
      { view: { groupBy: "region" } },
    );
    act(() => screen.getByText("polly 0").click());
    await screen.findByTestId("frame");
    await waitFor(() => expect(cypherCalls).toHaveLength(1));
    act(() => screen.getByText("polly 1").click());
    await waitFor(() => expect(screen.getByTestId("frame").textContent).toContain("region"));
    expect(screen.getByTestId("frame")).toHaveAttribute(
      "data-query",
      "MATCH (c:Shop:Customers) RETURN c LIMIT 25",
    );
    expect(cypherCalls).toHaveLength(1);
  });
});

describe("useRequestedView: a frame's Group by", () => {
  it("is set once per request, and the viewer's own change then stands", () => {
    const setClusterLevel = vi.fn();
    const { rerender, result } = renderHook(
      ({ view }: { view?: FrameView }) => useRequestedView(view, setClusterLevel),
      { initialProps: { view: { groupBy: "region", layout: "hierarchy", seq: 1 } as FrameView } },
    );
    expect(setClusterLevel).toHaveBeenCalledWith("region");
    expect(result.current).toEqual({ layout: "hierarchy", collapseGroups: undefined, seq: 1 });
    rerender({ view: { groupBy: "region", layout: "hierarchy", seq: 1 } });
    expect(setClusterLevel).toHaveBeenCalledTimes(1);
    rerender({ view: { groupBy: null, seq: 2 } });
    expect(setClusterLevel).toHaveBeenLastCalledWith("none");
  });
});
