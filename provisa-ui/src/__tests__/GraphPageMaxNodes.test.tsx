// Copyright (c) 2026 Kenneth Stott
// Canary: 9e3b7c15-4a62-4d8f-b0c1-5f2a8e6d9b37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The graph explorer draws a node result's rows, in order, up to "max nodes" distinct nodes, and
// says so over the canvas when the rows hold more. Max nodes is the viewer's setting in the side
// rail: uncapped, 300 by default, saved per scope and restored on a reload and on a return to the
// scope. A result that returns rows only (no graph columns) is not drawn, whatever its values.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import type { ReactNode } from "react";
import type { FrameData } from "../components/graph/graph-model";
import { bindStorageScope, getStored, setStored, storageScope } from "../lib/browserStorage";
import { MAX_NODES_KEY } from "../components/graph/graph-drawing";
import "../i18n";

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
// The canvas, as the frame hands it its note.
vi.mock("../components/graph/GraphCanvas", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph/GraphCanvas")>()),
  GraphCanvas: ({ note }: { note?: ReactNode }) => <div data-testid="canvas">{note}</div>,
}));
// The page's frames, as it hands them to their component.
const framesSeen: FrameData[] = [];
vi.mock("../components/graph/GraphFrame", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../components/graph/GraphFrame")>();
  return {
    GraphFrame: (props: Parameters<typeof actual.GraphFrame>[0]) => {
      framesSeen.push(props.frame);
      return <actual.GraphFrame {...props} />;
    },
  };
});
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";

const NODE = (id: number) => ({
  identity: `Customers:${id}`,
  id,
  label: "Customers",
  labels: ["Customers"],
  properties: { id },
});

function Run({ query }: { query: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query, autoRun: true } })}>run</button>
  );
}

function serve(answer: Record<string, unknown>) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      let body: unknown = {};
      // /data/cypher answers one result per statement; the query here is one statement.
      if (String(url).includes("/data/cypher"))
        body = { results: [{ statement: "MATCH", status: 200, body: answer }] };
      else if (String(url).includes("/data/graph-schema"))
        body = { node_labels: [], relationship_types: [] };
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }),
  );
}

function renderPage(query: string) {
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/graph"]}>
        <Run query={query} />
        <GraphPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

async function done(query: string): Promise<FrameData> {
  await waitFor(() =>
    expect(framesSeen.some((f) => f.status === "done" && f.query === query)).toBe(true),
  );
  return framesSeen.filter((f) => f.status === "done" && f.query === query).at(-1)!;
}

function renderPageOnly() {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/graph"]}>
        <GraphPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

const maxNodesField = () => screen.getByTestId("graph-max-nodes") as HTMLInputElement;

describe("GraphPage — max nodes", () => {
  beforeEach(() => {
    localStorage.clear();
    framesSeen.length = 0;
  });

  it("draws the rows up to max nodes distinct nodes and says so over the canvas", async () => {
    setStored(MAX_NODES_KEY, "2");
    serve({
      type: "cypher",
      columns: ["c"],
      rows: [{ c: NODE(1) }, { c: NODE(1) }, { c: NODE(2) }, { c: NODE(3) }],
      graph_columns: { c: "node" },
    });
    const query = "MATCH (c:Customers) RETURN c";
    renderPage(query);
    act(() => screen.getByText("run").click());
    const frame = await done(query);
    expect([...frame.nodes.keys()]).toEqual(["Customers:1", "Customers:2"]);
    expect(frame.truncated).toBe(true);
    const note = await screen.findByTestId("graph-cut-note");
    expect(note.textContent).toBe(
      "Showing the first 2 nodes; there are more. Raise the limit in the side menu.",
    );
    expect(screen.getByTestId("canvas").contains(note)).toBe(true);
  });

  it("draws every row whose nodes number at most max nodes, with no note", async () => {
    // 1,160 rows over 300 distinct nodes, under the default of 300.
    serve({
      type: "cypher",
      columns: ["c"],
      rows: Array.from({ length: 1160 }, (_, i) => ({ c: NODE((i % 300) + 1) })),
      graph_columns: { c: "node" },
    });
    const query = "MATCH (c:Customers)-[]-() RETURN c";
    renderPage(query);
    act(() => screen.getByText("run").click());
    const frame = await done(query);
    expect(frame.nodes.size).toBe(300);
    expect(frame.truncated).toBe(false);
  });

  it("is uncapped, 300 by default, and restored on a reload and on a return to its scope", async () => {
    const scope = storageScope();
    serve({ type: "cypher", columns: [], rows: [], graph_columns: {} });
    const first = renderPageOnly();
    fireEvent.click(screen.getByTestId("graph-sidebar-tab-settings"));
    await waitFor(() => expect(maxNodesField().value).toBe("300"));
    expect(screen.getByText("The most nodes a query draws")).toBeTruthy();
    fireEvent.change(maxNodesField(), { target: { value: "250000" } });
    await waitFor(() => expect(getStored(MAX_NODES_KEY)).toBe("250000"));
    first.unmount();

    // A reload: the page mounts again and reads it back.
    const reloaded = renderPageOnly();
    fireEvent.click(screen.getByTestId("graph-sidebar-tab-settings"));
    await waitFor(() => expect(maxNodesField().value).toBe("250000"));
    reloaded.unmount();

    // Another demo (scope) has its own setting, and this one's is there on the way back.
    try {
      bindStorageScope({ controlPlaneId: "cp-other-demo", orgId: "default" });
      const other = renderPageOnly();
      fireEvent.click(screen.getByTestId("graph-sidebar-tab-settings"));
      await waitFor(() => expect(maxNodesField().value).toBe("300"));
      other.unmount();
    } finally {
      bindStorageScope(scope);
    }
    renderPageOnly();
    fireEvent.click(screen.getByTestId("graph-sidebar-tab-settings"));
    await waitFor(() => expect(maxNodesField().value).toBe("250000"));
  });

  it("draws nothing of a rows-only result, however its values look", async () => {
    serve({
      type: "cypher",
      columns: ["c"],
      rows: [{ c: NODE(1) }],
      graph_columns: {},
    });
    const query = "MATCH (c:Customers) RETURN c {.*} AS c";
    renderPage(query);
    act(() => screen.getByText("run").click());
    const frame = await done(query);
    expect(frame.nodes.size).toBe(0);
    expect(frame.truncated).toBe(false);
  });
});
