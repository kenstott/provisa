// Copyright (c) 2026 Kenneth Stott
// Canary: 47f9559a-3661-456a-bb63-15fb8f0822fd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1224: a Cypher result over the large-result threshold is answered with a handle to the file
// it was delivered to and no rows. The graph explorer read only `rows`, so such a query -- a
// 1,500-row view, or a disconnected OPTIONAL MATCH crossing it with 200 customers -- drew an empty
// frame, as if it matched nothing. The frame now carries the handle and says so.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import type { FrameData } from "../components/graph/graph-model";

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
// The page's frames, as it hands them to their component.
const framesSeen: FrameData[] = [];
vi.mock("../components/graph/GraphFrame", () => ({
  GraphFrame: ({ frame }: { frame: FrameData }) => {
    framesSeen.push(frame);
    return null;
  },
}));
vi.mock("../components/graph/GraphSidebar", () => ({ Sidebar: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";

const QUERY =
  "MATCH (n:Shop:ViewSlowestPages) OPTIONAL MATCH (mCustomers:Shop:Customers) RETURN n, mCustomers";

function Run() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query: QUERY, autoRun: true } })}>
      run
    </button>
  );
}

/** /data/cypher's answer for a query of one statement: one result. */
const oneStatement = (body: unknown) => ({ results: [{ statement: "MATCH", status: 200, body }] });

describe("GraphPage — a result delivered to a file", () => {
  beforeEach(() => {
    localStorage.clear();
    framesSeen.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        const body = String(url).includes("/data/cypher")
          ? oneStatement({
              type: "cypher",
              columns: [],
              rows: [],
              redirect: {
                sink: "local",
                redirect_url: "/data/redirect-file/0123456789abcdef0123456789abcdef.csv",
                row_count: 300000,
                expires_in: 3600,
                content_type: "text/csv",
              },
            })
          : {};
        return new Response(JSON.stringify(body), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );
  });

  it("keeps the handle on the frame instead of an empty result", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <Run />
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    act(() => screen.getByText("run").click());
    await waitFor(() =>
      expect(framesSeen.some((f) => f.status === "done" && f.query === QUERY)).toBe(true),
    );
    const done = framesSeen.filter((f) => f.status === "done" && f.query === QUERY).at(-1)!;
    expect(done.rows).toEqual([]);
    expect(done.redirect).toEqual({
      url: "/data/redirect-file/0123456789abcdef0123456789abcdef.csv",
      rowCount: 300000,
      expiresIn: 3600,
      contentType: "text/csv",
    });
  });
});

describe("GraphFrame — a result delivered to a file", () => {
  it("says how many rows went to the file, with the link, where the graph would be", async () => {
    const { GraphFrame } = await vi.importActual<typeof import("../components/graph/GraphFrame")>(
      "../components/graph/GraphFrame",
    );
    const frame: FrameData = {
      id: "f1",
      query: QUERY,
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows: [],
      columns: [],
      redirect: {
        url: "/data/redirect-file/0123456789abcdef0123456789abcdef.csv",
        rowCount: 300000,
        expiresIn: 3600,
        contentType: "text/csv",
      },
    };
    render(
      <MantineProvider>
        <MemoryRouter>
          <GraphFrame
            frame={frame}
            onClose={() => {}}
            onRerun={() => {}}
            colorOverrides={{}}
            sizeOverrides={{}}
            labelProperty={{}}
            sizeByProperty={{}}
            sizeMultiplier={{}}
            relLineOverrides={{}}
            onColorChange={() => {}}
            pkMap={{}}
            labelToTableLabel={{}}
          />
        </MemoryRouter>
      </MantineProvider>,
    );
    const notice = await screen.findByTestId("result-redirected");
    expect(notice.querySelector("a")?.getAttribute("href")).toBe(
      "/data/redirect-file/0123456789abcdef0123456789abcdef.csv",
    );
  });
});
