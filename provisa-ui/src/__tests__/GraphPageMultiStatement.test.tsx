// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3fcbf4-0a9f-4527-a927-faff08b68e90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Cypher page with several statements in its box, as Neo4j Browser's editor holds them: the
// server answers each on its own (results: "each"), and the frame shows one at a time under a tab
// per statement, a failed one saying why while the others' rows still show.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import type { FrameData } from "../components/graph/graph-model";
import { render as renderWithProviders } from "../test-utils/render";

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
const shown: { pick: ((id: string, i: number) => void) | null } = { pick: null };
vi.mock("../components/graph/GraphFrame", () => ({
  GraphFrame: ({
    frame,
    onStatement,
  }: {
    frame: FrameData;
    onStatement?: (id: string, i: number) => void;
  }) => {
    framesSeen.push(frame);
    shown.pick = onStatement ?? null;
    return null;
  },
}));
vi.mock("../components/graph/GraphSidebar", () => ({ Sidebar: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";
import { FrameStatementTabs } from "../components/graph/FrameParts";

const QUERY = "MATCH (p:Pets) WHERE p.note = 'a;b' RETURN p.name;\nMATCH (s:Secret) RETURN s";
const sent: unknown[] = [];

function Run() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query: QUERY, autoRun: true } })}>
      run
    </button>
  );
}

describe("the Cypher page with two statements", () => {
  beforeEach(() => {
    localStorage.clear();
    framesSeen.length = 0;
    sent.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        let body: unknown = {};
        if (String(url).includes("/data/cypher")) {
          sent.push(JSON.parse(String(init?.body)));
          body = {
            results: [
              {
                statement: "MATCH (p:Pets) WHERE p.note = 'a;b' RETURN p.name",
                status: 200,
                body: { columns: ["p.name"], rows: [{ "p.name": "Rex" }, { "p.name": "Tom" }] },
              },
              {
                statement: "MATCH (s:Secret) RETURN s",
                status: 403,
                body: {
                  detail: "Label Secret is not visible to role org_admin",
                  code: "cypher.permission_denied",
                  params: {},
                },
              },
            ],
          };
        }
        return new Response(JSON.stringify(body), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );
  });

  it("asks for each statement's result, and shows each under its own tab", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <Run />
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    act(() => screen.getByText("run").click());
    await waitFor(() => expect(framesSeen.some((f) => f.statements?.length === 2)).toBe(true));
    expect(sent).toEqual([{ query: QUERY, params: {}, results: "each" }]);
    const frame = framesSeen.filter((f) => f.statements).at(-1)!;
    // The first statement's rows, though the second failed; the frame keeps the whole text.
    expect(frame.query).toBe(QUERY);
    expect(frame.status).toBe("done");
    expect(frame.rows).toEqual([{ "p.name": "Rex" }, { "p.name": "Tom" }]);
    expect(frame.statements![1]).toMatchObject({
      status: "error",
      error: "Label Secret is not visible to role org_admin",
    });

    act(() => shown.pick!(frame.id, 1));
    await waitFor(() => expect(framesSeen.at(-1)!.statementIndex).toBe(1));
    expect(framesSeen.at(-1)).toMatchObject({
      status: "error",
      error: "Label Secret is not visible to role org_admin",
      rows: [],
    });
  });

  it("names each tab by the label its statement matches, with its rows or that it failed", () => {
    const frame: FrameData = {
      id: "f1",
      query: QUERY,
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows: [],
      columns: [],
      statementIndex: 0,
      statements: [
        {
          query: "MATCH (p:Pets) RETURN p.name",
          status: "done",
          nodes: new Map(),
          edges: new Map(),
          rows: [{ "p.name": "Rex" }, { "p.name": "Tom" }],
          columns: ["p.name"],
        },
        {
          query: "MATCH (s:Secret) RETURN s",
          status: "error",
          nodes: new Map(),
          edges: new Map(),
          rows: [],
          columns: [],
          error: "not visible",
        },
      ],
    };
    const pick = vi.fn();
    renderWithProviders(<FrameStatementTabs frame={frame} onStatement={pick} />);
    expect(screen.getByTestId("graph-statement-tab-0")).toHaveTextContent("Result 1 · Pets");
    expect(screen.getByTestId("graph-statement-tab-rows-0")).toHaveTextContent("2");
    expect(screen.getByTestId("graph-statement-tab-1")).toHaveTextContent("Result 2 · Secret");
    expect(screen.getByTestId("graph-statement-tab-failed-1")).toBeInTheDocument();
    act(() => screen.getByTestId("graph-statement-tab-1").click());
    expect(pick).toHaveBeenCalledWith("f1", 1);
  });
});
