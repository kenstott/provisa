// Copyright (c) 2026 Kenneth Stott
// Canary: a3d6f019-4b28-4e7c-8c51-7f02e9b4d6c3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A frame shows only the answer to its latest request. A frame run again while its first request
// is still out asks again; when the first answer comes after the second, it is dropped, and the
// first request is aborted, since nothing will read it.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import "../i18n";
import type { FrameData } from "../components/graph/graph-model";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "all" }, selectedRoles: [{ id: "all" }], loading: false }),
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
// The frame as far as this test reads it: its state, the columns it shows, and a way to run it
// again with another query (as editing its query and pressing Enter does).
vi.mock("../components/graph/GraphFrame", () => ({
  GraphFrame: ({
    frame,
    onRerun,
  }: {
    frame: FrameData;
    onRerun: (id: string, query: string) => void;
  }) => (
    <div>
      <span data-testid="frame-status">{frame.status}</span>
      <span data-testid="frame-columns">{frame.columns.join(",")}</span>
      <button onClick={() => onRerun(frame.id, SECOND)}>rerun</button>
    </div>
  ),
}));

import { GraphPage } from "../pages/GraphPage";
import { graphStateOf } from "../components/graph/graph-persistence";

const FIRST = "MATCH (n:PetStore:Pets) RETURN n.name AS first LIMIT 25";
const SECOND = "MATCH (n:PetStore:Pets) RETURN n.name AS second LIMIT 25";

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
const answerOf = (column: string) =>
  json({
    results: [{ statement: column, status: 200, body: { rows: [], columns: [column] } }],
  });

interface Held {
  query: string;
  signal?: AbortSignal;
  answer: (r: Response) => void;
}

function serve(): Held[] {
  const held: Held[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (String(url).includes("/data/cypher")) {
        const { query } = JSON.parse(String(init?.body)) as { query: string };
        return new Promise<Response>((resolve) => {
          held.push({ query, signal: init?.signal ?? undefined, answer: resolve });
        });
      }
      if (String(url).includes("/data/graph-schema"))
        return json({ node_labels: [], relationship_types: [] });
      return json({});
    }),
  );
  return held;
}

function Run() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query: FIRST, autoRun: true } })}>
      run
    </button>
  );
}

describe("a frame run again while its first request is out", () => {
  beforeEach(() => {
    localStorage.clear();
    graphStateOf("cypher").frames = [];
  });

  it("shows the newer answer, never the older one that came after it", async () => {
    const held = serve();
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <Run />
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(screen.getByText("run"));
    await waitFor(() => expect(held).toHaveLength(1));
    fireEvent.click(await screen.findByText("rerun"));
    await waitFor(() => expect(held).toHaveLength(2));
    expect(held[1].query).toBe(SECOND);
    // The first request is no longer read: it is aborted.
    expect(held[0].signal?.aborted).toBe(true);

    await act(async () => held[1].answer(answerOf("second")));
    await waitFor(() => expect(screen.getByTestId("frame-columns")).toHaveTextContent("second"));
    await act(async () => held[0].answer(answerOf("first")));
    await act(async () => new Promise((r) => setTimeout(r, 20)));
    expect(screen.getByTestId("frame-columns")).toHaveTextContent("second");
    expect(screen.getByTestId("frame-status")).toHaveTextContent("done");
  });
});
