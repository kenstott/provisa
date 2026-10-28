// Copyright (c) 2026 Kenneth Stott
// Canary: a4e17c58-6d2b-4f93-8c05-1b7e9d3a2f60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2176: the graph session's pills -- the session's labels and relationship types, dashed and
// counted, a click running the same MATCH a model pill runs, Clear session dropping them all, and a
// re-read only when told a statement changed the session. The endpoint is stubbed.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import { SessionPills, type GraphSession } from "../SessionPills";

const FULL: GraphSession = {
  labels: [
    { label: "Draft", nodes: 3, tagged_model_nodes: 2 },
    { label: "Scratch", nodes: 1, tagged_model_nodes: 0 },
  ],
  types: [{ type: "LINKS", relationships: 4 }],
  empty: false,
};
const EMPTY: GraphSession = { labels: [], types: [], empty: true };

let current: GraphSession;
const calls: { url: string; method: string }[] = [];

beforeEach(() => {
  current = FULL;
  calls.length = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      calls.push({ url: String(url), method });
      if (method === "DELETE") {
        current = EMPTY;
        return new Response(JSON.stringify({ cleared: true }));
      }
      return new Response(JSON.stringify(current));
    }),
  );
});
afterEach(() => vi.unstubAllGlobals());

const mount = (changed = 0, handlers = { label: vi.fn(), rel: vi.fn() }) => {
  const view = render(
    <SessionPills
      roleHeader="analyst"
      changed={changed}
      onLabelClick={handlers.label}
      onRelClick={handlers.rel}
    />,
  );
  return { view, handlers };
};

describe("the session pills", () => {
  it("list the session's labels and types, dashed, with their counts", async () => {
    mount();
    const draft = await screen.findByTestId("graph-session-label-Draft");
    expect(draft).toHaveClass("graph-label-pill--session");
    // The nodes created and the model nodes tagged with it.
    expect(draft).toHaveTextContent("Draft(5)");
    expect(screen.getByTestId("graph-session-label-Scratch")).toHaveTextContent("Scratch(1)");
    const type = screen.getByTestId("graph-session-type-LINKS");
    expect(type).toHaveClass("graph-rel-badge--session");
    expect(type).toHaveTextContent("LINKS(4)");
  });

  it("run the same MATCH a model pill runs on a click", async () => {
    const { handlers } = mount();
    fireEvent.click(await screen.findByTestId("graph-session-label-Draft"));
    fireEvent.click(screen.getByTestId("graph-session-type-LINKS"));
    expect(handlers.label).toHaveBeenCalledWith("Draft");
    expect(handlers.rel).toHaveBeenCalledWith("LINKS");
  });

  it("read the session for the acting role, once, until a statement says it changed it", async () => {
    const { view } = mount(0);
    await screen.findByTestId("graph-session-label-Draft");
    expect(calls).toEqual([{ url: "/data/cypher/session", method: "GET" }]);
    current = { ...FULL, labels: [{ label: "Fresh", nodes: 1, tagged_model_nodes: 0 }] };
    view.rerender(
      <SessionPills roleHeader="analyst" changed={1} onLabelClick={vi.fn()} onRelClick={vi.fn()} />,
    );
    await screen.findByTestId("graph-session-label-Fresh");
    expect(calls.filter((c) => c.method === "GET")).toHaveLength(2);
  });

  it("clear the session with a DELETE, then show it empty", async () => {
    mount();
    fireEvent.click(await screen.findByTestId("graph-session-clear"));
    await screen.findByTestId("graph-session-empty");
    expect(calls.map((c) => c.method)).toEqual(["GET", "DELETE", "GET"]);
    expect(screen.queryByTestId("graph-session-clear")).toBeNull();
  });

  it("say so, in the server's words, where the session cannot be read", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () => new Response(JSON.stringify({ message: "no session store" }), { status: 500 }),
      ),
    );
    mount();
    await waitFor(() =>
      expect(screen.getByTestId("graph-session-error")).toHaveTextContent("no session store"),
    );
  });

  it("show an empty session as empty", async () => {
    current = EMPTY;
    mount();
    expect(await screen.findByTestId("graph-session-empty")).toBeInTheDocument();
  });
});
