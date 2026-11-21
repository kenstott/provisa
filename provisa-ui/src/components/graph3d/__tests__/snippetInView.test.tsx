// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e9d41-8c73-4f0a-b5d6-1e47a9c3f820
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128, REQ-2174: a snippet below the fold. The panel is on the page before it is seen, so the
// reader can scroll to it; only its read and its scene wait for it to come into view.

import { act } from "react";
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { DATA } from "./fixture";

const observed = vi.hoisted(() => ({ fire: [] as (() => void)[] }));
class FakeObserver {
  private cb: IntersectionObserverCallback;
  constructor(cb: IntersectionObserverCallback) {
    this.cb = cb;
  }
  observe() {
    observed.fire.push(() =>
      this.cb(
        [{ isIntersecting: true } as IntersectionObserverEntry],
        this as unknown as IntersectionObserver,
      ),
    );
  }
  disconnect() {}
}
vi.stubGlobal("IntersectionObserver", FakeObserver);

const reads = vi.hoisted(() => ({ meta: 0 }));
vi.mock("../ontologyData", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyData")>()),
  ontologyQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers] }),
  ontologyCountQuery: () => "count",
  ontologyGraph: () => DATA,
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => {
    reads.meta += 1;
    return { schema: null, roots: new Map() };
  },
  dataGraphql: async () => ({ _terms: [] }),
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));

import { OntologySnippet } from "../OntologyGraph";
import { productPreset } from "../ontologyPresets";

describe("a snippet not yet in view", () => {
  it("holds the panel's place under its test id, reads nothing, and draws once seen", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <OntologySnippet preset={productPreset("c360")} name="customer_360" testId="graph-panel" />,
    );
    // Before it is seen: the panel is there to scroll to, its view's height held, nothing read.
    const panel = screen.getByTestId("graph-panel");
    expect(panel.querySelector(".g3d-panel-view")).not.toBeNull();
    expect(screen.queryByTestId("graph-panel-stage")).not.toBeInTheDocument();
    expect(reads.meta).toBe(0);

    act(() => observed.fire.forEach((f) => f()));
    // Seen: it reads and draws, still one element under the panel's test id.
    await screen.findByTestId("graph-panel-stage", {}, { timeout: 10_000 });
    expect(reads.meta).toBeGreaterThan(0);
    expect(screen.getAllByTestId("graph-panel")).toHaveLength(1);
  });
});
