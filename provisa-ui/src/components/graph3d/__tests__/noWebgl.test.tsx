// Copyright (c) 2026 Kenneth Stott
// Canary: c93230b5-063a-48f5-a9e1-41662a4a2de6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: a browser with no WebGL. jsdom has none, so the real three.js scene is built here and
// fails as it would there: the stage says so and logs it, and never claims to be ready.

import { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
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

vi.mock("../ontologyData", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyData")>()),
  // The query stands for its scope; the server below answers it with the graph read.
  ontologyQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers] }),
  ontologyCountQuery: () => "count",
  ontologyGraph: () => DATA,
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => ({ schema: null, roots: new Map() }),
  dataGraphql: async (query: string) => {
    void query;
    return { _terms: [] };
  },
}));

// The product's snippet reads the ontology as the acting roles.
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));

import { OntologySnippet } from "../OntologyGraph";
import { productPreset } from "../ontologyPresets";

afterEach(() => vi.restoreAllMocks());

describe("the 3D stage without WebGL", () => {
  it("shows that the 3D view could not start, logs it, and is never ready", async () => {
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <OntologySnippet preset={productPreset("c360")} name="customer_360" testId="graph-panel" />,
    );
    act(() => observed.fire.forEach((f) => f()));
    const alert = await screen.findByTestId("graph-panel-stage-error", {}, { timeout: 10_000 });
    expect(alert).toHaveTextContent("The 3D view could not start");
    expect(screen.getByTestId("graph-panel-stage")).not.toHaveAttribute("data-ready");
    expect(logged.mock.calls.some((c) => c[0] === "The 3D view could not start:")).toBe(true);
  });
});
