// Copyright (c) 2026 Kenneth Stott
// Canary: 5202b1cf-4d78-45e9-91c4-69b62db68874
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D graph's view settings are remembered across reloads, per graph (graphSettings.ts): restored
// as the reader left them, a selection the graph no longer offers dropped rather than re-added, the
// defaults where storage is unavailable, and cleared by "Set Defaults".

import { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, renderHook, screen, waitFor } from "../../../../test-utils/render";
import { getStored, setStored } from "../../../../lib/browserStorage";
import {
  chosenNow,
  restoreSettings,
  selectionToSave,
  useGraphSettings,
  type GraphKey,
} from "../graphSettings";
import type { GraphData } from "../../../../components/graph3d/graphModel";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as Calls[] }));
vi.mock("../../../../components/graph3d/graphScene", () => ({
  GraphScene: class {
    constructor() {
      const calls: Calls = {};
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFrontal",
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));

import { GraphExplorer } from "../../../../components/graph3d/GraphExplorer";
import { GLOSSARY_SCOPE } from "../../../../components/graph3d/graphModel";

const TERRAIN: GraphKey = { kind: "profile-terrain", subject: 7 };
const DEFAULTS = { log: false as boolean, mode: "peak", excluded: [] as string[] };

afterEach(() => {
  vi.restoreAllMocks();
  made.scenes.length = 0;
});

describe("a graph's remembered settings", () => {
  it("are restored as they were left, after a reload, for that graph alone", () => {
    const first = renderHook(() => useGraphSettings(TERRAIN, DEFAULTS));
    act(() => first.result.current[1]("log", true));
    act(() => first.result.current[1]("excluded", ["status"]));
    first.unmount();

    const again = renderHook(() => useGraphSettings(TERRAIN, DEFAULTS));
    expect(again.result.current[0]).toEqual({ log: true, mode: "peak", excluded: ["status"] });
    // Another table's terrain keeps its own.
    const other = renderHook(() =>
      useGraphSettings({ kind: "profile-terrain", subject: 8 }, DEFAULTS),
    );
    expect(other.result.current[0]).toEqual(DEFAULTS);
  });

  it("keep a saved value only where it has its default's shape", () => {
    expect(restoreSettings(DEFAULTS, { log: "yes", mode: "share", excluded: [3] })).toEqual({
      log: false,
      mode: "share",
      excluded: [],
    });
  });

  it("drop a selection the graph no longer offers, and keep 'every choice' open to new ones", () => {
    expect(chosenNow(["sales", "gone"], ["sales", "finance"])).toEqual(["sales"]);
    expect(chosenNow(null, ["sales", "finance", "new"])).toEqual(["sales", "finance", "new"]);
    expect(selectionToSave(["finance", "sales"], ["sales", "finance"])).toBeNull();
    expect(selectionToSave(["sales"], ["sales", "finance"])).toEqual(["sales"]);
  });

  it("clear on reset, back to the defaults", () => {
    const view = renderHook(() => useGraphSettings(TERRAIN, DEFAULTS));
    act(() => view.result.current[1]("mode", "share"));
    expect(getStored("graph3d.profile-terrain.7")).not.toBeNull();
    act(() => view.result.current[2]());
    expect(view.result.current[0]).toEqual(DEFAULTS);
    expect(getStored("graph3d.profile-terrain.7")).toBeNull();
  });

  it("are the defaults where storage is unavailable, and still apply while the graph is open", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    const view = renderHook(() => useGraphSettings(TERRAIN, DEFAULTS));
    expect(view.result.current[0]).toEqual(DEFAULTS);
    act(() => view.result.current[1]("log", true));
    expect(view.result.current[0].log).toBe(true);
  });

  it("follow one another when the same graph is open twice", () => {
    const inPlace = renderHook(() => useGraphSettings(TERRAIN, DEFAULTS));
    const expanded = renderHook(() => useGraphSettings(TERRAIN, DEFAULTS));
    act(() => expanded.result.current[1]("mode", "share"));
    expect(inPlace.result.current[0].mode).toBe("share");
  });
});

describe("the Ontology View's remembered settings", () => {
  const g: GraphData = {
    nodes: [
      { id: "term:1", type: "term", label: "Customer", domains: ["sales"] },
      { id: "term:2", type: "term", label: "Supplier", domains: ["procurement"] },
      { id: "term:3", type: "term", label: "Invoice", domains: ["finance"] },
    ],
    edges: [],
  };
  const explorer = () => (
    <GraphExplorer
      data={g}
      scope={GLOSSARY_SCOPE}
      preset="ontology"
      settingsKey={{ kind: "ontology", subject: null }}
      exportName="ontology"
      testId="x"
    />
  );
  const drawn = () =>
    (made.scenes[made.scenes.length - 1].setData.mock.lastCall?.[0] as GraphData).nodes.map(
      (n) => n.id,
    );

  it("restore a domain filter, dropping a domain the ontology no longer has", async () => {
    setStored("graph3d.ontology.org", JSON.stringify({ domains: ["sales", "marketing"] }));
    render(explorer());
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    await waitFor(() => expect(drawn()).toEqual(["term:1"]));
    expect(screen.getByTestId("x-filter-domain")).toHaveTextContent("sales");
  });

  it("forget everything on Set Defaults", async () => {
    render(explorer());
    fireEvent.click(screen.getByRole("checkbox", { name: "Rotate" }));
    expect(screen.getByRole("checkbox", { name: "Rotate" })).toBeChecked();
    expect(getStored("graph3d.ontology.org")).not.toBeNull();
    fireEvent.click(screen.getByTestId("x-reset-settings"));
    expect(screen.getByRole("checkbox", { name: "Rotate" })).not.toBeChecked();
    expect(getStored("graph3d.ontology.org")).toBeNull();
  });
});
