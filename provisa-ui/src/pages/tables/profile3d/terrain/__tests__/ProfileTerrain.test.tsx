// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the terrain tab — the scene is driven from the panel, and the view expands to a modal.
// jsdom has no WebGL, so the three.js scene is replaced by a recorder.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../../../test-utils/render";

const scenes = vi.hoisted(() => [] as Record<string, ReturnType<typeof vi.fn>>[]);
vi.mock("../terrainScene", () => ({
  TerrainScene: class {
    calls: Record<string, ReturnType<typeof vi.fn>> = {
      setData: vi.fn(),
      setOptions: vi.fn(),
      setOrder: vi.fn(),
      setSelected: vi.fn(),
      setHover: vi.fn(),
      setRotate: vi.fn(),
      setTheme: vi.fn(),
      resetView: vi.fn(),
      focus: vi.fn(),
      dispose: vi.fn(),
    };
    constructor() {
      Object.assign(this, this.calls);
      scenes.push(this.calls);
    }
  },
}));

import { ProfileTerrain } from "../ProfileTerrain";

const RESULTS = {
  columns: [
    {
      column_name: "amount",
      family: "numeric",
      data_type: "double",
      row_count: 1000,
      null_count: 0,
      null_share: 0,
      distinct_count: 900,
    },
    {
      column_name: "status",
      family: "text",
      data_type: "varchar",
      row_count: 1000,
      null_count: 200,
      null_share: 0.2,
      distinct_count: 2,
    },
  ],
  quantiles: Array.from({ length: 101 }, (_, i) => ({
    column_name: "amount",
    measure: "value",
    q: i / 100,
    value: i,
  })),
  top_values: [
    { column_name: "status", kind: "frequency", rank: 1, value: "open", row_count: 500 },
    { column_name: "status", kind: "frequency", rank: 2, value: "closed", row_count: 300 },
  ],
};

beforeEach(() => {
  scenes.length = 0;
});

describe("ProfileTerrain", () => {
  it("builds the scene from the run and selects the first column", async () => {
    render(<ProfileTerrain results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(scenes).toHaveLength(1));
    const [cols, order] = scenes[0].setData.mock.calls[0];
    expect(cols.map((c: { name: string }) => c.name)).toEqual(["amount", "status"]);
    expect(order).toEqual(["amount", "status"]);
    expect(screen.getByTestId("terrain-detail")).toHaveTextContent("amount");
    expect(screen.getAllByTestId(/^terrain-bar-/)).toHaveLength(40);
  });

  it("selects a column from the list and lights a value's slice from its row", async () => {
    render(<ProfileTerrain results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("terrain-row-status"));
    expect(screen.getByTestId("terrain-detail")).toHaveTextContent("status");
    await waitFor(() => expect(scenes[0].setSelected).toHaveBeenLastCalledWith("status"));
    fireEvent.mouseEnter(screen.getByTestId("terrain-cat-1"));
    await waitFor(() =>
      expect(scenes[0].setHover).toHaveBeenLastCalledWith({ column: "status", bin: 1 }),
    );
  });

  it("re-orders the ridges and zooms to a column", async () => {
    render(<ProfileTerrain results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("terrain-row-status"));
    fireEvent.click(screen.getByTestId("terrain-focus"));
    await waitFor(() => expect(scenes[0].focus).toHaveBeenCalledWith("status"));
    expect(scenes[0].setRotate).toHaveBeenLastCalledWith(false);
  });

  it("opens the same terrain in a modal with a scene of its own", async () => {
    render(<ProfileTerrain results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("terrain-expand"));
    expect(await screen.findByTestId("terrain-expanded")).toBeInTheDocument();
    await waitFor(() => expect(scenes).toHaveLength(2));
    expect(screen.getByText("Profile terrain of orders")).toBeInTheDocument();
  });

  it("says so when the run profiled no columns", () => {
    render(<ProfileTerrain results={{ columns: [] }} tableName="orders" />);
    expect(screen.getByTestId("terrain-empty")).toBeInTheDocument();
    expect(scenes).toHaveLength(0);
  });
});
