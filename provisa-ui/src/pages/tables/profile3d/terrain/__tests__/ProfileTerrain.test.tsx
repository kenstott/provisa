// Copyright (c) 2026 Kenneth Stott
// Canary: 4bda1d9c-9d6c-49da-a964-75ed8ca4e3d4
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
    render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
    await waitFor(() => expect(scenes).toHaveLength(1));
    const [cols, order] = scenes[0].setData.mock.calls[0];
    expect(cols.map((c: { name: string }) => c.name)).toEqual(["amount", "status"]);
    expect(order).toEqual(["amount", "status"]);
    expect(screen.getByTestId("terrain-detail")).toHaveTextContent("amount");
    expect(screen.getAllByTestId(/^terrain-bar-/)).toHaveLength(40);
  });

  it("selects a column from the list and lights a value's slice from its row", async () => {
    render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
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
    render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
    await waitFor(() => expect(scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("terrain-row-status"));
    fireEvent.click(screen.getByTestId("terrain-focus"));
    await waitFor(() => expect(scenes[0].focus).toHaveBeenCalledWith("status"));
    expect(scenes[0].setRotate).toHaveBeenLastCalledWith(false);
  });

  it("opens the same terrain in a modal with a scene of its own", async () => {
    render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
    await waitFor(() => expect(scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("terrain-expand"));
    expect(await screen.findByTestId("terrain-expanded")).toBeInTheDocument();
    await waitFor(() => expect(scenes).toHaveLength(2));
    expect(screen.getByText("Profile terrain of orders")).toBeInTheDocument();
  });

  describe("which columns the terrain draws", () => {
    const drawn = () => scenes[0].setOrder.mock.lastCall?.[0] as string[];

    it("leaves an unchecked column out, the rest side by side, and still inspects it", async () => {
      render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
      await waitFor(() => expect(scenes).toHaveLength(1));
      expect(screen.getByTestId("terrain-include-status")).toBeChecked();
      fireEvent.click(screen.getByTestId("terrain-include-status"));
      expect(screen.getByTestId("terrain-include-status")).not.toBeChecked();
      await waitFor(() => expect(drawn()).toEqual(["amount"]));
      // Still listed, and still the one inspected once picked.
      fireEvent.click(screen.getByTestId("terrain-row-status"));
      expect(screen.getByTestId("terrain-detail")).toHaveTextContent("status");
      expect(screen.getByLabelText("Show status in the terrain")).toBe(
        screen.getByTestId("terrain-include-status"),
      );
    });

    it("puts every column in or takes every one out from the list head", async () => {
      render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
      await waitFor(() => expect(scenes).toHaveLength(1));
      fireEvent.click(
        screen.getByRole("button", { name: "Leave every column out of the terrain" }),
      );
      await waitFor(() => expect(drawn()).toEqual([]));
      fireEvent.click(screen.getByRole("button", { name: "Show every column in the terrain" }));
      await waitFor(() => expect(drawn()).toEqual(["amount", "status"]));
    });

    it("keeps the choice across reloads, per table", async () => {
      const first = render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
      await waitFor(() => expect(scenes).toHaveLength(1));
      fireEvent.click(screen.getByTestId("terrain-include-status"));
      first.unmount();
      const again = render(<ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />);
      expect(screen.getByTestId("terrain-include-status")).not.toBeChecked();
      again.unmount();
      render(<ProfileTerrain results={RESULTS} tableId={8} tableName="invoices" />);
      expect(screen.getByTestId("terrain-include-status")).toBeChecked();
    });
  });

  it("says so when the run profiled no columns", () => {
    render(<ProfileTerrain results={{ columns: [] }} tableId={7} tableName="orders" />);
    expect(screen.getByTestId("terrain-empty")).toBeInTheDocument();
    expect(scenes).toHaveLength(0);
  });
});
