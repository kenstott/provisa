// Copyright (c) 2026 Kenneth Stott
// Canary: 37139228-290b-4a34-9a53-52e88a705d37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A frame's Overview pills are buttons: a label's pill picks out its nodes in the frame's graph, a
// type's pill its edges, and the same pill again clears it. Which is on is said (aria-pressed).

import { describe, it, expect, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import { Inspector } from "../Inspector";
import type { CanvasHighlight } from "../canvas/canvas-types";

const OVERVIEW = {
  nodeCount: 3,
  edgeCount: 1,
  nodesByLabel: [
    ["Meta:RegisteredTables", 2],
    ["Meta:Domains", 1],
  ] as [string, number][],
  edgesByType: [["IS_DOMAIN", 1]] as [string, number][],
};

function show(highlight: CanvasHighlight | null, onHighlight = vi.fn()) {
  render(
    <Inspector
      selected={null}
      overviewData={OVERVIEW as never}
      highlight={highlight}
      onHighlight={onHighlight}
      colorOverrides={{}}
      onColorChange={() => {}}
      onClose={() => {}}
      width={320}
      onResizeStart={() => {}}
      relationships={[]}
      labelToTableId={{}}
    />,
  );
  return onHighlight;
}

describe("a frame's Overview pills", () => {
  it("pick out a label, or a type, in the frame's graph", () => {
    const onHighlight = show(null);
    fireEvent.click(screen.getByTestId("overview-pill-label-Meta:RegisteredTables"));
    expect(onHighlight).toHaveBeenLastCalledWith({
      kind: "label",
      value: "Meta:RegisteredTables",
    });
    fireEvent.click(screen.getByTestId("overview-pill-type-IS_DOMAIN"));
    expect(onHighlight).toHaveBeenLastCalledWith({ kind: "type", value: "IS_DOMAIN" });
  });

  it("say which is on, and clear it when the same pill is pressed again", () => {
    const onHighlight = show({ kind: "type", value: "IS_DOMAIN" });
    const on = screen.getByTestId("overview-pill-type-IS_DOMAIN");
    expect(on).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByTestId("overview-pill-label-Meta:Domains")).toHaveAttribute(
      "aria-pressed",
      "false",
    );
    fireEvent.click(on);
    expect(onHighlight).toHaveBeenLastCalledWith(null);
  });
});
