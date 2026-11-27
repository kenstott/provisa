// Copyright (c) 2026 Kenneth Stott
// Canary: ad4b83c3-48eb-49c5-af8f-b41b333b7860
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The dialog a 3D view, a graph or a grid opens in never scrolls: a column the window's height less
// its offsets, its body taking what the header leaves.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../../test-utils/render";
import { FitModal } from "../Expandable3D";

describe("the fit modal", () => {
  it("is a column of fixed height whose body takes the rest, and neither scrolls", async () => {
    render(
      <FitModal opened onClose={vi.fn()} title="Terrain" testId="fit">
        <div data-testid="inside" />
      </FitModal>,
    );
    const inside = await screen.findByTestId("inside");
    // The shared dialog layout (ModalLayout, REQ-2277 rule 10) makes the content a column that
    // does not scroll (App.css `.modal-layout__content`); `fill` keeps its body from scrolling as a
    // whole (`.modal-layout__scroll--fill`), so the view takes the height the header leaves.
    const scroll = inside.closest("[data-modal-scroll]") as HTMLElement;
    const content = inside.closest(".mantine-Modal-content") as HTMLElement;
    expect(content).toHaveClass("modal-layout__content");
    expect(content).toHaveStyle({ height: "95dvh" });
    expect(scroll).toHaveClass("modal-layout__scroll", "modal-layout__scroll--fill");
  });
});
