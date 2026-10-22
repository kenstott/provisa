// Copyright (c) 2026 Kenneth Stott
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
    const body = inside.closest(".mantine-Modal-body") as HTMLElement;
    const content = inside.closest(".mantine-Modal-content") as HTMLElement;
    expect(content).toHaveStyle({ height: "95dvh", overflow: "hidden", display: "flex" });
    expect(body).toHaveStyle({ flex: "1", minHeight: "0", overflow: "hidden", display: "flex" });
  });
});
