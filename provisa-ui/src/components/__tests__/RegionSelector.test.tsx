// Copyright (c) 2026 Kenneth Stott
// Canary: c0806e6f-851f-45f4-82f0-bd83bdd08b50
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1922: the region selector is absent when a deployment declares no regions, but a FAILED load
// of the region choices is an error to show, never silence — a 503 must not read as "no regions".

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../test-utils/render";
import { RegionSelector } from "../RegionSelector";
import { REGION_ALL } from "../../hooks/regionFilter";

describe("RegionSelector", () => {
  it("renders nothing when the deployment genuinely declares no regions", () => {
    render(
      <RegionSelector regions={[]} connected={null} value={REGION_ALL} onChange={vi.fn()} hidden={0} />,
    );
    // No control, no error, no count — the component returned null (the harness still injects Mantine
    // <style>, so assert on the component's own output rather than an empty container).
    expect(screen.queryByLabelText("Region")).toBeNull();
    expect(screen.queryByTestId("region-load-error")).toBeNull();
    expect(screen.queryByTestId("region-hidden-count")).toBeNull();
  });

  it("shows an error, not an empty absence, when the region choices failed to load", () => {
    render(
      <RegionSelector
        regions={[]}
        connected={null}
        value={REGION_ALL}
        onChange={vi.fn()}
        hidden={0}
        error
      />,
    );
    expect(screen.getByTestId("region-load-error")).toHaveTextContent(/could not be loaded/i);
    // The selector control itself is not rendered in the error state.
    expect(screen.queryByLabelText("Region")).toBeNull();
  });

  it("renders the selector and the hidden count when regions are present", () => {
    render(
      <RegionSelector
        regions={["eu", "us"]}
        connected="eu"
        value="us"
        onChange={vi.fn()}
        hidden={3}
      />,
    );
    // Mantine's Select exposes more than one "Region"-labelled input (the control plus a hidden
    // field), so assert at least one exists rather than exactly one.
    expect(screen.getAllByLabelText("Region").length).toBeGreaterThan(0);
    expect(screen.getByTestId("region-hidden-count")).toHaveTextContent("3");
    expect(screen.queryByTestId("region-load-error")).toBeNull();
  });
});
