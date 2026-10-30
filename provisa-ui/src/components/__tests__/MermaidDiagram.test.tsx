// Copyright (c) 2026 Kenneth Stott
// Canary: 9a41c6d3-7e28-4b0f-a5d2-3c8e61f9b047
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A chart Mermaid cannot read is reported in the diagram's own place, with Mermaid's message —
// never as Mermaid's error graphic appended to the page body. Real Mermaid, no mock.
import { describe, it, expect } from "vitest";
import { render, screen } from "../../test-utils/render";
import { MermaidDiagram } from "../MermaidDiagram";

const BROKEN = 'flowchart TD\n  route{{"engine route"}}\n  p1["PROJECTION"] --> route\n  p2["a"b"]';

describe("MermaidDiagram parse failure", () => {
  it("shows the error box with Mermaid's message inside the diagram area", async () => {
    const before = document.body.childElementCount;
    const { container } = render(<MermaidDiagram chart={BROKEN} />);
    const box = await screen.findByTestId("mermaid-error", undefined, { timeout: 10000 });
    expect(box.textContent).toContain("Diagram unavailable");
    expect(box.querySelector("pre")?.textContent).toMatch(/Parse error/);
    expect(container.querySelector(".stats-mermaid")?.contains(box)).toBe(true);
    expect(container.querySelector(".stats-mermaid svg")).toBeNull();
    // render() mounts one container on the body; Mermaid added nothing beside it.
    expect(document.body.childElementCount).toBe(before + 1);
    expect(document.querySelector('[id^="dmermaid-dag-"]')).toBeNull();
  });
});
