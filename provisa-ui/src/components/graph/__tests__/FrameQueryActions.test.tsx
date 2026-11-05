// Copyright (c) 2026 Kenneth Stott
// Canary: 79393535-e6f6-4396-94a9-ba092ce17761
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A result frame's query, on hover, as the query bar's: copy the statement, and delete the frame
// (not in the expanded view, which closes rather than deletes). The hover itself is CSS
// (GraphPage.css .gf-query-editor-wrap:hover), rendered in the harness.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { FrameQueryActions } from "../FrameParts";

describe("a frame query's actions", () => {
  it("copy the statement and delete the frame", () => {
    const writeText = vi.fn(() => Promise.resolve());
    Object.assign(navigator, { clipboard: { writeText } });
    const onDelete = vi.fn();
    render(<FrameQueryActions query="MATCH (n) RETURN n" onDelete={onDelete} />);
    const actions = screen.getByTestId("graph-frame-query-actions");
    fireEvent.click(within(actions).getByRole("button", { name: "Copy query" }));
    expect(writeText).toHaveBeenCalledWith("MATCH (n) RETURN n");
    fireEvent.click(within(actions).getByRole("button", { name: "Delete this result" }));
    expect(onDelete).toHaveBeenCalledTimes(1);
  });

  it("offer no delete where the frame cannot be deleted from (its expanded view)", () => {
    render(<FrameQueryActions query="MATCH (n) RETURN n" />);
    expect(screen.queryByTestId("graph-frame-delete")).toBeNull();
    expect(screen.getByRole("button", { name: "Copy query" })).toBeInTheDocument();
  });
});
