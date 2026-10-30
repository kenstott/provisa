// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7e41d2-6c35-4f9a-8d12-3a5e9c7f1b64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: the ERD opened on one table selects it and centres on it; a table the diagram does not
// hold is reported, never replaced by another.

import { describe, expect, it, vi } from "vitest";
import type { CyInstance } from "../components/graph/cytoscape-types";
import { focusErdTable } from "../components/erd/erd-focus";

function fakeCy(ids: string[]) {
  const unselect = vi.fn();
  const select = vi.fn();
  const center = vi.fn();
  const node = (id: string) => ({ empty: () => !ids.includes(id), select });
  const cy = {
    getElementById: (id: string) => node(id),
    $: () => ({ unselect }),
    center,
  };
  return { cy: cy as unknown as CyInstance, select, unselect, center };
}

describe("focusErdTable", () => {
  it("clears the selection, selects the table's node and centres on it", () => {
    const f = fakeCy(["t:7"]);
    expect(focusErdTable(f.cy, "7")).toBe(true);
    expect(f.unselect).toHaveBeenCalledOnce();
    expect(f.select).toHaveBeenCalledOnce();
    expect(f.center).toHaveBeenCalledOnce();
  });

  it("reports an unknown table and changes nothing", () => {
    const f = fakeCy(["t:7"]);
    expect(focusErdTable(f.cy, "99")).toBe(false);
    expect(f.unselect).not.toHaveBeenCalled();
    expect(f.select).not.toHaveBeenCalled();
    expect(f.center).not.toHaveBeenCalled();
  });
});
