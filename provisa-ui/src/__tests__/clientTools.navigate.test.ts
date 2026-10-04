// Copyright (c) 2026 Kenneth Stott
// Canary: 5a7c2e90-1d38-4b64-9f0a-8e3b6d1c4f27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi } from "vitest";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";

// REQ-1846: navigate passes an optional `state` payload straight through as react-router's
// navigate(route, { state }), the same mechanism an in-app link to /query already uses.

function ctx(): ClientToolContext {
  return {
    navigate: vi.fn(),
    confirm: vi.fn(),
    runMutation: vi.fn(),
    presentChoice: vi.fn(),
  };
}

describe("navigate client tool", () => {
  it("passes state through as the router's location state", async () => {
    const c = ctx();
    const state = { query: "query Iris { iris { id } }", autoRun: true };
    const out = await executeClientTool("navigate", { route: "/query", state }, c);
    expect(out).toEqual({ success: true, message: "Navigated to /query" });
    expect(c.navigate).toHaveBeenCalledWith("/query", { state });
  });

  it("navigates with no options when no state is given", async () => {
    const c = ctx();
    await executeClientTool("navigate", { route: "/sources" }, c);
    expect(c.navigate).toHaveBeenCalledWith("/sources", undefined);
  });

  it("ignores a non-object state rather than forwarding it", async () => {
    const c = ctx();
    await executeClientTool("navigate", { route: "/query", state: "SELECT 1" }, c);
    expect(c.navigate).toHaveBeenCalledWith("/query", undefined);
  });

  it("refuses a call with no route and does not navigate", async () => {
    const c = ctx();
    const out = await executeClientTool("navigate", {}, c);
    expect(out.success).toBe(false);
    expect(c.navigate).not.toHaveBeenCalled();
  });
});
