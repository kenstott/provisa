// Copyright (c) 2026 Kenneth Stott
// Canary: dbc8b284-8d30-483d-93b4-97197e86d13e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a form's region is the operator's own choice for what it is about (the source picked,
// this opening of the view dialog), else where the new table or view starts — for a view, the
// region the operator is connected to.

import { describe, expect, it } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { useRegionChoice } from "../useRegionQueries";

describe("useRegionChoice (REQ-1921)", () => {
  it("starts a view in the connected region, keeps the operator's choice, and starts again on reopening", () => {
    const { result, rerender } = renderHook(({ opening }) => useRegionChoice("us", opening), {
      initialProps: { opening: 1 },
    });
    expect(result.current[0]).toBe("us");
    act(() => result.current[1]("eu"));
    expect(result.current[0]).toBe("eu");
    act(() => result.current[1](null)); // No region, chosen
    expect(result.current[0]).toBeNull();
    rerender({ opening: 2 });
    expect(result.current[0]).toBe("us");
  });

  it("follows the start while nothing is chosen (the connected region arriving late)", () => {
    const { result, rerender } = renderHook(({ start }) => useRegionChoice(start, "crm"), {
      initialProps: { start: null as string | null },
    });
    expect(result.current[0]).toBeNull();
    rerender({ start: "eu" });
    expect(result.current[0]).toBe("eu");
  });
});
