// Copyright (c) 2026 Kenneth Stott
// Canary: 1b324286-9b09-4166-b101-9d75dcd8af33
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi } from "vitest";
import { renderHook } from "@testing-library/react";
import type { Capability } from "../types/auth";

let mockCapabilities: Capability[] = [];

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ capabilities: mockCapabilities }),
}));

import { useCapability, useCapabilities } from "../hooks/useCapability";

function setCaps(caps: Capability[]) {
  mockCapabilities = caps;
}

describe("useCapability", () => {
  it("returns false when no capabilities are granted", () => {
    setCaps([]);
    expect(renderHook(() => useCapability("query_development")).result.current).toBe(false);
  });

  it("returns true when the exact capability is present", () => {
    setCaps(["query_development"]);
    expect(renderHook(() => useCapability("query_development")).result.current).toBe(true);
  });

  it("returns false when a different capability is present", () => {
    setCaps(["usage"]);
    expect(renderHook(() => useCapability("query_development")).result.current).toBe(false);
  });

  it("treats admin as having every capability", () => {
    setCaps(["admin"]);
    expect(renderHook(() => useCapability("source_registration")).result.current).toBe(true);
  });
});

describe("useCapabilities", () => {
  it("returns false when no capabilities are granted", () => {
    setCaps([]);
    expect(renderHook(() => useCapabilities(["usage"])).result.current).toBe(false);
  });

  it("returns true only when ALL requested capabilities are present", () => {
    setCaps(["usage", "query_development"]);
    expect(renderHook(() => useCapabilities(["usage", "query_development"])).result.current).toBe(
      true,
    );
    expect(renderHook(() => useCapabilities(["usage", "approve_view"])).result.current).toBe(false);
  });

  it("admin short-circuits to true for any requested set", () => {
    setCaps(["admin"]);
    expect(
      renderHook(() => useCapabilities(["usage", "approve_view", "masking_config"])).result.current,
    ).toBe(true);
  });
});
