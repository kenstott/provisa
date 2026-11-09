// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1443: the Quality panel's contract reads run from effects and are not awaited there, so one
// still in flight when the panel goes (a tour step moving on, a table closed) rejected with nothing
// to catch it: Apollo's "The operation was aborted.", an uncaught page error (core e2e lane,
// tour-profiler-step).

import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderHook } from "@testing-library/react";

const read = vi.fn();

vi.mock("@apollo/client/react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@apollo/client/react")>()),
  useLazyQuery: () => [read],
  useMutation: () => [vi.fn()],
}));

import { useDqContract } from "../useAdminQueries";

const aborted = () => new DOMException("The operation was aborted.", "AbortError");
const parseVars = { checker: "soda", contractText: "dataset: pets" };
const catalogVars = { checker: "soda", dataset: "pets" };

describe("useDqContract, a read still in flight when its panel goes", () => {
  beforeEach(() => read.mockReset());

  it("answers null for a parse Apollo cut short", async () => {
    read.mockRejectedValue(aborted());
    const { result } = renderHook(() => useDqContract());
    await expect(result.current.parseContract(parseVars)).resolves.toBeNull();
  });

  it("answers null for a catalog read Apollo cut short", async () => {
    read.mockRejectedValue(aborted());
    const { result } = renderHook(() => useDqContract());
    await expect(result.current.checkCatalog(catalogVars)).resolves.toBeNull();
  });

  it("still rejects a read that failed some other way", async () => {
    read.mockRejectedValue(new Error("server said no"));
    const { result } = renderHook(() => useDqContract());
    await expect(result.current.parseContract(parseVars)).rejects.toThrow("server said no");
  });

  it("hands back the parsed contract when the read answers", async () => {
    const parsed = { dataset: "pets", checks: [], error: null };
    read.mockResolvedValue({ data: { dqContractParse: parsed } });
    const { result } = renderHook(() => useDqContract());
    await expect(result.current.parseContract(parseVars)).resolves.toEqual(parsed);
  });
});
