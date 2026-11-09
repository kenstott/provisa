// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1783, REQ-1989: a Kaggle search still in flight when the next one starts is aborted by
// Apollo, and the form's debounced search has nothing to catch that rejection: an uncaught page
// error, "The operation was aborted." (core e2e lane, kaggle-source.spec.ts:84, run 42541883433).

import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderHook } from "@testing-library/react";

const read = vi.fn();

vi.mock("@apollo/client/react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@apollo/client/react")>()),
  useLazyQuery: () => [read],
  useMutation: () => [vi.fn()],
}));

import { useKaggleDatasetsLazy } from "../useAdminQueries";

const aborted = () => new DOMException("The operation was aborted.", "AbortError");

describe("useKaggleDatasetsLazy, a search cut short", () => {
  beforeEach(() => {
    read.mockReset();
  });

  it("answers null for a search Apollo aborted, not an empty result", async () => {
    read.mockRejectedValue(aborted());
    const { result } = renderHook(() => useKaggleDatasetsLazy());
    await expect(result.current("token", "")).resolves.toBeNull();
  });

  it("still rejects a search that failed some other way", async () => {
    read.mockRejectedValue(new Error("kaggle said no"));
    const { result } = renderHook(() => useKaggleDatasetsLazy());
    await expect(result.current("token", "iris")).rejects.toThrow("kaggle said no");
  });

  it("hands back the datasets when the search answers", async () => {
    const datasets = [{ ref: "arshid/iris-flower-dataset", title: "Iris", subtitle: "" }];
    read.mockResolvedValue({ data: { kaggleDatasets: datasets } });
    const { result } = renderHook(() => useKaggleDatasetsLazy());
    await expect(result.current("token", "iris")).resolves.toEqual(datasets);
  });
});
