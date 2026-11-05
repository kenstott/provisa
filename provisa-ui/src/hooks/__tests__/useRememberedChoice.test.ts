// Copyright (c) 2026 Kenneth Stott
// Canary: 333cc4ed-12b7-41bb-b12a-dd00dfefa2e7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list's filter chip, remembered per viewer: restored as it was left, and a remembered choice the
// list no longer offers is dropped for the default.

import { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderHook } from "../../test-utils/render";
import { getStored, setStored } from "../../lib/browserStorage";
import { useRememberedChoice } from "../useRememberedChoice";

const CHOICES = ["pending", "resolved"] as const;
const useTab = () => useRememberedChoice("filter.requests.tab", CHOICES, "pending");

afterEach(() => vi.restoreAllMocks());

describe("a remembered choice", () => {
  it("is the default with nothing saved", () => {
    expect(renderHook(useTab).result.current[0]).toBe("pending");
  });

  it("is restored after a remount", () => {
    const first = renderHook(useTab);
    act(() => first.result.current[1]("resolved"));
    expect(first.result.current[0]).toBe("resolved");
    first.unmount();
    expect(renderHook(useTab).result.current[0]).toBe("resolved");
  });

  it("is dropped when it names what no longer exists", () => {
    setStored("filter.requests.tab", "archived");
    expect(renderHook(useTab).result.current[0]).toBe("pending");
    expect(getStored("filter.requests.tab")).toBeNull();
  });

  it("holds while the page is open where storage is blocked", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    const view = renderHook(useTab);
    expect(view.result.current[0]).toBe("pending");
    act(() => view.result.current[1]("resolved"));
    expect(view.result.current[0]).toBe("resolved");
  });
});
