// Copyright (c) 2026 Kenneth Stott
// Canary: 0855a64b-3b35-491f-a8d5-b0aa9ca2760d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { afterEach, describe, expect, it, vi } from "vitest";
import { uuid } from "../uuid";

const V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe("uuid", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("is an RFC 4122 version 4 id where crypto.randomUUID is undefined", () => {
    vi.stubGlobal("crypto", { getRandomValues: globalThis.crypto.getRandomValues.bind(globalThis.crypto) });
    expect("randomUUID" in crypto).toBe(false);
    const ids = Array.from({ length: 100 }, uuid);
    for (const id of ids) expect(id).toMatch(V4);
    expect(new Set(ids).size).toBe(100);
  });
});
