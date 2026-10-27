// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7b4f19-8e36-4c5a-a1f0-6b9e3c8d5a27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2168: an argument's default, as the command form writes it: text for a text type, the
// value its text reads as for any other, none for empty text.

import { describe, it, expect } from "vitest";
import { defaultText, withDefaultText } from "../types";

describe("argument defaults in the command form", () => {
  it("reads a default by the argument's type", () => {
    expect(withDefaultText({ name: "n", type: "Int" }, "10")).toEqual({
      name: "n",
      type: "Int",
      default: 10,
    });
    expect(withDefaultText({ name: "s", type: "String" }, "10")).toEqual({
      name: "s",
      type: "String",
      default: "10",
    });
    expect(withDefaultText({ name: "c", type: "JSON" }, '{"sample": 5}').default).toEqual({
      sample: 5,
    });
  });

  it("leaves the argument required when the text is empty", () => {
    expect(withDefaultText({ name: "n", type: "Int", default: 3 }, "")).toEqual({
      name: "n",
      type: "Int",
    });
  });

  it("writes a default back as text", () => {
    expect(defaultText({ name: "n", type: "Int" })).toBe("");
    expect(defaultText({ name: "c", type: "JSON", default: {} })).toBe("{}");
    expect(defaultText({ name: "s", type: "String", default: "x" })).toBe("x");
  });
});
