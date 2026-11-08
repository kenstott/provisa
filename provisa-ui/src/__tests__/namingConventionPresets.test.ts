// Copyright (c) 2026 Kenneth Stott
// Canary: f760c445-d569-45e0-84c7-e14ee35316f9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-195, REQ-416: applyConvention takes the conventions the server stores and names -- the
// presets and the Hasura/DDN literals naming.py resolves to them -- and refuses any other name,
// as naming.py does, rather than reading it as snake.

import { describe, expect, it } from "vitest";
import { PRESET_CONVENTIONS, VALID_CONVENTIONS, applyConvention, presetConvention } from "../naming";
import vectors from "../__fixtures__/naming-vectors.json";

describe("applyConvention takes the server's presets", () => {
  it("apollo_graphql names in camelCase", () => {
    expect(applyConvention("order_id", "apollo_graphql")).toBe("orderId");
    expect(applyConvention("Order_ID", "apollo_graphql")).toBe("orderID");
  });

  it("hasura_graphql and snake name in snake_case", () => {
    expect(applyConvention("orderId", "hasura_graphql")).toBe("order_id");
    expect(applyConvention("orderId", "snake")).toBe("order_id");
  });

  it("the presets and accepted names are naming.py's", () => {
    expect([...PRESET_CONVENTIONS]).toEqual(vectors.presets);
    expect([...VALID_CONVENTIONS].sort()).toEqual(vectors.valid_conventions);
  });

  it.each(Object.entries(vectors.preset_of))("presetConvention(%j) is %j", (name, preset) => {
    expect(presetConvention(name)).toBe(preset);
  });

  it.each(vectors.unknown_conventions)("refuses %j", (name) => {
    expect(() => applyConvention("orderId", name)).toThrow(/unknown naming convention/);
    expect(() => presetConvention(name)).toThrow(/unknown naming convention/);
  });
});
