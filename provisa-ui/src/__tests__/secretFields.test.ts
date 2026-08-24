// Copyright (c) 2026 Kenneth Stott
// Canary: b6a2778b-7311-44d4-8f75-8a5e59b2a8fb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** REQ-1575: what a settings form does with a value the server will not give it back. */

import { describe, it, expect } from "vitest";
import {
  missingRequired,
  secretPlaceholder,
  seedFields,
  type SecretAwareField,
} from "../components/admin/secretFields";

const FIELDS: SecretAwareField[] = [
  { config_key: "address", required: true },
  { config_key: "mount", required: false },
  { config_key: "token", required: true, secret: true },
];

describe("seedFields", () => {
  it("seeds what the server sent and leaves secret fields out entirely", () => {
    const seeded = seedFields(FIELDS, { address: "https://vault", mount: "kv" });
    expect(seeded).toEqual({ address: "https://vault", mount: "kv" });
    expect("token" in seeded).toBe(false);
  });

  it("does not echo a secret back even when the server sent one", () => {
    // A server that leaked a value is a defect being fixed elsewhere; the form still must not
    // put it in a box that a save would post back and a screen share would carry away.
    expect(seedFields(FIELDS, { token: "hvs.leaked" })).toEqual({ address: "", mount: "" });
  });

  it("seeds a missing config as empty rather than undefined", () => {
    expect(seedFields(FIELDS, undefined)).toEqual({ address: "", mount: "" });
  });
});

describe("missingRequired", () => {
  it("a stored secret counts as filled without retyping it", () => {
    expect(missingRequired(FIELDS, { address: "https://vault" }, { token: true })).toBe(false);
  });

  it("a required secret with nothing stored is missing", () => {
    expect(missingRequired(FIELDS, { address: "https://vault" }, { token: false })).toBe(true);
    expect(missingRequired(FIELDS, { address: "https://vault" }, undefined)).toBe(true);
  });

  it("emptying a stored secret on purpose is a clear, and leaves a required field unsatisfied", () => {
    expect(missingRequired(FIELDS, { address: "https://vault", token: "" }, { token: true })).toBe(
      true,
    );
  });

  it("a typed secret satisfies it, and an optional field never does", () => {
    expect(
      missingRequired(FIELDS, { address: "https://vault", token: "hvs.new" }, { token: false }),
    ).toBe(false);
    expect(missingRequired(FIELDS, { address: "", mount: "" }, { token: true })).toBe(true);
  });
});

describe("secretPlaceholder", () => {
  const labels = { set: "On file — type to replace", unset: "Not set" };

  it("says whether a value is on file and nothing about the value", () => {
    expect(secretPlaceholder(FIELDS[2], { token: true }, labels)).toBe(labels.set);
    expect(secretPlaceholder(FIELDS[2], { token: false }, labels)).toBe(labels.unset);
    expect(secretPlaceholder(FIELDS[2], undefined, labels)).toBe(labels.unset);
  });

  it("leaves a non-secret field's own placeholder alone", () => {
    expect(secretPlaceholder(FIELDS[0], { token: true }, labels)).toBeUndefined();
  });
});
