// Copyright (c) 2026 Kenneth Stott
// Canary: 04223d27-78a2-49b0-a679-2f6b19fce9ed
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2006: a live demo opens on its own: the environment a tour left picked in this browser is not
// the next visit's. An earlier visit left in Test (synthetic) opened the next sandbox's tour there,
// where a run's results hold nothing (provisa/synthetic/env_model.py) and its first statement over
// them answered "No results".

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ENV_STORAGE_KEY, selectedEnv } from "../lib/authFetch";
import { endVisit, getStored, removeStored, setStored, startVisit } from "../lib/browserStorage";

const pick = (env: string) => setStored(ENV_STORAGE_KEY, env, { global: true });

describe("a live demo's visit and the environment picked", () => {
  beforeEach(() => removeStored(ENV_STORAGE_KEY, { global: true }));
  afterEach(() => {
    endVisit();
    removeStored(ENV_STORAGE_KEY, { global: true });
  });

  it("starts in no environment, whatever the browser last picked", () => {
    pick("synthetic");
    startVisit();
    expect(selectedEnv()).toBeNull();
  });

  it("a visit's pick is its own, and the next visit starts without it", () => {
    startVisit();
    pick("synthetic");
    expect(selectedEnv()).toBe("synthetic");
    startVisit();
    expect(selectedEnv()).toBeNull();
  });

  it("leaves the browser's own pick as it was", () => {
    pick("branch-a");
    startVisit();
    pick("synthetic");
    endVisit();
    expect(getStored(ENV_STORAGE_KEY, { global: true })).toBe("branch-a");
  });
});
