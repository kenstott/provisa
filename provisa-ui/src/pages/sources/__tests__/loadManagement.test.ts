// Copyright (c) 2026 Kenneth Stott
// Canary: 4c9e2a71-8b3f-4d65-a0e7-1f6d8b2c5a93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import {
  maxLiveConcurrencyValid,
  sentinelPathValid,
  sourceLoadFieldsValid,
  ttlSignalMissingCacheTtl,
} from "../loadManagement";

describe("source load/timeliness client validation", () => {
  it("accepts an empty cap or a whole number >= 1", () => {
    expect(maxLiveConcurrencyValid("")).toBe(true);
    expect(maxLiveConcurrencyValid("1")).toBe(true);
    expect(maxLiveConcurrencyValid("12")).toBe(true);
    expect(maxLiveConcurrencyValid("0")).toBe(false);
    expect(maxLiveConcurrencyValid("2.5")).toBe(false);
  });

  it("accepts an empty sentinel or a file/ftp/sftp/http/https URL", () => {
    for (const ok of [
      "",
      "file:///d/_SUCCESS",
      "ftp://h/m",
      "sftp://h/m",
      "http://h/m",
      "HTTPS://h/m",
    ]) {
      expect(sentinelPathValid(ok)).toBe(true);
    }
    expect(sentinelPathValid("s3://b/m")).toBe(false);
    expect(sentinelPathValid("/local/path")).toBe(false);
  });

  it("refuses a ttl/ttl_probe signal with no landing Cache TTL only when the data lands", () => {
    expect(ttlSignalMissingCacheTtl("ttl", null, true)).toBe(true);
    expect(ttlSignalMissingCacheTtl("ttl_probe", null, true)).toBe(true);
    expect(ttlSignalMissingCacheTtl("ttl", null, false)).toBe(false);
    expect(ttlSignalMissingCacheTtl("ttl", 60, true)).toBe(false);
    expect(ttlSignalMissingCacheTtl("ttl", 0, true)).toBe(false);
    for (const other of ["probe", "native", "debezium", "kafka"]) {
      expect(ttlSignalMissingCacheTtl(other, null, true)).toBe(false);
    }
    expect(ttlSignalMissingCacheTtl(null, null, true)).toBe(false);
  });

  it("combines every rule", () => {
    const ok = {
      maxLiveConcurrency: "",
      sentinelPath: "",
      changeSignal: "ttl",
      cacheTtl: "60",
      replicate: 0,
      loadProtected: false,
    };
    expect(sourceLoadFieldsValid(ok)).toBe(true);
    expect(sourceLoadFieldsValid({ ...ok, maxLiveConcurrency: "0" })).toBe(false);
    expect(sourceLoadFieldsValid({ ...ok, sentinelPath: "s3://x" })).toBe(false);
    expect(sourceLoadFieldsValid({ ...ok, cacheTtl: "" })).toBe(false);
    expect(
      sourceLoadFieldsValid({
        ...ok,
        cacheTtl: "",
        replicate: null,
        loadProtected: true,
      }),
    ).toBe(false);
    // Read live: a ttl source with no Cache TTL saves; the server raises if it ever lands.
    expect(sourceLoadFieldsValid({ ...ok, cacheTtl: "", replicate: null })).toBe(true);
    // REQ-826: a Hot threshold says the tables are replicated once busy, so the clock is needed;
    // Never does not; and Never with load protection is refused outright.
    expect(sourceLoadFieldsValid({ ...ok, cacheTtl: "", replicate: 500 })).toBe(false);
    expect(sourceLoadFieldsValid({ ...ok, cacheTtl: "", replicate: -1 })).toBe(true);
    expect(sourceLoadFieldsValid({ ...ok, replicate: -1, loadProtected: true })).toBe(false);
    for (const signal of ["probe", "native", "debezium", "kafka"]) {
      expect(sourceLoadFieldsValid({ ...ok, cacheTtl: "", changeSignal: signal })).toBe(true);
    }
  });
});
