// Copyright (c) 2026 Kenneth Stott
// Canary: 0c7e2a95-48b1-4f6d-a3e8-b52d9f1c6e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: what a discovery run found, one line per kind it ran, and when semantics found none,
// why; requests split, answers cut short and requests that failed are said; what was left out is
// counted by kind.

import { describe, it, expect } from "vitest";
import i18n from "../../../i18n";
import { discoveryResultLines } from "../discoveryResult";
import type { DiscoveryReport, SemanticsReport } from "../../../api/admin";

const t = i18n.getFixedT("en");

const SEMANTICS: SemanticsReport = {
  suggested: 0,
  empty_reason: null,
  requests_total: 2,
  requests_done: 2,
  split: 0,
  proposed: 0,
  invalid: 0,
  below_threshold: 0,
  threshold: 0.7,
  cut_short: 0,
  unreadable: 0,
  failed: [],
  stopped: false,
};
const report = (over: Partial<DiscoveryReport>): DiscoveryReport => ({
  suggestions: 0,
  stored_ids: [],
  removed: {},
  ...over,
});

describe("discoveryResultLines", () => {
  it("counts foreign keys found and suggested", () => {
    expect(discoveryResultLines(report({ foreign_keys: { found: 3, suggested: 2 } }), t)).toEqual([
      "Foreign keys: 3 found, 2 suggested.",
    ]);
  });

  it("says why semantics found none", () => {
    const why = (empty_reason: SemanticsReport["empty_reason"]) =>
      discoveryResultLines(report({ semantics: { ...SEMANTICS, empty_reason } }), t)[0];
    expect(why("unreadable")).toBe("Semantics: none. The model's answer could not be read.");
    expect(why("none_proposed")).toBe("Semantics: none. The model proposed no relationships.");
    expect(why("below_threshold")).toBe(
      "Semantics: none. Every suggestion was below the 70% confidence threshold.",
    );
    expect(why("filtered")).toBe(
      "Semantics: none. Every suggestion was left out, for the reasons below.",
    );
  });

  it("says how many requests were split, cut short, or failed, and the rest continued", () => {
    const lines = discoveryResultLines(
      report({
        semantics: {
          ...SEMANTICS,
          suggested: 4,
          split: 2,
          cut_short: 1,
          failed: [{ group: "text", error: "429 rate limited" }],
        },
      }),
      t,
    );
    expect(lines).toEqual([
      "Semantics: 4 suggestions.",
      "2 requests were split in half and asked again.",
      "1 answer stopped at its length limit; the suggestions it finished are kept.",
      "A request for text columns failed: 429 rate limited",
    ]);
  });

  it("counts what was left out, by kind", () => {
    const lines = discoveryResultLines(
      report({
        removed: { same_column: 1, primary_key_to_primary_key: 0, matches_foreign_key: 2 },
        semantics: { ...SEMANTICS, suggested: 1, invalid: 1 },
      }),
      t,
    );
    expect(lines[lines.length - 1]).toBe(
      "Left out: naming a table or column that does not exist (1), from a column to itself (1), " +
        "matching a foreign key found in this run (2).",
    );
  });
});
