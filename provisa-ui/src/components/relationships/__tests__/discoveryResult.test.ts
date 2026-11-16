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
const failedRequest = (
  group: SemanticsReport["failed"][number]["group"],
  reason: string,
): SemanticsReport["failed"][number] => ({
  group,
  code: "llm.request_failed",
  params: { reason },
  message: `The request to the model failed: ${reason}`,
});
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
    expect(why("requests_failed")).not.toContain("proposed no relationships");
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
          failed: [failedRequest("text", "429 rate limited")],
        },
      }),
      t,
    );
    expect(lines).toEqual([
      "Semantics: 4 suggestions.",
      "1 of 2 requests failed, so the result is partial.",
      "2 requests were split in half and asked again.",
      "1 answer stopped at its length limit; the suggestions it finished are kept.",
      "For text columns: The request to the model failed: 429 rate limited",
    ]);
  });

  it("says the semantic pass failed, and why, when every request failed", () => {
    const lines = discoveryResultLines(
      report({
        foreign_keys: { found: 6, suggested: 6 },
        semantics: {
          ...SEMANTICS,
          requests_total: 3,
          requests_done: 3,
          empty_reason: "requests_failed",
          failed: [
            failedRequest("whole_number", "overloaded"),
            failedRequest("date", "overloaded"),
            {
              group: "timestamp",
              code: "llm.no_answer_in_time",
              params: { seconds: 3600 },
              message: "The model gave no answer within 3600 seconds.",
            },
          ],
        },
      }),
      t,
    );
    expect(lines).toEqual([
      "Foreign keys: 6 found, 6 suggested.",
      "Semantics failed: all 3 requests to the model failed.",
      "For whole numbers columns: The request to the model failed: overloaded",
      "For dates columns: The request to the model failed: overloaded",
      "For timestamps columns: The model gave no answer within 3600 seconds.",
    ]);
    expect(lines.join(" ")).not.toContain("proposed no relationships");
  });

  it("says one request failed when the only request failed", () => {
    const lines = discoveryResultLines(
      report({
        semantics: {
          ...SEMANTICS,
          requests_total: 1,
          requests_done: 1,
          empty_reason: "requests_failed",
          failed: [failedRequest("text", "overloaded")],
        },
      }),
      t,
    );
    expect(lines[0]).toBe("Semantics failed: the request to the model failed.");
  });

  it("says how many of the requests failed when the answered ones proposed none", () => {
    const lines = discoveryResultLines(
      report({
        semantics: {
          ...SEMANTICS,
          requests_total: 3,
          requests_done: 3,
          empty_reason: "none_proposed",
          failed: [failedRequest("text", "overloaded")],
        },
      }),
      t,
    );
    expect(lines.slice(0, 2)).toEqual([
      "Semantics: none. The model proposed no relationships.",
      "1 of 3 requests failed, so the result is partial.",
    ]);
  });

  it("words a failed request from the catalog, or from the server's message for a new code", () => {
    const lines = discoveryResultLines(
      report({
        semantics: {
          ...SEMANTICS,
          suggested: 1,
          failed: [
            {
              group: "text",
              code: "discovery.answer_unreadable",
              params: {},
              message: "server wording",
            },
            { group: "text", code: "llm.newer_code", params: {}, message: "A newer refusal." },
          ],
        },
      }),
      t,
    );
    expect(lines).toContain("For text columns: The model's answer held no readable list.");
    expect(lines).toContain("For text columns: A newer refusal.");
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
