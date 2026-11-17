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

/** What the run found: the lines after the one that says what it looked for. */
const found = (r: DiscoveryReport, tr: typeof t) => discoveryResultLines(r, tr).slice(1);

describe("discoveryResultLines", () => {
  it("counts foreign keys found and suggested", () => {
    expect(found(report({ foreign_keys: { found: 3, suggested: 2 } }), t)).toEqual([
      "Foreign keys: 3 found, 2 suggested.",
    ]);
  });

  it("says why semantics found none", () => {
    const why = (empty_reason: SemanticsReport["empty_reason"]) =>
      found(report({ semantics: { ...SEMANTICS, empty_reason } }), t)[0];
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
    const lines = found(
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
    const lines = found(
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
    const lines = found(
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
    const lines = found(
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
    const lines = found(
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
    const lines = found(
      report({
        removed: { same_column: 1, primary_key_to_primary_key: 0, matches_foreign_key: 2 },
        semantics: { ...SEMANTICS, suggested: 1, invalid: 1 },
      }),
      t,
    );
    expect(lines[lines.length - 1]).toBe(
      "Not suggested: not naming two compared columns (1), from a column to itself (1), " +
        "matching a foreign key found in this run (2).",
    );
  });

  it("says first the scope the run ran with, so a table outside it is plain", () => {
    const same = report({
      foreign_keys: { found: 6, suggested: 6 },
      semantics: { ...SEMANTICS, suggested: 2 },
      model: {
        vendor: "anthropic",
        model: "claude-fable-5-2",
        input_tokens: 1000000,
        output_tokens: 128000,
        limits_source: "known",
      },
      source_domains: ["pet-store", "shelter"],
      target_domains: ["pet-store", "shelter"],
      source_tables: 29,
      target_tables: 29,
      target_same_as_source: true,
    });
    expect(discoveryResultLines(same, t).slice(0, 4)).toEqual([
      "Looked for: Foreign keys, Semantics.",
      "Model: anthropic:claude-fable-5-2",
      "Source: 29 tables of pet-store, shelter.",
      "Target: the same as the source.",
    ]);
    const two = {
      ...same,
      target_same_as_source: false,
      source_tables: 1,
      source_domains: ["city"],
      target_tables: 24,
    };
    expect(discoveryResultLines(two, t).slice(2, 4)).toEqual([
      "Source: 1 table of city.",
      "Target: 24 tables of pet-store, shelter.",
    ]);
    // A run recorded before a run said its sets: the kinds, and what it found.
    expect(discoveryResultLines(report({ foreign_keys: { found: 3, suggested: 2 } }), t)).toEqual([
      "Looked for: Foreign keys.",
      "Foreign keys: 3 found, 2 suggested.",
    ]);
  });

  it("says how many Jev scored under the threshold, without that removing them", () => {
    const lines = found(
      report({
        semantics: {
          ...SEMANTICS,
          proposed: 30,
          suggested: 30,
          empty_reason: null,
          scored: 30,
          scored_below_threshold: 30,
        },
      }),
      t,
    );
    expect(lines).toEqual([
      "Semantics: 30 suggestions.",
      "Jev scored 30 proposals under 70%. Each is shown beside the model's score, which decides.",
    ]);
  });

  it("counts the model's own low confidence and the other reasons, Jev's apart", () => {
    const lines = found(
      report({
        removed: { weaker: 2 },
        semantics: {
          ...SEMANTICS,
          suggested: 3,
          below_threshold: 4,
          scored_below_threshold: 1,
          not_comparable: 5,
          scoring_failed: 1,
        },
      }),
      t,
    );
    expect(lines).toContain(
      "Jev could not score the proposals of 1 request, so they kept the model's confidence.",
    );
    expect(lines).toContain(
      "Jev scored 1 proposal under 70%. It is shown beside the model's score, which decides.",
    );
    expect(lines[lines.length - 1]).toBe(
      "Not suggested: given less than 70% confidence by the model (4), " +
        "between columns of different classes (5), " +
        "repeating a pair of columns a stronger proposal was kept for (2).",
    );
  });
});
