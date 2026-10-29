// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e2f41-0c7a-4d63-9e15-a2c4d7f80b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The charts the server builds (Analyze plan tree, SQL and GraphQL stats DAGs) for plan text full
// of quotes, brackets, braces, pipes, semicolons, `#`, `<>` and long SQL, read by Mermaid's own
// parser. The fixture is written by scripts/gen_mermaid_chart_fixtures.py from the real builders;
// tests/unit/test_mermaid_label.py fails when it is out of date.
import { describe, it, expect } from "vitest";
import mermaid from "mermaid";
import charts from "./fixtures/mermaidCharts.json";

describe("server-built Mermaid charts", () => {
  const cases = Object.entries(charts as Record<string, string>);

  it("covers the maintainer's term_edges statement", () => {
    expect(cases.map(([name]) => name)).toContain("analyze: term_edges projection (maintainer)");
  });

  it.each(cases)("%s parses", async (_name, chart) => {
    await expect(mermaid.parse(chart)).resolves.toBeTruthy();
  });
});
