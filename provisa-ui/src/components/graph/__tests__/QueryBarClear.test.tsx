// Copyright (c) 2026 Kenneth Stott
// Canary: 3f6b1c84-92d7-4e05-a8b3-5d0e7c2f9a16
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The trash can in the query bar's editor row clears the query, in the Cypher and SPARQL explorers.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import { QueryBar } from "../QueryBar";

function bar(language: "cypher" | "sparql", onQueryChange: (q: string) => void) {
  render(
    <MantineProvider>
      <QueryBar
        onRun={vi.fn()}
        initialQuery="MATCH (x:Pet) RETURN x"
        onQueryChange={onQueryChange}
        autoImpute={false}
        onToggleAutoImpute={vi.fn()}
        language={language}
      />
    </MantineProvider>,
  );
}

describe("the query bar's clear button", () => {
  it.each(["cypher", "sparql"] as const)("clears the %s query", (language) => {
    const changed = vi.fn();
    bar(language, changed);
    const clear = screen.getByTestId("clear-query-button");
    expect(clear).toHaveAttribute("aria-label", "Clear query");
    fireEvent.click(clear);
    expect(changed).toHaveBeenCalledWith("");
    const collapsed = document.querySelector(".gf-header-query-collapsed") as HTMLElement;
    // An empty query shows the language's placeholder.
    expect(collapsed.textContent).not.toContain("Pet");
  });
});
