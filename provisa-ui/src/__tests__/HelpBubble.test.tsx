// Copyright (c) 2026 Kenneth Stott
// Canary: c4aca815-dfb5-4a23-b2c0-b946101d6150
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: the "?" beside a page's add control, stating what the page is for.

import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "../test-utils/render";
import i18n from "../i18n";
import { HelpBubble } from "../components/HelpBubble";

const t = i18n.getFixedT("en");

// Every surface that carries the bubble, so a new page cannot ship the control
// without the copy — an empty dropdown is worse than no "?" at all.
const SURFACES = [
  "sourcesPage",
  "tablesPage",
  "relationshipsPage",
  "metricsPage",
  "tagsTab",
  "glossaryTab",
] as const;

describe("HelpBubble", () => {
  it("shows its copy on hover", async () => {
    render(
      <HelpBubble
        title="Title"
        paragraphs={["First.", "Second."]}
        ariaLabel="About this"
        testId="bubble"
      />,
    );
    const target = screen.getByTestId("bubble");
    expect(target).toHaveAttribute("aria-label", "About this");
    fireEvent.mouseEnter(target);
    expect(await screen.findByText("Title")).toBeInTheDocument();
    expect(screen.getByText("First.")).toBeInTheDocument();
    expect(screen.getByText("Second.")).toBeInTheDocument();
  });

  it.each(SURFACES)("%s carries purpose copy in English", (ns) => {
    for (const key of ["purposeTitle", "purposeBody", "purposeAdd", "purposeAria"]) {
      const value = t(`${ns}.${key}`);
      expect(value).not.toBe(`${ns}.${key}`);
      expect(value.length).toBeGreaterThan(0);
    }
  });
});
