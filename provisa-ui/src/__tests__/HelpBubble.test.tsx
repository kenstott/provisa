// Copyright (c) 2026 Kenneth Stott
// Canary: d79be538-6345-44d3-afed-f6e0e53c1f4d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: the "?" beside a page's add control, stating what the page is for.

import { describe, it, expect } from "vitest";
import { ActionIcon } from "@mantine/core";
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
  "securityPage",
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

  // An icon-only button is the opaque thing being explained, so it becomes the hover
  // target itself rather than growing a second "?" glyph beside it.
  it("uses a supplied control as its own hover target", async () => {
    const clicks: string[] = [];
    render(
      <HelpBubble
        title="Diagram"
        paragraphs={["What it draws."]}
        ariaLabel="View diagram"
        testId="icon-bubble"
        target={
          <ActionIcon aria-label="View diagram" onClick={() => clicks.push("hit")}>
            icon
          </ActionIcon>
        }
      />,
    );
    // No second control: the icon is the only thing rendered, and it still acts.
    expect(screen.queryByTestId("icon-bubble")).not.toBeInTheDocument();
    const icon = screen.getByLabelText("View diagram");
    fireEvent.mouseEnter(icon);
    expect(await screen.findByText("Diagram")).toBeInTheDocument();
    fireEvent.click(icon);
    expect(clicks).toEqual(["hit"]);
  });

  // The icon-only controls in each page's action row, which have no visible label at all.
  it.each([
    ["tablesPage", ["erdHelpTitle", "erdHelpBody", "purposeModel"]],
    [
      "relationshipsPage",
      [
        "erdHelpTitle",
        "erdHelpBody",
        "modelingHelpTitle",
        "modelingHelpBody",
        "suggestHelpTitle",
        "suggestHelpBody",
      ],
    ],
  ] as const)("%s carries copy for its icon-only controls", (ns, keys) => {
    for (const key of keys) {
      const value = t(`${ns}.${key}`);
      expect(value).not.toBe(`${ns}.${key}`);
      expect(value.length).toBeGreaterThan(0);
    }
  });

  it.each(SURFACES)("%s carries purpose copy in English", (ns) => {
    for (const key of ["purposeTitle", "purposeBody", "purposeAdd", "purposeAria"]) {
      const value = t(`${ns}.${key}`);
      expect(value).not.toBe(`${ns}.${key}`);
      expect(value.length).toBeGreaterThan(0);
    }
  });
});
