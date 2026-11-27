// Copyright (c) 2026 Kenneth Stott
// Canary: 1f7a2c9e-4b6d-4e3a-8c51-0d9e7b2a6f14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 7: the panel body's parts carry the classes the one rhythm (App.css) spaces.

import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { render, screen } from "@testing-library/react";
import { PanelBody, PanelSection, PanelStrip } from "../PanelBody";

const CSS = readFileSync(join(__dirname, "..", "..", "..", "App.css"), "utf8");

describe("PanelBody", () => {
  it("draws a strip, then a section's title and description, then its fields, as the rhythm's parts", () => {
    render(
      <PanelBody data-testid="body">
        <PanelStrip data-testid="strip">steps</PanelStrip>
        <PanelSection title="Hubs" description="What each hub is keyed by" data-testid="section">
          <div data-testid="field">Domain</div>
        </PanelSection>
      </PanelBody>,
    );
    const body = screen.getByTestId("body");
    expect(body).toHaveClass("panel-body");
    const [strip, section] = Array.from(body.children);
    expect(strip).toHaveClass("panel-strip");
    expect(section).toHaveClass("panel-body", "panel-section");
    const [head, field] = Array.from(section.children);
    expect(head).toHaveClass("panel-section__head");
    expect(head.children[0]).toHaveClass("panel-section__title");
    expect(head.children[0]).toHaveTextContent("Hubs");
    expect(head.children[1]).toHaveClass("panel-section__description");
    expect(head.children[1]).toHaveTextContent("What each hub is keyed by");
    expect(field).toHaveTextContent("Domain");
  });

  it("draws no description line when the section has none", () => {
    render(<PanelSection title="Values" data-testid="section" />);
    expect(screen.getByTestId("section").querySelector(".panel-section__description")).toBeNull();
  });

  it("spaces every gap from a token, each defined once", () => {
    for (const token of ["head", "strip", "title", "section", "group", "sections"]) {
      expect(CSS.match(new RegExp(`--panel-gap-${token}:`, "g")), token).toHaveLength(1);
    }
    expect(CSS).toMatch(
      /\.panel-body > \.panel-strip \+ \* \{\s*margin-top: var\(--panel-gap-strip\)/,
    );
    expect(CSS).toMatch(
      /\.panel-body > \.panel-section__head \+ \* \{\s*margin-top: var\(--panel-gap-section\)/,
    );
    expect(CSS).toMatch(/\.panel-section__head \{[^}]*gap: var\(--panel-gap-title\)/);
    expect(CSS).toMatch(/\.panel-body > \* \+ \* \{\s*margin-top: var\(--panel-gap-group\)/);
  });
});
