// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6b0c38-7d2e-4e95-8a4c-52b9e3d0a7c6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's To review section: a chip per finding, each opening to what was found, worded by its
// code in the reader's language, and the acts that fix it; nothing for a term with no finding.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { GlossaryFinding } from "../../../api/glossary";
import { TermToReview } from "../TermToReview";
import { tableNames } from "../termFindings";

const UNLINKED: GlossaryFinding = {
  kind: "unlinked_tables",
  code: "glossary.finding.unlinked_tables",
  message: "the class is grounded in tables that same-entity relationships do not all join",
  params: { tables: "pg.public.customers, crm.public.contacts" },
};
const NO_DATA: GlossaryFinding = {
  kind: "no_data",
  code: "glossary.finding.no_data",
  message: "the concept is connected to no term that holds data",
  params: {},
};

describe("a term's To review section", () => {
  it("shows nothing for a term with no finding", () => {
    render(<TermToReview findings={[]} fixesOf={() => []} />);
    expect(screen.queryByTestId("glossary-to-review")).toBeNull();
  });

  it("names each finding as a chip, closed until opened", () => {
    render(<TermToReview findings={[UNLINKED, NO_DATA]} fixesOf={() => []} />);
    expect(screen.getByTestId("glossary-finding-0")).toHaveTextContent("Unlinked tables");
    expect(screen.getByTestId("glossary-finding-1")).toHaveTextContent("No data");
    expect(screen.queryByTestId("glossary-finding-detail")).toBeNull();
  });

  it("opens a chip to its message, by its code and params, and its fixes", () => {
    const markSameEntity = vi.fn();
    render(
      <TermToReview
        findings={[UNLINKED, NO_DATA]}
        fixesOf={(f) =>
          f.kind === "unlinked_tables"
            ? [{ label: "Mark same-entity…", run: markSameEntity, testId: "fix-same" }]
            : []
        }
      />,
    );
    const chip = screen.getByTestId("glossary-finding-0");
    fireEvent.click(chip);
    expect(chip).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByTestId("glossary-finding-detail")).toHaveTextContent(
      "same-entity relationships don't all join: pg.public.customers, crm.public.contacts",
    );
    fireEvent.click(screen.getByTestId("fix-same"));
    expect(markSameEntity).toHaveBeenCalledTimes(1);
    // Another chip swaps the open finding; one with no fix offered says what was found alone.
    fireEvent.click(screen.getByTestId("glossary-finding-1"));
    expect(screen.getByTestId("glossary-finding-detail")).toHaveTextContent(
      "connected to no term that holds data",
    );
    expect(screen.queryByTestId("fix-same")).toBeNull();
  });

  it("finds an unlinked finding's tables on the Relationships page by their names", () => {
    expect(tableNames(UNLINKED.params.tables)).toBe("customers contacts");
  });
});
