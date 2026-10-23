// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6b0c38-7d2e-4e95-8a4c-52b9e3d0a7c6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's To review section: a chip per finding, each opening to what was found and the act that
// fixes it; nothing at all for a term with no finding.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { TermToReview } from "../TermToReview";
import type { TermFinding } from "../termFindings";

const UNLINKED: TermFinding = { kind: "unlinked_tables", params: {} };
const NO_DATA: TermFinding = { kind: "no_data", params: {} };

describe("a term's To review section", () => {
  it("shows nothing for a term with no finding", () => {
    render(<TermToReview findings={[]} fixes={{}} />);
    expect(screen.queryByTestId("glossary-to-review")).toBeNull();
  });

  it("names each finding as a chip, closed until opened", () => {
    render(<TermToReview findings={[UNLINKED, NO_DATA]} fixes={{}} />);
    expect(screen.getByTestId("glossary-finding-unlinked_tables")).toHaveTextContent(
      "Unlinked tables",
    );
    expect(screen.getByTestId("glossary-finding-no_data")).toHaveTextContent("No data");
    expect(screen.queryByTestId("glossary-finding-unlinked_tables-detail")).toBeNull();
  });

  it("opens a chip to its message and its fix, which acts on the finding", () => {
    const markSameEntity = vi.fn();
    render(
      <TermToReview findings={[UNLINKED, NO_DATA]} fixes={{ unlinked_tables: markSameEntity }} />,
    );
    const chip = screen.getByTestId("glossary-finding-unlinked_tables");
    fireEvent.click(chip);
    expect(chip).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByTestId("glossary-finding-unlinked_tables-detail")).toHaveTextContent(
      "not yet linked as the same entity",
    );
    fireEvent.click(screen.getByTestId("glossary-finding-unlinked_tables-fix"));
    expect(markSameEntity).toHaveBeenCalledWith(UNLINKED);
    // Another chip swaps the open finding; one with no fix offered says what was found alone.
    fireEvent.click(screen.getByTestId("glossary-finding-no_data"));
    expect(screen.queryByTestId("glossary-finding-unlinked_tables-detail")).toBeNull();
    expect(screen.getByTestId("glossary-finding-no_data-detail")).toHaveTextContent(
      "Names no column",
    );
    expect(screen.queryByTestId("glossary-finding-no_data-fix")).toBeNull();
  });
});
