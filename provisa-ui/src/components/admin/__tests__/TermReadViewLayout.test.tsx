// Copyright (c) 2026 Kenneth Stott
// Canary: ef3f3fe4-dddf-47fc-b3eb-6529005e21aa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130 (THE TERM PAGE USES THE SHARED SECTIONS AND FIELD LAYOUT): a term's page is laid out as
// the other detail pages are. Its graph section is titled Graph, as a product's is; each of its
// sections collapses with the shared collapsible, its open or closed state remembered; Graph opens
// by default.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import i18n from "../../../i18n";
import type { GlossaryTermDetail } from "../../../api/glossary";

vi.mock("../../graph3d/OntologyGraph", () => ({
  OntologySnippet: (props: { testId: string }) => <div data-testid={props.testId} />,
}));
vi.mock("../GlossaryGroundedTables", () => ({ GlossaryGroundedTables: () => null }));
vi.mock("../GlossaryRelationships", () => ({ GlossaryRelationships: () => null }));
vi.mock("../../TagControl", () => ({ TagControl: () => null }));

import { TermReadView } from "../TermReadView";

const term: GlossaryTermDetail = {
  id: 1,
  name: "Customer",
  definition: "A party that buys.",
  calculation_method: null,
  is_abstract: false,
  deprecated: false,
  ref_count: 0,
  table_count: 0,
  identity_gap: false,
  related_to: 0,
  export_excluded: false,
  retired: false,
  grounding: "not_grounded",
  agent_visible: true,
  domains: ["sales"],
  findings: 0,
  status: "draft",
  status_reason: null,
  status_by: null,
  retire_on: null,
  refs: [],
  edges_in: [],
  edges_out: [],
  experts: [],
  tags: [],
  pending_on: [],
  iri: null,
};

const t = i18n.getFixedT("en");

const view = (shown: GlossaryTermDetail = term) => (
  <TermReadView
    term={shown}
    relTypeOptions={[]}
    otherTermOptions={[]}
    relLabel={(_o, relType) => relType}
    navigateToTerm={vi.fn()}
    canEdit={false}
    onTagsChanged={vi.fn()}
  />
);

// Each section of the read view, by its toggle's test id and its heading.
const SECTIONS: [string, string][] = [
  ["glossary-term-graph-toggle", "Graph"],
  ["glossary-term-grounded-columns-toggle", "Grounded in columns"],
  ["glossary-term-grounded-tables-toggle", "Grounded in tables"],
  ["glossary-term-relationships-toggle", "Relationships"],
  ["glossary-term-experts-toggle", "Experts"],
];

describe("a term's page sections", () => {
  beforeEach(() => window.localStorage.clear());

  it("titles the term's graph Graph, as a product's is", () => {
    render(view());
    expect(screen.getByTestId("glossary-term-graph-toggle")).toHaveTextContent(/^Graph$/);
    expect(screen.queryByText("Term graph")).toBeNull();
  });

  it("makes each section a shared collapsible, in the page's order", () => {
    render(view());
    const toggles = SECTIONS.map(([id, title]) => {
      const toggle = screen.getByTestId(id);
      expect(toggle).toHaveTextContent(title);
      // The shared collapsible's control: a button that says whether it is open.
      expect(toggle).toHaveAttribute("aria-expanded");
      expect(toggle.closest(".provisa-collapsible")).not.toBeNull();
      return toggle;
    });
    for (let i = 1; i < toggles.length; i++)
      expect(
        toggles[i - 1].compareDocumentPosition(toggles[i]) & Node.DOCUMENT_POSITION_FOLLOWING,
      ).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  it("opens Graph by default, its snippet inside it", () => {
    render(view());
    expect(screen.getByTestId("glossary-term-graph-toggle")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByTestId("glossary-term-dag")).toBeInTheDocument();
  });

  it("keeps a section the reader closed closed after the page is drawn again", () => {
    const first = render(view());
    fireEvent.click(screen.getByTestId("glossary-term-experts-toggle"));
    expect(screen.getByTestId("glossary-term-experts-toggle")).toHaveAttribute(
      "aria-expanded",
      "false",
    );
    first.unmount();
    render(view());
    expect(screen.getByTestId("glossary-term-experts-toggle")).toHaveAttribute(
      "aria-expanded",
      "false",
    );
    // The others are as they were.
    expect(screen.getByTestId("glossary-term-graph-toggle")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
  });
});

// The read view's cells, as [label, size class].
function cells(): [string, string][] {
  const read = screen.getByTestId("glossary-read-fields");
  return [...read.querySelectorAll<HTMLElement>(":scope > .form-field")].map((c) => [
    c.querySelector("dt")?.textContent ?? "",
    [...c.classList].find((k) => k.startsWith("form-field--")) ?? "",
  ]);
}

describe("a term's page fields", () => {
  beforeEach(() => window.localStorage.clear());

  it("reads through the form layout's read form, in the edit form's order", () => {
    render(view({ ...term, calculation_method: "Sum of orders." }));
    const read = screen.getByTestId("glossary-read-fields");
    expect(read).toHaveClass("form-layout", "form-read");
    expect(cells()).toEqual([
      ["Name", "form-field--full"],
      ["Definition", "form-field--full"],
      ["Calculation method", "form-field--full"],
      [t("glossaryTab.excludeFromExportLabel"), "form-field--short"],
      ["Domains", "form-field--medium"],
      ["Role", "form-field--short"],
      ["Tags", "form-field--medium"],
      ["Status", "form-field--medium"],
      ["Grounding", "form-field--medium"],
    ]);
  });

  it("carries what a draft term lacks in the Draft badge's tip, with no paragraph under Status", async () => {
    render(view());
    const badge = screen.getByTestId("glossary-detail-status");
    expect(badge).toHaveTextContent("Draft");
    expect(badge.closest(".form-field")?.querySelector(".form-read__hint")).toBeNull();
    expect(screen.queryByRole("tooltip")).toBeNull();
    fireEvent.mouseEnter(badge);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(t("glossaryTab.draftDefined"));
    const grounding = screen.getByTestId("glossary-ungrounded");
    expect(grounding).toHaveTextContent(t("glossaryTab.draftUngrounded"));
    expect(grounding).toHaveClass("form-read__hint");
    expect(grounding.closest(".form-field")?.querySelector("dt")).toHaveTextContent("Grounding");
    expect(document.querySelector(".mantine-Alert-root")).toBeNull();
  });

  it("carries that a draft with no definition needs one in the badge's tip", async () => {
    render(view({ ...term, definition: null }));
    const badge = screen.getByTestId("glossary-detail-status");
    expect(badge.closest(".form-field")?.querySelector(".form-read__hint")).toBeNull();
    fireEvent.mouseEnter(badge);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(t("glossaryTab.draftUndefined"));
  });

  it("gives an approved term's badge no tip", () => {
    render(view({ ...term, status: "approved", grounding: "grounded", ref_count: 1 }));
    const badge = screen.getByTestId("glossary-detail-status");
    fireEvent.mouseEnter(badge);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("says beside Grounding which drafts a term waits on", () => {
    render(
      view({
        ...term,
        status: "approved",
        grounding: "grounded_pending",
        pending_on: [{ term_id: 4, name: "Order" }],
      }),
    );
    const pending = screen.getByTestId("glossary-pending-on");
    expect(pending).toHaveTextContent(t("glossaryTab.pendingOn", { terms: "Order" }));
    expect(pending.closest(".form-field")?.querySelector("dt")).toHaveTextContent("Grounding");
    expect(screen.queryByTestId("glossary-ungrounded")).toBeNull();
  });

  it("says nothing more of an approved, grounded term", () => {
    render(
      view({
        ...term,
        status: "approved",
        grounding: "grounded",
        ref_count: 1,
      }),
    );
    expect(screen.queryByTestId("glossary-ungrounded")).toBeNull();
    expect(screen.queryByTestId("glossary-pending-on")).toBeNull();
    expect(document.querySelector(".form-read__hint")).toBeNull();
  });
});
