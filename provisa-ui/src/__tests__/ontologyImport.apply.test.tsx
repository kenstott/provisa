// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8a2c63-d1e9-4b75-8c06-b3a7e5d920c1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: Apply, what it will do shown first; a refusal lists every problem that stops it, in
// the dialog, and changes nothing. A term made or aligned by an import shows its IRI, and that the
// IRI keeps it as curator work.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";
import { ACCOUNT, AGREEMENT, HOLDER, CLOSED, REVIEW } from "./ontologyImportFixtures";
import type { GlossaryTermDetail } from "../api/glossary";

const api = vi.hoisted(() => ({ applyOntologyImport: vi.fn() }));
vi.mock("../api/ontologyImport", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/ontologyImport")>()),
  ...api,
}));
vi.mock("../components/TagControl", () => ({ TagControl: () => null }));
vi.mock("../components/graph3d/OntologyGraph", () => ({ OntologySnippet: () => null }));
vi.mock("../components/admin/GlossaryGroundedTables", () => ({
  GlossaryGroundedTables: () => null,
}));
vi.mock("../components/admin/GlossaryRelationships", () => ({ GlossaryRelationships: () => null }));

import { ApplyRefused, applyPreview, type ImportReview } from "../api/ontologyImport";
import { ApplyImportDialog } from "../components/admin/ontologyImport/ApplyImportDialog";
import { TermReadView } from "../components/admin/TermReadView";

/** The fixture with account approved as the same entity as the term "account". */
const DECIDED: ImportReview = {
  ...REVIEW,
  items: [
    {
      ...ACCOUNT,
      alignments: [
        { ...ACCOUNT.alignments[0], status: "approved" },
        { ...ACCOUNT.alignments[1], status: "approved" },
      ],
    },
    AGREEMENT,
    HOLDER,
    CLOSED,
  ],
};

beforeEach(() => api.applyOntologyImport.mockReset());

describe("applyPreview", () => {
  it("counts what applying makes, aligns, writes, leaves waiting and leaves undecided", () => {
    expect(applyPreview(DECIDED.items)).toEqual({
      create: 2, // agreement, account holder
      sameAs: 1, // account is the term "account"
      edges: 1, // account KIND_OF agreement
      waiting: 1, // closed VALID_VALUE_OF account: closed is excluded
      excluded: 1,
      undecided: 1, // account holder's PART_OF
    });
  });

  it("makes an item a draft term until a same-entity proposal for it is approved", () => {
    expect(applyPreview(REVIEW.items)).toMatchObject({ create: 3, sameAs: 0, undecided: 3 });
  });
});

describe("ApplyImportDialog", () => {
  it("shows what applying will do before it is applied", () => {
    render(<ApplyImportDialog review={DECIDED} onClose={vi.fn()} onApplied={vi.fn()} />);
    expect(screen.getByTestId("ontology-apply-previewCreate")).toHaveTextContent(
      "2items become draft terms",
    );
    expect(screen.getByTestId("ontology-apply-previewSameAs")).toHaveTextContent(
      "1item is aligned with an existing term, which takes its IRI",
    );
    expect(screen.getByTestId("ontology-apply-previewEdges").dataset.count).toBe("1");
    expect(screen.getByTestId("ontology-apply-previewWaiting").dataset.count).toBe("1");
    expect(screen.getByTestId("ontology-apply-previewUndecided").dataset.count).toBe("1");
    expect(api.applyOntologyImport).not.toHaveBeenCalled();
  });

  it("applies the import and reports what it did", async () => {
    const result = { id: 7, created: [], aligned: [], edges: [], waiting: [] };
    api.applyOntologyImport.mockResolvedValue(result);
    const onApplied = vi.fn();
    render(<ApplyImportDialog review={DECIDED} onClose={vi.fn()} onApplied={onApplied} />);
    fireEvent.click(screen.getByTestId("ontology-apply-confirm"));
    await waitFor(() => expect(onApplied).toHaveBeenCalledWith(result));
    expect(api.applyOntologyImport).toHaveBeenCalledWith(7);
  });

  it("lists every problem a refused apply names, in the dialog, and applies nothing", async () => {
    const problems = [
      "the glossary already holds a term 'agreement': approve aligning the item with it as the same entity, rename the item, or exclude it",
      "'account' is approved as the same entity as 'account', 'client': reject all but one",
    ];
    api.applyOntologyImport.mockRejectedValue(new ApplyRefused(problems.join("; "), problems));
    const onApplied = vi.fn();
    render(<ApplyImportDialog review={DECIDED} onClose={vi.fn()} onApplied={onApplied} />);
    fireEvent.click(screen.getByTestId("ontology-apply-confirm"));
    const box = await screen.findByTestId("ontology-apply-problems");
    expect(box).toHaveTextContent("Nothing was applied. 2 problems to resolve first:");
    for (const p of problems) expect(box).toHaveTextContent(p);
    expect(box.querySelectorAll("li")).toHaveLength(2);
    expect(onApplied).not.toHaveBeenCalled();
    expect(screen.getByTestId("ontology-apply-dialog")).toBeInTheDocument();
  });
});

describe("a term's IRI", () => {
  const TERM = {
    id: 4,
    name: "account",
    definition: "A contractual arrangement to hold a balance.",
    domains: ["sales"],
    tags: [],
    status: "draft",
    grounding: "grounded",
    ref_count: 0,
    table_count: 1,
    refs: [],
    experts: [],
    export_excluded: false,
    iri: ACCOUNT.iri,
  } as unknown as GlossaryTermDetail;

  const view = (term: GlossaryTermDetail) =>
    render(
      <TermReadView
        term={term}
        relTypeOptions={[]}
        otherTermOptions={[]}
        relLabel={(_o, r) => r}
        navigateToTerm={vi.fn()}
        canEdit
        onTagsChanged={vi.fn()}
      />,
    );

  it("is shown on the term's panel, with that it keeps the term as curated", () => {
    view(TERM);
    expect(screen.getByText(ACCOUNT.iri as string)).toBeInTheDocument();
    expect(screen.getByTestId("glossary-detail-curated-by-iri")).toHaveTextContent(
      "Aligned with an imported concept",
    );
  });

  it("is not shown on a term with none", () => {
    view({ ...TERM, iri: null });
    expect(screen.queryByTestId("glossary-detail-curated-by-iri")).toBeNull();
  });
});
