// Copyright (c) 2026 Kenneth Stott
// Canary: 1f7d4b92-8e3a-4c65-b2d9-6a0c5e7f3b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634, REQ-2160, REQ-2145, REQ-2208: where the tag chip sits. A data product's panel shows its
// tags and, to its editor, the chip; a glossary term's page has the chip; a library document has
// one, and so does each of its sections, at the first of the section's passages.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, within } from "../test-utils/render";
import type { DataProduct } from "../types/admin";
import type { GlossaryTermDetail } from "../api/glossary";
import type { LibraryDocument, LibraryDocumentDetail } from "../api/referenceLibrary";
import { lifecycleProps } from "../pages/data-products/__tests__/lifecycleProps";

// The chip itself is TagControlGovernedObjects.test's; here, which object each mount names.
vi.mock("../components/TagControl", () => ({
  TagControl: (props: Record<string, unknown>) => (
    <span
      data-testid={`chip-${String(props.objectType)}`}
      data-target={JSON.stringify({ ...props, onChanged: undefined })}
      data-readonly={String(!!props.readOnly)}
      onClick={() => (props.onChanged as (() => void) | undefined)?.()}
    />
  ),
}));
vi.mock("../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../components/graph3d/OntologyGraph", () => ({ OntologySnippet: () => null }));
vi.mock("../components/admin/GlossaryGroundedTables", () => ({
  GlossaryGroundedTables: () => null,
}));
vi.mock("../components/admin/GlossaryRelationships", () => ({ GlossaryRelationships: () => null }));

const DOC: LibraryDocument = {
  id: 7,
  title: "Privacy policy",
  source_kind: "url",
  source: "https://example.com/privacy",
  media_type: "text/markdown",
  status: "kept",
  domains: ["sales"],
  embedding_model: "m",
  needs_reembedding: false,
  passages: 4,
  added_by: "org_admin",
  added_at: "2026-11-01T09:00:00+00:00",
  refreshed_at: null,
  replaced_at: null,
};
const DETAIL: LibraryDocumentDetail = {
  ...DOC,
  passage_list: [
    { ordinal: 0, section: null, page: null, text: "Opening" },
    { ordinal: 1, section: "Personal data", page: null, text: "One" },
    { ordinal: 2, section: "Personal data", page: null, text: "Two" },
    { ordinal: 3, section: null, page: 3, text: "Page three" },
  ],
};
vi.mock("../api/referenceLibrary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/referenceLibrary")>()),
  fetchLibrary: vi.fn(async () => [DOC]),
  fetchLibraryDocument: vi.fn(async () => DETAIL),
  fetchRetrieval: vi.fn(async () => ({ retrieval: "passage" })),
}));

const target = (el: HTMLElement) => JSON.parse(el.dataset.target as string);

describe("a data product's tags", () => {
  const PRODUCT = { id: "sales_kpis", name: "Sales KPIs", customProperties: {} } as DataProduct;

  async function panel(canEdit: boolean, tags: string[], onTagsChanged = vi.fn()) {
    const { DataProductDetailPanel } =
      await import("../pages/data-products/DataProductDetailPanel");
    render(
      <DataProductDetailPanel
        p={PRODUCT}
        core={{ product: PRODUCT, queries: { graphql: "", sql: "", jsonapi: "" }, tags }}
        coreError={null}
        reader={null}
        canEdit={canEdit}
        canDelete={false}
        owner
        seesConsumers
        lineageLoading={false}
        contractsVersion={0}
        lineageError=""
        lineageGraph={null}
        onEdit={vi.fn()}
        onDelete={vi.fn()}
        {...lifecycleProps()}
        onTagsChanged={onTagsChanged}
      />,
    );
    return screen.getByTestId("data-product-card-tags-sales_kpis");
  }

  it("is the chip, naming the product, for its editor, and its change reads the product again", async () => {
    const onTagsChanged = vi.fn();
    const card = await panel(true, ["certified"], onTagsChanged);
    const chip = within(card).getByTestId("chip-product");
    expect(target(chip)).toMatchObject({ objectType: "product", productId: "sales_kpis" });
    fireEvent.click(chip);
    expect(onTagsChanged).toHaveBeenCalledTimes(1);
  });

  it("is the list it reads, for any other reader", async () => {
    const card = await panel(false, ["certified", "gold"]);
    expect(within(card).queryByTestId("chip-product")).toBeNull();
    expect(within(card).getByTestId("data-product-detail-tags-sales_kpis")).toHaveTextContent(
      "certifiedgold",
    );
  });
});

describe("a glossary term's tags", () => {
  const TERM = {
    id: 4,
    name: "Order",
    definition: "A purchase",
    domains: ["sales"],
    tags: [],
    status: "approved",
    grounding: "grounded",
    ref_count: 0,
    table_count: 0,
    refs: [],
    experts: [],
    export_excluded: false,
  } as unknown as GlossaryTermDetail;

  async function view(canEdit: boolean, onTagsChanged = vi.fn()) {
    const { TermReadView } = await import("../components/admin/TermReadView");
    render(
      <TermReadView
        term={TERM}
        relTypeOptions={[]}
        otherTermOptions={[]}
        relLabel={(_o, r) => r}
        navigateToTerm={vi.fn()}
        canEdit={canEdit}
        onTagsChanged={onTagsChanged}
      />,
    );
    return screen.getByTestId("chip-term");
  }

  it("is the chip, naming the term, editable by its curator", async () => {
    const onTagsChanged = vi.fn();
    const chip = await view(true, onTagsChanged);
    expect(target(chip)).toMatchObject({ objectType: "term", termId: 4 });
    expect(chip.dataset.readonly).toBe("false");
    fireEvent.click(chip);
    expect(onTagsChanged).toHaveBeenCalledTimes(1);
  });

  it("is only read by a reader who does not curate the glossary", async () => {
    expect((await view(false)).dataset.readonly).toBe("true");
  });
});

describe("a library document's tags", () => {
  it("are on the document and on each section, at its first passage", async () => {
    const { LibraryPage } = await import("../pages/library/LibraryPage");
    render(<LibraryPage />);
    fireEvent.click(await screen.findByTestId("reflib-row-7"));
    expect(target(await screen.findByTestId("chip-document"))).toMatchObject({
      objectType: "document",
      documentId: 7,
    });
    await screen.findByTestId("reflib-passages");
    // the untitled opening is the document's; "Personal data" once, for its two passages; page 3
    expect(screen.getAllByTestId("chip-section").map(target)).toEqual([
      expect.objectContaining({
        documentId: 7,
        sectionHeading: "Personal data",
        sectionPage: null,
      }),
      expect.objectContaining({ documentId: 7, sectionHeading: null, sectionPage: 3 }),
    ]);
  });
});
