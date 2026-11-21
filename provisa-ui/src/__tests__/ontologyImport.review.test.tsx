// Copyright (c) 2026 Kenneth Stott
// Canary: 7e1c4a92-b5d8-4f03-a6e7-c29d0b51f84a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: an import's review. Its items by kind; each proposal with why it was proposed, what it
// says read in its direction, and the term's role, status and grounding; approved or rejected one
// at a time, a same-entity term taking the item's definition on request, or several approved at
// once; an item renamed, edited, excluded or included again; an alignment of the steward's own.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import { ACCOUNT, CLOSED, REVIEW } from "./ontologyImportFixtures";
import type { GlossaryTermSummary } from "../api/glossary";

const api = vi.hoisted(() => ({
  fetchOntologyImport: vi.fn(),
  approveAlignment: vi.fn(),
  rejectAlignment: vi.fn(),
  bulkApprove: vi.fn(),
  editImportItem: vi.fn(),
  addAlignment: vi.fn(),
  discardOntologyImport: vi.fn(),
}));
vi.mock("../api/ontologyImport", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/ontologyImport")>()),
  ...api,
}));

import { ImportReviewPanel } from "../components/admin/ontologyImport/ImportReviewPanel";

const CONTRACT = { id: 9, name: "contract" } as GlossaryTermSummary;

async function choose(testId: string, option: string) {
  const input = screen.getByTestId(testId);
  fireEvent.click(input);
  await waitFor(() => expect(input).toHaveAttribute("aria-controls"));
  const list = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(within(list as HTMLElement).getByRole("option", { name: option, hidden: true }));
}

function panel(onChanged = vi.fn()) {
  render(
    <ImportReviewPanel
      importId={7}
      terms={[CONTRACT]}
      onChanged={onChanged}
      onDiscarded={async () => undefined}
    />,
  );
  return onChanged;
}

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  api.fetchOntologyImport.mockResolvedValue(REVIEW);
  api.approveAlignment.mockResolvedValue(undefined);
  api.rejectAlignment.mockResolvedValue(undefined);
  api.editImportItem.mockResolvedValue(REVIEW);
});

describe("ImportReviewPanel", () => {
  it("shows the items by kind, each kind counted", async () => {
    panel();
    const kinds = await screen.findByTestId("ontology-review-kinds");
    expect(kinds).toHaveTextContent("Classes (2)");
    expect(kinds).toHaveTextContent("Properties (1)");
    expect(kinds).toHaveTextContent("Values (1)");
    expect(kinds).toHaveTextContent("Concepts (0)");
    expect(screen.getByTestId("ontology-item-1")).toHaveTextContent(ACCOUNT.iri as string);
    expect(screen.queryByTestId("ontology-item-3")).toBeNull();
    fireEvent.click(within(kinds).getByText("Properties (1)"));
    expect(await screen.findByTestId("ontology-item-3")).toBeInTheDocument();
    expect(screen.queryByTestId("ontology-item-1")).toBeNull();
  });

  it("gives each proposal its basis, its direction and the term's role, status and grounding", async () => {
    panel();
    await screen.findByTestId("ontology-item-1");
    expect(screen.getByTestId("ontology-alignment-basis-10")).toHaveTextContent("Name");
    expect(screen.getByTestId("ontology-alignment-phrase-10")).toHaveTextContent(
      "account is the same entity as account",
    );
    const target = screen.getByTestId("ontology-alignment-target-10");
    expect(within(target).getByText("Class")).toBeInTheDocument();
    expect(target.querySelector("[data-grounding='grounded']")).not.toBeNull();
    expect(screen.getByTestId("ontology-alignment-basis-11")).toHaveTextContent("Ontology");
    expect(screen.getByTestId("ontology-alignment-phrase-11")).toHaveTextContent(
      "account is a kind of agreement",
    );
    expect(screen.getByTestId("ontology-alignment-target-11")).toHaveTextContent("in this import");
    // A proposal read toward the item: the term is part of it.
    fireEvent.click(screen.getByText("Properties (1)"));
    expect(await screen.findByTestId("ontology-alignment-phrase-12")).toHaveTextContent(
      "account holder name is part of account holder",
    );
    expect(screen.getByTestId("ontology-alignment-basis-12")).toHaveTextContent("Qualifier");
    const property = screen.getByTestId("ontology-alignment-target-12");
    expect(within(property).getByText("Property")).toBeInTheDocument();
    expect(property.querySelector("[data-grounding]")).toBeNull();
  });

  it("approves a same-entity proposal with the item's definition, and rejects another", async () => {
    const onChanged = panel();
    await screen.findByTestId("ontology-item-1");
    fireEvent.click(screen.getByTestId("ontology-alignment-use-definition-10"));
    fireEvent.click(screen.getByTestId("ontology-alignment-approve-10"));
    await waitFor(() => expect(api.approveAlignment).toHaveBeenCalledWith(10, true));
    fireEvent.click(screen.getByTestId("ontology-alignment-reject-11"));
    await waitFor(() => expect(api.rejectAlignment).toHaveBeenCalledWith(11));
    expect(onChanged).toHaveBeenCalledTimes(2);
    expect(api.fetchOntologyImport).toHaveBeenCalledTimes(3);
  });

  it("offers the item's definition only on a same-entity proposal", async () => {
    panel();
    await screen.findByTestId("ontology-item-1");
    expect(screen.getByTestId("ontology-alignment-use-definition-10")).toBeInTheDocument();
    expect(screen.queryByTestId("ontology-alignment-use-definition-11")).toBeNull();
  });

  it("approves the selected proposals at once and names any refused", async () => {
    api.bulkApprove.mockResolvedValue({
      approved: [10],
      refused: [{ id: 11, message: "alignment 11 is in the glossary now" }],
    });
    panel();
    await screen.findByTestId("ontology-item-1");
    expect(screen.getByTestId("ontology-bulk-approve")).toBeDisabled();
    fireEvent.click(screen.getByTestId("ontology-select-shown"));
    expect(screen.getByTestId("ontology-bulk-approve")).toHaveTextContent("Approve 2 selected");
    fireEvent.click(screen.getByTestId("ontology-bulk-approve"));
    await waitFor(() => expect(api.bulkApprove).toHaveBeenCalledWith(7, [10, 11]));
    expect(await screen.findByTestId("ontology-review-error")).toHaveTextContent(
      "Alignment 11 was not approved: alignment 11 is in the glossary now",
    );
  });

  it("renames an item and edits its definition", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("ontology-item-edit-1"));
    fireEvent.change(screen.getByTestId("ontology-item-name"), {
      target: { value: "deposit account" },
    });
    fireEvent.change(screen.getByTestId("ontology-item-definition"), {
      target: { value: "An account holding deposits." },
    });
    fireEvent.click(screen.getByTestId("ontology-item-save"));
    await waitFor(() =>
      expect(api.editImportItem).toHaveBeenCalledWith(1, {
        name: "deposit account",
        definition: "An account holding deposits.",
      }),
    );
    await waitFor(() => expect(screen.queryByTestId("ontology-item-edit-dialog")).toBeNull());
  });

  it("excludes a pending item and includes an excluded one again", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("ontology-item-exclude-1"));
    await waitFor(() => expect(api.editImportItem).toHaveBeenCalledWith(1, { excluded: true }));
    fireEvent.click(screen.getByText("Values (1)"));
    const include = await screen.findByTestId(`ontology-item-exclude-${CLOSED.id}`);
    expect(include).toHaveTextContent("Include");
    fireEvent.click(include);
    await waitFor(() => expect(api.editImportItem).toHaveBeenCalledWith(4, { excluded: false }));
  });

  it("adds the steward's own alignment with a term, read in the direction chosen", async () => {
    api.addAlignment.mockResolvedValue({});
    panel();
    fireEvent.click(await screen.findByTestId("ontology-item-align-1"));
    await choose("ontology-add-alignment-type", "Kind of");
    await choose("ontology-add-alignment-target", "contract");
    await choose("ontology-add-alignment-direction", "account is a kind of contract");
    fireEvent.click(screen.getByTestId("ontology-add-alignment-save"));
    await waitFor(() =>
      expect(api.addAlignment).toHaveBeenCalledWith(7, {
        item_id: 1,
        rel_type: "KIND_OF",
        item_is_source: true,
        term_id: 9,
      }),
    );
  });

  it("aligns an item with another item of the import, never as the same entity", async () => {
    api.addAlignment.mockResolvedValue({});
    panel();
    fireEvent.click(await screen.findByTestId("ontology-item-align-1"));
    fireEvent.click(screen.getByRole("radio", { name: "With another item" }));
    const types = screen.getByTestId("ontology-add-alignment-type");
    expect(types).toHaveValue("Synonym");
    fireEvent.click(types);
    await waitFor(() => expect(types).toHaveAttribute("aria-controls"));
    const list = document.getElementById(types.getAttribute("aria-controls") as string);
    expect(
      within(list as HTMLElement).queryByRole("option", { name: "Same entity", hidden: true }),
    ).toBeNull();
    await choose("ontology-add-alignment-target", "agreement");
    await choose("ontology-add-alignment-direction", "agreement is a synonym of account");
    fireEvent.click(screen.getByTestId("ontology-add-alignment-save"));
    await waitFor(() =>
      expect(api.addAlignment).toHaveBeenCalledWith(7, {
        item_id: 1,
        rel_type: "SYNONYM_OF",
        item_is_source: false,
        other_item_id: 2,
      }),
    );
  });
});
