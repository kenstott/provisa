// Copyright (c) 2026 Kenneth Stott
// Canary: 948d6d25-410b-422d-9029-9e57c0c251ad
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's panel on the Ontology page is the standard detail panel (maintainer 2026-11-26): the
// term's name in a pinned head, and beside it its actions as icons with tips -- Edit, the
// toggles (approval, PII, exclusion from metadata export), Duplicate, Retire, Delete -- never a
// row of labelled buttons in the body. PII is turned on here with the reason the pii tag requires
// (its registry's reason policy), on this page; it never ends in the server's refusal for want
// of one.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";
import type { MutationResult, TagAssignment } from "../types/admin";
import i18n from "../i18n";

const t = i18n.getFixedT("en");

vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
  OntologyView: () => <div data-testid="ontology-view" />,
}));
vi.mock("../components/admin/TermEditForm", async () => {
  const { FormFooter, FormLayout } = await import("../components/form/FormLayout");
  return {
    // The form's own footer, as the real form has it: Save and Cancel, `panel`.
    TermEditForm: ({ onCancel }: { onCancel: () => void }) => (
      <div data-testid="term-edit-form">
        <FormLayout>
          <FormFooter
            panel
            onSave={() => undefined}
            onCancel={onCancel}
            saveProps={{ "data-testid": "glossary-edit-save" }}
            cancelProps={{ "data-testid": "glossary-edit-cancel" }}
          />
        </FormLayout>
      </div>
    ),
  };
});
vi.mock("../components/admin/TermReadView", () => ({
  TermReadView: () => <div data-testid="term-read-view" />,
}));
vi.mock("../components/admin/FindTermsDialog", () => ({
  FindTermsDialog: () => <div data-testid="find-terms-dialog" />,
}));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
  updateGlossaryTerm: vi.fn(async () => undefined),
}));
// The server's pii rule: its registry entry requires a reason (provisa/core/models.py), and an
// assignment without one is refused with schema.tag_reason_required.
const assignTag = vi.hoisted(() =>
  vi.fn(async (input: TagAssignment): Promise<MutationResult> =>
    input.tagId === "pii" && !input.reason
      ? {
          success: false,
          message: "Tag 'pii' requires reason",
          code: "schema.tag_reason_required",
          params: { tag: "pii", field: "reason" },
        }
      : { success: true, message: "" },
  ),
);
const unassignTag = vi.hoisted(() => vi.fn(async () => ({ success: true, message: "" })));
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useAssignTag: () => ({ assignTag, loading: false }),
  useUnassignTag: () => ({ unassignTag, loading: false }),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));
const auth = { capabilities: ["glossary_read", "glossary_rw"], activeOrgId: "acme" };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
const filter = {
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set<string>(),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: false,
};
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => filter }));

import { fetchGlossaryTerm, listGlossaryTerms, updateGlossaryTerm } from "../api/glossary";

// A Property: it means columns and is grounded in no table.
const NEIGHBORHOOD = {
  id: 48,
  name: "neighborhood",
  definition: "The area of a city a place is in.",
  status: "draft",
  domains: ["pet-store"],
  refs: [{ table_id: 1, column_name: "neighborhood" }],
  ref_count: 3,
  table_count: 0,
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
  export_excluded: false,
  curatable: true,
} as unknown as GlossaryTermDetail;

async function openTerm(detail: GlossaryTermDetail = NEIGHBORHOOD) {
  vi.mocked(fetchGlossaryTerm).mockResolvedValue(detail);
  render(<GlossaryTab />);
  fireEvent.click(await screen.findByText(detail.name));
  await screen.findByTestId("term-read-view");
}
const head = () => within(screen.getByTestId("glossary-panel-actions"));

beforeEach(() => {
  vi.clearAllMocks();
  auth.capabilities = ["glossary_read", "glossary_rw"];
  vi.mocked(listGlossaryTerms).mockResolvedValue([
    { id: 48, name: "neighborhood", status: "draft" } as GlossaryTermSummary,
  ]);
});

describe("PII, clicked in a term's panel", () => {
  it("asks for the reason the pii tag requires, here, and marks the term with it", async () => {
    await openTerm();
    fireEvent.click(head().getByTestId("glossary-pii-toggle"));
    // The reason is asked on this page: nothing was sent, and no refusal is shown.
    const reason = await screen.findByTestId("glossary-pii-reason");
    expect(assignTag).not.toHaveBeenCalled();
    expect(screen.queryByText("Tag 'pii' requires reason")).toBeNull();
    // Saved with no reason, it asks for one, and still sends nothing.
    fireEvent.click(screen.getByTestId("glossary-pii-apply"));
    expect(await screen.findByText(t("common.required"))).toBeInTheDocument();
    expect(assignTag).not.toHaveBeenCalled();

    fireEvent.change(reason, { target: { value: "Locates a customer's home" } });
    vi.mocked(fetchGlossaryTerm).mockResolvedValue({
      ...NEIGHBORHOOD,
      tags: [{ tag_id: "pii", reason: "Locates a customer's home" }],
    } as unknown as GlossaryTermDetail);
    fireEvent.click(screen.getByTestId("glossary-pii-apply"));

    await waitFor(() =>
      expect(assignTag).toHaveBeenCalledWith({
        tagId: "pii",
        objectType: "term",
        termId: 48,
        reason: "Locates a customer's home",
      }),
    );
    await waitFor(() =>
      expect(head().getByTestId("glossary-pii-toggle")).toHaveAttribute("aria-pressed", "true"),
    );
    expect(screen.queryByTestId("glossary-pii-refusal")).toBeNull();
    expect(screen.queryByTestId("glossary-error")).toBeNull();
  });

  it("unmarks a PII term at once", async () => {
    await openTerm({
      ...NEIGHBORHOOD,
      tags: [{ tag_id: "pii", reason: "x" }],
    } as unknown as GlossaryTermDetail);
    const pii = head().getByTestId("glossary-pii-toggle");
    expect(pii).toHaveAttribute("aria-pressed", "true");
    fireEvent.click(pii);
    await waitFor(() =>
      expect(unassignTag).toHaveBeenCalledWith({ tagId: "pii", objectType: "term", termId: 48 }),
    );
    expect(screen.queryByTestId("glossary-pii-reason")).toBeNull();
  });
});

describe("a term's panel head, read", () => {
  it("names the term and holds its actions as icons, in order", async () => {
    await openTerm();
    expect(screen.getByTestId("glossary-panel-title")).toHaveTextContent("neighborhood");
    const ids = [
      "glossary-edit-btn",
      "glossary-approve-btn",
      "glossary-pii-toggle",
      "glossary-export-excluded-toggle",
      "glossary-duplicate-btn",
      "glossary-retire-btn",
      "glossary-delete-btn",
    ];
    const actions = ids.map((id) => head().getByTestId(id));
    for (const [i, action] of actions.entries()) {
      // An icon, named, with no word of its own; the head's one style (no variant of its own).
      expect(action.querySelector("svg"), ids[i]).not.toBeNull();
      expect(action, ids[i]).toHaveTextContent("");
      expect(action, ids[i]).toHaveAccessibleName();
      expect(action, ids[i]).toHaveAttribute("data-variant", "subtle");
      if (i > 0)
        expect(
          actions[i - 1].compareDocumentPosition(action) & Node.DOCUMENT_POSITION_FOLLOWING,
        ).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    }
    // No row of labelled buttons left in the body.
    expect(
      within(screen.getByTestId("panel-delete-lock")).queryByTestId("glossary-edit-btn"),
    ).toBeNull();
    expect(screen.getByTestId("panel-delete-lock").querySelector("[data-toolbar]")).toBeNull();
  });

  it("shows each toggle's state, its name fixed and its tip naming the state", async () => {
    await openTerm();
    const approve = head().getByTestId("glossary-approve-btn");
    expect(approve).toHaveAccessibleName(t("glossaryTab.approve"));
    expect(approve).toHaveAttribute("aria-pressed", "false");
    expect(head().getByTestId("glossary-pii-toggle")).toHaveAccessibleName(
      t("glossaryTab.piiLabel"),
    );
    const exportToggle = head().getByTestId("glossary-export-excluded-toggle");
    expect(exportToggle).toHaveAccessibleName(t("glossaryTab.excludeFromExportLabel"));
    expect(exportToggle).toHaveAttribute("aria-pressed", "false");
  });

  it("approves a draft through its impact dialog", async () => {
    await openTerm();
    fireEvent.click(head().getByTestId("glossary-approve-btn"));
    expect(await screen.findByTestId("glossary-approve-dialog")).toBeInTheDocument();
  });

  it("shows an approved term's approval pressed, left by Deprecate or Retire", async () => {
    await openTerm({ ...NEIGHBORHOOD, status: "approved" } as GlossaryTermDetail);
    const approve = head().getByTestId("glossary-approve-btn");
    expect(approve).toHaveAttribute("aria-pressed", "true");
    expect(approve).toHaveAttribute("aria-disabled", "true");
    fireEvent.click(approve);
    expect(screen.queryByTestId("glossary-approve-dialog")).toBeNull();
    expect(head().getByTestId("glossary-deprecate-btn")).toBeInTheDocument();
  });

  it("turns export exclusion over at once", async () => {
    await openTerm();
    fireEvent.click(head().getByTestId("glossary-export-excluded-toggle"));
    await waitFor(() =>
      expect(updateGlossaryTerm).toHaveBeenCalledWith(48, { export_excluded: true }),
    );
  });
});

describe("a term's panel head, while the term is edited", () => {
  it("holds the form's Save and Cancel in place of the actions", async () => {
    await openTerm();
    fireEvent.click(head().getByTestId("glossary-edit-btn"));
    await screen.findByTestId("term-edit-form");
    await waitFor(() => expect(head().getByTestId("glossary-edit-save")).toBeInTheDocument());
    expect(head().getByTestId("glossary-edit-cancel")).toBeInTheDocument();
    for (const id of ["glossary-edit-btn", "glossary-pii-toggle", "glossary-delete-btn"])
      expect(head().queryByTestId(id), id).toBeNull();
    fireEvent.click(head().getByTestId("glossary-edit-cancel"));
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    expect(head().getByTestId("glossary-edit-btn")).toBeInTheDocument();
  });
});
