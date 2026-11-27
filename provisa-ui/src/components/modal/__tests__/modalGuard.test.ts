// Copyright (c) 2026 Kenneth Stott
// Canary: e17b5d93-2c48-4f6a-9b0e-8d3a6c1f5e72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 10: every modal dialog is the shared ModalLayout (components/modal/ModalLayout.tsx),
// whose buttons sit in a fixed bar at its bottom, outside its scrolling content. This guard reads
// every component's TSX and fails a file that draws a dialog with Mantine's Modal itself, or that
// puts a dialog's button in its content instead of a FormFooter (which the layout draws in the bar).
// The content of a dialog is held too where it is a component of its own file that the dialog
// renders.

import { describe, it, expect } from "vitest";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { checkModalRules, dialogContentImports, type ModalViolation } from "./modalRules";

const SRC = resolve(__dirname, "../../..");

/** The one file that draws Mantine's Modal: the shared layout itself. */
const THE_LAYOUT = "components/modal/ModalLayout.tsx";

/**
 * Permanent: the shared controls a dialog may render, which are no dialog's content: the form
 * layout itself (it draws FormFooter) and the one Save, Cancel and destructive confirm, and a
 * reusable input whose own chips are buttons.
 */
const SHARED_CONTROLS: Readonly<Record<string, string>> = {
  "components/form/FormLayout.tsx": "the shared form layout, which draws FormFooter",
  "components/form/CancelButton.tsx": "the one Cancel, drawn by FormFooter",
  "components/form/SaveButton.tsx": "the one Save, drawn by FormFooter",
  "components/form/DestructiveConfirmButton.tsx": "the one destructive confirm",
  "components/MultiSelect.tsx":
    "one reusable input: its selected values are chips with a remove button",
};

/**
 * Permanent: buttons in a dialog's content that are not the dialog's actions, by file and a marker
 * in the button's own JSX: each acts on a part of the content (a row, an option, a section), or is
 * passed to a component that draws it in its FormFooter. Each says why.
 */
const BUTTON_EXCEPTIONS: Readonly<Record<string, { marker: string; reason: string }[]>> = {
  "components/admin/RelatedToDialog.tsx": [
    {
      marker: "glossary-related-to-start-",
      reason: "each way to relate the two terms is a row with its own Start",
    },
  ],
  "components/admin/TermReviewQueue.tsx": [
    { marker: "find-terms-accept-${", reason: "an action of one draft card of the list" },
    { marker: "find-terms-merge-${", reason: "an action of one draft card of the list" },
    { marker: "find-terms-edit-${", reason: "an action of one draft card of the list" },
    { marker: "find-terms-reject-${", reason: "an action of one draft card of the list" },
    {
      marker: "find-terms-back",
      reason: "back from one scan's drafts to the list of scans, beside the scan's title",
    },
    { marker: "find-terms-accept-selected", reason: "a bulk action above the list of drafts" },
    {
      marker: "find-terms-propose-relationships",
      reason: "a bulk action above the list of drafts",
    },
  ],
  "components/graph/Neo4jExportModal.tsx": [
    {
      marker: "neo4j-download-script-btn",
      reason: "the first of the dialog's two ways, set above the server fields it is the other to",
    },
  ],
  "components/OrgSelectionPrompt.tsx": [
    {
      marker: "org-selection-option-",
      reason: "the organisations to choose from: the list is the choice",
    },
  ],
  "components/OrgSelector.tsx": [
    {
      marker: "org-selector-option-",
      reason: "the organisations to choose from: the list is the choice",
    },
  ],
  "components/PersonalAccessTokens.tsx": [
    {
      marker: "profile-pat-dismiss",
      reason: "dismisses the notice of a token just issued, in that notice",
    },
  ],
  "components/relationships/DiscoverRunView.tsx": [
    { marker: "discover-stop-", reason: "stops one run, on that run's line" },
    {
      marker: "discover-new-run",
      reason: "passed to ProposalsTable as newRun, which draws it in its FormFooter",
    },
    {
      marker: "discover-done",
      reason: "passed to ProposalsTable as newRun, which draws it in its FormFooter",
    },
  ],
  "components/relationships/DiscoverScopeForm.tsx": [
    {
      marker: "discover-reset-defaults",
      reason: "acts on the notice it ends: the form started from the last run",
    },
    {
      marker: "discover-add-new-tables",
      reason: "acts on the notice it ends: tables added since the last run",
    },
  ],
  "components/SqlModelingModal.tsx": [
    { marker: "sql-modeling-generate-sql", reason: "the workbench's question bar, a work surface" },
    { marker: "sql-modeling-run", reason: "the workbench's toolbar, a work surface" },
    { marker: "sql-modeling-extract-joins", reason: "the workbench's toolbar, a work surface" },
  ],
  "components/UserProfileModal.tsx": [
    {
      marker: "profile-delete-submit",
      reason: "deletes the account, in its own section of the profile, apart from its Save",
    },
  ],
  "pages/sql/ResultsGrid.tsx": [
    { marker: "download-csv-btn", reason: "the result grid's own toolbar" },
    { marker: "download-xlsx-btn", reason: "the result grid's own toolbar" },
    { marker: "copy-results-btn", reason: "the result grid's own toolbar" },
    { marker: "clear-filters-btn", reason: "the result grid's own toolbar" },
  ],
  "pages/tables/CalendarCreateModal.tsx": [
    { marker: "calendar-holiday-add", reason: "adds a holiday preset to the list above it" },
  ],
  "pages/tables/modeling/WizardFrame.tsx": [
    {
      marker: "data-list-add",
      reason:
        "a wizard step's Add: adds a hub, link, satellite, rule or dimension to the list above it",
    },
  ],
};

function componentFiles(dir: string, found: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      if (name !== "__tests__" && name !== "__mocks__" && name !== "test-utils") {
        componentFiles(path, found);
      }
    } else if (name.endsWith(".tsx") && !name.includes(".test.")) {
      found.push(relative(SRC, path));
    }
  }
  return found;
}

/** The file a relative import names, from the file that imports it; undefined if not a TSX file. */
function resolveImport(from: string, spec: string): string | undefined {
  const base = resolve(SRC, dirname(from), spec);
  for (const candidate of [`${base}.tsx`, join(base, "index.tsx")]) {
    if (existsSync(candidate)) return relative(SRC, candidate);
  }
  return undefined;
}

function read(file: string): string {
  return readFileSync(join(SRC, file), "utf8");
}

function describeViolations(found: Record<string, ModalViolation[]>): string {
  return Object.entries(found)
    .map(([f, vs]) => vs.map((v) => `${f}:${v.line} [${v.rule}] ${v.detail}`).join("\n"))
    .join("\n");
}

describe("modal guard (REQ-2277 rule 10)", () => {
  const files = componentFiles(SRC).filter((f) => f !== THE_LAYOUT);
  // The files whose components a dialog renders as its content.
  const content = new Set<string>();
  for (const f of files) {
    for (const spec of dialogContentImports(f, read(f))) {
      const target = resolveImport(f, spec);
      if (target !== undefined && target !== THE_LAYOUT && !(target in SHARED_CONTROLS)) {
        content.add(target);
      }
    }
  }

  it("every dialog is a ModalLayout with its buttons in the fixed bar", () => {
    const found: Record<string, ModalViolation[]> = {};
    for (const f of files) {
      const vs = checkModalRules(f, read(f), {
        dialogContent: content.has(f),
        contentButtons: (BUTTON_EXCEPTIONS[f] ?? []).map((e) => e.marker),
      });
      if (vs.length > 0) found[f] = vs;
    }
    expect(describeViolations(found)).toBe("");
  });

  it("every button exception still matches a button", () => {
    const stale: string[] = [];
    for (const [f, entries] of Object.entries(BUTTON_EXCEPTIONS)) {
      const text = existsSync(join(SRC, f)) ? read(f) : "";
      for (const e of entries) if (!text.includes(e.marker)) stale.push(`${f}: ${e.marker}`);
    }
    expect(stale).toEqual([]);
  });

  it("fails a dialog drawn with Mantine's Modal", () => {
    const src = `import { Modal } from "@mantine/core";\nexport const X = () => <Modal opened onClose={() => {}} />;`;
    expect(checkModalRules("x.tsx", src).map((v) => v.rule)).toEqual(["not-modal-layout"]);
  });

  it("fails a dialog's button in its scrolling content", () => {
    const src = `export const X = () => (
      <ModalLayout opened onClose={() => {}}>
        <Text>Body</Text>
        <Group><Button onClick={() => {}}>Apply</Button></Group>
      </ModalLayout>
    );`;
    expect(checkModalRules("x.tsx", src).map((v) => v.rule)).toEqual(["button-in-content"]);
  });

  it("passes a dialog whose buttons are its FormFooter", () => {
    const src = `export const X = () => (
      <ModalLayout opened onClose={() => {}}>
        <Text>Body</Text>
        <FormFooter onCancel={() => {}} action={<Button>Apply</Button>}><Button>More</Button></FormFooter>
      </ModalLayout>
    );`;
    expect(checkModalRules("x.tsx", src)).toEqual([]);
  });

  it("holds the buttons of a component of the same file that a dialog renders", () => {
    const src = `function Body() { return <Group><Button>Apply</Button></Group>; }
      function Row() { return <Button>Elsewhere</Button>; }
      export const X = () => <ModalLayout opened onClose={() => {}}><Body /></ModalLayout>;`;
    expect(checkModalRules("x.tsx", src).map((v) => [v.rule, v.line])).toEqual([
      ["button-in-content", 1],
    ]);
  });

  it("holds every button of a dialog's content file", () => {
    const src = `export const Body = () => <Stack><Button>Apply</Button></Stack>;`;
    expect(checkModalRules("x.tsx", src).map((v) => v.rule)).toEqual([]);
    expect(checkModalRules("x.tsx", src, { dialogContent: true }).map((v) => v.rule)).toEqual([
      "button-in-content",
    ]);
  });
});
