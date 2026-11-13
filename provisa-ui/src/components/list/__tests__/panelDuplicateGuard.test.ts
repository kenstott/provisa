// Copyright (c) 2026 Kenneth Stott
// Canary: a1d71855-83a1-4dc1-8519-379c83796ffd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating an object, the same on every list page:
//
// 1. "+ Object" opens the create form EMPTY: only its defaults (on Sources, the picked type's).
//    Nothing is read from the object selected in the list or the one last edited. The behavioural
//    test of this is per page, with `expectHoldsNoneOf` (test-utils/createForm.ts): select an
//    object, (edit it and cancel,) press "+ Object", and the form holds none of its values.
// 2. Duplicating is ASKED FOR: a Duplicate action in the object's detail panel head, beside Edit
//    and Delete (PanelDuplicate, for a caller who may create that kind of object). It opens the
//    CREATE form prefilled from the object, under a new identity (editable; an id that exists is
//    refused on save by the create's own validation): "<id>-copy" where the name takes a hyphen,
//    "<id>_copy" where it is a SQL, GraphQL or metric identifier, "<id>copy" where it takes no
//    separator at all (a region). Nothing that depends on
//    the object is copied with it, and no secret value: a credential is carried as the same
//    reference the object uses, or left empty with the form saying it must be entered.
//
// This guard reads the source: every detail panel that offers Edit in its head (a pencil in its
// <PanelActions>, or beside the detail's PanelDelete) offers PanelDuplicate too, except the
// panels listed below as not yet converted or as stated exceptions (with their reason). A panel
// taken off a list: the guard fails on a listed panel that already offers Duplicate, or that no
// longer offers Edit.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(__dirname, "..", "..", "..");

/** Panels that offer Edit in their head but not Duplicate yet. Empty: every panel is converted or
 * a stated exception below. */
const NOT_YET_CONVERTED: Readonly<Record<string, string>> = {};

/**
 * Panels whose Edit is not the Edit of an object a create form could copy, by file, with the
 * reason. Each is left without Duplicate on purpose: inventing a create form for it would be a
 * new feature, not the same action everywhere.
 */
const STATED_EXCEPTIONS: Readonly<Record<string, string>> = {
  "components/admin/RegionEngineTabs.tsx":
    "Admin › Federation regions: it edits the engine store of a region the licence grant already " +
    "lists (one tab per granted region); regions are not created here, so there is no create " +
    "form to open. Granted regions and stores are duplicated in Admin › Region grants " +
    "(GrantEditor).",
  "components/admin/ontologyImport/ImportReviewPanel.tsx":
    "Ontology import review: the panel is an import's review (Apply, Discard); its pencil edits " +
    "one proposed item of the import, an object that exists only inside the review. An import is " +
    "made by uploading a file, not by a create form a copy could fill.",
};

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sources(path);
    return /\.tsx$/.test(name) && !/\.test\.tsx$/.test(name) ? [path] : [];
  });
}

/** What a file renders inside each <PanelActions>…</PanelActions>. */
export function heads(text: string): string[] {
  const found: string[] = [];
  for (const m of text.matchAll(/<PanelActions>/g)) {
    const end = text.indexOf("</PanelActions>", m.index);
    found.push(text.slice(m.index, end < 0 ? text.length : end));
  }
  return found;
}

/** Whether the file's detail panel offers Edit in its head. */
export function offersEdit(text: string): boolean {
  if (heads(text).some((head) => /<Pencil\b/.test(head))) return true;
  // A detail whose actions are not portalled into a head: Edit beside the detail's Delete.
  return /<PanelDelete\b/.test(text) && /<Pencil\b|leftSection=\{<Pencil\b/.test(text);
}

/** Whether the file's detail panel offers Duplicate. */
export function offersDuplicate(text: string): boolean {
  return /<PanelDuplicate\b/.test(text);
}

describe("the rule itself", () => {
  it("finds Edit in a head, and Duplicate beside it", () => {
    const head = `<PanelActions><ActionIcon aria-label="Edit"><Pencil size={14} /></ActionIcon>
      <PanelDelete itemKey={x} /></PanelActions>`;
    expect(offersEdit(head)).toBe(true);
    expect(offersDuplicate(head)).toBe(false);
    expect(
      offersDuplicate(head.replace("<PanelDelete", "<PanelDuplicate name={x} />\n<PanelDelete")),
    ).toBe(true);
  });

  it("finds Edit beside a detail's Delete that is not in a head", () => {
    expect(
      offersEdit(
        `<Button leftSection={<Pencil size={14} />}>Edit</Button><PanelDelete itemKey={x} />`,
      ),
    ).toBe(true);
  });

  it("does not take a pencil outside a detail's head for its Edit", () => {
    expect(offersEdit(`<PanelActions><Button>Discover</Button></PanelActions><Pencil />`)).toBe(
      false,
    );
  });
});

describe("every detail panel that offers Edit in its head offers Duplicate", () => {
  const files = sources(SRC).map((file) => ({
    name: relative(SRC, file),
    text: readFileSync(file, "utf8"),
  }));
  const editing = files.filter((f) => offersEdit(f.text));

  it("looks at the detail panels that offer Edit", () => {
    expect(editing.length).toBeGreaterThan(8);
  });

  for (const { name, text } of editing) {
    it(name, () => {
      if (name in STATED_EXCEPTIONS) {
        expect(
          offersDuplicate(text),
          `${name} offers Duplicate: take it off STATED_EXCEPTIONS`,
        ).toBe(false);
        return;
      }
      if (name in NOT_YET_CONVERTED) {
        expect(
          offersDuplicate(text),
          `${name} offers Duplicate: take it off NOT_YET_CONVERTED`,
        ).toBe(false);
        return;
      }
      expect(
        offersDuplicate(text),
        `${name}: its head offers Edit but not Duplicate (PanelDuplicate)`,
      ).toBe(true);
    });
  }

  it("lists as not yet converted or excepted only panels that offer Edit", () => {
    const stale = [...Object.keys(NOT_YET_CONVERTED), ...Object.keys(STATED_EXCEPTIONS)].filter(
      (name) => !editing.some((f) => f.name === name),
    );
    expect(stale, "listed, but no longer offering Edit in a head: take them off").toEqual([]);
  });
});

describe("the exceptions", () => {
  it("give a reason each", () => {
    for (const [name, reason] of Object.entries(STATED_EXCEPTIONS))
      expect(reason.trim().length, `${name} is excepted without a reason`).toBeGreaterThan(40);
  });

  it("leave nothing not yet converted", () => {
    expect(Object.keys(NOT_YET_CONVERTED)).toEqual([]);
  });
});
