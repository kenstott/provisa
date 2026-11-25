// Copyright (c) 2026 Kenneth Stott
// Canary: 9b028897-de84-47bd-8bd2-aff7863c9cb0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source's read view is its edit form drawn read-only: for every kind of source it lists the
// fields the form shows for that kind, under the same labels, each with the value it holds.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// A code source's fields (REQ-1984, REQ-1993) upload to the active organisation.
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
import { cleanup, render, screen, within } from "../../../test-utils/render";
import { FormLayout, FormRead } from "../../../components/form/FormLayout";
import { SourceFormFields } from "../SourceFormFields";
import { SOURCE_TYPES } from "../constants";
import { sourceFormProps as props, sourceOf as source } from "../../../test-utils/sourceForm";

const strip = (text: string) => text.replace(/\s*\*\s*$/, "").trim();

/** The labels of the edit form's fields: each innermost field's control's label or name. */
function editLabels(root: HTMLElement): string[] {
  return [...root.querySelectorAll<HTMLElement>(".form-field")]
    .filter((f) => f.querySelector(".form-field") === null)
    .map((f) => {
      // A group's own label (a choice of options) before its options' labels.
      const label = f.querySelector(".mantine-InputWrapper-label") ?? f.querySelector("label");
      if (label?.textContent) return strip(label.textContent);
      const named = f.querySelector("[aria-label]");
      return strip(named?.getAttribute("aria-label") ?? "");
    })
    .filter(Boolean)
    .sort();
}

/** The read fields: only the cells directly in the read's flow are drawn (the rest of the form is
 * not), each its label above its value. */
function readLabels(root: HTMLElement): string[] {
  return [...root.querySelectorAll(":scope > .form-field > dt")]
    .map((d) => strip(d.textContent ?? ""))
    .sort();
}

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("[]", { status: 200 })),
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("a source read", () => {
  it.each(SOURCE_TYPES.map((s) => s.value))("lists the fields the %s form shows", (type) => {
    const p = props(source(type));
    const edit = render(
      <FormLayout>
        <SourceFormFields {...p} />
      </FormLayout>,
    );
    const shownInEdit = editLabels(edit.container);
    cleanup();
    render(
      <FormRead testId="read">
        <SourceFormFields {...p} />
      </FormRead>,
    );
    const read = screen.getByTestId("read");
    expect(readLabels(read)).toEqual(shownInEdit);
    // Every field is a cell in the flow itself: one wrapped in anything else would not be drawn.
    expect(read.querySelectorAll("dt")).toHaveLength(shownInEdit.length);
    // Nothing of the form is editable when read.
    expect(within(read).queryAllByRole("textbox")).toEqual([]);
    expect(within(read).queryAllByRole("checkbox")).toEqual([]);
  });

  it("shows each value as the form holds it: text, a choice by its label, a tick, a secret", () => {
    const p = props(
      source("postgresql", {
        description: "Orders",
        passwordRef: "${secret:pg}",
        cacheEnabled: false,
      }),
    );
    render(
      <FormRead testId="read">
        <SourceFormFields {...p} />
      </FormRead>,
    );
    const value = (label: string) =>
      [...screen.getByTestId("read").querySelectorAll("dt")].find(
        (d) => strip(d.textContent ?? "") === label,
      )?.nextElementSibling?.textContent;
    expect(value("Host")).toBe("db.example.test");
    expect(value("Description")).toBe("Orders");
    expect(value("Password")).toBe("Set");
    expect(value("Password")).not.toContain("secret");
  });

  it("shows an uploaded archive kept from before by its file's name and its hash", () => {
    const p = props(
      source("python", {
        mappingJson: JSON.stringify({
          runtime: "python",
          code: {
            kind: "upload",
            upload: "e80be40c584d433f9aa66108a9c9b9015264abfbe3ec43c992ae503ecc79d4ce",
            filename: "churn_model.tar.gz",
          },
        }),
      }),
    );
    render(
      <FormRead testId="read">
        <SourceFormFields {...p} />
      </FormRead>,
    );
    const archive = [...screen.getByTestId("read").querySelectorAll("dt")].find(
      (d) => strip(d.textContent ?? "") === "Archive",
    )?.nextElementSibling?.textContent;
    expect(archive).toBe("Kept: churn_model.tar.gz (e80be40c584d)");
  });
});
