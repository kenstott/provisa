// Copyright (c) 2026 Kenneth Stott
// Canary: 4e430b0a-770b-4e58-86bf-c2fd4031ce44
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every action in a detail panel's head has ONE style, defined once (PanelHeadStyle): no
// background, no border, one height. The exception is the shared Save and Cancel (SaveButton,
// CancelButton): they set their own look once, so a form's head shows the pair its foot shows
// and no page styles them. A page that gives a head action a variant or a size of its
// own brings back the mix of chips, boxes and bare icons this replaced, so it fails here.
//
// What is in a panel's head: what a view renders inside <PanelActions>, and the actions of a
// form whose footer says `panel` (they sit in the head while the form is open).

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(__dirname, "..", "..", "..");

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sources(path);
    return /\.tsx$/.test(name) && !/\.test\.tsx$/.test(name) ? [path] : [];
  });
}

/** The text of the JSX element that opens at `start`, to its own `/>` or closing tag. */
function element(text: string, start: number, name: string): string {
  let depth = 0;
  for (let i = start; i < text.length; i += 1) {
    const c = text[i];
    if (c === "{") depth += 1;
    else if (c === "}") depth -= 1;
    else if (depth === 0 && text.startsWith("/>", i)) return text.slice(start, i + 2);
    else if (depth === 0 && c === ">") {
      const end = text.indexOf(`</${name}>`, i);
      return text.slice(start, end < 0 ? text.length : end);
    }
  }
  return text.slice(start);
}

/** The element's own props: the opening tag at `start`, without what its `{...}` props hold. */
function ownProps(text: string, start: number): string {
  let depth = 0;
  let own = "";
  for (let i = start; i < text.length; i += 1) {
    const c = text[i];
    if (c === "{") depth += 1;
    else if (c === "}") depth -= 1;
    else if (depth === 0 && c === ">") return own;
    if (depth === 0) own += c;
  }
  return own;
}

/** Each Button or ActionIcon in `jsx` that sets its own variant or size, as it opens. */
export function ownStyled(jsx: string): string[] {
  const found: string[] = [];
  for (const tag of jsx.matchAll(/<(Button|ActionIcon)\b/g)) {
    // An icon in a prop (leftSection={<Upload size={14} />}) has a size of its own, not the button's.
    const own = ownProps(jsx, tag.index).match(/\b(variant|size)=/g);
    if (own) found.push(`<${tag[1]} ${[...new Set(own)].join(" ")}…>`);
  }
  return found;
}

/** The parts of a file that are rendered in a panel's head. */
export function headParts(text: string): string[] {
  const parts: string[] = [];
  for (const m of text.matchAll(/<PanelActions>/g))
    parts.push(element(text, m.index, "PanelActions"));
  for (const m of text.matchAll(/<FormFooter\b/g)) {
    const footer = element(text, m.index, "FormFooter");
    if (
      /^<FormFooter[^>]*?\n?\s+panel\b/.test(footer) ||
      /\spanel(\s|=|\/|>)/.test(footer.split("action=")[0])
    ) {
      parts.push(footer);
    }
  }
  return parts;
}

describe("the rule itself", () => {
  it("finds a head action that styles itself, and leaves one that only keeps its colour", () => {
    const head = `<PanelActions>
      <Button variant="default" size="compact-xs" onClick={go}>Discover</Button>
      <ActionIcon color="red" aria-label="Delete" onClick={() => remove(x)}><Trash2 /></ActionIcon>
    </PanelActions>`;
    expect(headParts(head)).toHaveLength(1);
    expect(ownStyled(headParts(head)[0])).toEqual(["<Button variant= size=…>"]);
  });

  it("reads an action of a form whose footer sits in the head", () => {
    const form = `<FormFooter
      panel
      onCancel={onCancel}
      action={<Button variant="default" onClick={() => open()}>Open</Button>}
    />`;
    expect(ownStyled(headParts(form)[0])).toEqual(["<Button variant=…>"]);
    // A footer that stays at the form's foot is not the head's.
    expect(
      headParts(`<FormFooter onSave={save} action={<Button variant="light">Run</Button>} />`),
    ).toEqual([]);
  });

  it("reads the button's own props, not those of an icon in one of them", () => {
    expect(
      ownStyled(
        `<PanelActions><Button leftSection={<Upload size={14} />}>Apply</Button></PanelActions>`,
      ),
    ).toEqual([]);
    expect(
      ownStyled(
        `<PanelActions><Button leftSection={<Upload size={14} />} size="xs">Apply</Button></PanelActions>`,
      ),
    ).toEqual(["<Button size=…>"]);
  });

  it("does not mistake a size on something that is not a button", () => {
    expect(
      ownStyled(`<PanelActions><Checkbox size="xs" /><Pencil size={14} /></PanelActions>`),
    ).toEqual([]);
  });
});

describe("no action in a panel's head sets its own variant or size", () => {
  const files = sources(SRC).filter((file) => {
    const text = readFileSync(file, "utf8");
    return text.includes("<PanelActions>") || /<FormFooter\b/.test(text);
  });

  it("looks at every file that renders into a panel's head", () => {
    expect(files.length).toBeGreaterThan(8);
  });

  for (const file of files) {
    const name = relative(SRC, file);
    it(name, () => {
      const own = headParts(readFileSync(file, "utf8")).flatMap(ownStyled);
      expect(own, `${name}: a head action styles itself (PanelHeadStyle is the one style)`).toEqual(
        [],
      );
    });
  }
});
