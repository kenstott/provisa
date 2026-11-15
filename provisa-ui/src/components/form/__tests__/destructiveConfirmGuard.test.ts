// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2f8a14-7c3b-4e91-b6a0-9e1c4d7f2a58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// One destructive confirm (maintainer, 2026-11-12: "wouldn't a destructive Confirm button be
// bordered, with red background and white letters?"). components/form/DestructiveConfirmButton.tsx
// draws every button that confirms a destructive action: filled red (white label), Mantine's
// standard size, the same as the Cancel beside it. No other file styles a destructive confirm: a
// red Button anywhere else is either a trigger that opens a confirm (a light, subtle, outline or
// default red) or one of the stated exceptions below.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = resolve(__dirname, "../../..");
const THE_CONFIRM = "components/form/DestructiveConfirmButton.tsx";

/** Red and filled, yet not a confirm: each says why. */
const EXCEPTIONS: Record<string, string> = {
  "components/relationships/CandidatesTable.tsx":
    "a row's Reject of a suggested relationship: it opens no confirm and deletes nothing",
};

function componentFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) return name === "__tests__" ? [] : componentFiles(p);
    return name.endsWith(".tsx") && !name.includes(".test.") ? [relative(SRC, p)] : [];
  });
}

/** The red buttons of a file that are filled: no `variant`, or `variant="filled"`. */
export function filledRedButtons(text: string): number {
  let n = 0;
  for (const m of text.matchAll(/<Button\b([^>]*?)>/gs)) {
    const props = m[1];
    if (!/\bcolor="red"/.test(props)) continue;
    const variant = /\bvariant="(\w+)"/.exec(props)?.[1];
    if (variant === undefined || variant === "filled") n += 1;
  }
  return n;
}

describe("no file but the destructive confirm styles a destructive confirm", () => {
  it("the confirm itself is filled red and full size, with Cancel's size", () => {
    const text = readFileSync(join(SRC, THE_CONFIRM), "utf8");
    expect(text).toContain('variant="filled"');
    expect(text).toContain('color="red"');
    expect(text).toContain('size="sm"');
  });

  it("the guard reads a filled red button", () => {
    expect(filledRedButtons('<Button color="red" onClick={x}>')).toBe(1);
    expect(filledRedButtons('<Button\n color="red"\n variant="filled">')).toBe(1);
    expect(filledRedButtons('<Button color="red" variant="light">')).toBe(0);
    expect(filledRedButtons('<Button variant="default" color="red">')).toBe(0);
  });

  for (const file of componentFiles(SRC).filter((f) => f !== THE_CONFIRM)) {
    it(file, () => {
      const filled = filledRedButtons(readFileSync(join(SRC, file), "utf8"));
      if (file in EXCEPTIONS) return;
      expect(
        filled,
        `${file}: a filled red Button of its own; a destructive confirm is DestructiveConfirmButton (${THE_CONFIRM})`,
      ).toBe(0);
    });
  }

  it("each stated exception still has a filled red button", () => {
    for (const file of Object.keys(EXCEPTIONS)) {
      expect(filledRedButtons(readFileSync(join(SRC, file), "utf8")), file).toBeGreaterThan(0);
    }
  });
});
