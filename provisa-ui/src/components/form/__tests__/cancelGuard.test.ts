// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// One Cancel (maintainer, 2026-11-12: "the styling for cancel buttons is not consistent. Use one
// consistent style."). components/form/CancelButton.tsx draws every Cancel: bordered, the cross,
// one size, the shared word. A button anywhere else whose label is "Cancel" is a Cancel of its
// own look, so it fails here.
//
// Not a Cancel, and not read: a button that says something else (Discard, Dismiss, Close, Go
// back, Keep it, Cancel trial, Stop recording), and an icon-only close whose "Cancel" is only its
// accessible name.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = resolve(__dirname, "../../..");

/** The file that IS the Cancel: the only one that may draw a button labelled Cancel. */
const THE_CANCEL = "components/form/CancelButton.tsx";

/** The elements that are buttons with a label of their own. */
const BUTTON = /<(Button|button|UnstyledButton|Menu\.Item)\b/g;

type Catalog = { [key: string]: string | Catalog };

/** Every English catalog merged: en.json and the per-namespace files, as the app loads them. */
function englishCatalog(): Catalog {
  const dir = join(SRC, "i18n/locales/en");
  const merged: Catalog = JSON.parse(readFileSync(join(SRC, "i18n/locales/en.json"), "utf8"));
  for (const name of readdirSync(dir).filter((n) => n.endsWith(".json") && !n.startsWith("."))) {
    Object.assign(merged, JSON.parse(readFileSync(join(dir, name), "utf8")));
  }
  return merged;
}

/** A label that says Cancel and nothing else, whatever marks surround the word. */
export function saysCancel(text: string): boolean {
  return (
    text
      .replace(/[^\p{L}]+/gu, " ")
      .trim()
      .toLowerCase() === "cancel"
  );
}

/** The dotted keys whose English text is the word Cancel. */
export function cancelKeys(catalog: Catalog, prefix = ""): Set<string> {
  const keys = new Set<string>();
  for (const [k, v] of Object.entries(catalog)) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (typeof v === "string") {
      if (saysCancel(v)) keys.add(key);
    } else {
      for (const inner of cancelKeys(v, key)) keys.add(inner);
    }
  }
  return keys;
}

/** The body of the JSX element that opens at `start`: from its opening tag to its closing tag. */
function element(text: string, start: number, name: string): string {
  let depth = 0;
  for (let i = start; i < text.length; i += 1) {
    const c = text[i];
    if (c === "{") depth += 1;
    else if (c === "}") depth -= 1;
    else if (depth === 0 && text.startsWith("/>", i)) return "";
    else if (depth === 0 && c === ">") {
      const end = text.indexOf(`</${name}>`, i);
      return end < 0 ? "" : text.slice(i + 1, end);
    }
  }
  return "";
}

/** Each button in `source` whose label is Cancel: a cancel key's t() call, or the bare word. */
export function cancelButtons(source: string, keys: Set<string>): string[] {
  const found: string[] = [];
  for (const m of source.matchAll(BUTTON)) {
    const body = element(source, m.index, m[1]);
    const said = [...body.matchAll(/\bt(?:ranslate)?\(\s*["'`]([\w.:-]+)["'`]/g)].map((c) =>
      c[1].replace(/^[\w-]+:/, ""),
    );
    const bare = body.replace(/\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}/g, " ").replace(/<[^>]*>/g, " ");
    if (said.some((key) => keys.has(key)) || saysCancel(bare)) {
      const line = source.slice(0, m.index).split("\n").length;
      found.push(`line ${line}: <${m[1]}>`);
    }
  }
  return found;
}

function componentFiles(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) {
      if (name !== "__tests__" && name !== "node_modules") out.push(...componentFiles(p));
    } else if (name.endsWith(".tsx") && !name.includes(".test.")) {
      out.push(relative(SRC, p));
    }
  }
  return out;
}

describe("the rule itself", () => {
  const keys = cancelKeys({ x: { cancel: "Cancel", stop: "◼ Cancel", trial: "Cancel trial" } });

  it("knows a key that says Cancel, with or without marks, and not one that says more", () => {
    expect([...keys].sort()).toEqual(["x.cancel", "x.stop"]);
  });

  it("finds a Button, a native button and a menu item labelled Cancel", () => {
    const src = `
      <Button variant="light" size="xs" onClick={close}>{t("x.cancel")}</Button>
      <button type="button" className="pill" onClick={exit}>
        {t("x.stop")}
      </button>
      <Menu.Item onClick={close}>Cancel</Menu.Item>`;
    expect(cancelButtons(src, keys)).toEqual([
      "line 2: <Button>",
      "line 3: <button>",
      "line 6: <Menu.Item>",
    ]);
  });

  it("leaves a button that says something else, and an icon-only close", () => {
    const src = `
      <Button onClick={stop}>{t("x.trial")}</Button>
      <Button onClick={close}>{t("x.close")}</Button>
      <ActionIcon aria-label={t("x.cancel")} onClick={close}><X size={14} /></ActionIcon>
      <Button leftSection={<X size={14} />} onClick={go}>Run</Button>`;
    expect(cancelButtons(src, keys)).toEqual([]);
  });
});

describe("no file but the Cancel draws a button labelled Cancel", () => {
  const keys = cancelKeys(englishCatalog());
  const files = componentFiles(SRC).filter((f) => f !== THE_CANCEL);

  it("reads the cancel words of the catalogs and every component", () => {
    expect(keys.has("common.cancel")).toBe(true);
    expect(files.length).toBeGreaterThan(100);
  });

  it("the Cancel itself is labelled with the shared word", () => {
    expect(readFileSync(join(SRC, THE_CANCEL), "utf8")).toContain('t("common.cancel")');
  });

  for (const file of files) {
    it(file, () => {
      const found = cancelButtons(readFileSync(join(SRC, file), "utf8"), keys);
      expect(found, `${file}: a Cancel of its own; use CancelButton (${THE_CANCEL})`).toEqual([]);
    });
  }
});
