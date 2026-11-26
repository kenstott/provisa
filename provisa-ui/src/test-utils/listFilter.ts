// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4f2c71-6b8e-4a35-b0e2-71c5e9a8d346
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list's filter finds a row by every text the row shows, partially and in any case
// (listFilterGuard.test.ts holds every list page to it). The check reads the rendered list: each
// text a row shows is typed into the page's filter, upper-cased and cut to its middle, and the row
// must still be listed.

import { act, fireEvent, waitFor } from "@testing-library/react";
import { expect } from "vitest";

/** The rows of a standard list: its body rows that show an item, not a group heading or note. */
function listRows(table: HTMLElement): HTMLTableRowElement[] {
  return [...table.querySelectorAll<HTMLTableRowElement>("tbody > tr")].filter(
    (tr) => !tr.querySelector("td[colspan]"),
  );
}

/** What a cell shows: the text of each element holding text directly, the actions column and
 * hidden text left out. A text without a letter or digit (a dash for none) is not searched for. */
function shownTexts(row: HTMLTableRowElement): string[] {
  const texts = new Set<string>();
  const walker = row.ownerDocument.createTreeWalker(row, NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    const el = node.parentElement;
    if (!el || el.closest(".list-actions, [aria-hidden='true'], button, [role='tooltip']")) continue;
    const text = (node.textContent ?? "").trim();
    if (/[\p{L}\p{N}]/u.test(text)) texts.add(text);
  }
  return [...texts];
}

/** The search typed for a shown text: its middle, upper-cased, so a match must be partial and
 * case-insensitive. A short text is typed whole. */
function partialOf(text: string): string {
  // A quote or a leading - is search syntax (lib/searchItems.ts), not text.
  const word = text
    .replaceAll('"', "")
    .split(/\s+/)
    .map((w) => w.replace(/^-+/, ""))
    .sort((a, b) => b.length - a.length)[0];
  const cut = word.length > 4 ? word.slice(1, -1) : word;
  return cut.toUpperCase();
}

const rowName = (row: HTMLTableRowElement) => row.textContent ?? "";

/**
 * Every text a row of ``table`` shows finds that row through ``filter``. A text every row shows
 * alike (a label printed in each row) is not searched for; give the list two rows that differ.
 * The table is looked up afresh after each search, as a list may redraw it.
 */
export async function expectFilterFindsEveryCell(
  filter: HTMLElement,
  table: () => HTMLElement,
): Promise<void> {
  const rows = listRows(table());
  expect(rows.length).toBeGreaterThan(1);
  const perRow = rows.map((row) => ({ name: rowName(row), texts: shownTexts(row) }));
  const everywhere = new Set(
    perRow[0].texts.filter((text) => perRow.every((r) => r.texts.includes(text))),
  );
  const missed: string[] = [];
  for (const { name, texts } of perRow) {
    for (const text of texts) {
      if (everywhere.has(text)) continue;
      const typed = partialOf(text);
      await act(async () => {
        fireEvent.change(filter, { target: { value: typed } });
      });
      try {
        await waitFor(
          () => {
            expect(listRows(table()).map(rowName)).toContain(name);
          },
          { timeout: 300 },
        );
      } catch {
        missed.push(`"${typed}" (from "${text}") misses the row "${name}"`);
      }
    }
  }
  await act(async () => {
    fireEvent.change(filter, { target: { value: "" } });
  });
  expect(missed).toEqual([]);
}
