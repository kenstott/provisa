// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect } from "react";
import { accessibleName, hidden } from "./uiActions";

/**
 * REQ-2115: what Polly's read_screen tool reports — the screen the user is looking at, read from
 * their own session. Each query editor publishes its text from its own state (never scraped from
 * the DOM), keyed by the route that shows it: the Python tab stays mounted while hidden, so only
 * the editor of the route on screen is reported. Each mounted result grid publishes its columns
 * and row count, never its rows.
 */
export type EditorLanguage = "sql" | "graphql" | "cypher" | "python";

export interface ScreenEditor {
  language: EditorLanguage;
  text: string;
  /** The open query tab's title; null on a surface with no tabs. */
  tab: string | null;
}

export interface ScreenGrid {
  columns: string[];
  rows: number;
  /** The rows held are part of a longer answer (cut at a row limit). */
  partial: boolean;
}

const editors = new Map<string, ScreenEditor>();
const grids = new Map<symbol, ScreenGrid>();

/** Publish the editor `route` shows, while the calling component is mounted. */
export function useScreenEditor(route: string, editor: ScreenEditor): void {
  const { language, text, tab } = editor;
  useEffect(() => {
    editors.set(route, { language, text, tab });
    return () => {
      editors.delete(route);
    };
  }, [route, language, text, tab]);
}

/** Publish a result grid on screen, while the calling component is mounted. */
export function useScreenGrid(grid: ScreenGrid): void {
  const { columns, rows, partial } = grid;
  useEffect(() => {
    const id = Symbol("grid");
    grids.set(id, { columns, rows, partial });
    return () => {
      grids.delete(id);
    };
  }, [columns, rows, partial]);
}

const FIELD_SELECTOR = "input, textarea, select";
// Polly's own chat and the code editors are not form fields: the editors are reported from their
// own state above.
const NOT_FIELDS = "[data-testid='chat-panel'], .cm-editor, .graphiql-container, .monaco-editor";

function onScreen(el: HTMLElement): boolean {
  return !hidden(el) && el.checkVisibility();
}

interface ScreenField {
  label: string;
  value: string | boolean;
}

function fieldValue(el: HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement) {
  if (el instanceof HTMLInputElement && (el.type === "checkbox" || el.type === "radio")) {
    return el.checked;
  }
  // A password field shows one dot per character, and so does this.
  if (el instanceof HTMLInputElement && el.type === "password") return "•".repeat(el.value.length);
  return el.value;
}

function screenFields(): ScreenField[] {
  const fields: ScreenField[] = [];
  for (const el of document.querySelectorAll<HTMLInputElement>(FIELD_SELECTOR)) {
    if (el.type === "hidden" || el.closest(NOT_FIELDS) || !onScreen(el)) continue;
    const label = accessibleName(el).trim();
    if (label) fields.push({ label, value: fieldValue(el) });
  }
  return fields;
}

export interface Screen {
  route: string;
  /** The editor of the route on screen; null on a route with none. */
  editor: ScreenEditor | null;
  fields: ScreenField[];
  results: ScreenGrid[];
}

/** The screen as read_screen reports it. */
export function readScreen(): Screen {
  return {
    route: `${window.location.pathname}${window.location.search}`,
    editor: editors.get(window.location.pathname) ?? null,
    fields: screenFields(),
    results: [...grids.values()],
  };
}
