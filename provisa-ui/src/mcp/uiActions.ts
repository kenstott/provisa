// Copyright (c) 2026 Kenneth Stott
// Canary: 4cf378ec-0428-4a78-9592-578c9e5b9694
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2115: Polly operating the screen in the user's browser, one step at a time, as the user
 * would: fill a field, choose an option, press a button, or show an element (a list scrolled
 * through end to end). The steps ride her navigate tool (clientTools.ts): the route first, then
 * these on the screen it opens. Each step acts on the page's own elements through the same DOM
 * events a person's input raises, so the page's own validation, requests and permission checks
 * apply unchanged.
 *
 * An element is named by its test id or by its accessible role and name, never guessed: one the
 * screen does not have, or has more than one of, is answered back by name. A press that changes
 * something (a form's Save or action, a submit, a step she marks `changes`) waits for the user's
 * yes in the conversation.
 */

import "./uiActions.css";

export type UiTarget = { testId: string } | { role: string; name: string };

export type UiAction =
  | { do: "fill"; target: UiTarget; value: string }
  | { do: "choose"; target: UiTarget; option: string }
  | { do: "press"; target: UiTarget; changes?: boolean }
  | { do: "show"; target: UiTarget };

export interface UiActionContext {
  /** The user's yes or no in the conversation for a press that changes something. */
  confirm: (question: string) => Promise<boolean>;
  /** How long a step lets itself be seen; 0 in tests. */
  paceMs: number;
  /** How long a step waits for its element to appear (a screen still mounting). */
  waitMs: number;
  /** Stops the steps between one and the next (a tour left while they run). */
  signal?: AbortSignal;
}

export interface UiActionsResult {
  success: boolean;
  /** One line per step taken, the last naming why the steps stopped when they did. */
  steps: string[];
}

const HIGHLIGHT = "polly-acting";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** What a target is: `element with test id "x"`, `button named "Save"`. */
function targetNoun(target: UiTarget): string {
  return "testId" in target
    ? `element with test id "${target.testId}"`
    : `${target.role} named "${target.name}"`;
}

/** How a target reads in a step's report. */
export function describeTarget(target: UiTarget): string {
  return `the ${targetNoun(target)}`;
}

function normalized(text: string | null | undefined): string {
  return (text ?? "").replace(/\s+/g, " ").trim().toLowerCase();
}

function hidden(el: Element): boolean {
  return el.closest("[hidden], [aria-hidden='true'], [inert]") !== null;
}

/** The elements of each role an element is named by: its tag's implicit role, or its own. */
const ROLE_SELECTORS: Record<string, string> = {
  button: "button, [role='button'], input[type='button'], input[type='submit']",
  textbox:
    "textarea, [role='textbox'], input:not([type]):not([aria-haspopup]), " +
    "input[type='text']:not([aria-haspopup]), input[type='email'], input[type='url'], " +
    "input[type='password'], input[type='search']:not([aria-haspopup]), input[type='number']",
  combobox: "[role='combobox'], select, input[aria-haspopup='listbox']",
  checkbox: "input[type='checkbox'], [role='checkbox']",
  radio: "input[type='radio'], [role='radio']",
  switch: "[role='switch']",
  option: "[role='option'], option",
  tab: "[role='tab']",
  link: "a[href], [role='link']",
};

/** An element's text as assistive technology reads it: without its aria-hidden parts (a required
 * field's label hides its asterisk so). */
function spokenText(el: Element): string {
  let text = "";
  for (const child of el.childNodes) {
    if (child.nodeType === Node.TEXT_NODE) text += child.textContent ?? "";
    else if (child instanceof Element && child.getAttribute("aria-hidden") !== "true") {
      text += spokenText(child);
    }
  }
  return text;
}

function accessibleName(el: Element): string {
  const label = el.getAttribute("aria-label");
  if (label) return label;
  const by = el.getAttribute("aria-labelledby");
  if (by) {
    return by
      .split(/\s+/)
      .map((id) => {
        const named = document.getElementById(id);
        return named ? spokenText(named) : "";
      })
      .join(" ");
  }
  if (el.id) {
    const forLabel = document.querySelector(`label[for="${CSS.escape(el.id)}"]`);
    if (forLabel) return spokenText(forLabel);
  }
  const wrapping = el.closest("label");
  if (wrapping) return spokenText(wrapping);
  const tag = el.tagName.toLowerCase();
  if (tag !== "input" && tag !== "textarea" && tag !== "select") {
    const text = spokenText(el);
    if (text.trim()) return text;
  }
  return el.getAttribute("title") ?? el.getAttribute("placeholder") ?? "";
}

type Found = { el: HTMLElement } | { refusal: string };

function find(target: UiTarget): Found {
  if ("testId" in target) {
    const all = [
      ...document.querySelectorAll<HTMLElement>(`[data-testid="${CSS.escape(target.testId)}"]`),
    ].filter((el) => !hidden(el));
    if (all.length === 1) return { el: all[0] };
    return all.length === 0
      ? { refusal: `The screen has no ${targetNoun(target)}.` }
      : { refusal: `The screen has ${all.length} elements with test id "${target.testId}".` };
  }
  const selector = ROLE_SELECTORS[target.role];
  if (selector === undefined) {
    return {
      refusal:
        `"${target.role}" is not a role this can name; ` +
        `use one of ${Object.keys(ROLE_SELECTORS).join(", ")}, or a test id.`,
    };
  }
  const wanted = normalized(target.name);
  const all = [...document.querySelectorAll<HTMLElement>(selector)].filter(
    (el) => !hidden(el) && normalized(accessibleName(el)) === wanted,
  );
  if (all.length === 1) return { el: all[0] };
  return all.length === 0
    ? { refusal: `The screen has no ${targetNoun(target)}.` }
    : { refusal: `The screen has ${all.length} of the ${targetNoun(target)}; name it by test id.` };
}

/** The target once the screen has it, or why it does not within the wait. */
async function located(target: UiTarget, waitMs: number): Promise<Found> {
  const deadline = Date.now() + waitMs;
  for (;;) {
    const found = find(target);
    if ("el" in found || Date.now() >= deadline) return found;
    await sleep(100);
  }
}

async function highlighted(el: HTMLElement, paceMs: number): Promise<void> {
  el.scrollIntoView?.({ block: "center", behavior: paceMs > 0 ? "smooth" : "auto" });
  el.classList.add(HIGHLIGHT);
  await sleep(paceMs);
}

function release(el: HTMLElement) {
  el.classList.remove(HIGHLIGHT);
}

/** Types `value` into a field as a person's typing would arrive: React reads the input event. */
function typeInto(el: HTMLElement, value: string): string | null {
  if (!(el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement)) {
    return "it is not a text field";
  }
  const proto = el instanceof HTMLInputElement ? HTMLInputElement : HTMLTextAreaElement;
  Object.getOwnPropertyDescriptor(proto.prototype, "value")?.set?.call(el, value);
  el.dispatchEvent(new Event("input", { bubbles: true }));
  el.dispatchEvent(new Event("change", { bubbles: true }));
  return null;
}

/** A press as the page's own handlers take one: the element's click. */
function click(el: HTMLElement) {
  el.focus();
  el.click();
}

/** Picks `option` in a select or a combobox, from the options it offers. */
async function chooseIn(el: HTMLElement, option: string, ctx: UiActionContext): Promise<string> {
  const wanted = normalized(option);
  if (el instanceof HTMLSelectElement) {
    const match = [...el.options].find((o) => normalized(o.textContent) === wanted);
    if (!match) return `it offers no option "${option}"`;
    el.value = match.value;
    el.dispatchEvent(new Event("change", { bubbles: true }));
    return "";
  }
  click(el);
  const deadline = Date.now() + ctx.waitMs;
  for (;;) {
    // Read each time: the list it controls is named once the open dropdown has rendered.
    const listId = el.getAttribute("aria-controls");
    const list = listId ? document.getElementById(listId) : null;
    const options = list ? [...list.querySelectorAll<HTMLElement>("[role='option']")] : [];
    const match = options.find((o) => normalized(accessibleName(o)) === wanted);
    if (match) {
      await highlighted(match, ctx.paceMs);
      click(match);
      release(match);
      return "";
    }
    if (Date.now() >= deadline) {
      const offered = options.map((o) => accessibleName(o).trim()).filter(Boolean);
      return offered.length
        ? `it offers no option "${option}"; it offers ${offered.map((o) => `"${o}"`).join(", ")}`
        : `it opened no list of options`;
    }
    await sleep(100);
  }
}

/** Whether pressing `el` changes something, so the user is asked first. */
function changes(el: HTMLElement, declared: boolean | undefined): boolean {
  if (declared) return true;
  const marked = el.closest("[data-changes]")?.getAttribute("data-changes");
  if (marked === "false") return false;
  if (marked === "true") return true;
  if (el instanceof HTMLButtonElement && el.type === "submit") return true;
  return el.closest(".form-footer") !== null;
}

/** Shows `el` whole: a list that scrolls is scrolled through to its end, then back. */
async function show(el: HTMLElement, paceMs: number): Promise<void> {
  const list =
    el.scrollHeight > el.clientHeight
      ? el
      : ([...el.querySelectorAll<HTMLElement>("*")].find(
          (c) => c.scrollHeight > c.clientHeight && getComputedStyle(c).overflowY !== "visible",
        ) ?? null);
  if (list === null || paceMs === 0) {
    await sleep(paceMs * 3);
    return;
  }
  const steps = 12;
  const end = list.scrollHeight - list.clientHeight;
  for (let i = 1; i <= steps; i++) {
    list.scrollTo({ top: (end * i) / steps, behavior: "smooth" });
    await sleep(paceMs / 2);
  }
  await sleep(paceMs);
  list.scrollTo({ top: 0, behavior: "smooth" });
  await sleep(paceMs / 2);
}

function actionLabel(action: UiAction): string {
  const on = describeTarget(action.target);
  switch (action.do) {
    case "fill":
      return `Filled ${on} with "${action.value}".`;
    case "choose":
      return `Chose "${action.option}" in ${on}.`;
    case "press":
      return `Pressed ${on}.`;
    case "show":
      return `Showed ${on}.`;
  }
}

/** Runs `actions` in order on the screen now open, stopping at the first that cannot be done. */
export async function runUiActions(
  actions: readonly UiAction[],
  ctx: UiActionContext,
): Promise<UiActionsResult> {
  const steps: string[] = [];
  for (const action of actions) {
    if (ctx.signal?.aborted) {
      steps.push("Stopped: the steps were abandoned.");
      return { success: false, steps };
    }
    const found = await located(action.target, ctx.waitMs);
    if ("refusal" in found) {
      steps.push(found.refusal);
      return { success: false, steps };
    }
    const el = found.el;
    await highlighted(el, ctx.paceMs);
    try {
      if (action.do === "fill") {
        const why = typeInto(el, action.value);
        if (why) {
          steps.push(`Could not fill ${describeTarget(action.target)}: ${why}.`);
          return { success: false, steps };
        }
      } else if (action.do === "choose") {
        const why = await chooseIn(el, action.option, ctx);
        if (why) {
          steps.push(`Could not choose in ${describeTarget(action.target)}: ${why}.`);
          return { success: false, steps };
        }
      } else if (action.do === "press") {
        if (changes(el, action.changes)) {
          const name = accessibleName(el).trim() || describeTarget(action.target);
          if (!(await ctx.confirm(`Press "${name}"? It changes something.`))) {
            steps.push(`The user declined: ${describeTarget(action.target)} was not pressed.`);
            return { success: false, steps };
          }
        }
        click(el);
      } else {
        await show(el, ctx.paceMs);
      }
    } finally {
      release(el);
    }
    steps.push(actionLabel(action));
    await sleep(ctx.paceMs);
  }
  return { success: true, steps };
}

/** The actions Polly sent, checked: a malformed one is answered back by its position. */
export function parseUiActions(raw: unknown): UiAction[] | string {
  if (!Array.isArray(raw)) return "actions must be a list";
  const out: UiAction[] = [];
  for (const [i, a] of raw.entries()) {
    const at = `actions[${i}]`;
    if (!a || typeof a !== "object") return `${at} is not an object`;
    const { do: kind, target } = a as Record<string, unknown>;
    if (!target || typeof target !== "object") return `${at}.target is required`;
    const t = target as Record<string, unknown>;
    const parsedTarget: UiTarget | null =
      typeof t.testId === "string"
        ? { testId: t.testId }
        : typeof t.role === "string" && typeof t.name === "string"
          ? { role: t.role, name: t.name }
          : null;
    if (parsedTarget === null) return `${at}.target names neither a testId nor a role and name`;
    const rest = a as Record<string, unknown>;
    if (kind === "fill" && typeof rest.value === "string") {
      out.push({ do: "fill", target: parsedTarget, value: rest.value });
    } else if (kind === "choose" && typeof rest.option === "string") {
      out.push({ do: "choose", target: parsedTarget, option: rest.option });
    } else if (kind === "press") {
      out.push({ do: "press", target: parsedTarget, changes: rest.changes === true });
    } else if (kind === "show") {
      out.push({ do: "show", target: parsedTarget });
    } else {
      return `${at}: "do" must be fill (with value), choose (with option), press or show`;
    }
  }
  return out;
}
