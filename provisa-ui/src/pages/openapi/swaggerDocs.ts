// Copyright (c) 2026 Kenneth Stott
// Canary: 91e3a7d0-6b24-4c8f-a5d7-08f2c4be1d63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** The OpenAPI page's docs are a Swagger UI in a frame: nothing outside it holds what is typed
 * there, so what the page hands it and what it reads back both go through the frame's own DOM. */

type ParamInput = HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;

const PARAM_INPUT = "input, select, textarea";
const BODY_INPUT = ".opblock-section-request-body textarea, textarea.body-param__text";

/** A call as the page is handed it: `METHOD /path?query`. Null when it is not one. */
export function parseCall(openApiUrl: string) {
  const call = openApiUrl.trim().match(/^(\S+)\s+([^?\s]+)(?:\?(.*))?$/);
  if (!call) return null;
  const [, method, path, query = ""] = call;
  return { method: method.toLowerCase(), path, params: new URLSearchParams(query) };
}

function operationPath(block: Element): string {
  return block.querySelector("[data-path]")?.getAttribute("data-path") ?? "";
}

function operationMethod(block: Element): string {
  return block.querySelector(".opblock-summary-method")?.textContent?.trim().toLowerCase() ?? "";
}

/** The operation block of `method fullPath`. data-path is relative to the server base (e.g.
 * /pet-store/inquiries), but NL gives the full path (/data/rest/pet-store/inquiries). */
export function findOperation(doc: Document, method: string, fullPath: string) {
  return Array.from(doc.querySelectorAll<HTMLElement>(".opblock")).find(
    (b) => operationMethod(b) === method && fullPath.endsWith(operationPath(b)),
  );
}

export function paramInput(block: Element, name: string): ParamInput | null {
  return block.querySelector<ParamInput>(
    `tr[data-param-name="${CSS.escape(name)}"] :is(${PARAM_INPUT})`,
  );
}

export function bodyInput(block: Element): HTMLTextAreaElement | null {
  return block.querySelector<HTMLTextAreaElement>(BODY_INPUT);
}

/** Sets a field as typing would: the value setter of the element's own kind, from the docs frame's
 * own window (a table's parameters are one JSON object, typed in a textarea, which the input
 * setter refuses: "Illegal invocation"), then the events Swagger UI's React tree listens for. */
export function typeInto(input: ParamInput, value: string): void {
  const view = (input.ownerDocument.defaultView ?? window) as typeof window;
  if (input instanceof view.HTMLSelectElement && input.multiple) {
    // A list parameter of enum values (`fields`) is a multi-select: comma-separated in the URL.
    const chosen = new Set(value.split(","));
    for (const option of input.options) option.selected = chosen.has(option.value);
  } else {
    const proto =
      input.tagName === "SELECT"
        ? view.HTMLSelectElement.prototype
        : input.tagName === "TEXTAREA"
          ? view.HTMLTextAreaElement.prototype
          : view.HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(proto, "value")?.set?.call(input, value);
  }
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function inputValue(input: ParamInput): string {
  const view = (input.ownerDocument.defaultView ?? window) as typeof window;
  if (input instanceof view.HTMLSelectElement && input.multiple) {
    return Array.from(input.selectedOptions, (o) => o.value).join(",");
  }
  return input.value;
}

/** What Swagger UI sent: the URL it shows for the last call. */
function requestUrl(block: Element): string | null {
  return block.querySelector(".request-url pre")?.textContent?.trim() || null;
}

export interface ShownOperation {
  operation: string;
  parameters: Record<string, string>;
  body: string | null;
  requestUrl: string | null;
}

/** The operation open in the docs, the last opened when there are several; null when none is. */
export function shownOperation(doc: Document | null | undefined): ShownOperation | null {
  const open = Array.from(doc?.querySelectorAll(".opblock.is-open") ?? []);
  const block = open[open.length - 1];
  if (!block) return null;
  const parameters: Record<string, string> = {};
  for (const row of block.querySelectorAll("tr[data-param-name]")) {
    const name = row.getAttribute("data-param-name") ?? "";
    const input = row.querySelector<ParamInput>(PARAM_INPUT);
    const value = input ? inputValue(input) : "";
    if (value) parameters[name] = value;
  }
  return {
    operation: `${operationMethod(block).toUpperCase()} ${operationPath(block)}`,
    parameters,
    body: bodyInput(block)?.value ?? null,
    requestUrl: requestUrl(block),
  };
}
