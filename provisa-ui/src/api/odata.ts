// Copyright (c) 2026 Kenneth Stott
// Canary: 4704f8fe-9051-45e4-b6c5-6c9531bc5e42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-1949, REQ-2193

// The OData explorer's reads of the Provisa OData service (provisa/api/odata): the role's
// $metadata and one page of an entity set. Each is sent as the acting role (X-Provisa-Role), with
// the signed-in credential the app's fetch carries, so the page shows what that role is served.

import type { StatementWarning } from "./admin";
import { requestFailed, serverMessage } from "../i18n/serverMessage";

/** The service root, as provisa/api/odata/headers.py serves it. */
export const SERVICE_ROOT = "/data/odata";

/** The body annotation a read's statement warnings ride in, ahead of `value` (router.py). */
const WARNINGS_ANNOTATION = "@Provisa.warnings";
const WARNINGS_HEADER = "X-Provisa-Warnings";

function roleHeaders(role: string, accept: string): Record<string, string> {
  return { Accept: accept, ...(role ? { "X-Provisa-Role": role } : {}) };
}

/** Why a request was refused, in the service's own error body (`{error: {code, message}}`). */
async function refusal(resp: Response, op: string): Promise<string> {
  const text = await resp.text();
  try {
    const body: unknown = JSON.parse(text);
    if (body !== null && typeof body === "object") {
      const shape = body as { error?: { code?: string; message?: string }; detail?: string };
      return serverMessage(shape.error ?? shape, requestFailed(op, resp.status));
    }
  } catch {
    // Not JSON (a proxy's page, a bare message): its text is all there is to say.
  }
  return text || requestFailed(op, resp.status);
}

/** The role's CSDL document, as XML text. */
export async function fetchODataMetadata(role: string): Promise<string> {
  const resp = await fetch(`${SERVICE_ROOT}/$metadata`, {
    headers: roleHeaders(role, "application/xml"),
  });
  if (!resp.ok) throw new Error(await refusal(resp, "OData $metadata"));
  return resp.text();
}

/** One page of an entity set's read. */
export interface ODataPage {
  rows: Record<string, unknown>[];
  /** The page's columns: every property the rows carry, in their order, annotations left out. */
  columns: string[];
  /** The service's link to the next page, as it gave it; null on the last page. */
  nextLink: string | null;
  /** `@odata.count`, when the read asked for it. */
  count: number | null;
  /** What the answer says about itself: a row limit that cut it among them. */
  warnings: StatementWarning[];
}

/** The path and query of a service URL, so it is read from this page's own origin: a nextLink is
 * absolute, written from the host the service saw, which behind a proxy is not this page's. */
export function servicePath(url: string): string {
  const parsed = new URL(url, window.location.origin);
  if (!parsed.pathname.startsWith(SERVICE_ROOT)) {
    throw new Error(`not an address of the OData service: ${url}`);
  }
  return `${parsed.pathname}${parsed.search}`;
}

/** An annotation key (`@odata.etag`, `Orders@odata.nextLink`) rather than a property. */
function isAnnotation(key: string): boolean {
  return key.includes("@");
}

export function columnsOf(rows: Record<string, unknown>[]): string[] {
  const seen = new Set<string>();
  for (const row of rows) {
    for (const key of Object.keys(row)) if (!isAnnotation(key)) seen.add(key);
  }
  return [...seen];
}

function headerWarnings(resp: Response): StatementWarning[] {
  const raw = resp.headers.get(WARNINGS_HEADER);
  if (!raw) return [];
  const parsed: unknown = JSON.parse(raw);
  return Array.isArray(parsed) ? (parsed as StatementWarning[]) : [];
}

/** The warnings of a read, from its body annotation and its header: the same warnings, said
 * twice; each is kept once. */
export function readWarnings(
  body: Record<string, unknown>,
  header: StatementWarning[],
): StatementWarning[] {
  const annotated = Array.isArray(body[WARNINGS_ANNOTATION])
    ? (body[WARNINGS_ANNOTATION] as StatementWarning[])
    : [];
  const kept = new Map<string, StatementWarning>();
  for (const w of [...annotated, ...header]) kept.set(`${w.code}\u0000${w.message}`, w);
  return [...kept.values()];
}

/** One page of a read at `url` (a service URL, absolute or a path). */
export async function runODataQuery(url: string, role: string): Promise<ODataPage> {
  const resp = await fetch(servicePath(url), { headers: roleHeaders(role, "application/json") });
  if (!resp.ok) throw new Error(await refusal(resp, "OData read"));
  const body = (await resp.json()) as Record<string, unknown>;
  const rows = Array.isArray(body.value) ? (body.value as Record<string, unknown>[]) : [];
  const next = body["@odata.nextLink"];
  const count = body["@odata.count"];
  return {
    rows,
    columns: columnsOf(rows),
    nextLink: typeof next === "string" ? next : null,
    count: typeof count === "number" ? count : null,
    warnings: readWarnings(body, headerWarnings(resp)),
  };
}
