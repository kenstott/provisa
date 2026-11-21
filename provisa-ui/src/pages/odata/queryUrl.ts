// Copyright (c) 2026 Kenneth Stott
// Canary: 0a78dfaf-d963-4815-8896-5c1738fde5a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2193

// The OData explorer's query: the system query options the builder sets, and the URL they make.

import type { ODataEntitySet } from "./metadata";

export interface OrderKey {
  property: string;
  descending: boolean;
}

export interface ODataQuery {
  /** The properties read; empty reads every property. */
  select: string[];
  /** The navigation properties expanded. */
  expand: string[];
  /** A $filter expression as written; blank filters nothing. */
  filter: string;
  orderBy: OrderKey[];
  top: number | null;
  skip: number | null;
  count: boolean;
}

export const EMPTY_QUERY: ODataQuery = {
  select: [],
  expand: [],
  filter: "",
  orderBy: [],
  top: null,
  skip: null,
  count: false,
};

/** One option value encoded for a query string. Encoded as a URI component, so `&`, `=`, `#`,
 * `+` and spaces are escaped (the service reads a bare `+` as a space), then the characters OData
 * writes in its own syntax and a query string allows (`$ , / : @`) are given back, so the URL reads
 * as the OData it is. */
export function encodeOptionValue(value: string): string {
  return encodeURIComponent(value).replace(/%(24|2C|2F|3A|40)/gi, (escaped) =>
    decodeURIComponent(escaped),
  );
}

/** The query string the options make, `$filter` first, without its `?`; empty when none is set. */
export function queryString(query: ODataQuery): string {
  const options: [string, string][] = [];
  const filter = query.filter.trim();
  if (filter) options.push(["$filter", filter]);
  if (query.select.length) options.push(["$select", query.select.join(",")]);
  if (query.expand.length) options.push(["$expand", query.expand.join(",")]);
  if (query.orderBy.length) {
    options.push([
      "$orderby",
      query.orderBy.map((k) => (k.descending ? `${k.property} desc` : k.property)).join(","),
    ]);
  }
  if (query.top !== null) options.push(["$top", String(query.top)]);
  if (query.skip !== null && query.skip > 0) options.push(["$skip", String(query.skip)]);
  if (query.count) options.push(["$count", "true"]);
  return options.map(([name, value]) => `${name}=${encodeOptionValue(value)}`).join("&");
}

/** The read of entity set `set` under the service root `root` with the options of `query`. */
export function buildQueryUrl(root: string, set: string, query: ODataQuery): string {
  const qs = queryString(query);
  return `${root}/${encodeURIComponent(set)}${qs ? `?${qs}` : ""}`;
}

/** Why a handed read could not be set in the explorer: an `odataPage.*` message key and its
 * values, so the page says it in the reader's language. */
export class HandoffRefused extends Error {
  readonly key: string;
  readonly values: Record<string, string>;
  constructor(key: string, values: Record<string, string>) {
    super(`${key} ${JSON.stringify(values)}`);
    this.key = key;
    this.values = values;
  }
}

/** A read handed to the explorer (Polly, a link): the entity set it reads and its options. */
export interface HandedRead {
  set: string;
  query: ODataQuery;
}

function list(value: string): string[] {
  return value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function wholeNumber(option: string, value: string): number {
  if (!/^\d+$/.test(value.trim())) {
    throw new HandoffRefused("odataPage.handoffNumber", { option, value });
  }
  return Number(value.trim());
}

function orderKey(term: string): OrderKey {
  const [property, direction, ...rest] = term.split(/\s+/);
  if (rest.length > 0 || (direction !== undefined && !/^(asc|desc)$/i.test(direction))) {
    throw new HandoffRefused("odataPage.handoffOption", { option: "$orderby", value: term });
  }
  return { property, descending: direction?.toLowerCase() === "desc" };
}

/** The entity set and options of a read of the service under `root` (a path, `/data/odata`), as
 * the builder holds them. A URL the builder cannot hold (an entity's key or a navigation in the
 * path, a nested $expand, an option it has no field for) is refused, never read in part. */
export function parseQueryUrl(url: string, root: string): HandedRead {
  const parsed = new URL(url, window.location.origin);
  if (!parsed.pathname.startsWith(`${root}/`)) {
    throw new HandoffRefused("odataPage.handoffNotService", { url });
  }
  const path = parsed.pathname.slice(root.length + 1);
  if (!path || path.includes("/") || path.includes("(") || path.startsWith("$")) {
    throw new HandoffRefused("odataPage.handoffPath", { path });
  }
  const query: ODataQuery = { ...EMPTY_QUERY };
  for (const [name, value] of parsed.searchParams) {
    switch (name) {
      case "$filter":
        query.filter = value;
        break;
      case "$select":
        query.select = list(value);
        break;
      case "$expand":
        if (value.includes("(")) {
          throw new HandoffRefused("odataPage.handoffOption", { option: name, value });
        }
        query.expand = list(value);
        break;
      case "$orderby":
        query.orderBy = list(value).map(orderKey);
        break;
      case "$top":
        query.top = wholeNumber(name, value);
        break;
      case "$skip":
        query.skip = wholeNumber(name, value);
        break;
      case "$count":
        if (!/^(true|false)$/.test(value)) {
          throw new HandoffRefused("odataPage.handoffOption", { option: name, value });
        }
        query.count = value === "true";
        break;
      default:
        throw new HandoffRefused("odataPage.handoffOption", { option: name, value });
    }
  }
  return { set: decodeURIComponent(path), query };
}

/** Refuses a handed read that names a member `entity` does not have: the builder could not show it,
 * and the service would refuse it. */
export function checkHandedRead(read: HandedRead, entity: ODataEntitySet): void {
  const properties = new Set(entity.properties.map((p) => p.name));
  const navigations = new Set(entity.navigations.map((n) => n.name));
  const members: [string, string[], Set<string>][] = [
    ["$select", read.query.select, properties],
    ["$expand", read.query.expand, navigations],
    ["$orderby", read.query.orderBy.map((k) => k.property), properties],
  ];
  for (const [option, names, known] of members) {
    const unknown = names.find((n) => !known.has(n));
    if (unknown !== undefined) {
      throw new HandoffRefused("odataPage.handoffMember", {
        set: entity.name,
        option,
        name: unknown,
      });
    }
  }
}
