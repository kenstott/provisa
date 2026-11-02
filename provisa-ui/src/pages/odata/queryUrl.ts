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
