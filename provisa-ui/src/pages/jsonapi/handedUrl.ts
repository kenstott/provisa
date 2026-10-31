// Copyright (c) 2026 Kenneth Stott
// Canary: 2f4b8c16-e07a-4d53-b1c9-6a0d3e97f152
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** A JSON:API URL handed to the Explorer (NL "Open in JSON:API", Polly), read into what its
 * pickers hold. What no picker can hold stays in `handedParams`, sent with the request and shown
 * on the page, so a handed request is never narrowed or widened on the way in. */
export interface HandedUrl {
  domainId: string;
  tableName: string;
  pageSize: string;
  aggregate: boolean;
  funcs: string[];
  groupBy: string;
  includeNodes: boolean;
  include: string[];
  fields: string[];
  filter: { field: string; op: string; value: string } | null;
  sort: { field: string; dir: "asc" | "desc" } | null;
  handedParams: [string, string][];
}

const FILTER_KEY = /^filter\[([^\]]+)\](?:\[([^\]]+)\])?$/;

export function parseHandedUrl(navUrl: string, filterOps: string[]): HandedUrl | null {
  const match = navUrl.match(/^\/data\/jsonapi\/([^/]+)\/([^?]+)/);
  if (!match) return null;
  const [, domainId, tableName] = match;
  const qs = navUrl.includes("?") ? navUrl.split("?")[1] : "";
  const params = new URLSearchParams(qs);
  const aggregateParam = params.get("aggregate");

  let filter: HandedUrl["filter"] = null;
  let sort: HandedUrl["sort"] = null;
  const fieldsKey = `fields[${tableName}]`;
  const handedParams: [string, string][] = [];
  const pickerKeys = new Set(["aggregate", "groupBy", "includeNodes", "include", fieldsKey]);
  // The page sets these itself: the acting role, and the total its result count shows.
  const pageKeys = new Set(["role", "page[size]", "page[total]"]);
  params.forEach((value, key) => {
    if (pickerKeys.has(key) || pageKeys.has(key)) return;
    const filterKey = key.match(FILTER_KEY);
    const op = filterKey?.[2] ?? "eq";
    if (filterKey && filter === null && filterOps.includes(op)) {
      filter = { field: filterKey[1], op, value };
    } else if (key === "sort" && !value.includes(",")) {
      sort = value.startsWith("-")
        ? { field: value.slice(1), dir: "desc" }
        : { field: value, dir: "asc" };
    } else {
      handedParams.push([key, value]);
    }
  });

  return {
    domainId,
    tableName,
    pageSize: params.get("page[size]") ?? "20",
    // REQ-1359/REQ-1361: NL "Open in JSON:API" carries ?groupBy=...&aggregate=... for
    // aggregate/group-by queries. `aggregate` is either "true" (every function — the SQL
    // branch matched no specific agg call) or a comma-separated func list (REQ-1361).
    aggregate: aggregateParam !== null,
    funcs:
      aggregateParam && aggregateParam !== "true" ? aggregateParam.split(",").filter(Boolean) : [],
    groupBy: params.get("groupBy") ?? "",
    // REQ-1359: NL forwards includeNodes=true for group-by queries whose plan carries a nodes
    // array (see runner.py::_generate_jsonapi_query) — must survive the round-trip or the
    // replicated JSON:API call silently drops the nodes.
    includeNodes: params.get("includeNodes") === "true" || params.get("includeNodes") === "1",
    // REQ-1408: ?include= carries the nodes projection as relationship names and "rel.col"
    // dot-paths (runner.py::_generate_jsonapi_query). Without seeding the picker from it the
    // page rebuilt the URL without the projection and the related columns vanished.
    include: (params.get("include") ?? "").split(",").filter(Boolean),
    fields: (params.get(fieldsKey) ?? "").split(",").filter(Boolean),
    filter,
    sort,
    handedParams,
  };
}
