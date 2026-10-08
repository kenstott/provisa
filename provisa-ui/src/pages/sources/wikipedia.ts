// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7b1c93-a25f-4d68-9b30-8c1f6e2a7d54
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1960: a Wikipedia source's setup. What the operator chooses rides in the source's
// federation hints under the brand; the server makes it the crawl of the files source that
// carries the brand, so a setting left empty here is the brand's default there.

export const WIKIPEDIA = "wikipedia";

/** A crawl setting the operator may state in place of the brand's default. */
export interface WikipediaCrawlField {
  key: string; // the crawl setting, as the source's mapping names it
  kind: "text" | "number" | "list";
  placeholder: string; // the brand's default, shown while the field is empty
}

export const WIKIPEDIA_CRAWL_FIELDS: WikipediaCrawlField[] = [
  { key: "request_delay", kind: "text", placeholder: "1 seconds" },
  { key: "html_cache_ttl", kind: "text", placeholder: "1 days" },
  { key: "html_table_min_rows", kind: "number", placeholder: "2" },
  { key: "html_table_max_rows", kind: "number", placeholder: "" },
  { key: "table_selector", kind: "text", placeholder: "table.wikitable" },
  { key: "content_selector", kind: "text", placeholder: "#mw-content-text .mw-parser-output" },
  {
    key: "link_selector",
    kind: "text",
    placeholder: "a[rel='mw:WikiLink']:not(.new):not(.mw-selflink):not(.mw-magiclink-isbn)",
  },
  { key: "remove_selectors", kind: "list", placeholder: ".navbox\n.infobox\n.reflist" },
  { key: "link_exclude_patterns", kind: "list", placeholder: "" },
  { key: "allowed_file_extensions", kind: "list", placeholder: "csv\ntsv\nxlsx\nxls\njson\nparquet" },
  { key: "max_html_size", kind: "text", placeholder: "10MB" },
  { key: "max_data_file_size", kind: "text", placeholder: "100MB" },
];

/** The language editions whose "See also" heading the server knows. */
export const WIKIPEDIA_KNOWN_SEE_ALSO = new Set([
  "en", "de", "es", "it", "pt", "nl", "pl", "sv", "ja", "zh", "ru",
]);

const lines = (text: string | undefined) =>
  (text ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

/** The fields still needed before the source can be saved. */
export function wikipediaMissing(fields: Record<string, string>): string[] {
  const missing: string[] = [];
  if (lines(fields.wp_pages).length === 0) missing.push("wp_pages");
  const language = (fields.wp_language ?? "").trim() || "en";
  if (
    fields.wp_follow_see_also !== "true" &&
    !WIKIPEDIA_KNOWN_SEE_ALSO.has(language) &&
    !fields.wp_see_also_heading?.trim()
  ) {
    missing.push("wp_see_also_heading");
  }
  return missing;
}

/** The source's federation hints for what the fields say. */
export function wikipediaHints(fields: Record<string, string>): Record<string, unknown> {
  const settings: Record<string, unknown> = {
    pages: lines(fields.wp_pages),
    follow_see_also: fields.wp_follow_see_also === "true",
  };
  for (const [field, name] of [
    ["wp_language", "language"],
    ["wp_see_also_heading", "see_also_heading"],
    ["wp_contact", "contact"],
  ] as const) {
    const value = fields[field]?.trim();
    if (value) settings[name] = value;
  }
  for (const [field, name] of [
    ["wp_max_depth", "max_depth"],
    ["wp_max_pages", "max_pages"],
  ] as const) {
    const value = fields[field]?.trim();
    if (value) settings[name] = Number(value);
  }
  const crawl: Record<string, unknown> = {};
  for (const f of WIKIPEDIA_CRAWL_FIELDS) {
    const raw = fields[`wpc_${f.key}`];
    if (f.kind === "list") {
      if (lines(raw).length) crawl[f.key] = lines(raw);
    } else if (raw?.trim()) {
      crawl[f.key] = f.kind === "number" ? Number(raw.trim()) : raw.trim();
    }
  }
  if (Object.keys(crawl).length) settings.crawl = crawl;
  return { brand: WIKIPEDIA, [WIKIPEDIA]: settings };
}

/** The fields a stored Wikipedia source's hints carry, for the form that edits it. */
export function wikipediaFieldsFromHints(federationHintsJson: string): Record<string, string> {
  const hints = JSON.parse(federationHintsJson) as Record<string, unknown>;
  const settings = (hints[WIKIPEDIA] ?? {}) as Record<string, unknown>;
  const fields: Record<string, string> = {
    wp_pages: ((settings.pages as string[] | undefined) ?? []).join("\n"),
    wp_follow_see_also: settings.follow_see_also ? "true" : "",
  };
  for (const [field, name] of [
    ["wp_language", "language"],
    ["wp_see_also_heading", "see_also_heading"],
    ["wp_contact", "contact"],
    ["wp_max_depth", "max_depth"],
    ["wp_max_pages", "max_pages"],
  ] as const) {
    if (settings[name] !== undefined) fields[field] = String(settings[name]);
  }
  const crawl = (settings.crawl ?? {}) as Record<string, unknown>;
  for (const f of WIKIPEDIA_CRAWL_FIELDS) {
    const value = crawl[f.key];
    if (value === undefined) continue;
    fields[`wpc_${f.key}`] = Array.isArray(value) ? value.join("\n") : String(value);
  }
  return fields;
}
