// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// Build-time doc staging: copy the FULL published documentation set into
// public/guides-md/ so Vite bundles it into dist. The in-app Docs page (/docs)
// reads it SAME-ORIGIN so the complete docs are available offline/airgapped.
// A live-from-repo fallback in the UI only kicks in when a doc isn't bundled
// and the machine is online.
//
// Single source of truth: ../mkdocs.yml nav. The in-app reader and the hosted
// MkDocs site (https://provisa.dev/docs/) therefore publish the same set. The
// nav "Home: index.md" entry maps to README.md — the reader rewrites relative
// links/images itself, so it consumes the README directly rather than the
// generated docs/index.md.
//
// Runs as the `prebuild` npm hook. Output (public/guides-md/) is git-ignored.

import { readFileSync, writeFileSync, mkdirSync, rmSync, cpSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";

const HERE = dirname(fileURLToPath(import.meta.url));
const UI_ROOT = join(HERE, "..");
const REPO_ROOT = join(UI_ROOT, "..");
// Served under /guides-md/ (NOT /docs — that path is FastAPI's reserved Swagger UI).
const OUT_DIR = join(UI_ROOT, "public", "guides-md");

// Flatten the mkdocs nav (list of `{Title: "path.md"}` leaves and
// `{Section: [ ...nested ]}` groups) into ordered {title, section, path} leaves.
// The in-app sidebar is flat, so `section` is carried to disambiguate leaves
// that share a label across sections (e.g. two "Overview" pages).
function flattenNav(nav, section = null) {
  const out = [];
  for (const item of nav) {
    for (const [title, value] of Object.entries(item)) {
      if (typeof value === "string") out.push({ title, section, path: value });
      else if (Array.isArray(value)) out.push(...flattenNav(value, title));
    }
  }
  return out;
}

const mkdocs = parse(readFileSync(join(REPO_ROOT, "mkdocs.yml"), "utf8"));
if (!Array.isArray(mkdocs.nav)) throw new Error("mkdocs.yml: nav missing or not a list");

// Map each nav leaf to a bundled doc entry. `repoPath` drives the live
// GitHub-raw fallback in the UI; `slug` is the same-origin filename.
// Normalize labels first (Home -> Overview), then qualify any that recur across
// sections with their section name so the flat sidebar has no duplicate labels.
const leaves = flattenNav(mkdocs.nav).map((l) => ({
  ...l,
  title: l.path === "index.md" && l.title === "Home" ? "Overview" : l.title,
}));
const labelCounts = leaves.reduce((m, l) => m.set(l.title, (m.get(l.title) ?? 0) + 1), new Map());
const DOCS = leaves.map(({ title, section, path }) => {
  const label = labelCounts.get(title) > 1 && section ? `${section}: ${title}` : title;
  if (path === "index.md") {
    // The site homepage is generated from README; the in-app reader consumes
    // the README source directly (it rewrites relative paths at render time).
    return { slug: "readme", title: label, repoPath: "README.md" };
  }
  return { slug: path.replace(/\.md$/, "").replace(/\//g, "-"), title: label, repoPath: `docs/${path}` };
});

rmSync(OUT_DIR, { recursive: true, force: true });
mkdirSync(OUT_DIR, { recursive: true });

const manifest = [];
for (const doc of DOCS) {
  const src = join(REPO_ROOT, doc.repoPath);
  try {
    const md = readFileSync(src, "utf8");
    writeFileSync(join(OUT_DIR, `${doc.slug}.md`), md);
    manifest.push(doc);
  } catch {
    // A doc missing at build time isn't fatal — the UI live-fallback can still fetch it.
    console.warn(`[copy-docs] skipped (not found): ${doc.repoPath}`);
    manifest.push(doc);
  }
}

// Bundle the doc images (docs reference docs/images/*.png) so figures render offline.
// The UI's img renderer maps relative image paths to /guides-md/images/<basename>.
const IMAGES_SRC = join(REPO_ROOT, "docs", "images");
if (existsSync(IMAGES_SRC)) {
  cpSync(IMAGES_SRC, join(OUT_DIR, "images"), { recursive: true });
}

writeFileSync(join(OUT_DIR, "manifest.json"), JSON.stringify(manifest, null, 2));
console.log(`[copy-docs] staged ${manifest.length} docs to public/guides-md/`);
