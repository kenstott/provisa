// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// Build-time doc staging: copy a curated, user-facing set of markdown docs from the
// repo into public/docs/ so Vite bundles them into dist. The in-app Docs page reads
// them SAME-ORIGIN (works airgapped, no network). A live-from-repo fallback in the UI
// only kicks in when a doc isn't bundled and the machine is online.
//
// Runs as the `prebuild` npm hook. Output (public/docs/) is git-ignored and regenerated.

import { readFileSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const UI_ROOT = join(HERE, "..");
const REPO_ROOT = join(UI_ROOT, "..");
const OUT_DIR = join(UI_ROOT, "public", "docs");

// Curated allowlist — user-facing docs only (no internal/marketing/release material).
// `repoPath` is used for the live GitHub-raw fallback in the UI.
const DOCS = [
  { slug: "readme",        title: "Overview",         repoPath: "README.md" },
  { slug: "quickstart",    title: "Getting Started",  repoPath: "docs/quickstart.md" },
  { slug: "sources",       title: "Sources",          repoPath: "docs/sources.md" },
  { slug: "configuration", title: "Configuration",    repoPath: "docs/configuration.md" },
  { slug: "admin",         title: "Administration",   repoPath: "docs/admin.md" },
  { slug: "api-reference", title: "API Reference",    repoPath: "docs/api-reference.md" },
  { slug: "architecture",  title: "Architecture",     repoPath: "docs/architecture.md" },
  { slug: "cypher",        title: "Cypher",           repoPath: "docs/cypher.md" },
  { slug: "pgwire",        title: "Postgres Wire",    repoPath: "docs/pgwire.md" },
  { slug: "import",        title: "Import / Export",  repoPath: "docs/import.md" },
  { slug: "integrations",  title: "Integrations",     repoPath: "docs/integrations.md" },
  { slug: "python-client", title: "Python Client",    repoPath: "docs/python-client.md" },
  { slug: "security",      title: "Security",         repoPath: "docs/security.md" },
  { slug: "deployment",    title: "Deployment",       repoPath: "docs/deployment.md" },
  { slug: "multitenant",   title: "Multi-tenant",     repoPath: "docs/multitenant.md" },
  { slug: "subscriptions", title: "Subscriptions",    repoPath: "docs/subscriptions.md" },
];

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

writeFileSync(join(OUT_DIR, "manifest.json"), JSON.stringify(manifest, null, 2));
console.log(`[copy-docs] staged ${manifest.length} docs to public/docs/`);
