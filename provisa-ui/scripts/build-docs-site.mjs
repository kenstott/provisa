// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// Build-time docs staging: build the full MkDocs Material site into
// public/docs-site/ so Vite bundles it into dist and the app serves it
// same-origin under /docs-site/. This is the OFFLINE fallback for the in-app
// /docs reader (client-side lunr search works airgapped); the reader prefers
// the hosted site when reachable.
//
// Runs as the `prebuild` npm hook. Output (public/docs-site/) is git-ignored.
// Requires Python + mkdocs-material on the build host. Set SKIP_DOCS_SITE=1 to
// skip for pure-frontend dev (the offline copy is then simply absent).

import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

if (process.env.SKIP_DOCS_SITE === "1") {
  console.warn("[build-docs-site] SKIP_DOCS_SITE=1 — offline docs will NOT be bundled");
  process.exit(0);
}

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(HERE, "..", "..");
const OUT_DIR = join(HERE, "..", "public", "docs-site");
const PYTHON = process.env.PYTHON_BIN || "python3";
const MKDOCS = process.env.MKDOCS_BIN || "mkdocs";

function run(cmd, cmdArgs) {
  const r = spawnSync(cmd, cmdArgs, { cwd: REPO_ROOT, stdio: "inherit" });
  if (r.error) {
    console.error(`[build-docs-site] failed to run ${cmd}: ${r.error.message}`);
    console.error("[build-docs-site] install docs deps: pip install mkdocs-material pymdown-extensions");
    process.exit(1);
  }
  if (r.status !== 0) process.exit(r.status ?? 1);
}

// 1. Regenerate the homepage from README (single source of truth).
run(PYTHON, ["scripts/gen_docs_index.py"]);
// 2. Build the static site into public/docs-site (--strict catches broken links).
run(MKDOCS, ["build", "--strict", "-f", "mkdocs.yml", "-d", OUT_DIR]);

console.log(`[build-docs-site] built MkDocs site to ${OUT_DIR}`);
