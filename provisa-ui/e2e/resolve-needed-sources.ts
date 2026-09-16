// Copyright (c) 2026 Kenneth Stott
// Canary: b40658f1-dcdd-40ff-996d-2cbe01238a81
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What global-setup.ts actually needs to start for THIS invocation: the shared demo-source-
// containers.ts stack is 9 containers (~5-6GB: Splunk ~1.6GB, Cassandra ~1.4GB, Elasticsearch
// ~1GB, two Neo4j instances between the export target and the demo one), and the separate neo4j
// export container is its own ~380MB on top. A run scoped to one file, or one -g pattern within
// source-to-query.spec.ts, needs at most a handful of these — starting the rest was pure waste.
//
// This derives the exact subset from the Playwright invocation's own positional spec-file
// arguments and -g/--grep pattern (the same command line every caller already types), so nothing
// needs to be remembered or declared per-run.

import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

import { DEMO_SOURCES, type DemoSource } from "./demo-source-containers";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Files that pull in one or more DEMO_SOURCES containers by a FIXED set, regardless of any -g
// filter (each drives exactly one source per file, so there is no finer split to make) —
// confirmed by grep for E2E_*_PORT usage in each.
const FIXED_FILE_NEEDS: Record<string, readonly DemoSource[]> = {
  "hasura-import.spec.ts": ["chinook"],
};

// Files whose tests are each scoped to a single source, named "<sourcename>: ..." — parsing their
// own test titles (rather than hardcoding the same list a second time here) keeps this in sync
// automatically as tests are added/renamed/removed there.
const SOURCE_TO_QUERY_FILE = "source-to-query.spec.ts";
const TITLE_SCOPED_FILES = [SOURCE_TO_QUERY_FILE];
// engine-swap.spec.ts needs NO entry anywhere in this module (REQ-1730, 2026-09-16 redesign):
// every one of its 13 tests — including the 7 that used to ride the shared demo-source-
// containers.ts fleet this module provisions — now starts and tears down its own container from
// inside its own scoped test.describe (see that file's per-type beforeAll/afterAll). A run of
// that file asks this resolver for nothing and gets nothing, by design: a single test creates the
// resources it needs and tears them down, never depends on what global-setup/another test left
// running.

// The neo4j EXPORT container (a separate fixture from the "neo4j" demo source above) is used by
// exactly this one file — see neo4j-container.ts's own module comment.
const NEO4J_EXPORT_FILE = "neo4j-docker-export.spec.ts";

function argSpecFiles(): string[] {
  return process.argv.filter((a) => a.endsWith(".spec.ts")).map((a) => path.basename(a));
}

function grepPattern(): string | undefined {
  const argv = process.argv;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "-g" || argv[i] === "--grep") return argv[i + 1];
    if (argv[i].startsWith("--grep=")) return argv[i].slice("--grep=".length);
  }
  return undefined;
}

function titleScopedNeeds(file: string, grep: string | undefined): DemoSource[] {
  const text = fs.readFileSync(path.resolve(__dirname, file), "utf8");
  const titles = [...text.matchAll(/test\(\s*"([^"]+)"/g)].map((m) => m[1]);
  const matched = grep ? titles.filter((t) => new RegExp(grep).test(t)) : titles;
  const leadingNames = matched.map((t) => t.split(":")[0].trim());
  return DEMO_SOURCES.filter((s) => leadingNames.includes(s));
}

export interface NeededSources {
  /** The exact demo-source-containers.ts subset this invocation needs. */
  sources: DemoSource[];
  /** Whether the separate neo4j export container is needed. */
  neo4jExport: boolean;
}

export function resolveNeededSources(): NeededSources {
  const files = argSpecFiles();
  if (files.length === 0) {
    // No positional spec-file filter — a whole-suite run. Every prior behavior is preserved:
    // start everything, since some file in the run needs each of them.
    return { sources: [...DEMO_SOURCES], neo4jExport: true };
  }
  const grep = grepPattern();
  const sourceSet = new Set<DemoSource>();
  let neo4jExport = false;
  for (const f of files) {
    if (TITLE_SCOPED_FILES.includes(f)) {
      for (const s of titleScopedNeeds(f, grep)) sourceSet.add(s);
    } else if (FIXED_FILE_NEEDS[f]) {
      for (const s of FIXED_FILE_NEEDS[f]) sourceSet.add(s);
    }
    if (f === NEO4J_EXPORT_FILE) neo4jExport = true;
  }
  return { sources: [...sourceSet], neo4jExport };
}
