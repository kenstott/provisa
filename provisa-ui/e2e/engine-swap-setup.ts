// Copyright (c) 2026 Kenneth Stott
// Canary: 330c3fe8-08fc-4973-ac36-6f85d5d8fb8b
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730: the per-file setup every engine-swap*.spec.ts file shares -- one Trino stabilization,
// one set of file-lake fixtures, and one zombie-source sweep per worker. Split out of
// engine-swap.spec.ts when that file was divided to stay under the max-lines lint rule.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import { test } from "./coverage";
import {
  FILE_LAKE_HOST_DIR,
  MAKE_FILE_LAKE_FIXTURES,
  PYTHON,
  sweepZombieSwapSources,
  waitForTrinoStable,
} from "./engine-swap-helpers";

/** Registers the file-wide hooks in the calling spec file and returns its file-lake table
 * paths, read after the hooks have run. */
export function registerSwapFileHooks(): { deltaTablePath: () => string; icebergTablePath: () => string } {
  // REQ-1730 redesign (2026-09-16, "one source = one test"): this used to be ONE test registering
  // all 12-13 types in sequence, sharing one beforeAll that booted every container (firebird +
  // airport + singlestore + the 7-container shared demo-source stack) regardless of which type
  // anyone actually wanted to check. That meant: a single type could not be targeted or debugged in
  // isolation (a `-g mongodb` run still paid for and waited on all thirteen), a failure on type #2
  // blocked ever finding out whether #3-13 worked, and the whole suite had to be rerun from zero on
  // every iteration. Splitting into one independent test per type fixes all three: `-g firebird`
  // now provisions and tears down ONLY firebird's own container (see resolve-needed-sources.ts's
  // title parsing, the same mechanism source-to-query.spec.ts already used), one type's failure
  // reports on its own and does not block the others, and each test can be rerun alone. The cost is
  // each type's own Trino reload/requery pass no longer amortizes across the batch — reloadEngineBackend
  // runs once per test instead of once for all thirteen. Given today's actual experience (isolating
  // one failure meant reruning the full 10+ minute batch, repeatedly), that trade is worth it.
  //
  // Trino cold-start stabilization is the one thing still shared file-wide: waitForTrinoStable()
  // runs ONCE per worker process (Playwright dedupes an outer-scope beforeAll across every test in
  // the file that runs in it), not once per type.
  test.beforeAll(async () => {
    // Can take a few minutes on top of the default per-hook timeout — see
    // source-to-query-olap-lake-trino.spec.ts's same pattern for its own multi-minute cold-init
    // beforeAlls.
    test.setTimeout(360000);
    await waitForTrinoStable();
  });

  // Written ONCE per worker (like waitForTrinoStable above), under FILE_LAKE_HOST_DIR — the SAME
  // fixed directory docker-compose.core.yml mounts into Trino, not a random per-run temp dir
  // (source-to-query-file-lake.spec.ts's own os.tmpdir() approach doesn't work here precisely
  // because it's random: a static docker-compose volume mount can't follow it). Each run gets its
  // own never-before-seen subdirectory (a timestamp), and nothing under the mount is ever deleted:
  // Docker Desktop's VirtioFS attaches the bind mount to the directory's inode at container start,
  // and a delete+recreate cycle ANYWHERE beneath the mount point silently poisons its directory
  // cache for the WHOLE mount, not just the touched subpath (verified live 2026-09-16: rmSync+
  // mkdirSync of the mount root orphaned it outright; even rmSync+recreate of a child directory,
  // with the mount root itself left alone, broke live sync for the entire tree minutes later,
  // including previously-synced top-level files). Never rm anything here — old runs' subdirectories
  // are simply abandoned (an accepted, bounded amount of test-only disk growth), not cleaned up.
  let deltaTablePath = "";
  let icebergTablePath = "";
  test.beforeAll(() => {
    fs.mkdirSync(FILE_LAKE_HOST_DIR, { recursive: true });
    const runDir = path.join(FILE_LAKE_HOST_DIR, `run_${Date.now()}`);
    fs.mkdirSync(runDir, { recursive: true });
    const out = execFileSync(PYTHON, [MAKE_FILE_LAKE_FIXTURES, runDir], {
      encoding: "utf8",
    });
    [deltaTablePath, icebergTablePath] = out.trim().split("\n");
  });

  test.afterAll(sweepZombieSwapSources);
  return { deltaTablePath: () => deltaTablePath, icebergTablePath: () => icebergTablePath };
}
