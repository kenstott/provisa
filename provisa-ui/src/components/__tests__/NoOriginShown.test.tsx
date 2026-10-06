// Copyright (c) 2026 Kenneth Stott
// Canary: fccfd1d1-10e9-4ddc-8b09-52b277ee337f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A configuration seeds the model store once; objects carry no visible mark of having been seeded
// (REQ-1919). No page shows an origin, no catalog carries the words for one, and no change is
// followed by a notice that a later load of the config re-applies the file.

import { readdirSync, readFileSync, statSync } from "fs";
import { join, resolve } from "path";
import { describe, it, expect } from "vitest";

const SRC = resolve(__dirname, "..", "..");

function files(dir: string, keep: (name: string) => boolean): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : files(path, keep);
    return keep(name) ? [path] : [];
  });
}

const SOURCES = files(SRC, (n) => /\.(tsx?|graphql)$/.test(n) && !/\.test\.tsx?$/.test(n));
const CATALOGS = files(join(SRC, "i18n", "locales"), (n) => n.endsWith(".json"));

describe("no origin is shown (REQ-1919)", () => {
  it("no page renders an origin badge", () => {
    const marking = SOURCES.filter((f) => /OriginBadge|origin-badge|originBadge\./.test(readFileSync(f, "utf-8")));
    expect(marking).toEqual([]);
  });

  it("no admin query asks for an object's origin", () => {
    const graphql = readFileSync(join(SRC, "hooks", "admin.graphql"), "utf-8");
    expect(graphql).not.toMatch(/^\s*origin\s*$/m);
  });

  it("no catalog in any language carries an origin badge or a config-reapply notice", () => {
    const offending = CATALOGS.filter((f) =>
      /"originBadge"|config_object_edited|config_object_deleted/.test(readFileSync(f, "utf-8")),
    );
    expect(offending).toEqual([]);
  });

  it("no change is followed by a notice that a later config load re-applies the file", () => {
    const announcing = SOURCES.filter((f) => /announceWarnings|mutationWarnings/.test(readFileSync(f, "utf-8")));
    expect(announcing).toEqual([]);
  });
});
