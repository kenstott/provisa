// Copyright (c) 2026 Kenneth Stott
// Canary: 8c895c2b-a120-4a09-bd91-9263844b292c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The vite config's packages are located once, at start: present gives its directory; absent, or
// installed without a file the config reads, stops the start with the package's name and
// `npm install` instead of an ENOENT on every request for the file.

// @vitest-environment node
import fs from "fs";
import os from "os";
import path from "path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MissingPackageError, installedPackageDir } from "../installedPackage";

const ROOT = path.resolve(__dirname, "../../..");

describe("installedPackageDir", () => {
  let fake: string;
  beforeEach(() => {
    fake = fs.mkdtempSync(path.join(os.tmpdir(), "installed-package-"));
    fs.writeFileSync(path.join(fake, "package.json"), "{}");
  });
  afterEach(() => fs.rmSync(fake, { recursive: true, force: true }));

  it("gives the directory of an installed package holding the files asked for", () => {
    expect(
      installedPackageDir(ROOT, "swagger-ui-dist", ["swagger-ui-bundle.js", "swagger-ui.css"]),
    ).toBe(path.join(ROOT, "node_modules/swagger-ui-dist"));
  });

  it("finds a package whose exports map omits ./package.json", () => {
    expect(
      installedPackageDir(ROOT, "@neo4j-cypher/codemirror", ["lib/cypher-state-definitions.js"]),
    ).toBe(path.join(ROOT, "node_modules/@neo4j-cypher/codemirror"));
  });

  it("names a package that is not installed and the command that installs it", () => {
    const pkg = "provisa-test-absent-package-0b7d";
    expect(() => installedPackageDir(fake, pkg, [])).toThrow(MissingPackageError);
    expect(() => installedPackageDir(fake, pkg, [])).toThrow(
      `npm package "${pkg}" is not installed under ${path.join(fake, "node_modules")}: ` +
        "run `npm install` in provisa-ui",
    );
  });

  it("names an installed package that lacks a file the config reads", () => {
    const pkg = "provisa-test-old-package-0b7d";
    const dir = path.join(fake, "node_modules", pkg);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, "package.json"), "{}");
    fs.writeFileSync(path.join(dir, "present.js"), "");
    expect(installedPackageDir(fake, pkg, ["present.js"])).toBe(dir);
    expect(() => installedPackageDir(fake, pkg, ["present.js", "absent.js"])).toThrow(
      `npm package "${pkg}" at ${dir} has no absent.js: run \`npm install\` in provisa-ui`,
    );
  });
});
