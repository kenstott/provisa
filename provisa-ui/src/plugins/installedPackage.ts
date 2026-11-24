// Copyright (c) 2026 Kenneth Stott
// Canary: baa1f218-0cb3-4b3e-8c58-cb65b1bedf94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// vite.config.ts and its plugins read some npm packages' files straight from node_modules. A
// node_modules installed before such a package was added to package.json used to surface only as a
// raw ENOENT on each request for the file; this locates the package once, when the dev server or
// the build starts, and refuses to start with the package's name and the command that installs it.

import fs from "fs";
import { createRequire } from "module";
import path from "path";

/** The error a missing or incomplete package raises: its name and how to install it. */
export class MissingPackageError extends Error {
  constructor(pkg: string, detail: string) {
    super(
      `npm package "${pkg}" ${detail}: run \`npm install\` in provisa-ui, then restart the ` +
        "dev server or rerun the build.",
    );
    this.name = "MissingPackageError";
  }
}

/** The directory ``pkg`` is installed in, as Node resolves it from ``root`` (provisa-ui).
 *
 * Found through the resolver's search paths rather than ``require.resolve(pkg + "/package.json")``:
 * a package whose ``exports`` map omits ./package.json (@neo4j-cypher/codemirror) refuses that.
 * Each of ``files`` (paths inside the package) must exist too, since an older install of the
 * package can lack a file a newer version ships. Throws {@link MissingPackageError} otherwise. */
export function installedPackageDir(root: string, pkg: string, files: readonly string[]): string {
  const require = createRequire(path.join(root, "package.json"));
  const searchPaths = require.resolve.paths(pkg);
  if (searchPaths === null) throw new Error(`"${pkg}" is a Node core module, not an npm package`);
  const dir = searchPaths
    .map((nodeModules) => path.join(nodeModules, pkg))
    .find((candidate) => fs.existsSync(path.join(candidate, "package.json")));
  if (dir === undefined) {
    throw new MissingPackageError(pkg, `is not installed under ${path.join(root, "node_modules")}`);
  }
  const missing = files.filter((file) => !fs.existsSync(path.join(dir, file)));
  if (missing.length > 0) {
    throw new MissingPackageError(pkg, `at ${dir} has no ${missing.join(", ")}`);
  }
  return dir;
}
