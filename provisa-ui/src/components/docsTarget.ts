// Copyright (c) 2026 Kenneth Stott
// Canary: 5b0e7a3c-2f41-4d8e-9c6a-81d2e4f7a903
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Maintainer 2026-11-28: every (?) help and every page command ends with a "Read the
// documentation" link. Each names its docs page as a DocsTarget; docsTargets.test.ts fails a target
// with no page under docs/, a page missing from mkdocs.yml's nav, or a section the page lacks.

/** A page of the docs site as the in-app Docs route's ?page= takes it: a path under docs/ ending in
 *  /, with an optional #section. Written as a literal where it is used, so the docs test reads it. */
export type DocsTarget = `${string}/` | `${string}/#${string}`;

/** The in-app Docs route for a docs target. */
export function docsHref(target: DocsTarget): string {
  const [page, section] = target.split("#");
  return `/docs?page=${page}${section === undefined ? "" : `#${section}`}`;
}
