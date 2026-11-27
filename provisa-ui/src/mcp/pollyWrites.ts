// Copyright (c) 2026 Kenneth Stott
// Canary: 0d4b7e21-6c3a-4f58-9e1d-2a7c5b8f9e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * A change Polly makes reaches the screen the way a person's edit does. The glossary's pages read
 * over REST (`/admin/glossary/*`), never Apollo, so they read again on "provisa:glossary-changed":
 * the term list, the open term's panel (GlossaryTab) and the ontology graph (useOntology).
 */
export const GLOSSARY_CHANGED = "provisa:glossary-changed";

/** REQ-1845: every server tool (REQ-1835) that changes the glossary. */
export const GLOSSARY_TOOL_NAMES: ReadonlySet<string> = new Set([
  "create_glossary_term",
  "update_glossary_term",
  "delete_glossary_term",
  "add_glossary_term_edge",
  "remove_glossary_term_edge",
  // REQ-2130: a term grounded in a table, or let go of it.
  "ground_glossary_term_to_table",
  "unground_glossary_term_from_table",
  // REQ-2144: a term's status, which the Ontology page's Status filter reads.
  "approve_glossary_term",
  "deprecate_glossary_term",
  "retire_glossary_term",
  "restore_glossary_term",
  // REQ-2145: a term's tags.
  "tag_glossary_term",
  "untag_glossary_term",
]);

/** The glossary's own admin REST routes, which a page's edit calls and admin_request reaches. */
const GLOSSARY_ROUTE = "/admin/glossary";

/** Whether an admin_request to `path` is to one of the glossary's routes. */
export function isGlossaryPath(path: string): boolean {
  const route = path.split(/[?#]/, 1)[0];
  return route === GLOSSARY_ROUTE || route.startsWith(`${GLOSSARY_ROUTE}/`);
}

export function announceGlossaryChange(): void {
  window.dispatchEvent(new CustomEvent(GLOSSARY_CHANGED));
}

/** After a server tool Polly ran, whatever its outcome: the glossary's pages read again. */
export function serverToolRan(name: string): void {
  if (GLOSSARY_TOOL_NAMES.has(name)) announceGlossaryChange();
}

/** After a change Polly sent through admin_request, whatever its outcome: every active Apollo
 * query is read again (`refetchActive`), and the glossary's pages when it was to one of its
 * routes. */
export function adminRequestSent(path: string, refetchActive: () => void): void {
  refetchActive();
  if (isGlossaryPath(path)) announceGlossaryChange();
}
