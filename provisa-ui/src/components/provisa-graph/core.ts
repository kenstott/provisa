// Copyright (c) 2026 Kenneth Stott
// Canary: 9e1d4b7a-2c63-4f18-8a5e-b0c7d3f9e246
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the graph library and its extensions, registered once. ProvisaGraph is the only code
// that imports them (graphGuard.test.ts).

import cytoscape from "cytoscape";
import elkRaw from "cytoscape-elk";
import fcoseRaw from "cytoscape-fcose";
import svgRaw from "cytoscape-svg";
import provisaElk, { PROVISA_ELK } from "./elkLayout";

type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
// CJS bundles: .default may or may not be present, depending on the bundler.
const ext = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);

/** Registers ``m`` unless an extension of that name is already: cytoscape.use throws on a second
 * registration (another surface registered the same one first). */
function register(type: "layout" | "core", name: string, m: CyExtModule): void {
  const registered = (cytoscape as unknown as (type: string, name: string) => unknown)(type, name);
  if (!registered) cytoscape.use(ext(m));
}
register("layout", "elk", elkRaw as CyExtModule);
register("layout", "fcose", fcoseRaw as CyExtModule);
register("layout", PROVISA_ELK, provisaElk as unknown as CyExtModule);
register("core", "svg", svgRaw as CyExtModule);

export { cytoscape };
export type { EventObject, ElementDefinition } from "cytoscape";
export type NodeSingular = cytoscape.NodeSingular;
export type EdgeSingular = cytoscape.EdgeSingular;
/** A core, with the resize cytoscape's typings leave out. */
export type Core = ReturnType<typeof cytoscape> & { resize(): void };
