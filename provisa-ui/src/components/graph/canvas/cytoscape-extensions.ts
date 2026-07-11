// Copyright (c) 2026 Kenneth Stott
// Canary: 3a7f2c91-e084-4b5d-9f1e-82d6c0b4a53e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import cytoscape from "cytoscape";
import fcoseRaw from "cytoscape-fcose";
import layoutUtilitiesRaw from "cytoscape-layout-utilities";
import cytoscapeSvgRaw from "cytoscape-svg";

// CJS bundles — .default may or may not be present depending on bundler
type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
const _interopExt = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);
const fcose = _interopExt(fcoseRaw as CyExtModule);
const layoutUtilities = _interopExt(layoutUtilitiesRaw as CyExtModule);
const cytoscapeSvg = _interopExt(cytoscapeSvgRaw as CyExtModule);
try {
  cytoscape.use(fcose);
} catch {
  /* already registered */
}
try {
  cytoscape.use(layoutUtilities);
} catch {
  /* already registered */
}
try {
  cytoscape.use(cytoscapeSvg);
} catch {
  /* already registered */
}
