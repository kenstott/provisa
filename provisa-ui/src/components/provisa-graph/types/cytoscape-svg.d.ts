// Copyright (c) 2026 Kenneth Stott
// Canary: 45bfe4b3-545b-45d3-b61e-223c7d8fb2d5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

declare module "cytoscape-svg" {
  import cytoscape from "cytoscape";
  const svg: cytoscape.Ext;
  export default svg;
}

declare module "cytoscape" {
  interface Core {
    svg(options?: { full?: boolean; scale?: number; quality?: number; bg?: string }): string;
  }
}
