// Copyright (c) 2026 Kenneth Stott
// Canary: e3f4a5b6-c7d8-4e9f-a0b1-c2d3e4f5a6b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from 'vitest';
import type { GEdge } from '../../components/graph/graph-model';

function makeEdge(
  identity: string,
  startLabel: string,
  startId: number,
  endLabel: string,
  endId: number,
  type: string
): GEdge {
  return {
    identity,
    start: startId,
    end: endId,
    type,
    properties: {},
    startNode: { id: startId, label: startLabel, tableLabel: startLabel, properties: {} },
    endNode: { id: endId, label: endLabel, tableLabel: endLabel, properties: {} },
  };
}

function computeOverlayEdges(
  frameEdges: Map<string, GEdge>,
  overlayData: Map<string, { edges: Map<string, GEdge> }>
): Map<string, GEdge> {
  if (overlayData.size === 0) return new Map();
  const frameFingerprints = new Set<string>();
  frameEdges.forEach((e) => {
    frameFingerprints.add(
      `${e.startNode.label}:${e.startNode.id}→${e.endNode.label}:${e.endNode.id}:${e.type}`
    );
    frameFingerprints.add(
      `${e.endNode.label}:${e.endNode.id}→${e.startNode.label}:${e.startNode.id}:${e.type}`
    );
  });
  const m = new Map<string, GEdge>();
  for (const d of overlayData.values()) {
    d.edges.forEach((e, k) => {
      if (frameEdges.has(k)) return;
      const fp = `${e.startNode.label}:${e.startNode.id}→${e.endNode.label}:${e.endNode.id}:${e.type}`;
      if (frameFingerprints.has(fp)) return;
      m.set(k, e);
    });
  }
  return m;
}

describe('overlayEdges deduplication', () => {
  it('excludes imputed edge when frame already has same identity', () => {
    const edge = makeEdge('WORKS_AT:1-10', 'Person', 1, 'Company', 10, 'WORKS_AT');
    const frameEdges = new Map([['WORKS_AT:1-10', edge]]);
    const imputedEdge = makeEdge('WORKS_AT:1-10', 'Person', 1, 'Company', 10, 'WORKS_AT');
    const overlayData = new Map([
      ['__remaining_rels', { edges: new Map([['WORKS_AT:1-10', imputedEdge]]) }],
    ]);

    const result = computeOverlayEdges(frameEdges, overlayData);
    expect(result.size).toBe(0);
  });

  it('excludes imputed edge when frame has same edge traversed backward (different identity key)', () => {
    // Frame edge: backward traversal query produced canonical identity WORKS_AT:1-10
    // (after backend fix, identity is canonical regardless of traversal direction)
    const frameEdge = makeEdge('WORKS_AT:1-10', 'Person', 1, 'Company', 10, 'WORKS_AT');
    const frameEdges = new Map([['WORKS_AT:1-10', frameEdge]]);

    // Imputed edge always uses forward canonical direction: same identity
    const imputedEdge = makeEdge('WORKS_AT:1-10', 'Person', 1, 'Company', 10, 'WORKS_AT');
    const overlayData = new Map([
      ['__remaining_rels', { edges: new Map([['WORKS_AT:1-10', imputedEdge]]) }],
    ]);

    const result = computeOverlayEdges(frameEdges, overlayData);
    expect(result.size).toBe(0);
  });

  it('excludes imputed edge when frame edge has flipped start/end but same type (fingerprint match)', () => {
    // If for any reason frame has the edge stored with end/start swapped but same type
    const frameEdge = makeEdge('WORKS_AT:1-10', 'Company', 10, 'Person', 1, 'WORKS_AT');
    const frameEdges = new Map([['WORKS_AT:1-10', frameEdge]]);

    // Imputed edge is canonical forward
    const imputedEdge = makeEdge('WORKS_AT:1-10', 'Person', 1, 'Company', 10, 'WORKS_AT');
    const overlayData = new Map([
      ['__remaining_rels', { edges: new Map([['WORKS_AT:1-10', imputedEdge]]) }],
    ]);

    const result = computeOverlayEdges(frameEdges, overlayData);
    expect(result.size).toBe(0);
  });

  it('includes imputed edge when no frame edge covers it', () => {
    const frameEdge = makeEdge('OTHER:2-20', 'Person', 2, 'Company', 20, 'OTHER');
    const frameEdges = new Map([['OTHER:2-20', frameEdge]]);

    const imputedEdge = makeEdge('WORKS_AT:1-10', 'Person', 1, 'Company', 10, 'WORKS_AT');
    const overlayData = new Map([
      ['__remaining_rels', { edges: new Map([['WORKS_AT:1-10', imputedEdge]]) }],
    ]);

    const result = computeOverlayEdges(frameEdges, overlayData);
    expect(result.size).toBe(1);
    expect(result.has('WORKS_AT:1-10')).toBe(true);
  });

  it('returns empty map when overlayData is empty', () => {
    const frameEdge = makeEdge('WORKS_AT:1-10', 'Person', 1, 'Company', 10, 'WORKS_AT');
    const frameEdges = new Map([['WORKS_AT:1-10', frameEdge]]);

    const result = computeOverlayEdges(frameEdges, new Map());
    expect(result.size).toBe(0);
  });
});
