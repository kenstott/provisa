// Copyright (c) 2026 Kenneth Stott
// Canary: c3098d93-40c5-4e3d-bd49-c39fa2e5ed09
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: the view a 3D stage opens on and eases back to -- the principal axes of its points give
// the first direction, refined by scoring the projected scene for hidden nodes, crossing edges,
// the smallest gap and overlapping labels -- and the label sizing set against that view.

import * as THREE from "three";
import { describe, expect, it } from "vitest";
import {
  bestView,
  labelFit,
  loosen,
  principalAxes,
  viewScore,
  type ViewNode,
} from "../bestView";

const node = (x: number, y: number, z: number, label = 1): ViewNode => ({
  at: new THREE.Vector3(x, y, z),
  radius: 0.3,
  label,
});

/** A grid of nodes in the plane through the origin spanned by ``u`` and ``v``, a little depth. */
function plane(u: THREE.Vector3, v: THREE.Vector3, n = 6, step = 2): ViewNode[] {
  const out: ViewNode[] = [];
  const normal = u.clone().cross(v).normalize();
  for (let i = 0; i < n; i++)
    for (let j = 0; j < n; j++) {
      const p = u
        .clone()
        .multiplyScalar((i - (n - 1) / 2) * step)
        .add(v.clone().multiplyScalar((j - (n - 1) / 2) * step))
        // A little depth, never zero, so the plane is the mostly-flat case.
        .add(normal.clone().multiplyScalar(((i * 7 + j * 3) % 5) * 0.06 - 0.12));
      out.push({ at: p, radius: 0.3, label: 0.8 });
    }
  return out;
}

/** Edges between grid neighbours (row-major). */
function gridEdges(n = 6): [number, number][] {
  const e: [number, number][] = [];
  for (let i = 0; i < n; i++)
    for (let j = 0; j < n; j++) {
      if (j + 1 < n) e.push([i * n + j, i * n + j + 1]);
      if (i + 1 < n) e.push([i * n + j, (i + 1) * n + j]);
    }
  return e;
}

describe("principal axes", () => {
  it("orders the axes from most to least spread", () => {
    const pts = [];
    for (let i = -5; i <= 5; i++) for (let j = -2; j <= 2; j++) pts.push(new THREE.Vector3(i * 3, j, 0.1 * ((i + j) % 2)));
    const { axes, spread } = principalAxes(pts);
    expect(spread[0]).toBeGreaterThan(spread[1]);
    expect(spread[1]).toBeGreaterThan(spread[2]);
    expect(Math.abs(axes[0].x)).toBeCloseTo(1, 3);
    expect(Math.abs(axes[1].y)).toBeCloseTo(1, 3);
    expect(Math.abs(axes[2].z)).toBeCloseTo(1, 3);
  });
});

describe("the best view", () => {
  it.each([
    ["the floor", new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 0, 1)],
    ["a wall", new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 1, 0)],
    ["a tilted plane", new THREE.Vector3(1, 1, 0).normalize(), new THREE.Vector3(0, 0.3, 1).normalize()],
  ])("faces a graph lying mostly in %s", (_name, u, v) => {
    const nodes = plane(u, v);
    const view = bestView(nodes, gridEdges());
    const normal = u.clone().cross(v).normalize();
    // Within 15 degrees of the plane's normal, either side.
    expect(Math.abs(view.dir.dot(normal))).toBeGreaterThan(Math.cos((15 * Math.PI) / 180));
  });

  it("turns about the axis that keeps the plane facing the viewer, and stands upright", () => {
    const u = new THREE.Vector3(1, 0, 0);
    const v = new THREE.Vector3(0, 0, 1);
    const view = bestView(plane(u, v), gridEdges());
    // The turn axis is the plane's normal: a turn about it moves the plane within itself.
    expect(Math.abs(view.axis.dot(new THREE.Vector3(0, 1, 0)))).toBeGreaterThan(0.99);
    // Up is square to the line of sight, so the camera can look along it.
    expect(Math.abs(view.up.dot(view.dir))).toBeLessThan(1e-6);
    expect(view.up.length()).toBeCloseTo(1, 6);
  });

  it("scores a view that hides nodes behind each other as worse than one that shows them", () => {
    const nodes = plane(new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 1, 0));
    const edges = gridEdges();
    const faceOn = viewScore(new THREE.Vector3(0, 0, 1), nodes, edges);
    const edgeOn = viewScore(new THREE.Vector3(1, 0, 0), nodes, edges);
    expect(faceOn).toBeLessThan(edgeOn);
  });

  it("refines past the principal direction when that view crosses or hides more", () => {
    // Two parallel layers stacked along z: the least-spread axis is z, and looking along it puts
    // every node of one layer over its twin in the other. A view off that axis shows both.
    const nodes: ViewNode[] = [];
    for (let i = 0; i < 5; i++)
      for (let j = 0; j < 5; j++)
        for (const z of [-1.2, 1.2]) nodes.push(node((i - 2) * 3, (j - 2) * 3, z, 0.6));
    const view = bestView(nodes, []);
    const along = viewScore(new THREE.Vector3(0, 0, 1), nodes, []);
    expect(viewScore(view.dir, nodes, [])).toBeLessThan(along);
    expect(Math.abs(view.dir.z)).toBeLessThan(0.999);
  });

  it("is the same view for the same graph", () => {
    const nodes = plane(new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 0.4, 1).normalize());
    const a = bestView(nodes, gridEdges());
    const b = bestView(nodes, gridEdges());
    expect(a.dir.equals(b.dir)).toBe(true);
    expect(a.up.equals(b.up)).toBe(true);
  });

  it("has a view for one node or none", () => {
    for (const nodes of [[], [node(0, 0, 0)]]) {
      const view = bestView(nodes, []);
      expect(view.dir.length()).toBeCloseTo(1, 6);
      expect(Math.abs(view.up.dot(view.dir))).toBeLessThan(1e-6);
    }
  });
});

describe("label sizing against the view", () => {
  const dir = new THREE.Vector3(0, 0, 1);
  const up = new THREE.Vector3(0, 1, 0);

  it("keeps full size where the smallest gap already fits a label", () => {
    const nodes = [node(0, 0, 0, 1), node(3, 0, 0, 1), node(0, 3, 0, 1)];
    expect(labelFit(nodes, dir, up).scale).toBe(1);
  });

  it("shrinks nodes and labels until the smallest gap is a label's width", () => {
    const nodes = [node(0, 0, 0, 2), node(1.5, 0, 0, 2), node(0, 4, 0, 2)];
    const { scale, crowded } = labelFit(nodes, dir, up);
    expect(scale).toBeCloseTo(0.75, 6);
    expect(crowded).toEqual([]);
  });

  it("names the pairs sizing alone cannot part, and loosens only those", () => {
    const nodes = [node(0, 0, 0, 2), node(0.2, 0, 0, 2), node(6, 0, 0, 2), node(6, 6, 0, 2)];
    const { scale, crowded } = labelFit(nodes, dir, up);
    expect(scale).toBeGreaterThan(0);
    expect(crowded).toEqual([[0, 1]]);
    const moved = loosen(nodes, crowded, dir, up, scale);
    const gap = moved[0].distanceTo(moved[1]);
    expect(gap).toBeGreaterThanOrEqual(2 * scale - 1e-6);
    // The rest of the layout stays where it was.
    expect(moved[2].equals(nodes[2].at)).toBe(true);
    expect(moved[3].equals(nodes[3].at)).toBe(true);
    // Parted in the view's plane: neither moves along the line of sight.
    expect(moved[0].z).toBeCloseTo(0, 9);
    expect(moved[1].z).toBeCloseTo(0, 9);
  });
});
