// Copyright (c) 2026 Kenneth Stott
// Canary: 98d40312-7ecd-4c26-907d-63ce74b98d7b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: the view that shows the most of a 3D stage's content. The principal axes of its points
// give a first direction -- looking along the axis they spread least on -- which is refined by
// scoring evenly spread directions on the scene projected along each: nodes hidden behind others,
// edges crossing, the smallest gap between nodes and labels drawn over each other. Against that
// view the labels are sized so the smallest gap holds one, and only the pairs sizing cannot part
// are moved apart, in the view's plane.

import * as THREE from "three";

/** A point of the content: where it is, its radius, and its label's width (world units). */
export interface ViewNode {
  at: THREE.Vector3;
  radius: number;
  label: number;
}

/** Where the camera looks from (``dir``, from the target toward the camera), which way is up on
 * screen, and the axis a turn goes about: the normal of the plane the content spreads on most, so
 * a turn moves that plane within itself and it keeps facing the viewer. */
export interface View {
  dir: THREE.Vector3;
  up: THREE.Vector3;
  axis: THREE.Vector3;
}

/** The smallest a node and its label are drawn, as a share of full size, before pairs are moved. */
export const MIN_LABEL_SCALE = 0.6;

/** A label's height, as a share of its width's unit (one line of text above its node). */
const LABEL_HEIGHT = 0.45;
/** Directions scored besides the principal one, spread evenly over a half sphere. */
const CANDIDATES = 64;
/** At most this many nodes and edges are scored per view: a large graph is sampled evenly. */
const SAMPLE_NODES = 160;
const SAMPLE_EDGES = 240;
/** The side a view is seen from when either would do: in front, a little above. */
const FRONT = new THREE.Vector3(0.35, 0.4, 0.85).normalize();

/** The axes the points spread along, most to least, and the variance along each. */
export function principalAxes(points: THREE.Vector3[]): {
  axes: [THREE.Vector3, THREE.Vector3, THREE.Vector3];
  spread: [number, number, number];
} {
  const n = Math.max(1, points.length);
  const mean = new THREE.Vector3();
  for (const p of points) mean.add(p);
  mean.divideScalar(n);
  const c = [
    [0, 0, 0],
    [0, 0, 0],
    [0, 0, 0],
  ];
  for (const p of points) {
    const d = [p.x - mean.x, p.y - mean.y, p.z - mean.z];
    for (let i = 0; i < 3; i++) for (let j = 0; j < 3; j++) c[i][j] += (d[i] * d[j]) / n;
  }
  const { values, vectors } = jacobi(c);
  const order = [0, 1, 2].sort((a, b) => values[b] - values[a]);
  const axis = (k: number) =>
    new THREE.Vector3(vectors[0][k], vectors[1][k], vectors[2][k]).normalize();
  return {
    axes: [axis(order[0]), axis(order[1]), axis(order[2])],
    spread: [values[order[0]], values[order[1]], values[order[2]]],
  };
}

/** How much a view along ``dir`` hides: lower is better. Hidden nodes and overlapping labels,
 * crossing edges, less of the content's spread in view, and a smaller smallest gap all add. */
export function viewScore(
  dir: THREE.Vector3,
  nodes: ViewNode[],
  edges: [number, number][],
): number {
  if (nodes.length < 2) return 0;
  const d = dir.clone().normalize();
  const { u, v } = basis(d, null);
  const kept = sample(nodes.length, SAMPLE_NODES);
  const flat = kept.map((i) => {
    const p = nodes[i].at;
    return { x: p.dot(u), y: p.dot(v), r: nodes[i].radius, w: nodes[i].label };
  });
  let hidden = 0;
  let labels = 0;
  let nearest = Infinity;
  for (let i = 0; i < flat.length; i++)
    for (let j = i + 1; j < flat.length; j++) {
      const a = flat[i];
      const b = flat[j];
      const dist = Math.hypot(a.x - b.x, a.y - b.y);
      if (dist < (a.r + b.r) * 0.9) hidden++;
      nearest = Math.min(nearest, dist - a.r - b.r);
      // Each label is centred over its node.
      if (Math.abs(a.x - b.x) < (a.w + b.w) / 2 && Math.abs(a.y + a.r - (b.y + b.r)) < LABEL_HEIGHT)
        labels++;
    }
  const spreadAll = spreadOf(nodes.map((n) => n.at));
  const spreadSeen = spreadOf(nodes.map((n) => n.at.clone().projectOnPlane(d)));
  const seen = spreadAll > 0 ? spreadSeen / spreadAll : 1;
  const scale = Math.sqrt(Math.max(spreadSeen, 1e-9));
  const pairs = (flat.length * (flat.length - 1)) / 2;
  const crossings = countCrossings(edges, nodes, u, v);
  return (
    (4 * hidden) / pairs +
    (2 * labels) / pairs +
    crossings / Math.max(1, Math.min(edges.length, SAMPLE_EDGES)) -
    1.5 * seen -
    Math.max(0, nearest) / scale
  );
}

/** The best view of ``nodes`` and the ``edges`` between them (indices into ``nodes``). */
export function bestView(nodes: ViewNode[], edges: [number, number][]): View {
  if (nodes.length < 3) return viewOf(FRONT.clone(), null, new THREE.Vector3(0, 1, 0));
  const { axes } = principalAxes(nodes.map((n) => n.at));
  const first = facing(axes[2].clone());
  let best = first;
  let bestScore = viewScore(first, nodes, edges);
  for (const cand of hemisphere(first)) {
    const score = viewScore(cand, nodes, edges);
    // The principal direction stands unless a candidate is plainly better.
    if (score < bestScore - 1e-6) {
      best = cand;
      bestScore = score;
    }
  }
  return viewOf(best, axes[0], axes[2]);
}

/**
 * Nodes and labels sized against the view along ``dir``: the share of full size (down to
 * MIN_LABEL_SCALE) at which the smallest gap between two nodes holds the wider label of the two,
 * and the pairs still too close at the smallest size.
 */
export function labelFit(
  nodes: ViewNode[],
  dir: THREE.Vector3,
  up: THREE.Vector3,
): { scale: number; crowded: [number, number][] } {
  const flat = projected(nodes, dir, up);
  let scale = 1;
  const crowded: [number, number][] = [];
  for (let i = 0; i < flat.length; i++)
    for (let j = i + 1; j < flat.length; j++) {
      const gap = Math.hypot(flat[i].x - flat[j].x, flat[i].y - flat[j].y);
      const need = Math.max(nodes[i].label, nodes[j].label);
      if (need <= 0) continue;
      const fits = gap / need;
      if (fits >= MIN_LABEL_SCALE) scale = Math.min(scale, fits);
      else crowded.push([i, j]);
    }
  return { scale: crowded.length > 0 ? MIN_LABEL_SCALE : scale, crowded };
}

/**
 * The nodes' positions with each ``crowded`` pair moved apart in the view's plane until the gap
 * holds the wider label at ``scale``: the layout re-run only where sizing cannot part it. Every
 * other node stays where it is, and nothing moves along the line of sight.
 */
export function loosen(
  nodes: ViewNode[],
  crowded: [number, number][],
  dir: THREE.Vector3,
  up: THREE.Vector3,
  scale: number,
): THREE.Vector3[] {
  const out = nodes.map((n) => n.at.clone());
  const { u, v } = basis(dir.clone().normalize(), up);
  for (let pass = 0; pass < 8; pass++) {
    let moved = false;
    for (const [i, j] of crowded) {
      const need = Math.max(nodes[i].label, nodes[j].label) * scale;
      const delta = out[j].clone().sub(out[i]);
      const along = new THREE.Vector2(delta.dot(u), delta.dot(v));
      const gap = along.length();
      if (gap >= need - 1e-9) continue;
      // Two nodes on one spot part sideways on screen.
      const unit = gap > 1e-9 ? along.divideScalar(gap) : new THREE.Vector2(1, 0);
      const push = (need - gap) / 2;
      const step = u
        .clone()
        .multiplyScalar(unit.x * push)
        .add(v.clone().multiplyScalar(unit.y * push));
      out[i].sub(step);
      out[j].add(step);
      moved = true;
    }
    if (!moved) break;
  }
  return out;
}

// ---------- internals ----------

/** ``dir`` turned to the side a view is seen from (FRONT's half). */
function facing(dir: THREE.Vector3): THREE.Vector3 {
  return dir.dot(FRONT) < 0 ? dir.negate() : dir;
}

/** The view along ``dir`` with ``wide`` (the content's widest axis) across the screen, up square to
 * both, pointing up in the world where it can; turning about ``axis``. */
function viewOf(dir: THREE.Vector3, wide: THREE.Vector3 | null, axis: THREE.Vector3): View {
  const d = dir.clone().normalize();
  const { u } = basis(d, null, wide);
  const up = d.clone().cross(u).normalize();
  if (up.y < 0) up.negate();
  const a = axis.clone().normalize();
  if (a.dot(d) < 0) a.negate();
  return { dir: d, up, axis: a };
}

/** Two unit vectors square to ``dir`` and each other: ``u`` along ``right`` (or ``up`` crossed with
 * ``dir``) where given, else along whichever world axis lies flattest to the view. */
function basis(
  dir: THREE.Vector3,
  up: THREE.Vector3 | null,
  right: THREE.Vector3 | null = null,
): { u: THREE.Vector3; v: THREE.Vector3 } {
  let u: THREE.Vector3;
  if (right && right.clone().projectOnPlane(dir).lengthSq() > 1e-9)
    u = right.clone().projectOnPlane(dir).normalize();
  else if (up && up.clone().cross(dir).lengthSq() > 1e-9) u = up.clone().cross(dir).normalize();
  else {
    const world = [
      new THREE.Vector3(1, 0, 0),
      new THREE.Vector3(0, 1, 0),
      new THREE.Vector3(0, 0, 1),
    ];
    const flattest = world.sort((a, b) => Math.abs(a.dot(dir)) - Math.abs(b.dot(dir)))[0];
    u = flattest.projectOnPlane(dir).normalize();
  }
  const v = dir.clone().cross(u).normalize();
  return { u, v };
}

function projected(nodes: ViewNode[], dir: THREE.Vector3, up: THREE.Vector3) {
  const { u, v } = basis(dir.clone().normalize(), up);
  return nodes.map((n) => ({ x: n.at.dot(u), y: n.at.dot(v) }));
}

/** The total variance of ``points`` about their mean. */
function spreadOf(points: THREE.Vector3[]): number {
  if (points.length === 0) return 0;
  const mean = new THREE.Vector3();
  for (const p of points) mean.add(p);
  mean.divideScalar(points.length);
  let sum = 0;
  for (const p of points) sum += p.distanceToSquared(mean);
  return sum / points.length;
}

/** ``count`` indices out of ``n``, evenly spread; every index when there are no more than that. */
function sample(n: number, count: number): number[] {
  if (n <= count) return Array.from({ length: n }, (_, i) => i);
  return Array.from({ length: count }, (_, i) => Math.floor((i * n) / count));
}

/** Directions spread evenly over the half sphere ``toward`` faces (a golden spiral). */
function hemisphere(toward: THREE.Vector3): THREE.Vector3[] {
  const out: THREE.Vector3[] = [];
  for (let i = 0; i < CANDIDATES * 2; i++) {
    const y = 1 - (2 * (i + 0.5)) / (CANDIDATES * 2);
    const r = Math.sqrt(1 - y * y);
    const phi = i * Math.PI * (3 - Math.sqrt(5));
    const d = new THREE.Vector3(Math.cos(phi) * r, y, Math.sin(phi) * r);
    if (d.dot(toward) > 0) out.push(d);
  }
  return out;
}

/** How many pairs of edges cross on screen, edges sharing a node apart. */
function countCrossings(
  edges: [number, number][],
  nodes: ViewNode[],
  u: THREE.Vector3,
  v: THREE.Vector3,
): number {
  const kept = sample(edges.length, SAMPLE_EDGES).map((i) => edges[i]);
  const at = (i: number) => [nodes[i].at.dot(u), nodes[i].at.dot(v)] as const;
  const seg = kept.map(([a, b]) => ({ a, b, p: at(a), q: at(b) }));
  let count = 0;
  for (let i = 0; i < seg.length; i++)
    for (let j = i + 1; j < seg.length; j++) {
      const s = seg[i];
      const t = seg[j];
      if (s.a === t.a || s.a === t.b || s.b === t.a || s.b === t.b) continue;
      if (crosses(s.p, s.q, t.p, t.q)) count++;
    }
  return count;
}

function crosses(
  p: readonly [number, number],
  q: readonly [number, number],
  r: readonly [number, number],
  s: readonly [number, number],
): boolean {
  const side = (
    a: readonly [number, number],
    b: readonly [number, number],
    c: readonly [number, number],
  ) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]);
  const d1 = side(r, s, p);
  const d2 = side(r, s, q);
  const d3 = side(p, q, r);
  const d4 = side(p, q, s);
  return d1 * d2 < 0 && d3 * d4 < 0;
}

/** Eigenvalues and eigenvectors (columns) of a symmetric 3×3 matrix (cyclic Jacobi). */
function jacobi(m: number[][]): { values: number[]; vectors: number[][] } {
  const a = m.map((row) => [...row]);
  const v = [
    [1, 0, 0],
    [0, 1, 0],
    [0, 0, 1],
  ];
  for (let sweep = 0; sweep < 50; sweep++) {
    const off = a[0][1] ** 2 + a[0][2] ** 2 + a[1][2] ** 2;
    if (off < 1e-18) break;
    for (const [p, q] of [
      [0, 1],
      [0, 2],
      [1, 2],
    ]) {
      if (Math.abs(a[p][q]) < 1e-15) continue;
      const theta = (a[q][q] - a[p][p]) / (2 * a[p][q]);
      const t = Math.sign(theta || 1) / (Math.abs(theta) + Math.sqrt(theta * theta + 1));
      const c = 1 / Math.sqrt(t * t + 1);
      const s = t * c;
      for (let k = 0; k < 3; k++) {
        const akp = a[k][p];
        const akq = a[k][q];
        a[k][p] = c * akp - s * akq;
        a[k][q] = s * akp + c * akq;
      }
      for (let k = 0; k < 3; k++) {
        const apk = a[p][k];
        const aqk = a[q][k];
        a[p][k] = c * apk - s * aqk;
        a[q][k] = s * apk + c * aqk;
      }
      for (let k = 0; k < 3; k++) {
        const vkp = v[k][p];
        const vkq = v[k][q];
        v[k][p] = c * vkp - s * vkq;
        v[k][q] = s * vkp + c * vkq;
      }
    }
  }
  return { values: [a[0][0], a[1][1], a[2][2]], vectors: v };
}
