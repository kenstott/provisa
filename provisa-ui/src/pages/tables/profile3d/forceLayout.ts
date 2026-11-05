// Copyright (c) 2026 Kenneth Stott
// Canary: 0f3b8d2e-5a41-4c7e-9e1d-2b6a7c9f4e10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124, REQ-2128: the force layout the relationships map and the data product graph share.

const LAYOUT_RADIUS = 6;
const ITERATIONS = 360;
/** The centre's pull on a node of a graph the springs join in one part. */
const GRAVITY = 0.05;

/** A spring between two nodes; ``strength`` (0 to 1) pulls them closer. */
export interface Spring {
  a: string;
  b: string;
  strength: number;
}

/** Fruchterman–Reingold in three dimensions from a golden-spiral start; deterministic, so the same
 * graph always lands the same way. Positions are scaled to fit ``radius``. ``attraction`` (1 the
 * usual spacing) draws the nodes together above 1 and apart below: it shortens the springs' rest
 * length and the charge's reach, and the whole lands within ``radius / attraction``. ``flat`` lays
 * it out in the z = 0 plane (a 2D view, REQ-2174), from a golden-angle disc. */
export function forceLayout(
  ids: string[],
  springs: Spring[],
  radius = LAYOUT_RADIUS,
  attraction = 1,
  flat = false,
): Map<string, [number, number, number]> {
  const n = ids.length;
  const index = new Map(ids.map((id, i) => [id, i]));
  const p = ids.map((_, i) => {
    if (flat) {
      const r = Math.sqrt((i + 0.5) / n) * radius;
      const phi = i * Math.PI * (3 - Math.sqrt(5));
      return [Math.cos(phi) * r, Math.sin(phi) * r, 0];
    }
    const y = n === 1 ? 0 : 1 - (2 * i) / (n - 1);
    const r = Math.sqrt(1 - y * y);
    const phi = i * Math.PI * (3 - Math.sqrt(5));
    return [Math.cos(phi) * r * radius, y * radius, Math.sin(phi) * r * radius];
  });
  const k = Math.cbrt((4 * radius ** 3) / Math.max(1, n)) / attraction;
  const links = springs.map(
    (e) => [index.get(e.a) as number, index.get(e.b) as number, e.strength] as const,
  );
  // Every other node pushes a part the springs do not join to the rest outward, whatever its size,
  // and only the centre's pull holds it: a small one (a data product whose tables reach nothing
  // drawn) would settle far off, and the scale that fits it would crush the rest to the middle. So
  // each part is held to the centre as the whole graph would be, in proportion to its share.
  const part = parts(n, links);
  const sizes = new Map<number, number>();
  for (const r of part) sizes.set(r, (sizes.get(r) ?? 0) + 1);
  const hold = part.map((r) => (GRAVITY * n) / (sizes.get(r) as number));
  let temp = radius / 2;
  for (let it = 0; it < ITERATIONS; it++) {
    const d = p.map(() => [0, 0, 0]);
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const dx = p[i][0] - p[j][0];
        const dy = p[i][1] - p[j][1];
        const dz = p[i][2] - p[j][2];
        const dist = Math.max(0.01, Math.hypot(dx, dy, dz));
        const f = (k * k) / dist / dist;
        d[i][0] += dx * f;
        d[i][1] += dy * f;
        d[i][2] += dz * f;
        d[j][0] -= dx * f;
        d[j][1] -= dy * f;
        d[j][2] -= dz * f;
      }
    }
    for (const [i, j, w] of links) {
      const dx = p[i][0] - p[j][0];
      const dy = p[i][1] - p[j][1];
      const dz = p[i][2] - p[j][2];
      const dist = Math.max(0.01, Math.hypot(dx, dy, dz));
      const f = ((dist / k) * (0.3 + w)) / 1.3;
      d[i][0] -= dx * f;
      d[i][1] -= dy * f;
      d[i][2] -= dz * f;
      d[j][0] += dx * f;
      d[j][1] += dy * f;
      d[j][2] += dz * f;
    }
    for (let i = 0; i < n; i++) {
      // A pull toward the centre keeps unconnected columns from drifting off.
      d[i][0] -= p[i][0] * hold[i];
      d[i][1] -= p[i][1] * hold[i];
      d[i][2] -= p[i][2] * hold[i];
      const len = Math.max(0.0001, Math.hypot(d[i][0], d[i][1], d[i][2]));
      const step = Math.min(len, temp);
      p[i][0] += (d[i][0] / len) * step;
      p[i][1] += (d[i][1] / len) * step;
      p[i][2] += flat ? 0 : (d[i][2] / len) * step;
    }
    temp *= 0.985;
  }
  let maxR = 0;
  for (const q of p) maxR = Math.max(maxR, Math.hypot(q[0], q[1], q[2]));
  const scale = maxR > 0 ? radius / attraction / maxR : 1;
  return new Map(ids.map((id, i) => [id, [p[i][0] * scale, p[i][1] * scale, p[i][2] * scale]]));
}

/** Each node's connected part, named by one of its nodes. */
function parts(n: number, links: readonly (readonly [number, number, number])[]): number[] {
  const up = Array.from({ length: n }, (_, i) => i);
  const root = (i: number): number => (up[i] === i ? i : (up[i] = root(up[i])));
  for (const [i, j] of links) up[root(i)] = root(j);
  return up.map((_, i) => root(i));
}
