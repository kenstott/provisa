// Copyright (c) 2026 Kenneth Stott
// Canary: b824a633-33a4-4158-a79d-f2774241b6f1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: one ridge — a strip whose height follows a distribution, with strata-textured walls —
// as the terrain and the drift strata both draw it. A ridge runs along local z.

import * as THREE from "three";
import type { SceneTheme } from "./baseScene";

export interface RidgePoint {
  z: number;
  h: number;
}

export interface RidgeMaterials {
  top: THREE.MeshStandardMaterial;
  wall: THREE.MeshStandardMaterial;
}

const STRATA = 0.55;

function mulberry32(seed: number) {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function strataTexture(theme: SceneTheme): THREE.CanvasTexture {
  const c = document.createElement("canvas");
  c.width = 16;
  c.height = 128;
  const g = c.getContext("2d") as CanvasRenderingContext2D;
  g.fillStyle = theme.wallA;
  g.fillRect(0, 0, 16, 128);
  const r = mulberry32(5);
  for (let y = 0; y < 128; y += 32) {
    g.fillStyle = theme.wallB;
    g.fillRect(0, y + 6, 16, 12 + Math.floor(r() * 6));
    g.fillStyle = theme.wallLine;
    g.fillRect(0, y + 5, 16, 1);
  }
  for (let i = 0; i < 260; i++) {
    g.fillStyle = r() < 0.5 ? "rgba(0,0,0,0.10)" : "rgba(255,255,255,0.08)";
    g.fillRect(Math.floor(r() * 16), Math.floor(r() * 128), 1, 1);
  }
  const t = new THREE.CanvasTexture(c);
  t.wrapS = t.wrapT = THREE.RepeatWrapping;
  return t;
}

export function ridgeMaterials(theme: SceneTheme): RidgeMaterials {
  return {
    top: new THREE.MeshStandardMaterial({
      vertexColors: true,
      roughness: 0.86,
      metalness: 0,
      side: THREE.DoubleSide,
    }),
    wall: new THREE.MeshStandardMaterial({
      map: strataTexture(theme),
      roughness: 0.95,
      metalness: 0,
      side: THREE.DoubleSide,
    }),
  };
}

export function disposeRidgeMaterials(m: RidgeMaterials) {
  m.wall.map?.dispose();
  m.wall.dispose();
  m.top.dispose();
}

export function heightAtZ(pts: RidgePoint[], z: number): number {
  for (let k = 1; k < pts.length; k++) {
    const a = pts[k - 1];
    const b = pts[k];
    if ((z <= a.z && z >= b.z) || (z >= a.z && z <= b.z))
      return a.z === b.z ? a.h : a.h + ((b.h - a.h) * (a.z - z)) / (a.z - b.z);
  }
  return pts[pts.length - 1].h;
}

/** The ridge's top surface and walls, ``width`` wide, coloured by ``color(height)``. Each mesh is
 * tagged ``userData.part = "body"`` for picking. */
export function buildRidge(
  pts: RidgePoint[],
  width: number,
  smooth: boolean,
  mats: RidgeMaterials,
  color: (h: number) => THREE.Color,
): THREE.Mesh[] {
  const W = width;
  const pos: number[] = [];
  const col: number[] = [];
  const idx: number[] = [];
  pts.forEach((pt, k) => {
    pos.push(-W / 2, pt.h, pt.z, W / 2, pt.h, pt.z);
    const rc = color(pt.h);
    col.push(rc.r, rc.g, rc.b, rc.r, rc.g, rc.b);
    if (k > 0) {
      const a = (k - 1) * 2;
      idx.push(a, a + 1, a + 2, a + 1, a + 3, a + 2);
    }
  });
  let top = new THREE.BufferGeometry();
  top.setAttribute("position", new THREE.Float32BufferAttribute(pos, 3));
  top.setAttribute("color", new THREE.Float32BufferAttribute(col, 3));
  top.setIndex(idx);
  if (!smooth) top = top.toNonIndexed();
  top.computeVertexNormals();
  const topMesh = new THREE.Mesh(top, mats.top);

  const wp: number[] = [];
  const wu: number[] = [];
  const quad = (...vs: number[][]) => {
    for (const v of [vs[0], vs[1], vs[2], vs[0], vs[2], vs[3]]) {
      wp.push(v[0], v[1], v[2]);
      wu.push(v[3], v[4]);
    }
  };
  for (const x of [-W / 2, W / 2]) {
    for (let k = 1; k < pts.length; k++) {
      const a = pts[k - 1];
      const b = pts[k];
      quad(
        [x, 0, a.z, a.z / 10, 0],
        [x, 0, b.z, b.z / 10, 0],
        [x, b.h, b.z, b.z / 10, b.h / STRATA],
        [x, a.h, a.z, a.z / 10, a.h / STRATA],
      );
    }
  }
  for (const pt of [pts[0], pts[pts.length - 1]]) {
    quad(
      [-W / 2, 0, pt.z, 0, 0],
      [W / 2, 0, pt.z, W, 0],
      [W / 2, pt.h, pt.z, W, pt.h / STRATA],
      [-W / 2, pt.h, pt.z, 0, pt.h / STRATA],
    );
  }
  const wall = new THREE.BufferGeometry();
  wall.setAttribute("position", new THREE.Float32BufferAttribute(wp, 3));
  wall.setAttribute("uv", new THREE.Float32BufferAttribute(wu, 2));
  wall.computeVertexNormals();
  const wallMesh = new THREE.Mesh(wall, mats.wall);
  for (const m of [topMesh, wallMesh]) {
    m.castShadow = m.receiveShadow = true;
    m.userData.part = "body";
  }
  return [topMesh, wallMesh];
}
