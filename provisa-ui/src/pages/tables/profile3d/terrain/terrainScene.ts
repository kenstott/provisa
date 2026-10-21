// Copyright (c) 2026 Kenneth Stott
// Canary: 7bd868a5-097e-4975-adfb-97b30b1bab75
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the profile terrain's scene — a ridge per column, a null block in front of each,
// quantile marks across them.

import * as THREE from "three";
import { BaseScene, type SceneCallbacks, type SceneTheme } from "../baseScene";
import {
  buildRidge,
  disposeRidgeMaterials,
  heightAtZ,
  ridgeMaterials,
  type RidgeMaterials,
  type RidgePoint,
} from "../ridge";
import {
  heightShare,
  maxShareOf,
  tOfValue,
  type HeightMode,
  type TerrainColumn,
} from "./terrainModel";

export interface TerrainOptions {
  mode: HeightMode;
  log: boolean;
  quantiles: boolean;
}

/** A hovered bin of a column; ``"null"`` is its null block. */
export interface TerrainHover {
  column: string;
  bin: number | "null";
}

const H = 4;
const D = 10;
const W = 0.74;
const S = 1.18;
const EPS = 0.04;
const NULL_Z = D / 2 + 0.95;
const LABEL_Z = D / 2 + 1.75;

const zOf = (t: number) => D / 2 - t * D;

interface ColumnGroup {
  group: THREE.Group;
  column: TerrainColumn;
  label: HTMLDivElement;
  quantiles: THREE.Group;
  targetX: number;
}

export class TerrainScene extends BaseScene<TerrainHover> {
  private world: THREE.Group | null = null;
  private groups: ColumnGroup[] = [];
  private selRect: THREE.Mesh | null = null;
  private hoverMesh: THREE.Mesh | null = null;
  private ridge: RidgeMaterials | null = null;
  private mats: Record<string, THREE.Material> = {};
  private columns: TerrainColumn[] = [];
  private order: string[] = [];
  private options: TerrainOptions;
  private maxShare = 1;
  private selected: string | null = null;
  private hover: TerrainHover | null = null;
  private readonly axisText: { low: string; high: string; nulls: string };

  constructor(
    canvas: HTMLCanvasElement,
    labelsEl: HTMLElement,
    callbacks: SceneCallbacks<TerrainHover>,
    theme: SceneTheme,
    options: TerrainOptions,
    axisText: { low: string; high: string; nulls: string },
  ) {
    super(canvas, labelsEl, callbacks, theme);
    this.options = options;
    this.axisText = axisText;
    this.controls.maxPolarAngle = Math.PI / 2 - 0.04;
    this.buildMaterials();
  }

  // ---------- public ----------

  setData(columns: TerrainColumn[], order: string[], resetCamera: boolean) {
    this.columns = columns;
    this.order = order;
    this.rebuild();
    if (resetCamera) this.resetView(true);
  }

  setOptions(options: TerrainOptions) {
    const rebuild = options.mode !== this.options.mode || options.log !== this.options.log;
    this.options = options;
    if (rebuild) this.rebuild();
    else for (const g of this.groups) g.quantiles.visible = options.quantiles;
  }

  setOrder(order: string[]) {
    this.order = order;
    for (const g of this.groups) g.targetX = this.xOf(g.column.name);
  }

  setSelected(name: string | null) {
    this.selected = name;
    for (const g of this.groups) g.label.classList.toggle("is-selected", g.column.name === name);
    if (this.selRect) this.selRect.visible = name != null;
  }

  setHover(h: TerrainHover | null) {
    if (h?.column === this.hover?.column && h?.bin === this.hover?.bin) return;
    this.hover = h;
    if (this.hoverMesh) {
      this.hoverMesh.removeFromParent();
      this.hoverMesh.geometry.dispose();
      this.hoverMesh = null;
    }
    for (const g of this.groups)
      g.label.classList.toggle("is-hovered", g.column.name === h?.column);
    if (!h) return;
    const g = this.groups.find((x) => x.column.name === h.column);
    if (!g) return;
    const c = g.column;
    let mesh: THREE.Mesh;
    if (h.bin === "null") {
      // A null block is drawn only for a column with a null share (addNullBlock).
      const hN = Math.max(0.03, (c.nullShare as number) * H);
      mesh = new THREE.Mesh(new THREE.BoxGeometry(W + 0.1, hN + 0.08, 0.8), this.mats.hl);
      mesh.position.set(0, (hN + 0.08) / 2, NULL_Z);
    } else {
      const B = c.bins.length;
      const za = zOf(h.bin / B);
      const zb = zOf((h.bin + 1) / B);
      const hh = this.heightOf(c, c.bins[h.bin].count) + 0.08;
      mesh = new THREE.Mesh(
        new THREE.BoxGeometry(W + 0.1, hh, Math.max(0.04, Math.abs(za - zb))),
        this.mats.hl,
      );
      mesh.position.set(0, hh / 2, (za + zb) / 2);
    }
    g.group.add(mesh);
    this.hoverMesh = mesh;
  }

  resetView(instant = false) {
    const X = this.columns.length * S;
    let dist = Math.max(X, D) * 1.25;
    if (this.aspect < 1.1) dist *= (1.55 / Math.max(this.aspect, 0.5)) * 0.75;
    const tgt = new THREE.Vector3(0, 0.9, 0.6);
    this.flyTo(
      tgt.clone().add(new THREE.Vector3(X * 0.36, dist * 0.58, dist * 0.92)),
      tgt,
      instant,
    );
  }

  focus(name: string) {
    const tgt = new THREE.Vector3(this.xOf(name), 1.1, 0.4);
    this.flyTo(tgt.clone().add(new THREE.Vector3(3.2, 3.6, 7.6)), tgt, false);
  }

  // ---------- BaseScene ----------

  protected applyTheme() {
    this.buildMaterials();
    this.rebuild();
  }

  protected disposeContent() {
    this.clearWorld();
    this.disposeMaterials();
  }

  protected pickHit(ray: THREE.Raycaster): TerrainHover | null {
    const targets: THREE.Object3D[] = [];
    for (const g of this.groups)
      for (const ch of g.group.children) if (ch.userData.part) targets.push(ch);
    const hit = ray.intersectObjects(targets, false)[0];
    if (!hit) return null;
    const g = this.groups.find((x) => x.group === hit.object.parent);
    if (!g) return null;
    if (hit.object.userData.part === "null") return { column: g.column.name, bin: "null" };
    const local = g.group.worldToLocal(hit.point.clone());
    const B = g.column.bins.length;
    return {
      column: g.column.name,
      bin: Math.min(B - 1, Math.max(0, Math.floor(((D / 2 - local.z) / D) * B))),
    };
  }

  protected onFrame() {
    for (const g of this.groups) {
      const dx = g.targetX - g.group.position.x;
      g.group.position.x += this.reduced ? dx : dx * 0.12;
      if (g.column.name === this.selected && this.selRect)
        this.selRect.position.x = g.group.position.x;
    }
  }

  // ---------- building ----------

  private xOf(name: string): number {
    return (this.order.indexOf(name) - (this.order.length - 1) / 2) * S;
  }

  private disposeMaterials() {
    Object.values(this.mats).forEach((m) => m.dispose());
    if (this.ridge) disposeRidgeMaterials(this.ridge);
  }

  private buildMaterials() {
    this.disposeMaterials();
    const th = this.theme;
    this.ridge = ridgeMaterials(th);
    this.mats = {
      ghost: new THREE.MeshBasicMaterial({ color: th.grid, wireframe: true }),
      nul: new THREE.MeshStandardMaterial({ color: th.nul, roughness: 0.7 }),
      qTail: new THREE.MeshBasicMaterial({ color: th.qTail }),
      qMid: new THREE.MeshBasicMaterial({ color: th.qMid }),
      hl: new THREE.MeshBasicMaterial({
        color: th.hl,
        transparent: true,
        opacity: 0.45,
        depthWrite: false,
      }),
      sel: new THREE.MeshBasicMaterial({
        color: th.accent,
        transparent: true,
        opacity: th.dark ? 0.18 : 0.16,
        depthWrite: false,
      }),
      ground: new THREE.MeshStandardMaterial({ color: th.ground, roughness: 1 }),
      grid: new THREE.LineBasicMaterial({ color: th.grid }),
    };
  }

  private heightOf(c: TerrainColumn, count: number): number {
    return EPS + heightShare(c, count, this.options.mode, this.options.log, this.maxShare) * H;
  }

  private ridgePoints(c: TerrainColumn): RidgePoint[] {
    const B = c.bins.length;
    const pts: RidgePoint[] = [];
    if (!c.smooth) {
      for (let j = 0; j < B; j++) {
        const h = this.heightOf(c, c.bins[j].count);
        pts.push({ z: zOf(j / B), h }, { z: zOf((j + 1) / B), h });
      }
      return pts;
    }
    pts.push({ z: zOf(0), h: this.heightOf(c, c.bins[0].count) });
    for (let j = 0; j < B; j++)
      pts.push({ z: zOf((j + 0.5) / B), h: this.heightOf(c, c.bins[j].count) });
    pts.push({ z: zOf(1), h: this.heightOf(c, c.bins[B - 1].count) });
    return pts;
  }

  private buildColumn(c: TerrainColumn): { group: THREE.Group; quantiles: THREE.Group } {
    const group = new THREE.Group();
    const quantiles = new THREE.Group();
    if (c.bins.length === 0) {
      // Values withheld from this role: an outline where the ridge would stand.
      const ghost = new THREE.Mesh(new THREE.BoxGeometry(W, 0.6, D, 1, 1, 6), this.mats.ghost);
      ghost.position.set(0, 0.3, 0);
      group.add(ghost);
      this.addNullBlock(c, group);
      return { group, quantiles };
    }
    const pts = this.ridgePoints(c);
    for (const m of buildRidge(pts, W, c.smooth, this.ridge as RidgeMaterials, (h) =>
      this.rampColor((h - EPS) / H),
    ))
      group.add(m);
    this.addNullBlock(c, group);
    if (c.quantiles) {
      const marks: [number, THREE.Material][] = [
        [c.quantiles.p5, this.mats.qTail],
        [c.quantiles.p50, this.mats.qMid],
        [c.quantiles.p95, this.mats.qTail],
      ];
      for (const [v, mat] of marks) {
        const z = zOf(tOfValue(c, v));
        const h = heightAtZ(pts, z);
        const bar = new THREE.Mesh(new THREE.BoxGeometry(W + 0.12, 0.05, 0.07), mat);
        bar.position.set(0, h + 0.025, z);
        quantiles.add(bar);
        for (const sx of [-1, 1]) {
          const drop = new THREE.Mesh(new THREE.BoxGeometry(0.02, h, 0.07), mat);
          drop.position.set(sx * (W / 2 + 0.06), h / 2, z);
          quantiles.add(drop);
        }
      }
    }
    quantiles.visible = this.options.quantiles;
    group.add(quantiles);
    return { group, quantiles };
  }

  private addNullBlock(c: TerrainColumn, group: THREE.Group) {
    if (!c.nullShare) return;
    const hN = Math.max(0.03, c.nullShare * H);
    const m = new THREE.Mesh(new THREE.BoxGeometry(W, hN, 0.7), this.mats.nul);
    m.position.set(0, hN / 2, NULL_Z);
    m.castShadow = m.receiveShadow = true;
    m.userData.part = "null";
    group.add(m);
  }

  private clearWorld() {
    if (this.world) {
      this.disposeTree(this.world);
      this.scene.remove(this.world);
    }
    this.clearPins();
    this.groups = [];
    this.hoverMesh = null;
    this.hover = null;
  }

  private rebuild() {
    this.clearWorld();
    const world = new THREE.Group();
    this.world = world;
    this.scene.add(world);
    this.maxShare = maxShareOf(this.columns);
    const X = this.columns.length * S;
    const ground = new THREE.Mesh(new THREE.PlaneGeometry(X + 3, D + 5), this.mats.ground);
    ground.rotation.x = -Math.PI / 2;
    ground.position.set(0, 0, 0.9);
    ground.receiveShadow = true;
    world.add(ground);
    const gp: number[] = [];
    for (const t of [0, 0.25, 0.5, 0.75, 1])
      gp.push(-X / 2 - 0.6, 0.003, zOf(t), X / 2 + 0.6, 0.003, zOf(t));
    const grid = new THREE.BufferGeometry();
    grid.setAttribute("position", new THREE.Float32BufferAttribute(gp, 3));
    world.add(new THREE.LineSegments(grid, this.mats.grid));
    this.selRect = new THREE.Mesh(new THREE.PlaneGeometry(W + 0.34, D + 2.6), this.mats.sel);
    this.selRect.rotation.x = -Math.PI / 2;
    this.selRect.position.set(0, 0.004, 0.75);
    world.add(this.selRect);
    for (const c of this.columns) {
      const built = this.buildColumn(c);
      const x = this.xOf(c.name);
      built.group.position.x = x;
      world.add(built.group);
      const group = built.group;
      const label = this.pin(
        c.name,
        "p3d-label",
        () => new THREE.Vector3(group.position.x - 0.15, 0.01, LABEL_Z),
        32,
      );
      this.groups.push({ ...built, column: c, label, targetX: x });
    }
    const axisX = -X / 2 - 0.55;
    this.pin(
      this.axisText.low,
      "p3d-label p3d-axis",
      () => new THREE.Vector3(axisX, 0.01, zOf(0) - 0.2),
      0,
      true,
    );
    this.pin(
      this.axisText.high,
      "p3d-label p3d-axis",
      () => new THREE.Vector3(axisX, 0.01, zOf(1) + 0.2),
      0,
      true,
    );
    this.pin(
      this.axisText.nulls,
      "p3d-label p3d-axis",
      () => new THREE.Vector3(axisX, 0.01, NULL_Z),
      0,
      true,
    );
    this.fitShadow(Math.max(X, D) * 0.75 + 2);
    this.setSelected(this.selected);
  }
}
