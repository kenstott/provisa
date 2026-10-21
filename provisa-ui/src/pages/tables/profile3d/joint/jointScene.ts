// Copyright (c) 2026 Kenneth Stott
// Canary: a61cf469-e9ae-4ee6-a2ba-4a521d889a41
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the joint cloud's scene — a cube of slots, one sphere per occupied cell, sized by its
// rows and coloured by its rows or by its lift over independence.

import * as THREE from "three";
import { BaseScene, type SceneCallbacks, type SceneTheme } from "../baseScene";
import type { JointModel } from "./jointModel";

export type JointColor = "rows" | "lift";
/** A hovered cell, by its index in the model's cells. */
export interface JointHover {
  cell: number;
}

const SIZE = 10;

export class JointScene extends BaseScene<JointHover> {
  private world: THREE.Group | null = null;
  private points: THREE.InstancedMesh | null = null;
  private hl: THREE.Mesh | null = null;
  private mats: Record<string, THREE.Material> = {};
  private model: JointModel | null = null;
  private color: JointColor;
  private hover: JointHover | null = null;
  private axisNames: (axis: number, slot: number) => string;

  constructor(
    canvas: HTMLCanvasElement,
    labelsEl: HTMLElement,
    callbacks: SceneCallbacks<JointHover>,
    theme: SceneTheme,
    color: JointColor,
    axisNames: (axis: number, slot: number) => string,
  ) {
    super(canvas, labelsEl, callbacks, theme);
    this.color = color;
    this.axisNames = axisNames;
    this.buildMaterials();
  }

  // ---------- public ----------

  setData(
    model: JointModel,
    axisNames: (axis: number, slot: number) => string,
    resetCamera: boolean,
  ) {
    this.model = model;
    this.axisNames = axisNames;
    this.rebuild();
    if (resetCamera) this.resetView(true);
  }

  setColor(color: JointColor) {
    this.color = color;
    this.paint();
  }

  setHover(h: JointHover | null) {
    if (h?.cell === this.hover?.cell) return;
    this.hover = h;
    if (this.hl) this.hl.visible = false;
    if (!h || !this.model || !this.hl) return;
    const c = this.model.cells[h.cell];
    this.hl.position.copy(this.cellPosition(c.x, c.y, c.z));
    this.hl.scale.setScalar(this.cellRadius(c.count) * 1.5);
    this.hl.visible = true;
  }

  resetView(instant = false) {
    const dist = this.aspect < 1.1 ? 30 / Math.max(this.aspect, 0.5) : 25;
    this.flyTo(
      new THREE.Vector3(dist * 0.62, dist * 0.42, dist * 0.68),
      new THREE.Vector3(0, 0, 0),
      instant,
    );
  }

  // ---------- BaseScene ----------

  protected applyTheme() {
    this.buildMaterials();
    this.rebuild();
  }

  protected disposeContent() {
    this.clearWorld();
    Object.values(this.mats).forEach((m) => m.dispose());
  }

  protected pickHit(ray: THREE.Raycaster): JointHover | null {
    if (!this.points) return null;
    const hit = ray.intersectObject(this.points, false)[0];
    return hit?.instanceId != null ? { cell: hit.instanceId } : null;
  }

  // ---------- building ----------

  private buildMaterials() {
    Object.values(this.mats).forEach((m) => m.dispose());
    const th = this.theme;
    this.mats = {
      point: new THREE.MeshStandardMaterial({ roughness: 0.45, metalness: 0.05 }),
      hl: new THREE.MeshBasicMaterial({
        color: th.hl,
        transparent: true,
        opacity: 0.4,
        depthWrite: false,
      }),
      frame: new THREE.LineBasicMaterial({ color: th.grid }),
      floor: new THREE.MeshStandardMaterial({
        color: th.ground,
        roughness: 1,
        transparent: true,
        opacity: 0.6,
      }),
    };
  }

  private slots(axis: number): number {
    return (this.model as JointModel).axes[axis].slots.length;
  }

  private coord(axis: number, slot: number): number {
    const n = this.slots(axis);
    return n === 1 ? 0 : -SIZE / 2 + ((slot + 0.5) / n) * SIZE;
  }

  private cellPosition(x: number, y: number, z: number): THREE.Vector3 {
    return new THREE.Vector3(this.coord(0, x), this.coord(1, y), this.coord(2, z));
  }

  private cellRadius(count: number): number {
    const m = this.model as JointModel;
    const slot = SIZE / Math.max(this.slots(0), this.slots(1), this.slots(2));
    return Math.max(0.06, Math.cbrt(count / m.maxCount) * slot * 0.48);
  }

  private cellColor(i: number): THREE.Color {
    const c = (this.model as JointModel).cells[i];
    if (this.color === "rows")
      return this.rampColor(Math.cbrt(c.count / (this.model as JointModel).maxCount));
    // Lift on a log scale: blue below independence, the grid colour at it, orange above.
    const t = Math.max(-1, Math.min(1, Math.log2(c.lift) / 2));
    const mid = new THREE.Color(this.theme.grid);
    return t < 0
      ? mid.lerp(new THREE.Color(this.theme.cool), -t)
      : mid.lerp(new THREE.Color(this.theme.warm), t);
  }

  private paint() {
    if (!this.points || !this.model) return;
    for (let i = 0; i < this.model.cells.length; i++) this.points.setColorAt(i, this.cellColor(i));
    if (this.points.instanceColor) this.points.instanceColor.needsUpdate = true;
  }

  private clearWorld() {
    if (this.world) {
      this.disposeTree(this.world);
      this.scene.remove(this.world);
    }
    this.points?.dispose();
    this.points = null;
    this.hl = null;
    this.hover = null;
    this.clearPins();
  }

  private rebuild() {
    this.clearWorld();
    const m = this.model;
    if (!m) return;
    const world = new THREE.Group();
    this.world = world;
    this.scene.add(world);
    const depth = this.slots(2) === 1 ? 1.2 : SIZE;
    const box = new THREE.EdgesGeometry(new THREE.BoxGeometry(SIZE, SIZE, depth));
    world.add(new THREE.LineSegments(box, this.mats.frame));
    const floor = new THREE.Mesh(new THREE.PlaneGeometry(SIZE, depth), this.mats.floor);
    floor.rotation.x = -Math.PI / 2;
    floor.position.y = -SIZE / 2;
    floor.receiveShadow = true;
    world.add(floor);
    const sphere = new THREE.SphereGeometry(1, 18, 12);
    const points = new THREE.InstancedMesh(sphere, this.mats.point, m.cells.length);
    const mat = new THREE.Matrix4();
    m.cells.forEach((c, i) => {
      const p = this.cellPosition(c.x, c.y, c.z);
      const r = this.cellRadius(c.count);
      mat.makeScale(r, r, r).setPosition(p);
      points.setMatrixAt(i, mat);
    });
    points.castShadow = true;
    world.add(points);
    this.points = points;
    this.paint();
    this.hl = new THREE.Mesh(new THREE.SphereGeometry(1, 18, 12), this.mats.hl);
    this.hl.visible = false;
    world.add(this.hl);
    this.pinAxes(depth);
    this.fitShadow(SIZE);
  }

  private pinAxes(depth: number) {
    const m = this.model as JointModel;
    const h = SIZE / 2;
    const d = depth / 2;
    m.axes[0].slots.forEach((_, i) => {
      const x = this.coord(0, i);
      this.pin(
        this.axisNames(0, i),
        "p3d-label p3d-tick",
        () => new THREE.Vector3(x, -h - 0.3, d + 0.4),
        32,
      );
    });
    m.axes[1].slots.forEach((_, i) => {
      const y = this.coord(1, i);
      this.pin(
        this.axisNames(1, i),
        "p3d-label p3d-tick",
        () => new THREE.Vector3(-h - 0.3, y, d),
        0,
        true,
      );
    });
    if (this.slots(2) > 1) {
      m.axes[2].slots.forEach((_, i) => {
        const z = this.coord(2, i);
        this.pin(
          this.axisNames(2, i),
          "p3d-label p3d-tick",
          () => new THREE.Vector3(h + 0.4, -h - 0.3, z),
        );
      });
      this.pin(
        m.axes[2].column,
        "p3d-label p3d-axis-title",
        () => new THREE.Vector3(h + 1.6, -h - 1.2, 0),
      );
    }
    this.pin(
      m.axes[0].column,
      "p3d-label p3d-axis-title",
      () => new THREE.Vector3(0, -h - 2.2, d + 1.2),
    );
    this.pin(
      m.axes[1].column,
      "p3d-label p3d-axis-title",
      () => new THREE.Vector3(-h - 0.4, h + 0.8, d),
      0,
      true,
    );
  }
}
