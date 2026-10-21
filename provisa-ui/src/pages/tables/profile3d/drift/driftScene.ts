// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the drift strata's scene — one ridge per run, oldest at the back, the same bins left to
// right, with the median traced from run to run and the runs that drifted marked.

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
import type { StrataModel } from "./driftModel";

/** A hovered bin of a run (by its index, oldest first); ``"null"`` is the run's null block and
 * ``"run"`` the whole ridge. */
export interface StrataHover {
  run: number;
  bin: number | "null" | "run";
}

const H = 4;
const D = 12;
const W = 0.8;
const GAP = 1.25;
const EPS = 0.03;

const zOf = (t: number) => -D / 2 + t * D;

interface RunGroup {
  group: THREE.Group;
  pts: RidgePoint[];
  label: HTMLDivElement;
}

export class DriftScene extends BaseScene<StrataHover> {
  private world: THREE.Group | null = null;
  private runs: RunGroup[] = [];
  private hoverMesh: THREE.Mesh | null = null;
  private ridge: RidgeMaterials | null = null;
  private mats: Record<string, THREE.Material> = {};
  private model: StrataModel | null = null;
  private selected: number | null = null;
  private hover: StrataHover | null = null;
  private runLabel: (i: number) => string;
  private axisText: { low: string; high: string; nulls: string };

  constructor(
    canvas: HTMLCanvasElement,
    labelsEl: HTMLElement,
    callbacks: SceneCallbacks<StrataHover>,
    theme: SceneTheme,
    runLabel: (i: number) => string,
    axisText: { low: string; high: string; nulls: string },
  ) {
    super(canvas, labelsEl, callbacks, theme);
    this.runLabel = runLabel;
    this.axisText = axisText;
    this.controls.maxPolarAngle = Math.PI / 2 - 0.04;
    this.buildMaterials();
  }

  // ---------- public ----------

  setData(model: StrataModel, resetCamera: boolean) {
    this.model = model;
    this.rebuild();
    if (resetCamera) this.resetView(true);
  }

  setSelected(run: number | null) {
    this.selected = run;
    this.runs.forEach((r, i) => r.label.classList.toggle("is-selected", i === run));
  }

  setHover(h: StrataHover | null) {
    if (h?.run === this.hover?.run && h?.bin === this.hover?.bin) return;
    this.hover = h;
    if (this.hoverMesh) {
      this.hoverMesh.removeFromParent();
      this.hoverMesh.geometry.dispose();
      this.hoverMesh = null;
    }
    this.runs.forEach((r, i) => r.label.classList.toggle("is-hovered", i === h?.run));
    const m = this.model;
    if (!h || !m) return;
    const g = this.runs[h.run];
    const run = m.runs[h.run];
    let mesh: THREE.Mesh;
    if (h.bin === "run") {
      const top = Math.max(EPS, ...g.pts.map((p) => p.h)) + 0.1;
      mesh = new THREE.Mesh(new THREE.BoxGeometry(W + 0.14, top, D + 0.1), this.mats.hl);
      mesh.position.set(0, top / 2, 0);
    } else if (h.bin === "null") {
      const hN = Math.max(0.03, (run.nullShare as number) * H);
      mesh = new THREE.Mesh(new THREE.BoxGeometry(W + 0.1, hN + 0.08, 0.8), this.mats.hl);
      mesh.position.set(0, (hN + 0.08) / 2, zOf(0) - 1.0);
    } else {
      const B = m.labels.length;
      const za = zOf(h.bin / B);
      const zb = zOf((h.bin + 1) / B);
      const hh = this.heightOf(run.shares[h.bin]) + 0.08;
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
    const n = (this.model as StrataModel).runs.length;
    const depth = n * GAP;
    let dist = Math.max(D, depth) * 1.35;
    if (this.aspect < 1.1) dist *= (1.55 / Math.max(this.aspect, 0.5)) * 0.75;
    const tgt = new THREE.Vector3(0, 0.8, 0);
    this.flyTo(
      tgt.clone().add(new THREE.Vector3(-dist * 0.3, dist * 0.55, dist * 0.9)),
      tgt,
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
    if (this.ridge) disposeRidgeMaterials(this.ridge);
  }

  protected pickHit(ray: THREE.Raycaster): StrataHover | null {
    const targets: THREE.Object3D[] = [];
    for (const r of this.runs)
      for (const ch of r.group.children) if (ch.userData.part) targets.push(ch);
    const hit = ray.intersectObjects(targets, false)[0];
    if (!hit || !this.model) return null;
    const run = this.runs.findIndex((r) => r.group === hit.object.parent);
    if (run < 0) return null;
    if (hit.object.userData.part === "null") return { run, bin: "null" };
    const local = this.runs[run].group.worldToLocal(hit.point.clone());
    const B = this.model.labels.length;
    return { run, bin: Math.min(B - 1, Math.max(0, Math.floor(((local.z + D / 2) / D) * B))) };
  }

  // ---------- building ----------

  private buildMaterials() {
    Object.values(this.mats).forEach((m) => m.dispose());
    if (this.ridge) disposeRidgeMaterials(this.ridge);
    const th = this.theme;
    this.ridge = ridgeMaterials(th);
    this.mats = {
      nul: new THREE.MeshStandardMaterial({ color: th.nul, roughness: 0.7 }),
      median: new THREE.MeshStandardMaterial({ color: th.qMid, roughness: 0.4 }),
      hl: new THREE.MeshBasicMaterial({
        color: th.hl,
        transparent: true,
        opacity: 0.45,
        depthWrite: false,
      }),
      drift: new THREE.MeshBasicMaterial({
        color: th.warm,
        transparent: true,
        opacity: 0.22,
        depthWrite: false,
      }),
      ground: new THREE.MeshStandardMaterial({ color: th.ground, roughness: 1 }),
      grid: new THREE.LineBasicMaterial({ color: th.grid }),
    };
  }

  private heightOf(share: number): number {
    return EPS + (share / (this.model as StrataModel).maxShare) * H;
  }

  private worldZ(i: number): number {
    const n = (this.model as StrataModel).runs.length;
    return (i - (n - 1) / 2) * GAP;
  }

  private clearWorld() {
    if (this.world) {
      this.disposeTree(this.world);
      this.scene.remove(this.world);
    }
    this.clearPins();
    this.runs = [];
    this.hoverMesh = null;
    this.hover = null;
  }

  private rebuild() {
    this.clearWorld();
    const m = this.model;
    if (!m) return;
    const world = new THREE.Group();
    this.world = world;
    this.scene.add(world);
    const n = m.runs.length;
    const depth = n * GAP;
    const ground = new THREE.Mesh(new THREE.PlaneGeometry(D + 5, depth + 2), this.mats.ground);
    ground.rotation.x = -Math.PI / 2;
    ground.position.set(-0.6, 0, 0);
    ground.receiveShadow = true;
    world.add(ground);
    const gp: number[] = [];
    for (const t of [0, 0.25, 0.5, 0.75, 1])
      gp.push(zOf(t), 0.003, -depth / 2 - 0.4, zOf(t), 0.003, depth / 2 + 0.4);
    const grid = new THREE.BufferGeometry();
    grid.setAttribute("position", new THREE.Float32BufferAttribute(gp, 3));
    world.add(new THREE.LineSegments(grid, this.mats.grid));
    const B = m.labels.length;
    const trail: THREE.Vector3[] = [];
    m.runs.forEach((run, i) => {
      const group = new THREE.Group();
      // A ridge runs along local z; turned a quarter about y it runs left to right.
      group.rotation.y = Math.PI / 2;
      group.position.z = this.worldZ(i);
      world.add(group);
      const pts: RidgePoint[] = [];
      if (run.shares.length === B && B > 0) {
        if (m.smooth) {
          pts.push({ z: zOf(0), h: this.heightOf(run.shares[0]) });
          for (let j = 0; j < B; j++)
            pts.push({ z: zOf((j + 0.5) / B), h: this.heightOf(run.shares[j]) });
          pts.push({ z: zOf(1), h: this.heightOf(run.shares[B - 1]) });
        } else {
          for (let j = 0; j < B; j++) {
            const h = this.heightOf(run.shares[j]);
            pts.push({ z: zOf(j / B), h }, { z: zOf((j + 1) / B), h });
          }
        }
        for (const mesh of buildRidge(pts, W, m.smooth, this.ridge as RidgeMaterials, (h) =>
          this.rampColor((h - EPS) / H),
        ))
          group.add(mesh);
        if (run.medianT != null) {
          const z = zOf(run.medianT);
          trail.push(new THREE.Vector3(z, heightAtZ(pts, z) + 0.06, this.worldZ(i)));
        }
      }
      if (run.driftReason != null) {
        const band = new THREE.Mesh(new THREE.PlaneGeometry(W + 0.5, D + 0.4), this.mats.drift);
        band.rotation.x = -Math.PI / 2;
        band.position.y = 0.006;
        group.add(band);
      }
      if (run.nullShare) {
        const hN = Math.max(0.03, run.nullShare * H);
        const block = new THREE.Mesh(new THREE.BoxGeometry(W, hN, 0.7), this.mats.nul);
        block.position.set(0, hN / 2, zOf(0) - 1.0);
        block.castShadow = block.receiveShadow = true;
        block.userData.part = "null";
        group.add(block);
      }
      const z = this.worldZ(i);
      const label = this.pin(
        this.runLabel(i),
        "p3d-label",
        () => new THREE.Vector3(zOf(1) + 0.6, 0.01, z),
      );
      label.classList.toggle("is-warn", run.driftReason != null);
      this.runs.push({ group, pts, label });
    });
    if (trail.length >= 2) {
      const tube = new THREE.TubeGeometry(
        new THREE.CatmullRomCurve3(trail),
        trail.length * 12,
        0.05,
        8,
        false,
      );
      world.add(new THREE.Mesh(tube, this.mats.median));
    }
    const front = depth / 2 + 0.8;
    this.pin(this.axisText.low, "p3d-label p3d-axis", () => new THREE.Vector3(zOf(0), 0.01, front));
    this.pin(
      this.axisText.high,
      "p3d-label p3d-axis",
      () => new THREE.Vector3(zOf(1), 0.01, front),
      0,
      true,
    );
    this.pin(
      this.axisText.nulls,
      "p3d-label p3d-axis",
      () => new THREE.Vector3(zOf(0) - 1.0, 0.01, front),
    );
    this.fitShadow(Math.max(D, depth) * 0.75 + 2);
    this.setSelected(this.selected);
  }
}
