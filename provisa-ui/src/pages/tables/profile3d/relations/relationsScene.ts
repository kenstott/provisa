// Copyright (c) 2026 Kenneth Stott
// Canary: 7521a838-5259-446b-a7a8-b904728ab6a0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the relationships map's scene — a sphere per column, a rod per relationship, thicker
// the stronger it is, an arrowhead on a dependence pointing at the column that depends.

import * as THREE from "three";
import { BaseScene, type SceneCallbacks, type SceneTheme } from "../baseScene";
import type { RelEdge, RelationsModel, RelNode } from "./relationsModel";

export type RelHover = { kind: "node"; id: string } | { kind: "edge"; id: string };

interface NodeMesh {
  node: RelNode;
  mesh: THREE.Mesh;
  label: HTMLDivElement;
}

interface EdgeMesh {
  edge: RelEdge;
  meshes: THREE.Mesh[];
}

const UP = new THREE.Vector3(0, 1, 0);

export class RelationsScene extends BaseScene<RelHover> {
  private world: THREE.Group | null = null;
  private nodes: NodeMesh[] = [];
  private edgeMeshes: EdgeMesh[] = [];
  private mats: Record<string, THREE.Material> = {};
  private model: RelationsModel = { nodes: [], edges: [] };
  private edges: RelEdge[] = [];
  private selected: string | null = null;
  private hover: RelHover | null = null;
  private halo: THREE.Mesh | null = null;

  constructor(
    canvas: HTMLCanvasElement,
    labelsEl: HTMLElement,
    callbacks: SceneCallbacks<RelHover>,
    theme: SceneTheme,
  ) {
    super(canvas, labelsEl, callbacks, theme);
    this.buildMaterials();
  }

  // ---------- public ----------

  setData(model: RelationsModel, edges: RelEdge[], resetCamera: boolean) {
    this.model = model;
    this.edges = edges;
    this.rebuild();
    if (resetCamera) this.resetView(true);
  }

  setEdges(edges: RelEdge[]) {
    this.edges = edges;
    this.rebuild();
  }

  setSelected(id: string | null) {
    this.selected = id;
    this.paint();
  }

  setHover(h: RelHover | null) {
    if (h?.kind === this.hover?.kind && h?.id === this.hover?.id) return;
    this.hover = h;
    this.paint();
  }

  resetView(instant = false) {
    const dist = this.aspect < 1.1 ? 30 / Math.max(this.aspect, 0.5) : 24;
    this.flyTo(
      new THREE.Vector3(dist * 0.45, dist * 0.35, dist * 0.82),
      new THREE.Vector3(0, 0, 0),
      instant,
    );
  }

  focus(id: string) {
    const n = this.nodes.find((x) => x.node.id === id);
    if (!n) return;
    const tgt = n.mesh.position.clone();
    this.flyTo(tgt.clone().add(new THREE.Vector3(3.5, 2.5, 6.5)), tgt, false);
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

  protected pickHit(ray: THREE.Raycaster): RelHover | null {
    const targets: THREE.Object3D[] = [];
    for (const n of this.nodes) targets.push(n.mesh);
    for (const e of this.edgeMeshes) targets.push(...e.meshes);
    const hit = ray.intersectObjects(targets, false)[0];
    if (!hit) return null;
    const ud = hit.object.userData as { node?: string; edge?: string };
    if (ud.node != null) return { kind: "node", id: ud.node };
    if (ud.edge != null) return { kind: "edge", id: ud.edge };
    return null;
  }

  // ---------- building ----------

  private buildMaterials() {
    Object.values(this.mats).forEach((m) => m.dispose());
    const th = this.theme;
    const fam = th.family;
    const solid = (color: string) =>
      new THREE.MeshStandardMaterial({ color, roughness: 0.55, metalness: 0.05 });
    const rod = (color: string) =>
      new THREE.MeshStandardMaterial({ color, roughness: 0.6, transparent: true, opacity: 0.9 });
    this.mats = {
      numeric: solid(fam.numeric),
      temporal: solid(fam.temporal),
      text: solid(fam.text),
      boolean: solid(fam.boolean),
      other: solid(fam.other),
      foreign: solid(fam.foreign),
      positive: rod(th.cool),
      negative: rod(th.warm),
      ratio: rod(th.qTail),
      dependence: rod(th.hl),
      dim: new THREE.MeshStandardMaterial({
        color: th.grid,
        roughness: 0.8,
        transparent: true,
        opacity: 0.25,
      }),
      halo: new THREE.MeshBasicMaterial({
        color: th.accent,
        transparent: true,
        opacity: 0.28,
        depthWrite: false,
      }),
      grid: new THREE.LineBasicMaterial({ color: th.grid, transparent: true, opacity: 0.6 }),
    };
  }

  private edgeMaterial(e: RelEdge): THREE.Material {
    if (e.kind === "dependence") return this.mats.dependence;
    if (e.kind === "correlation_ratio") return this.mats.ratio;
    return e.value >= 0 ? this.mats.positive : this.mats.negative;
  }

  private nodeRadius(degree: number): number {
    return 0.22 + 0.07 * Math.sqrt(degree);
  }

  private clearWorld() {
    if (this.world) {
      this.disposeTree(this.world);
      this.scene.remove(this.world);
    }
    this.clearPins();
    this.nodes = [];
    this.edgeMeshes = [];
    this.halo = null;
  }

  private rebuild() {
    this.clearWorld();
    const world = new THREE.Group();
    this.world = world;
    this.scene.add(world);
    const degree = new Map<string, number>();
    for (const e of this.edges) {
      degree.set(e.a, (degree.get(e.a) ?? 0) + 1);
      degree.set(e.b, (degree.get(e.b) ?? 0) + 1);
    }
    const pos = new Map<string, THREE.Vector3>();
    let minY = 0;
    for (const node of this.model.nodes) {
      const p = new THREE.Vector3(node.x, node.y, node.z);
      pos.set(node.id, p);
      minY = Math.min(minY, p.y);
      const r = this.nodeRadius(degree.get(node.id) ?? 0);
      const mesh = new THREE.Mesh(new THREE.SphereGeometry(r, 24, 16), this.mats[node.family]);
      mesh.position.copy(p);
      mesh.castShadow = true;
      mesh.userData.node = node.id;
      world.add(mesh);
      const label = this.pin(node.id, "p3d-label p3d-node-label", () =>
        mesh.position.clone().add(new THREE.Vector3(0, r + 0.25, 0)),
      );
      this.nodes.push({ node, mesh, label });
    }
    for (const e of this.edges) {
      const a = pos.get(e.a) as THREE.Vector3;
      const b = pos.get(e.b) as THREE.Vector3;
      const dir = b.clone().sub(a);
      const len = dir.length();
      const radius = 0.02 + 0.07 * e.strength;
      const mat = this.edgeMaterial(e);
      const rodMesh = new THREE.Mesh(
        new THREE.CylinderGeometry(radius, radius, len, 8, 1, true),
        mat,
      );
      rodMesh.position.copy(a.clone().add(b).multiplyScalar(0.5));
      rodMesh.quaternion.setFromUnitVectors(UP, dir.clone().normalize());
      rodMesh.userData.edge = e.id;
      world.add(rodMesh);
      const meshes = [rodMesh];
      if (e.kind === "dependence") {
        const rb = this.nodeRadius(degree.get(e.b) ?? 0);
        const cone = new THREE.Mesh(new THREE.ConeGeometry(radius * 3 + 0.05, 0.45, 12), mat);
        cone.position.copy(
          b.clone().sub(
            dir
              .clone()
              .normalize()
              .multiplyScalar(rb + 0.25),
          ),
        );
        cone.quaternion.setFromUnitVectors(UP, dir.clone().normalize());
        cone.userData.edge = e.id;
        world.add(cone);
        meshes.push(cone);
      }
      this.edgeMeshes.push({ edge: e, meshes });
    }
    const grid = new THREE.GridHelper(18, 18, this.theme.grid, this.theme.grid);
    (grid.material as THREE.Material).dispose();
    grid.material = this.mats.grid as THREE.LineBasicMaterial;
    grid.position.y = minY - 1.5;
    world.add(grid);
    this.fitShadow(10);
    this.paint();
  }

  /** Light what is hovered or selected and fade the edges that do not touch it. */
  private paint() {
    const focus =
      this.hover?.kind === "node"
        ? this.hover.id
        : this.hover?.kind === "edge"
          ? null
          : this.selected;
    const edgeFocus = this.hover?.kind === "edge" ? this.hover.id : null;
    for (const em of this.edgeMeshes) {
      const touches =
        edgeFocus != null
          ? em.edge.id === edgeFocus
          : focus == null || em.edge.a === focus || em.edge.b === focus;
      for (const m of em.meshes) m.material = touches ? this.edgeMaterial(em.edge) : this.mats.dim;
    }
    for (const n of this.nodes) {
      n.label.classList.toggle("is-selected", n.node.id === this.selected);
      n.label.classList.toggle(
        "is-hovered",
        this.hover?.kind === "node" && n.node.id === this.hover.id,
      );
    }
    if (this.halo) {
      this.halo.removeFromParent();
      this.halo.geometry.dispose();
      this.halo = null;
    }
    const sel = this.nodes.find((n) => n.node.id === this.selected);
    if (sel && this.world) {
      const r = (sel.mesh.geometry as THREE.SphereGeometry).parameters.radius;
      this.halo = new THREE.Mesh(new THREE.SphereGeometry(r * 1.7, 24, 16), this.mats.halo);
      this.halo.position.copy(sel.mesh.position);
      this.world.add(this.halo);
    }
  }
}
