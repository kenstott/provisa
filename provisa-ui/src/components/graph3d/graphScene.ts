// Copyright (c) 2026 Kenneth Stott
// Canary: 5e2d8b41-7c93-4a06-b1f8-2d9e6c4a7b53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the data product graph's scene — a shape per node type, a rod per edge coloured by its
// kind, an arrowhead on lineage pointing downstream. Imported only through a dynamic import(), so
// three.js loads when the graph is first shown.

import * as THREE from "three";
import {
  BaseScene,
  type SceneCallbacks,
  type SceneTheme,
} from "../../pages/tables/profile3d/baseScene";
import type { EdgeKind, GraphData, GraphEdge, GraphNode, NodeType } from "./graphModel";

export type GraphHover = { kind: "node"; id: string } | { kind: "edge"; id: string };

interface NodeMesh {
  node: GraphNode;
  mesh: THREE.Mesh;
  radius: number;
  label: HTMLDivElement;
}

interface EdgeMesh {
  edge: GraphEdge;
  meshes: THREE.Mesh[];
}

const UP = new THREE.Vector3(0, 1, 0);

/** Whose label stays when two collide: the lower rank. */
const LABEL_RANK: Record<NodeType, number> = {
  product: 0,
  domain: 1,
  table: 2,
  term: 3,
  command: 4,
  checker: 5,
  relation: 6,
  tag: 7,
  profile: 8,
  column: 9,
};

const SIZE: Record<NodeType, number> = {
  product: 0.75,
  domain: 0.95,
  table: 0.45,
  column: 0.2,
  command: 0.38,
  checker: 0.36,
  profile: 0.32,
  tag: 0.28,
  term: 0.36,
  relation: 0.36,
};

function geometry(type: NodeType, r: number): THREE.BufferGeometry {
  switch (type) {
    case "product":
      return new THREE.IcosahedronGeometry(r, 0);
    case "domain":
      return new THREE.TorusGeometry(r, r * 0.18, 10, 32);
    case "table":
    case "relation":
      return new THREE.BoxGeometry(r * 1.6, r * 1.1, r * 1.1);
    case "command":
      return new THREE.ConeGeometry(r, r * 1.8, 16);
    case "checker":
      return new THREE.TetrahedronGeometry(r * 1.3, 0);
    case "profile":
      return new THREE.CylinderGeometry(r, r, r * 1.4, 16);
    case "tag":
      return new THREE.DodecahedronGeometry(r, 0);
    case "term":
      return new THREE.OctahedronGeometry(r * 1.2, 0);
    case "column":
      return new THREE.SphereGeometry(r, 16, 12);
  }
}

export class GraphScene extends BaseScene<GraphHover> {
  private world: THREE.Group | null = null;
  private nodes: NodeMesh[] = [];
  private edgeMeshes: EdgeMesh[] = [];
  private mats: Record<string, THREE.Material> = {};
  private graph: GraphData = { nodes: [], edges: [] };
  private at = new Map<string, [number, number, number]>();
  private selected: string | null = null;
  private hover: GraphHover | null = null;
  private halo: THREE.Mesh | null = null;
  private extent = 8;
  private extentX = 8;
  private extentY = 8;
  private frontal = false;

  constructor(
    canvas: HTMLCanvasElement,
    labelsEl: HTMLElement,
    callbacks: SceneCallbacks<GraphHover>,
    theme: SceneTheme,
  ) {
    super(canvas, labelsEl, callbacks, theme);
    this.buildMaterials();
  }

  // ---------- public ----------

  setData(graph: GraphData, at: Map<string, [number, number, number]>, resetCamera: boolean) {
    this.graph = graph;
    this.at = at;
    this.rebuild();
    if (resetCamera) this.resetView(true);
  }

  setSelected(id: string | null) {
    this.selected = id;
    this.paint();
  }

  setHover(h: GraphHover | null) {
    if (h?.kind === this.hover?.kind && h?.id === this.hover?.id) return;
    this.hover = h;
    this.paint();
  }

  /** A flat layout (a tree, lineage lanes) is seen from the front; a force layout from above. */
  setFrontal(on: boolean) {
    this.frontal = on;
  }

  resetView(instant = false) {
    if (this.frontal) {
      // Back far enough that the whole layout, and a margin round it, fits the field of view.
      const half = Math.tan(THREE.MathUtils.degToRad(this.camera.fov / 2));
      const fit = Math.max(this.extentY + 2, (this.extentX + 2) / Math.max(this.aspect, 0.5));
      this.flyTo(new THREE.Vector3(0, 0, fit / half), new THREE.Vector3(0, 0, 0), instant);
      return;
    }
    const base = this.extent * 2.6 + 6;
    const dist = this.aspect < 1.1 ? (base * 1.25) / Math.max(this.aspect, 0.5) : base;
    this.flyTo(
      new THREE.Vector3(dist * 0.35, dist * 0.4, dist * 0.85),
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

  /** No two labels drawn over each other: a label colliding with one ranked ahead of it is hidden
   * (its node's full name is in the hover card). The selected and hovered nodes rank first, then
   * the node types the graph is about, nearest the camera first. */
  protected onLabelsPlaced() {
    const lead = (n: NodeMesh) =>
      n.node.id === this.selected || (this.hover?.kind === "node" && this.hover.id === n.node.id)
        ? -1
        : LABEL_RANK[n.node.type];
    const ranked = this.nodes
      .filter((n) => n.label.style.visibility !== "hidden")
      .map((n) => ({ n, rank: lead(n), d: n.mesh.position.distanceTo(this.camera.position) }))
      .sort((a, b) => a.rank - b.rank || a.d - b.d);
    const kept: DOMRect[] = [];
    for (const { n } of ranked) {
      const r = n.label.getBoundingClientRect();
      const hit = kept.some(
        (k) =>
          r.left < k.right + 2 &&
          r.right > k.left - 2 &&
          r.top < k.bottom + 1 &&
          r.bottom > k.top - 1,
      );
      if (hit) n.label.style.visibility = "hidden";
      else kept.push(r);
    }
  }

  protected applyTheme() {
    this.buildMaterials();
    this.rebuild();
  }

  protected disposeContent() {
    this.clearWorld();
    Object.values(this.mats).forEach((m) => m.dispose());
  }

  protected pickHit(ray: THREE.Raycaster): GraphHover | null {
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
    // A little light of their own, so a shape's faces turned from the sun still read on the canvas.
    const solid = (color: string) =>
      new THREE.MeshStandardMaterial({
        color,
        emissive: color,
        emissiveIntensity: 0.3,
        roughness: 0.5,
        metalness: 0.05,
      });
    const rod = (color: string, opacity = 0.85) =>
      new THREE.MeshStandardMaterial({ color, roughness: 0.6, transparent: true, opacity });
    this.mats = {
      product: solid(th.accent),
      domain: solid(th.family.temporal),
      table: solid(th.family.numeric),
      relation: solid(th.family.foreign),
      column: solid(th.family.other),
      command: solid(th.warm),
      checker: solid(th.qTail),
      profile: solid(th.qMid),
      tag: solid(th.family.boolean),
      term: solid(th.cool),
      structure: rod(th.family.other, 0.55),
      link: rod(th.accent),
      quality: rod(th.qTail),
      tagged: rod(th.family.boolean, 0.6),
      meaning: rod(th.cool),
      relationship: rod(th.family.text),
      outside: rod(th.family.foreign, 0.6),
      lineage: rod(th.hl),
      dim: new THREE.MeshStandardMaterial({
        color: th.grid,
        roughness: 0.8,
        transparent: true,
        opacity: 0.2,
      }),
      halo: new THREE.MeshBasicMaterial({
        color: th.accent,
        transparent: true,
        opacity: 0.16,
        depthWrite: false,
      }),
      grid: new THREE.LineBasicMaterial({ color: th.grid, transparent: true, opacity: 0.6 }),
    };
  }

  /** REQ-2130: an abstract term drawn hollow (its outline alone), a deprecated one faded. */
  private nodeMaterial(node: GraphNode): THREE.Material {
    if (!node.hollow && node.status !== "deprecated") return this.mats[node.type];
    const key = `${node.type}:${node.hollow ? "hollow" : "solid"}:${node.status === "deprecated" ? "dim" : "lit"}`;
    let m = this.mats[key];
    if (!m) {
      const base = this.mats[node.type] as THREE.MeshStandardMaterial;
      const made = base.clone();
      made.wireframe = node.hollow === true;
      if (node.status === "deprecated") {
        made.transparent = true;
        made.opacity = 0.3;
      }
      this.mats[key] = made;
      m = made;
    }
    return m;
  }

  private edgeMaterial(kind: EdgeKind): THREE.Material {
    switch (kind) {
      case "in_domain":
      case "member":
      case "has_column":
      case "command":
      case "owns":
        return this.mats.structure;
      case "product_link":
        return this.mats.link;
      case "checks":
      case "profiled":
        return this.mats.quality;
      case "tagged":
        return this.mats.tagged;
      case "term_edge":
      case "term_binding":
        return this.mats.meaning;
      case "relationship":
        return this.mats.relationship;
      case "relationship_outside":
        return this.mats.outside;
      case "lineage":
      case "column_lineage":
        return this.mats.lineage;
    }
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
    const pos = new Map<string, THREE.Vector3>();
    let minY = 0;
    let extent = 1;
    let extentX = 1;
    let extentY = 1;
    for (const node of this.graph.nodes) {
      const xyz = this.at.get(node.id);
      if (!xyz) continue;
      const p = new THREE.Vector3(...xyz);
      pos.set(node.id, p);
      minY = Math.min(minY, p.y);
      extent = Math.max(extent, Math.abs(p.x), Math.abs(p.y), Math.abs(p.z));
      extentX = Math.max(extentX, Math.abs(p.x));
      extentY = Math.max(extentY, Math.abs(p.y));
      const r = SIZE[node.type];
      const mesh = new THREE.Mesh(geometry(node.type, r), this.nodeMaterial(node));
      mesh.position.copy(p);
      if (node.type === "domain") mesh.rotation.x = Math.PI / 2;
      mesh.castShadow = true;
      mesh.userData.node = node.id;
      world.add(mesh);
      // REQ-2130: a proposal says why beside its name; a deprecated term's name is dimmed too.
      const label = this.pin(
        node.note ? `${node.label} · ${node.note}` : node.label,
        `p3d-label p3d-node-label g3d-label-${node.type}` +
          (node.status === "proposed" ? " g3d-label-proposed" : "") +
          (node.status === "deprecated" ? " g3d-label-dim" : ""),
        () => mesh.position.clone().add(new THREE.Vector3(0, r + 0.3, 0)),
      );
      this.nodes.push({ node, mesh, radius: r, label });
    }
    this.extent = extent;
    this.extentX = extentX;
    this.extentY = extentY;
    const radiusOf = new Map(this.nodes.map((n) => [n.node.id, n.radius]));
    for (const e of this.graph.edges) {
      const a = pos.get(e.a);
      const b = pos.get(e.b);
      if (!a || !b) continue;
      const dir = b.clone().sub(a);
      const len = dir.length();
      const thick = 0.025 + 0.02 * Math.min(4, (e.paths ?? 1) - 1);
      const mat = this.edgeMaterial(e.kind);
      const rodMesh = new THREE.Mesh(
        new THREE.CylinderGeometry(thick, thick, len, 6, 1, true),
        mat,
      );
      rodMesh.position.copy(a.clone().add(b).multiplyScalar(0.5));
      rodMesh.quaternion.setFromUnitVectors(UP, dir.clone().normalize());
      rodMesh.userData.edge = e.id;
      world.add(rodMesh);
      const meshes = [rodMesh];
      if (e.kind === "lineage" || e.kind === "column_lineage") {
        const rb = radiusOf.get(e.b) ?? 0.3;
        const cone = new THREE.Mesh(new THREE.ConeGeometry(thick * 3 + 0.06, 0.4, 10), mat);
        cone.position.copy(
          b.clone().sub(
            dir
              .clone()
              .normalize()
              .multiplyScalar(rb + 0.3),
          ),
        );
        cone.quaternion.setFromUnitVectors(UP, dir.clone().normalize());
        cone.userData.edge = e.id;
        world.add(cone);
        meshes.push(cone);
      }
      this.edgeMeshes.push({ edge: e, meshes });
    }
    const size = Math.ceil(extent * 2.4);
    const grid = new THREE.GridHelper(size, size, this.theme.grid, this.theme.grid);
    (grid.material as THREE.Material).dispose();
    grid.material = this.mats.grid as THREE.LineBasicMaterial;
    grid.position.y = minY - 1.5;
    world.add(grid);
    this.fitShadow(extent + 4);
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
      for (const m of em.meshes)
        m.material = touches ? this.edgeMaterial(em.edge.kind) : this.mats.dim;
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
      this.halo = new THREE.Mesh(
        new THREE.SphereGeometry(sel.radius * 1.35, 24, 16),
        this.mats.halo,
      );
      this.halo.position.copy(sel.mesh.position);
      this.world.add(this.halo);
    }
  }
}
