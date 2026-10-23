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
// kind, an arrowhead on each directed edge (DIRECTED). Imported only through a dynamic import(), so
// three.js loads when the graph is first shown.

import * as THREE from "three";
import {
  BaseScene,
  type SceneCallbacks,
  type SceneTheme,
  type VectorScene,
} from "../../pages/tables/profile3d/baseScene";
import type { EdgeKind, GraphData, GraphEdge, GraphNode, NodeType } from "./graphModel";
import { labelClass, nodeLook, type Colour, type Shape } from "./nodeLook";

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

/** The materials edges are drawn with, each with a faded copy for the edges away from a pick. */
const EDGE_MATERIALS = [
  "structure",
  "link",
  "quality",
  "tagged",
  "meaning",
  "relationship",
  "outside",
  "lineage",
] as const;

/** The edges that run one way, drawn with an arrowhead at the end they point to: lineage
 * downstream, a term edge from its subject (Customer KIND_OF Party), a check and a profile to the
 * table they read. Membership and containment have no direction worth drawing. */
const DIRECTED: ReadonlySet<EdgeKind> = new Set([
  "lineage",
  "column_lineage",
  "term_edge",
  "checks",
  "profiled",
]);

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

function geometry(shape: Shape, r: number): THREE.BufferGeometry {
  switch (shape) {
    case "icosahedron":
      return new THREE.IcosahedronGeometry(r, 0);
    case "sphere":
      // A domain was sized as a thin ring; solid, it sits at about the size of the other nodes.
      return new THREE.SphereGeometry(r * 0.55, 20, 14);
    case "box":
      return new THREE.BoxGeometry(r * 1.6, r * 1.1, r * 1.1);
    case "cube":
      return new THREE.BoxGeometry(r * 1.3, r * 1.3, r * 1.3);
    case "cone":
      return new THREE.ConeGeometry(r, r * 1.8, 16);
    case "tetrahedron":
      return new THREE.TetrahedronGeometry(r * 1.3, 0);
    case "cylinder":
      return new THREE.CylinderGeometry(r, r, r * 1.4, 16);
    case "dodecahedron":
      return new THREE.DodecahedronGeometry(r, 0);
    case "diamond":
      return new THREE.OctahedronGeometry(r * 1.2, 0);
    case "torus":
      return new THREE.TorusGeometry(r * 0.75, r * 0.3, 10, 24);
  }
}

export class GraphScene extends BaseScene<GraphHover> {
  private world: THREE.Group | null = null;
  private nodes: NodeMesh[] = [];
  private edgeMeshes: EdgeMesh[] = [];
  private mats: Record<string, THREE.Material> = {};
  /** Each edge material dimmed, for the edges away from what is picked: the same hue, less light. */
  private faded: Record<string, THREE.Material> = {};
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

  /** Nodes, edges and arrowheads, flat (an SVG export). */
  get canVector(): boolean {
    return true;
  }

  vector(): VectorScene {
    return this.vectorOf([
      ...this.edgeMeshes.flatMap((e) => e.meshes),
      ...this.nodes.map((n) => n.mesh),
    ]);
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
    Object.values(this.faded).forEach((m) => m.dispose());
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
    Object.values(this.faded).forEach((m) => m.dispose());
    const th = this.theme;
    // Lit by their own colour too: an edge is a thin rod, and the sun alone leaves most of it dark.
    const rod = (color: string, opacity = 0.95) =>
      new THREE.MeshStandardMaterial({
        color,
        emissive: color,
        emissiveIntensity: th.dark ? 0.55 : 0.25,
        roughness: 0.6,
        transparent: true,
        opacity,
      });
    this.mats = {
      structure: rod(th.family.other, 0.8),
      link: rod(th.accent),
      quality: rod(th.qTail),
      tagged: rod(th.family.boolean, 0.8),
      meaning: rod(th.cool),
      relationship: rod(th.family.text),
      outside: rod(th.family.foreign, 0.8),
      lineage: rod(th.hl),
      halo: new THREE.MeshBasicMaterial({
        color: th.accent,
        transparent: true,
        opacity: 0.16,
        depthWrite: false,
      }),
      grid: new THREE.LineBasicMaterial({ color: th.grid, transparent: true, opacity: 0.9 }),
    };
    this.faded = {};
    for (const key of EDGE_MATERIALS) {
      const lit = this.mats[key] as THREE.MeshStandardMaterial;
      const dim = lit.clone();
      dim.emissiveIntensity = lit.emissiveIntensity * 0.45;
      dim.opacity = lit.opacity * 0.6;
      this.faded[key] = dim;
    }
  }

  /** A node's material: solid and opaque, with a little light of its own so a shape's faces
   * turned from the sun still read on the canvas. */
  private solid(color: string): THREE.Material {
    return new THREE.MeshStandardMaterial({
      color,
      emissive: color,
      emissiveIntensity: this.theme.dark ? 0.5 : 0.3,
      roughness: 0.5,
      metalness: 0.05,
    });
  }

  /** A node's solid, opaque material in its look's colour (nodeLook): shared by every node of it. */
  private nodeMaterial(node: GraphNode): THREE.Material {
    const { colour } = nodeLook(node);
    const key = `look:${colour}`;
    let m = this.mats[key];
    if (!m) {
      m = this.solid(this.colourOf(colour));
      this.mats[key] = m;
    }
    return m;
  }

  private colourOf(c: Colour): string {
    const th = this.theme;
    switch (c) {
      case "accent":
        return th.accent;
      case "fam-temporal":
        return th.family.temporal;
      case "fam-numeric":
        return th.family.numeric;
      case "fam-foreign":
        return th.family.foreign;
      case "fam-other":
        return th.family.other;
      case "warm":
        return th.warm;
      case "q-tail":
        return th.qTail;
      case "q-mid":
        return th.qMid;
      case "fam-boolean":
        return th.family.boolean;
      case "cool":
        return th.cool;
      case "hl":
        return th.hl;
    }
  }

  /** ``kind``'s material; ``faded`` for an edge away from what is picked (same hue, dimmer). */
  private edgeMaterial(kind: EdgeKind, faded = false): THREE.Material {
    const key = this.edgeMaterialKey(kind);
    return (faded ? this.faded : this.mats)[key];
  }

  private edgeMaterialKey(kind: EdgeKind): string {
    switch (kind) {
      case "in_domain":
      case "member":
      case "has_column":
      case "command":
      case "owns":
        return "structure";
      case "product_link":
        return "link";
      case "checks":
      case "profiled":
        return "quality";
      case "tagged":
        return "tagged";
      case "term_edge":
      case "term_binding":
        return "meaning";
      case "relationship":
        return "relationship";
      case "relationship_outside":
        return "outside";
      case "lineage":
      case "column_lineage":
        return "lineage";
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
      const mesh = new THREE.Mesh(geometry(nodeLook(node).shape, r), this.nodeMaterial(node));
      mesh.position.copy(p);
      mesh.castShadow = true;
      mesh.userData.node = node.id;
      world.add(mesh);
      // REQ-2130: a proposal says why beside its name; a deprecated term's name is dimmed too.
      const label = this.pin(
        node.label,
        [
          "p3d-label p3d-node-label",
          `g3d-label-${node.type}`,
          labelClass(node.status),
          node.identityGap ? "g3d-label-gap" : "",
        ]
          .filter(Boolean)
          .join(" "),
        () => mesh.position.clone().add(new THREE.Vector3(0, r + 0.3, 0)),
      );
      this.nodes.push({ node, mesh, radius: r, label });
    }
    this.extent = extent;
    this.extentX = extentX;
    this.extentY = extentY;
    const nodeOf = new Map(this.nodes.map((n) => [n.node.id, n]));
    const HEAD = 0.4; // an arrowhead's length
    for (const e of this.graph.edges) {
      const a = pos.get(e.a);
      const b = pos.get(e.b);
      if (!a || !b) continue;
      const dir = b.clone().sub(a);
      const len = dir.length();
      const unit = dir.clone().normalize();
      const thick = 0.045 + 0.02 * Math.min(4, (e.paths ?? 1) - 1);
      const mat = this.edgeMaterial(e.kind);
      // A directed edge ends at its arrowhead's base, the head's tip on the target's surface (its
      // radius at its drawn scale), so no line shows past the tip; any other edge runs centre to
      // centre, inside both nodes.
      const target = nodeOf.get(e.b);
      if (!target) throw new Error(`edge ${e.id} ends at ${e.b}, which is not drawn`);
      const surface = target.radius * target.mesh.scale.x;
      const directed = DIRECTED.has(e.kind);
      const rodLen = directed ? Math.max(0.01, len - surface - HEAD) : len;
      const rodMesh = new THREE.Mesh(
        new THREE.CylinderGeometry(thick, thick, rodLen, 6, 1, true),
        mat,
      );
      rodMesh.position.copy(a.clone().add(unit.clone().multiplyScalar(rodLen / 2)));
      rodMesh.quaternion.setFromUnitVectors(UP, unit);
      rodMesh.userData.edge = e.id;
      world.add(rodMesh);
      const meshes = [rodMesh];
      if (directed) {
        const cone = new THREE.Mesh(new THREE.ConeGeometry(thick * 3 + 0.06, HEAD, 10), mat);
        cone.position.copy(b.clone().sub(unit.clone().multiplyScalar(surface + HEAD / 2)));
        cone.quaternion.setFromUnitVectors(UP, unit);
        cone.userData.edge = e.id;
        world.add(cone);
        meshes.push(cone);
      }
      this.edgeMeshes.push({ edge: e, meshes });
    }
    const size = Math.ceil(extent * 2.4);
    this.layFloor(world, size, minY - 1.5, this.mats.grid as THREE.LineBasicMaterial);
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
      // One hue whether picked or not: the picked node's edges thicker and full bright, the rest
      // thinner and dimmer but plainly there; with nothing picked, every edge as drawn.
      const picked = edgeFocus != null || focus != null;
      const width = !picked ? 1 : touches ? 1.4 : 0.6;
      for (const m of em.meshes) {
        m.material = this.edgeMaterial(em.edge.kind, !touches);
        m.scale.set(width, 1, width);
      }
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
