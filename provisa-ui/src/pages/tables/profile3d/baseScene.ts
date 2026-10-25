// Copyright (c) 2026 Kenneth Stott
// Canary: 97c4d914-20bf-45ae-bb85-e42f387d5b06
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: what every 3D profile view shares — renderer, orbit camera, lights, HTML labels pinned
// to scene points, pointer picking and camera moves. Each view's scene extends it and is imported
// only through a dynamic import(), so three.js loads the first time a 3D view is shown.

import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";

export interface SceneTheme {
  canvas: string;
  ground: string;
  grid: string;
  ramp: [string, string, string, string, string];
  wallA: string;
  wallB: string;
  wallLine: string;
  nul: string;
  qTail: string;
  qMid: string;
  hl: string;
  accent: string;
  warm: string;
  cool: string;
  family: Record<"numeric" | "temporal" | "text" | "boolean" | "other" | "foreign", string>;
  dark: boolean;
}

export interface SceneCallbacks<H> {
  onHover: (h: H | null, x: number, y: number) => void;
  onSelect: (h: H) => void;
  onUserMove: () => void;
}

interface PinnedLabel {
  el: HTMLDivElement;
  at: () => THREE.Vector3;
  rotate: number;
  alignEnd: boolean;
  /** Where the last frame put it on the stage, in CSS pixels; null while it is behind the camera. */
  placed: { x: number; y: number } | null;
}

/** A pinned label as the last frame drew it: what an export paints in place of the DOM layer. */
export interface PlacedLabel {
  el: HTMLElement;
  x: number;
  y: number;
  rotate: number;
  alignEnd: boolean;
}

/** One frame of a full orbit, at ``fraction`` of the way round (0 is the view it started from). */
export interface Orbit {
  frame: (fraction: number, draw: (src: HTMLCanvasElement) => void) => void;
  end: () => void;
}

/** A scene projected flat, for an SVG: what the stage shows, in CSS pixels. Each shape is a drawn
 * mesh's silhouette (the outline of its projected vertices) in its material's colour, listed back
 * to front, so later shapes cover earlier ones as they do on the stage. */
export interface VectorScene {
  width: number;
  height: number;
  /** Line segments drawn under everything else (the floor grid). */
  floor: { x1: number; y1: number; x2: number; y2: number; color: string; opacity: number }[];
  shapes: { points: [number, number][]; color: string; opacity: number }[];
}

const UP_AXIS = new THREE.Vector3(0, 1, 0);

export abstract class BaseScene<H> {
  protected renderer: THREE.WebGLRenderer;
  protected scene = new THREE.Scene();
  protected camera = new THREE.PerspectiveCamera(38, 1, 0.1, 600);
  protected controls: OrbitControls;
  protected hemi = new THREE.HemisphereLight(0xffffff, 0x444444, 0.7);
  protected sun = new THREE.DirectionalLight(0xffffff, 0.85);
  protected theme: SceneTheme;
  protected ramp: THREE.Color[] = [];
  protected readonly reduced: boolean;
  protected canvas: HTMLCanvasElement;
  protected labelsEl: HTMLElement;
  protected callbacks: SceneCallbacks<H>;
  private labels: PinnedLabel[] = [];
  private tween: {
    p0: THREE.Vector3;
    p1: THREE.Vector3;
    t0: THREE.Vector3;
    t1: THREE.Vector3;
    start: number;
    dur: number;
  } | null = null;
  private frame = 0;
  private ray = new THREE.Raycaster();
  private down: { x: number; y: number } | null = null;
  private dragging = false;
  private resizeObserver: ResizeObserver;
  private disposers: (() => void)[] = [];
  /** The floor grid under the content, as last laid, and whether it is drawn (setGrid). */
  private floor: THREE.GridHelper | null = null;
  private floorShown = true;
  /** Set while an export drives the camera (orbit()): the loop then draws nothing of its own. */
  private held = false;

  constructor(
    canvas: HTMLCanvasElement,
    labelsEl: HTMLElement,
    callbacks: SceneCallbacks<H>,
    theme: SceneTheme,
  ) {
    this.canvas = canvas;
    this.labelsEl = labelsEl;
    this.callbacks = callbacks;
    this.theme = theme;
    this.reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    this.renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.renderer.shadowMap.enabled = true;
    this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    // The theme's colours are sRGB (CSS tokens), which three.js takes into linear space; the output
    // is converted back, or every colour lands far darker than its token and the stage reads black.
    this.renderer.outputColorSpace = THREE.SRGBColorSpace;
    this.controls = new OrbitControls(this.camera, canvas);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.08;
    this.controls.screenSpacePanning = true;
    this.controls.minDistance = 2;
    this.controls.maxDistance = 120;
    this.controls.autoRotateSpeed = 0.6;
    this.scene.add(this.hemi);
    this.sun.position.set(-9, 15, 11);
    this.sun.castShadow = true;
    this.sun.shadow.mapSize.set(2048, 2048);
    this.sun.shadow.bias = -0.0008;
    this.scene.add(this.sun);
    this.applyBaseTheme();
    this.bindPointer();
    this.resizeObserver = new ResizeObserver(() => this.resize());
    this.resizeObserver.observe(canvas.parentElement as HTMLElement);
    this.resize();
    this.frame = requestAnimationFrame(this.loop);
  }

  // ---------- for subclasses ----------

  /** The thing under the pointer, from a ray already cast through it. */
  protected abstract pickHit(ray: THREE.Raycaster): H | null;
  /** Rebuild whatever is drawn in the theme's colours. */
  protected abstract applyTheme(): void;
  /** Free everything the subclass built. */
  protected abstract disposeContent(): void;
  /** Called once per frame before rendering. */
  protected onFrame(): void {}
  /** Called once per frame after the labels are placed, before rendering (REQ-2128: a scene may
   * hide labels that would collide). */
  protected onLabelsPlaced(): void {}

  protected rampColor(t: number): THREE.Color {
    const x = Math.min(1, Math.max(0, t)) * 4;
    const i = Math.min(3, Math.floor(x));
    return this.ramp[i].clone().lerp(this.ramp[i + 1], x - i);
  }

  protected pin(
    text: string,
    cls: string,
    at: () => THREE.Vector3,
    rotate = 0,
    alignEnd = false,
  ): HTMLDivElement {
    const el = document.createElement("div");
    el.className = cls;
    el.textContent = text;
    this.labelsEl.appendChild(el);
    this.labels.push({ el, at, rotate, alignEnd, placed: null });
    return el;
  }

  protected clearPins() {
    this.labelsEl.textContent = "";
    this.labels = [];
  }

  protected fitShadow(extent: number) {
    const sh = this.sun.shadow.camera;
    sh.left = -extent;
    sh.right = extent;
    sh.top = extent;
    sh.bottom = -extent;
    sh.near = 1;
    sh.far = extent * 4 + 20;
    sh.updateProjectionMatrix();
  }

  protected flyTo(pos: THREE.Vector3, tgt: THREE.Vector3, instant: boolean) {
    if (instant || this.reduced) {
      this.camera.position.copy(pos);
      this.controls.target.copy(tgt);
      this.tween = null;
      return;
    }
    this.tween = {
      p0: this.camera.position.clone(),
      p1: pos,
      t0: this.controls.target.clone(),
      t1: tgt,
      start: performance.now(),
      dur: 750,
    };
  }

  protected get aspect(): number {
    return this.camera.aspect;
  }

  /** Lays a size×size floor grid at height ``y`` in ``world``, drawn or not as setGrid last said. */
  protected layFloor(world: THREE.Object3D, size: number, y: number, mat: THREE.LineBasicMaterial) {
    const grid = new THREE.GridHelper(size, size, this.theme.grid, this.theme.grid);
    (grid.material as THREE.Material).dispose();
    grid.material = mat;
    grid.position.y = y;
    grid.visible = this.floorShown;
    world.add(grid);
    this.floor = grid;
  }

  protected disposeTree(root: THREE.Object3D) {
    root.traverse((n) => {
      const mesh = n as THREE.Mesh;
      if (mesh.geometry) mesh.geometry.dispose();
    });
  }

  // ---------- public ----------

  setTheme(theme: SceneTheme) {
    this.theme = theme;
    this.applyBaseTheme();
    this.applyTheme();
  }

  /** Draws the floor grid, or leaves it out. */
  setGrid(shown: boolean) {
    this.floorShown = shown;
    if (this.floor) this.floor.visible = shown;
  }

  setRotate(on: boolean) {
    this.controls.autoRotate = on && !this.reduced;
  }

  /** The stage's background, in the theme: what an export fills behind the frame. */
  get background(): string {
    return this.theme.canvas;
  }

  /** The labels as the last frame placed them, the ones in view. */
  placedLabels(): PlacedLabel[] {
    return this.labels.flatMap((l) =>
      l.placed
        ? [{ el: l.el, x: l.placed.x, y: l.placed.y, rotate: l.rotate, alignEnd: l.alignEnd }]
        : [],
    );
  }

  /**
   * Renders one frame of the current view at ``scale`` times the stage's CSS size and hands the
   * canvas to ``draw`` in the same task, while its drawing buffer still holds the frame -- so the
   * renderer never needs preserveDrawingBuffer. The usual size is put back after.
   */
  snapshot(scale: number, draw: (src: HTMLCanvasElement) => void) {
    const usual = this.renderer.getPixelRatio();
    this.renderer.setPixelRatio(scale);
    this.resize();
    this.placeLabels();
    this.renderer.render(this.scene, this.camera);
    try {
      draw(this.renderer.domElement);
    } finally {
      this.renderer.setPixelRatio(usual);
      this.resize();
      this.renderer.render(this.scene, this.camera);
    }
  }

  /**
   * A full turn about the view's target, for a recording: the loop stops moving the camera, each
   * frame is drawn when asked for, and end() puts the camera back where it was. The orbit starts
   * from the current view and turns at an even step, so the same view records the same video.
   */
  orbit(scale: number): Orbit {
    const position = this.camera.position.clone();
    const target = this.controls.target.clone();
    const rotating = this.controls.autoRotate;
    const usual = this.renderer.getPixelRatio();
    const offset = position.clone().sub(target);
    this.held = true;
    this.tween = null;
    this.controls.autoRotate = false;
    this.renderer.setPixelRatio(scale);
    this.resize();
    return {
      frame: (fraction, draw) => {
        const turned = offset.clone().applyAxisAngle(UP_AXIS, fraction * Math.PI * 2);
        this.camera.position.copy(target).add(turned);
        this.camera.lookAt(target);
        this.onFrame();
        this.placeLabels();
        this.renderer.render(this.scene, this.camera);
        draw(this.renderer.domElement);
      },
      end: () => {
        this.camera.position.copy(position);
        this.controls.target.copy(target);
        this.camera.lookAt(target);
        this.controls.autoRotate = rotating;
        this.renderer.setPixelRatio(usual);
        this.resize();
        this.held = false;
      },
    };
  }

  /** The scene projected flat for an SVG, or null where it cannot be drawn faithfully that way
   * (a surface, a cloud): those scenes offer raster images only. */
  vector(): VectorScene | null {
    return null;
  }

  /** Whether vector() draws this scene: what decides if an SVG is offered. */
  get canVector(): boolean {
    return false;
  }

  /** A world point on the stage, in CSS pixels, and its depth (larger is further). */
  protected toStage(p: THREE.Vector3): { x: number; y: number; z: number } {
    const parent = this.canvas.parentElement as HTMLElement;
    const v = p.clone().project(this.camera);
    return {
      x: ((v.x + 1) / 2) * parent.clientWidth,
      y: ((1 - v.y) / 2) * parent.clientHeight,
      z: v.z,
    };
  }

  /** ``meshes`` as their silhouettes, back to front, over the floor grid: a scene's vector() for
   * content made of solid meshes (nodes, edges, arrowheads). Labels are the export's own layer. */
  protected vectorOf(meshes: THREE.Mesh[]): VectorScene {
    // The camera as it is now, though no frame has been rendered since it moved.
    this.camera.updateMatrixWorld();
    const shapes = meshes
      .flatMap((m) => {
        const s = this.silhouette(m);
        return s ? [s] : [];
      })
      .sort((a, b) => b.depth - a.depth)
      .map(({ points, color, opacity }) => ({ points, color, opacity }));
    return { ...this.stageSize, floor: this.floorLines(), shapes };
  }

  /** A mesh as the camera sees it: the outline of its projected vertices, in its material's
   * colour, with its depth for ordering; null while it is behind the camera. */
  protected silhouette(
    mesh: THREE.Mesh,
  ): { points: [number, number][]; color: string; opacity: number; depth: number } | null {
    mesh.updateMatrixWorld();
    const pos = mesh.geometry.getAttribute("position");
    const pts: [number, number][] = [];
    let depth = 0;
    for (let i = 0; i < pos.count; i++) {
      const p = this.toStage(
        new THREE.Vector3().fromBufferAttribute(pos, i).applyMatrix4(mesh.matrixWorld),
      );
      if (p.z > 1) return null;
      pts.push([p.x, p.y]);
      depth += p.z;
    }
    const mat = mesh.material as THREE.MeshStandardMaterial;
    return {
      points: hull(pts),
      color: `#${mat.color.getHexString()}`,
      opacity: mat.transparent ? mat.opacity : 1,
      depth: depth / pos.count,
    };
  }

  /** The stage's size in CSS pixels. */
  protected get stageSize(): { width: number; height: number } {
    const parent = this.canvas.parentElement as HTMLElement;
    return { width: parent.clientWidth, height: parent.clientHeight };
  }

  /** The floor grid's lines on the stage, while it is drawn. */
  protected floorLines(): VectorScene["floor"] {
    const grid = this.floor;
    if (!grid || !grid.visible) return [];
    grid.updateMatrixWorld();
    const pos = grid.geometry.getAttribute("position");
    const mat = grid.material as THREE.LineBasicMaterial;
    const out: VectorScene["floor"] = [];
    for (let i = 0; i + 1 < pos.count; i += 2) {
      const a = this.toStage(
        new THREE.Vector3().fromBufferAttribute(pos, i).applyMatrix4(grid.matrixWorld),
      );
      const b = this.toStage(
        new THREE.Vector3().fromBufferAttribute(pos, i + 1).applyMatrix4(grid.matrixWorld),
      );
      if (a.z > 1 || b.z > 1) continue;
      out.push({
        x1: a.x,
        y1: a.y,
        x2: b.x,
        y2: b.y,
        color: `#${mat.color.getHexString()}`,
        opacity: mat.opacity,
      });
    }
    return out;
  }

  dispose() {
    cancelAnimationFrame(this.frame);
    this.resizeObserver.disconnect();
    this.disposers.forEach((d) => d());
    this.disposeContent();
    this.clearPins();
    this.controls.dispose();
    this.renderer.dispose();
  }

  // ---------- internals ----------

  private applyBaseTheme() {
    const th = this.theme;
    this.ramp = th.ramp.map((c) => new THREE.Color(c));
    this.renderer.setClearColor(th.canvas);
    this.hemi.color.set(th.dark ? 0xcfe0ff : 0xffffff);
    this.hemi.groundColor.set(th.ground);
    this.hemi.intensity = th.dark ? 0.85 : 0.8;
    this.sun.intensity = th.dark ? 1.1 : 0.85;
  }

  private pick(cx: number, cy: number): H | null {
    const r = this.canvas.getBoundingClientRect();
    const ndc = new THREE.Vector2(
      ((cx - r.left) / r.width) * 2 - 1,
      -((cy - r.top) / r.height) * 2 + 1,
    );
    this.ray.setFromCamera(ndc, this.camera);
    return this.pickHit(this.ray);
  }

  private bindPointer() {
    const c = this.canvas;
    const local = (e: PointerEvent) => {
      const r = c.getBoundingClientRect();
      return [e.clientX - r.left, e.clientY - r.top] as const;
    };
    const onDown = (e: PointerEvent) => {
      this.down = { x: e.clientX, y: e.clientY };
      this.dragging = true;
      this.tween = null;
      this.callbacks.onUserMove();
    };
    const onUp = (e: PointerEvent) => {
      this.dragging = false;
      if (
        this.down &&
        Math.hypot(e.clientX - this.down.x, e.clientY - this.down.y) < 6 &&
        e.target === c
      ) {
        const h = this.pick(e.clientX, e.clientY);
        this.callbacks.onHover(h, ...local(e));
        if (h) this.callbacks.onSelect(h);
      }
      this.down = null;
    };
    const onMove = (e: PointerEvent) => {
      if (this.dragging || e.pointerType === "touch") return;
      this.callbacks.onHover(this.pick(e.clientX, e.clientY), ...local(e));
    };
    const onLeave = () => {
      if (!this.dragging) this.callbacks.onHover(null, 0, 0);
    };
    const onWheel = () => this.callbacks.onUserMove();
    c.addEventListener("pointerdown", onDown);
    window.addEventListener("pointerup", onUp);
    c.addEventListener("pointermove", onMove);
    c.addEventListener("pointerleave", onLeave);
    c.addEventListener("wheel", onWheel, { passive: true });
    this.disposers.push(() => {
      c.removeEventListener("pointerdown", onDown);
      window.removeEventListener("pointerup", onUp);
      c.removeEventListener("pointermove", onMove);
      c.removeEventListener("pointerleave", onLeave);
      c.removeEventListener("wheel", onWheel);
    });
  }

  private resize() {
    const parent = this.canvas.parentElement as HTMLElement;
    const w = parent.clientWidth;
    const h = parent.clientHeight;
    if (!w || !h) return;
    this.renderer.setSize(w, h, false);
    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();
  }

  private loop = (now: number) => {
    this.frame = requestAnimationFrame(this.loop);
    if (this.held) return;
    if (this.tween) {
      const tw = this.tween;
      const k = Math.min(1, (now - tw.start) / tw.dur);
      const e = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
      this.camera.position.lerpVectors(tw.p0, tw.p1, e);
      this.controls.target.lerpVectors(tw.t0, tw.t1, e);
      if (k >= 1) this.tween = null;
    }
    this.controls.update();
    this.onFrame();
    this.placeLabels();
    this.renderer.render(this.scene, this.camera);
  };

  /** Puts every pinned label where the camera now sees its point. */
  private placeLabels() {
    const parent = this.canvas.parentElement as HTMLElement;
    const w = parent.clientWidth;
    const h = parent.clientHeight;
    for (const l of this.labels) {
      const p = l.at().project(this.camera);
      if (p.z > 1) {
        l.el.style.visibility = "hidden";
        l.placed = null;
        continue;
      }
      l.el.style.visibility = "visible";
      const x = ((p.x + 1) / 2) * w;
      const y = ((1 - p.y) / 2) * h;
      l.placed = { x, y };
      l.el.style.transform =
        `translate(${x.toFixed(1)}px,${y.toFixed(1)}px)` +
        (l.rotate ? ` rotate(${l.rotate}deg)` : "") +
        (l.alignEnd ? " translate(-100%,-50%)" : "");
    }
    this.onLabelsPlaced();
  }
}

/** The convex outline of a set of points (Andrew's monotone chain), counter-clockwise. */
function hull(points: [number, number][]): [number, number][] {
  const p = [...points].sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  if (p.length < 3) return p;
  const cross = (o: [number, number], a: [number, number], b: [number, number]) =>
    (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  const lower: [number, number][] = [];
  for (const q of p) {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], q) <= 0)
      lower.pop();
    lower.push(q);
  }
  const upper: [number, number][] = [];
  for (const q of [...p].reverse()) {
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], q) <= 0)
      upper.pop();
    upper.push(q);
  }
  return lower.slice(0, -1).concat(upper.slice(0, -1));
}
